(ns bitool.ops.source-health
  (:require [bitool.db :as db]
            [bitool.operations :as operations]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

;; ---------------------------------------------------------------------------
;; Source health monitoring
;;
;; Queries the operations_endpoint_freshness_status table to provide
;; per-source-type health views (Kafka, file/mainframe, API), circuit breaker
;; state derivation, and data-loss risk analysis.
;; ---------------------------------------------------------------------------

(def ^:private freshness-table "operations_endpoint_freshness_status")
(def ^:private run-table "execution_run")
(def ^:private artifact-table "ingest_batch_artifact_store")

(defn- db-opts
  [conn]
  (jdbc/with-options conn {:builder-fn rs/as-unqualified-lower-maps}))

(defn- as-long
  [v]
  (long (or v 0)))

(defn- overdue?
  [freshness-lag-seconds freshness-sla-seconds]
  (and (some? freshness-sla-seconds)
       (> (as-long freshness-lag-seconds) (as-long freshness-sla-seconds))))

(defn- non-blank [s]
  (when (and (some? s) (not= "" (string/trim (str s)))) s))

;; ---------------------------------------------------------------------------
;; Source queries by type
;; ---------------------------------------------------------------------------

(defn kafka-sources
  "Return all Kafka-type sources from the endpoint freshness table for a given
   workspace.  Enriches each row with derived freshness_lag_seconds and an
   overdue? flag."
  [{:keys [workspace-key]}]
  (try
    (operations/ensure-operations-tables!)
    (let [workspace-key (non-blank workspace-key)
          ws-clause (if workspace-key " AND workspace_key = ?" "")
          ws-params (if workspace-key [workspace-key] [])
          rows (jdbc/execute!
                (db-opts db/ds)
                (into [(str "SELECT *,
                              GREATEST(0, CAST(EXTRACT(EPOCH FROM (now() - COALESCE(last_success_at_utc, updated_at_utc))) AS BIGINT)) AS freshness_lag_seconds
                       FROM " freshness-table "
                       WHERE LOWER(source_system) LIKE '%kafka%'"
                       ws-clause
                       " ORDER BY freshness_lag_seconds DESC, updated_at_utc DESC")]
                 ws-params))]
      (mapv (fn [row]
              (assoc row
                     :freshness_lag_seconds (as-long (:freshness_lag_seconds row))
                     :overdue (overdue? (:freshness_lag_seconds row)
                                        (:freshness_sla_seconds row))))
            rows))
    (catch Exception e
      (log/warn e "kafka-sources query failed" {:workspace_key workspace-key})
      [])))

(defn file-sources
  "Return all file/mainframe-type sources from the endpoint freshness table."
  [{:keys [workspace-key]}]
  (try
    (operations/ensure-operations-tables!)
    (let [workspace-key (non-blank workspace-key)
          ws-clause (if workspace-key " AND workspace_key = ?" "")
          ws-params (if workspace-key [workspace-key] [])
          rows (jdbc/execute!
                (db-opts db/ds)
                (into [(str "SELECT *,
                              GREATEST(0, CAST(EXTRACT(EPOCH FROM (now() - COALESCE(last_success_at_utc, updated_at_utc))) AS BIGINT)) AS freshness_lag_seconds
                       FROM " freshness-table "
                       WHERE (LOWER(source_system) LIKE '%file%'
                              OR LOWER(source_system) LIKE '%mainframe%')"
                       ws-clause
                       " ORDER BY freshness_lag_seconds DESC, updated_at_utc DESC")]
                 ws-params))]
      (mapv (fn [row]
              (assoc row
                     :freshness_lag_seconds (as-long (:freshness_lag_seconds row))
                     :overdue (overdue? (:freshness_lag_seconds row)
                                        (:freshness_sla_seconds row))))
            rows))
    (catch Exception e
      (log/warn e "file-sources query failed" {:workspace_key workspace-key})
      [])))

(defn api-sources
  "Return sources that are NOT Kafka and NOT file/mainframe -- i.e. API and
   other connector types."
  [{:keys [workspace-key]}]
  (try
    (operations/ensure-operations-tables!)
    (let [workspace-key (non-blank workspace-key)
          ws-clause (if workspace-key " AND f.workspace_key = ?" "")
          ws-params (if workspace-key [workspace-key] [])
          rows (jdbc/execute!
                (db-opts db/ds)
                (into [(str "SELECT f.*,
                              GREATEST(0, CAST(EXTRACT(EPOCH FROM (now() - COALESCE(f.last_success_at_utc, f.updated_at_utc))) AS BIGINT)) AS freshness_lag_seconds,
                              CASE WHEN er.started_at_utc IS NOT NULL AND er.finished_at_utc IS NOT NULL
                                   THEN CAST(EXTRACT(EPOCH FROM (er.finished_at_utc - er.started_at_utc)) AS BIGINT)
                                   ELSE NULL END AS last_run_duration_seconds
                       FROM " freshness-table " f
                       LEFT JOIN " run-table " er ON er.run_id = f.run_id
                       WHERE LOWER(f.source_system) NOT LIKE '%kafka%'
                         AND LOWER(f.source_system) NOT LIKE '%file%'
                         AND LOWER(f.source_system) NOT LIKE '%mainframe%'"
                       ws-clause
                       " ORDER BY freshness_lag_seconds DESC, f.updated_at_utc DESC")]
                 ws-params))]
      (mapv (fn [row]
              (assoc row
                     :freshness_lag_seconds (as-long (:freshness_lag_seconds row))
                     :overdue (overdue? (:freshness_lag_seconds row)
                                        (:freshness_sla_seconds row))))
            rows))
    (catch Exception e
      (log/warn e "api-sources query failed" {:workspace_key workspace-key})
      [])))

;; ---------------------------------------------------------------------------
;; Circuit breaker state
;; ---------------------------------------------------------------------------

(defn circuit-breaker-state
  "Derive circuit breaker state for a source by examining recent execution_run
   failures.  Returns a map:

     {:source_id      <source-id>
      :state          \"closed\" | \"half_open\" | \"open\"
      :recent_failures <int>
      :recent_successes <int>
      :last_failure_at <timestamp or nil>
      :last_success_at <timestamp or nil>}

   Heuristic:
     - >= 5 consecutive failures with no success in between -> \"open\"
     - >= 3 failures with at least 1 success -> \"half_open\"
     - else -> \"closed\""
  [source-id]
  (try
    (let [recent (jdbc/execute!
                  (db-opts db/ds)
                  [(str "SELECT status, finished_at_utc
                         FROM " run-table "
                         WHERE source_system = ?
                         ORDER BY COALESCE(finished_at_utc, started_at_utc) DESC
                         LIMIT 10")
                   (str source-id)])
          failures  (count (filter #(= "failed" (:status %)) recent))
          successes (count (filter #(= "succeeded" (:status %)) recent))
          last-failure  (some #(when (= "failed" (:status %)) (:finished_at_utc %)) recent)
          last-success  (some #(when (= "succeeded" (:status %)) (:finished_at_utc %)) recent)
          ;; Count leading consecutive failures (before any success)
          consecutive-failures (count (take-while #(= "failed" (:status %)) recent))
          state (cond
                  (>= consecutive-failures 5) "open"
                  (>= failures 3)             "half_open"
                  :else                       "closed")]
      {:source_id        (str source-id)
       :state            state
       :recent_failures  failures
       :recent_successes successes
       :consecutive_failures consecutive-failures
       :last_failure_at  last-failure
       :last_success_at  last-success})
    (catch Exception e
      (log/warn e "circuit-breaker-state derivation failed" {:source_id source-id})
      {:source_id        (str source-id)
       :state            "unknown"
       :recent_failures  0
       :recent_successes 0
       :consecutive_failures 0
       :last_failure_at  nil
       :last_success_at  nil})))

(defn reset-circuit-breaker!
  "Request a circuit breaker reset for a source.

   Resets all runtime circuit-breaker entries whose source key starts with the
   provided source-id."
  [source-id operator]
  (try
    (require 'bitool.ingest.runtime)
    (let [breaker-var (ns-resolve 'bitool.ingest.runtime 'source-circuit-breaker-state)
          breaker-atom (when breaker-var @breaker-var)
          prefix (str source-id "::")
          removed (atom 0)]
      (when-not (instance? clojure.lang.IAtom breaker-atom)
        (throw (ex-info "Runtime circuit breaker state is unavailable"
                        {:source_id source-id :status 503})))
      (swap! breaker-atom
             (fn [state]
               (reduce-kv
                (fn [acc k v]
                  (if (or (= (str k) (str source-id))
                          (string/starts-with? (str k) prefix))
                    (do (swap! removed inc) acc)
                    (assoc acc k v)))
                {}
                state)))
      (require 'bitool.control-plane)
      ((resolve 'bitool.control-plane/record-audit-event!)
       {:event_type "ops.circuit_breaker.reset"
        :actor      (or operator "system")
        :details    {:source_id (str source-id)
                     :removed_keys @removed}})
      {:status    "reset"
       :source_id (str source-id)
       :removed_keys @removed
       :message   (str "Cleared " @removed " circuit-breaker entr" (if (= 1 @removed) "y" "ies"))})
    (catch Exception e
      (log/warn e "reset-circuit-breaker! failed" {:source_id source-id})
      {:status    "error"
       :source_id (str source-id)
       :message   (.getMessage e)})))

;; ---------------------------------------------------------------------------
;; Data-loss risk analysis
;; ---------------------------------------------------------------------------

(defn data-loss-risk
  "Analyze potential data-loss risks for a workspace.

   Checks for:
     1. Stale sources -- no successful run within 2x the SLA window
     2. High-lag sources -- freshness lag > SLA
     3. Checkpoint-manifest gaps -- batches in artifact store without a
        matching checkpoint advance (heuristic: runs with batches that ended
        in failure)

   Returns:
     {:stale_sources      [...]
      :high_lag_sources   [...]
      :checkpoint_gaps    [...]
      :risk_level         \"low\" | \"medium\" | \"high\" | \"critical\"}"
  [{:keys [workspace-key]}]
  (try
    (operations/ensure-operations-tables!)
    (let [workspace-key (non-blank workspace-key)
          ws-clause (if workspace-key " AND workspace_key = ?" "")
          ws-params (if workspace-key [workspace-key] [])

          ;; 1. Stale sources: last_success_at_utc older than 2x SLA
          stale-sources
          (try
            (jdbc/execute!
             (db-opts db/ds)
             (into [(str "SELECT *,
                           GREATEST(0, CAST(EXTRACT(EPOCH FROM (now() - COALESCE(last_success_at_utc, updated_at_utc))) AS BIGINT)) AS freshness_lag_seconds
                    FROM " freshness-table "
                    WHERE EXTRACT(EPOCH FROM (now() - COALESCE(last_success_at_utc, updated_at_utc)))
                          > (freshness_sla_seconds * 2)"
                    ws-clause
                    " ORDER BY freshness_lag_seconds DESC")]
              ws-params))
            (catch Exception e
              (log/debug e "stale-sources sub-query failed")
              []))

          ;; 2. High-lag sources: freshness lag > SLA (but not necessarily 2x)
          high-lag-sources
          (try
            (jdbc/execute!
             (db-opts db/ds)
             (into [(str "SELECT *,
                           GREATEST(0, CAST(EXTRACT(EPOCH FROM (now() - COALESCE(last_success_at_utc, updated_at_utc))) AS BIGINT)) AS freshness_lag_seconds
                    FROM " freshness-table "
                    WHERE EXTRACT(EPOCH FROM (now() - COALESCE(last_success_at_utc, updated_at_utc)))
                          > freshness_sla_seconds"
                    ws-clause
                    " ORDER BY freshness_lag_seconds DESC")]
              ws-params))
            (catch Exception e
              (log/debug e "high-lag-sources sub-query failed")
              []))

          ;; 3. Checkpoint-manifest gaps: failed runs that produced batches
          ;;    (the batches were written but the checkpoint was never advanced)
          checkpoint-gaps
          (try
            (let [er-ws (if workspace-key " AND er.workspace_key = ?" "")
                  er-params (if workspace-key [workspace-key] [])]
              (jdbc/execute!
               (db-opts db/ds)
               (into [(str "SELECT er.run_id, er.source_system, er.endpoint_name, er.status,
                             er.finished_at_utc, COUNT(a.artifact_id) AS batch_count
                      FROM " run-table " er
                      JOIN " artifact-table " a ON a.run_id = er.run_id::text
                      WHERE er.status = 'failed'
                        AND er.finished_at_utc >= now() - interval '24 hours'"
                      er-ws
                      " GROUP BY er.run_id, er.source_system, er.endpoint_name,
                               er.status, er.finished_at_utc
                      HAVING COUNT(a.artifact_id) > 0
                      ORDER BY er.finished_at_utc DESC
                      LIMIT 50")]
                er-params)))
            (catch Exception e
              (log/debug e "checkpoint-gaps sub-query failed")
              []))

          ;; Derive overall risk level
          stale-count (count stale-sources)
          gap-count   (count checkpoint-gaps)
          risk-level  (cond
                        (or (>= stale-count 3)
                            (>= gap-count 5))       "critical"
                        (or (>= stale-count 1)
                            (>= gap-count 2))       "high"
                        (> (count high-lag-sources) 0) "medium"
                        :else                          "low")]

      {:stale_sources    (vec stale-sources)
       :high_lag_sources (vec high-lag-sources)
       :checkpoint_gaps  (vec checkpoint-gaps)
       :risk_level       risk-level})
    (catch Exception e
      (log/warn e "data-loss-risk analysis failed" {:workspace_key workspace-key})
      {:stale_sources    []
       :high_lag_sources []
       :checkpoint_gaps  []
       :risk_level       "unknown"})))

(defn kafka-stream-detail
  "Return detail for a single Kafka source.

   Uses freshness + recent execution_run telemetry to produce a runtime view."
  [source-id]
  (try
    (let [freshness-row (jdbc/execute-one!
                         (db-opts db/ds)
                         [(str "SELECT *,
                                       GREATEST(0, CAST(EXTRACT(EPOCH FROM (now() - COALESCE(last_success_at_utc, updated_at_utc))) AS BIGINT)) AS freshness_lag_seconds
                                FROM " freshness-table "
                                WHERE source_system = ?
                                ORDER BY updated_at_utc DESC
                                LIMIT 1")
                          (str source-id)])
          cb-state     (circuit-breaker-state source-id)
          latest-run   (jdbc/execute-one!
                        (db-opts db/ds)
                        [(str "SELECT run_id, status, started_at_utc, finished_at_utc, rows_written
                               FROM " run-table "
                               WHERE source_system = ?
                               ORDER BY COALESCE(finished_at_utc, started_at_utc) DESC
                               LIMIT 1")
                         (str source-id)])
          throughput-row (jdbc/execute-one!
                          (db-opts db/ds)
                          [(str "SELECT COALESCE(SUM(rows_written), 0) AS rows_last_5m
                                 FROM " run-table "
                                 WHERE source_system = ?
                                   AND COALESCE(finished_at_utc, started_at_utc) >= now() - interval '5 minutes'")
                           (str source-id)])]
      {:source_id       (str source-id)
       :source_type     "kafka"
       :freshness       (when freshness-row
                          {:status              (:status freshness-row)
                           :last_success_at_utc (:last_success_at_utc freshness-row)
                           :freshness_lag_seconds (when (:freshness_lag_seconds freshness-row)
                                                    (long (:freshness_lag_seconds freshness-row)))
                           :freshness_sla_seconds (when (:freshness_sla_seconds freshness-row)
                                                   (long (:freshness_sla_seconds freshness-row)))
                           :overdue?            (when (and (:freshness_lag_seconds freshness-row)
                                                           (:freshness_sla_seconds freshness-row))
                                                  (> (long (:freshness_lag_seconds freshness-row))
                                                     (long (:freshness_sla_seconds freshness-row))))})
       :circuit_breaker cb-state
       :latest_run      latest-run
       :partitions      []
       :consumer_lag    (some-> freshness-row :freshness_lag_seconds as-long)
       :throughput      {:rows_last_5m (as-long (:rows_last_5m throughput-row))
                         :last_run_rows (as-long (:rows_written latest-run))}})
    (catch Exception e
      (log/warn e "kafka-stream-detail failed" {:source_id source-id})
      {:source_id       (str source-id)
      :source_type     "kafka"
       :freshness       nil
       :circuit_breaker nil
       :latest_run      nil
       :partitions      []
       :consumer_lag    0
       :throughput      {:rows_last_5m 0
                         :last_run_rows 0}})))
