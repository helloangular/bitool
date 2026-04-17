(ns bitool.ops.dashboard
  (:require [bitool.db :as db]
            [bitool.ingest.execution :as ingest-execution]
            [bitool.operations :as operations]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- db-opts
  [conn]
  (jdbc/with-options conn {:builder-fn rs/as-unqualified-lower-maps}))

(def ^:private execution-request-table "execution_request")
(def ^:private execution-run-table "execution_run")
(def ^:private endpoint-freshness-table "operations_endpoint_freshness_status")
(def ^:private usage-meter-table "operations_usage_meter_daily")
(def ^:private ops-alert-table "ops_alert")
(def ^:private artifact-store-table "ingest_batch_artifact_store")
(def ^:private audit-event-table "control_plane_audit_event")
(def ^:private schema-profile-table "schema_profile_snapshot")
(def ^:private model-proposal-table "model_proposal")
(def ^:private model-release-table "model_release")
(def ^:private bad-record-action-table "ops_bad_record_action")
(defonce ^:private bad-record-actions-ready? (atom false))

(defn- source-key
  [source-system endpoint-name]
  (string/join "::" [(or source-system "") (or endpoint-name "")]))

(defn- infer-source-kind
  [source-system]
  (let [v (string/lower-case (str (or source-system "")))]
    (cond
      (string/includes? v "kafka") :kafka
      (or (string/includes? v "file")
          (string/includes? v "mainframe")) :file
      :else :api)))

(defn- parse-json-safe
  [value]
  (cond
    (nil? value) nil
    (map? value) value
    (vector? value) value
    (string? value)
    (try
      (json/parse-string value true)
      (catch Exception _
        nil))
    :else nil))

(defn- fmt-lag
  [seconds]
  (when (some? seconds)
    (let [seconds (long seconds)]
      (cond
        (< seconds 60) (str seconds "s")
        (< seconds 3600) (str (long (Math/floor (/ seconds 60.0))) "m")
        (< seconds 86400) (str (long (Math/floor (/ seconds 3600.0))) "h")
        :else (str (long (Math/floor (/ seconds 86400.0))) "d")))))

(defn- as-long
  [v]
  (long (or v 0)))

(defn- csv-escape
  [value]
  (let [s (str (or value ""))]
    (if (or (string/includes? s ",")
            (string/includes? s "\"")
            (string/includes? s "\n")
            (string/includes? s "\r"))
      (str "\"" (string/replace s "\"" "\"\"") "\"")
      s)))

(defn- time-range->cutoff
  "Convert a human time-range string to a SQL cutoff expression.
   Accepts \"1h\", \"6h\", \"24h\", \"7d\", \"30d\".
   Returns a vector [sql-fragment & params] for WHERE clauses."
  [time-range]
  (let [time-range (or time-range "24h")
        [_ amount unit] (re-matches #"(\d+)(h|d)" time-range)]
    (when (and amount unit)
      (let [hours (case unit
                    "h" (Long/parseLong amount)
                    "d" (* 24 (Long/parseLong amount)))]
        [(str "now() - (? || ' hours')::interval")
         hours]))))

(defn- safe-query
  "Execute a query, returning empty results if the table does not exist.
   Catches PostgreSQL undefined_table errors (42P01) and returns fallback."
  ([conn sql-vec]
   (safe-query conn sql-vec []))
  ([conn sql-vec fallback]
   (try
     (jdbc/execute! (db-opts conn) sql-vec)
     (catch org.postgresql.util.PSQLException e
       (if (= "42P01" (.getSQLState e))
         (do (log/debug "Table not found, returning fallback" {:sql (first sql-vec)})
             fallback)
         (throw e)))
     (catch Exception e
       (if (and (.getMessage e)
                (string/includes? (.getMessage e) "does not exist"))
         (do (log/debug "Table not found, returning fallback" {:sql (first sql-vec)})
             fallback)
         (throw e))))))

(defn- safe-query-one
  "Execute a query expecting one result. Returns nil on table-not-found."
  [conn sql-vec]
  (try
    (jdbc/execute-one! (db-opts conn) sql-vec)
    (catch org.postgresql.util.PSQLException e
      (if (= "42P01" (.getSQLState e))
        (do (log/debug "Table not found, returning nil" {:sql (first sql-vec)})
            nil)
        (throw e)))
    (catch Exception e
      (if (and (.getMessage e)
               (string/includes? (.getMessage e) "does not exist"))
        (do (log/debug "Table not found, returning nil" {:sql (first sql-vec)})
            nil)
        (throw e)))))

(defn- ensure-bad-record-action-table!
  []
  (when-not @bad-record-actions-ready?
    (locking bad-record-actions-ready?
      (when-not @bad-record-actions-ready?
        (jdbc/execute!
         db/ds
         [(str "CREATE TABLE IF NOT EXISTS " bad-record-action-table " ("
               "artifact_id BIGINT PRIMARY KEY, "
               "state VARCHAR(32) NOT NULL, "
               "updated_by TEXT NOT NULL, "
               "workspace_key TEXT NULL, "
               "updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")])
        (jdbc/execute!
         db/ds
         [(str "CREATE INDEX IF NOT EXISTS idx_ops_bad_record_action_state "
               "ON " bad-record-action-table " (state, updated_at_utc DESC)")])
        (reset! bad-record-actions-ready? true))))
  nil)

;; ---------------------------------------------------------------------------
;; 1. pipeline-kpis
;; ---------------------------------------------------------------------------

(defn pipeline-kpis
  "Returns a map of top-level pipeline KPIs for a workspace within a time range."
  [{:keys [workspace-key time-range]}]
  (let [[cutoff-expr cutoff-hours] (time-range->cutoff (or time-range "24h"))
        cutoff-expr (or cutoff-expr "now() - '24 hours'::interval")
        cutoff-hours (or cutoff-hours 24)
        ws-clause (when workspace-key " AND workspace_key = ?")
        ws-params (when workspace-key [workspace-key])
        active-sources
        (or (:cnt
             (safe-query-one
              db/ds
              (into [(str "SELECT COUNT(*) AS cnt
                           FROM " endpoint-freshness-table
                          (when workspace-key " WHERE workspace_key = ?"))]
                    ws-params)))
            0)
        running-jobs
        (or (:cnt
             (safe-query-one
              db/ds
              (into [(str "SELECT COUNT(*) AS cnt
                           FROM " execution-request-table
                          " WHERE status IN ('queued','leased','running','recovering_orphan')"
                          ws-clause)]
                    ws-params)))
            0)
        failed-jobs
        (or (:cnt
             (safe-query-one
              db/ds
              (into [(str "SELECT COUNT(*) AS cnt
                           FROM " execution-run-table
                          " WHERE status = 'failed'
                            AND started_at_utc >= " cutoff-expr
                          ws-clause)
                     cutoff-hours]
                    ws-params)))
            0)
        batches-committed
        (or (:cnt
             (safe-query-one
              db/ds
              (into [(str "SELECT COUNT(DISTINCT batch_id) AS cnt
                           FROM " artifact-store-table
                          " WHERE created_at_utc >= " cutoff-expr)
                     cutoff-hours]
                    [])))
            0)
        bad-records 0
        avg-freshness-seconds
        (or (:avg_lag
             (safe-query-one
              db/ds
              (into [(str "SELECT AVG(GREATEST(0, EXTRACT(EPOCH FROM (now() - COALESCE(last_success_at_utc, updated_at_utc))))) AS avg_lag
                           FROM " endpoint-freshness-table
                          (when workspace-key " WHERE workspace_key = ?"))]
                    ws-params)))
            0.0)]
    {:active_sources (long active-sources)
     :running_jobs (long running-jobs)
     :bad_records (long bad-records)
     :failed_jobs (long failed-jobs)
     :batches_committed (long batches-committed)
     :avg_freshness (or (fmt-lag avg-freshness-seconds) "0s")
     :active_sources_sub "sources with freshness tracking"
     :running_jobs_sub "queued, leased, or running"
     :bad_records_sub "global bad-record detail not yet indexed"
     :failed_jobs_sub (str "failed in last " cutoff-hours "h")
     :batches_committed_sub (str "distinct batches in last " cutoff-hours "h")
     :avg_freshness_sub "average source lag"}))

;; ---------------------------------------------------------------------------
;; 2. source-status-list
;; ---------------------------------------------------------------------------

(defn source-status-list
  "Per-source status rows from freshness table joined with latest execution_run."
  [{:keys [workspace-key]}]
  (let [ws-clause (when workspace-key " WHERE f.workspace_key = ?")
        ws-params (when workspace-key [workspace-key])]
    (mapv
     (fn [row]
       (let [lag (as-long (:freshness_lag_seconds row))]
         {:graph_id (:graph_id row)
          :node_id (:api_node_id row)
          :source_key (source-key (:source_system row) (:endpoint_name row))
          :name (:endpoint_name row)
          :source_type (name (infer-source-kind (:source_system row)))
          :status (if (and (:freshness_sla_seconds row)
                           (> lag (as-long (:freshness_sla_seconds row))))
                    "stale"
                    (or (:last_run_status row) (:status row) "healthy"))
          :last_run (or (:last_run_finished row) (:last_run_started row) (:last_success_at_utc row))
          :throughput (when (some? (:rows_written row))
                        (str (as-long (:rows_written row)) " rows"))
          :freshness (or (fmt-lag lag) "0s")
          :bad_records 0
          :trend_values []}))
     (safe-query
      db/ds
      (into [(str "SELECT f.graph_id,
                           f.api_node_id,
                           f.source_system,
                           f.endpoint_name,
                           f.target_table,
                           f.status,
                           f.last_success_at_utc,
                           f.max_watermark,
                           f.rows_written,
                           f.freshness_sla_seconds,
                           f.updated_at_utc,
                           GREATEST(0, CAST(EXTRACT(EPOCH FROM (now() - COALESCE(f.last_success_at_utc, f.updated_at_utc))) AS BIGINT)) AS freshness_lag_seconds,
                           r.run_id AS last_run_id,
                           r.status AS last_run_status,
                           r.started_at_utc AS last_run_started,
                           r.finished_at_utc AS last_run_finished
                    FROM " endpoint-freshness-table " f
                    LEFT JOIN LATERAL (
                      SELECT run_id, status, started_at_utc, finished_at_utc
                      FROM " execution-run-table " er
                      WHERE er.graph_id = f.graph_id
                        AND er.node_id = f.api_node_id
                      ORDER BY er.started_at_utc DESC
                      LIMIT 1
                    ) r ON true"
                   ws-clause
                   " ORDER BY freshness_lag_seconds ASC, f.updated_at_utc DESC")]
            ws-params)))))

;; ---------------------------------------------------------------------------
;; 3. recent-activity
;; ---------------------------------------------------------------------------

(defn recent-activity
  "UNION of recent execution_run events and audit_event entries, ordered by timestamp DESC."
  [{:keys [workspace-key limit]}]
  (let [limit (max 1 (long (or limit 50)))
        ws-clause-run (when workspace-key " WHERE workspace_key = ?")
        ws-clause-audit (when workspace-key
                          (str " WHERE graph_id IN (SELECT DISTINCT graph_id FROM "
                               execution-run-table " WHERE workspace_key = ?)"))
        ws-params (when workspace-key [workspace-key])
        rows (safe-query
              db/ds
              (into [(str "(SELECT 'execution_run' AS event_source,
                                   run_id::text AS event_id,
                                   status AS event_type,
                                   graph_id,
                                   node_id,
                                   endpoint_name,
                                   source_system,
                                   error_message,
                                   COALESCE(finished_at_utc, started_at_utc) AS event_at
                            FROM " execution-run-table
                           ws-clause-run
                           " ORDER BY COALESCE(finished_at_utc, started_at_utc) DESC
                            LIMIT ?)"
                           " UNION ALL "
                           "(SELECT 'audit_event' AS event_source,
                                    id::text AS event_id,
                                    event_type,
                                    graph_id,
                                    node_id,
                                    NULL AS endpoint_name,
                                    NULL AS source_system,
                                    details_json AS error_message,
                                    created_at_utc AS event_at
                            FROM " audit-event-table
                           ws-clause-audit
                           " ORDER BY created_at_utc DESC
                            LIMIT ?)"
                           " ORDER BY event_at DESC
                            LIMIT ?")]
                    (concat ws-params [limit]
                            (when workspace-key ws-params)
                            [limit limit])))]
    (mapv (fn [row]
            {:timestamp (:event_at row)
             :event_type (:event_type row)
             :source (or (when (:endpoint_name row)
                           (source-key (:source_system row) (:endpoint_name row)))
                         "control-plane")
             :details (or (:error_message row) (:event_source row))})
          rows)))

;; ---------------------------------------------------------------------------
;; 4. batch-summary
;; ---------------------------------------------------------------------------

(defn batch-summary
  "Return a lightweight batch overview derived from the artifact store."
  [{:keys [workspace-key]}]
  (let [ws-clause (when workspace-key " AND er.workspace_key = ?")
        ws-params (when workspace-key [workspace-key])
        rows (safe-query
              db/ds
              (into [(str "WITH batch_rows AS (
                            SELECT a.batch_id,
                                   a.source_system,
                                   a.endpoint_name,
                                   COUNT(*) AS artifact_count,
                                   BOOL_OR(a.archived_at_utc IS NOT NULL) AS archived,
                                   MAX(a.created_at_utc) AS last_created_at_utc,
                                   MAX(COALESCE(er.status, 'succeeded')) AS run_status
                            FROM " artifact-store-table " a
                            LEFT JOIN " execution-run-table " er ON er.run_id::text = a.run_id
                            WHERE a.batch_id IS NOT NULL"
                          ws-clause
                          " GROUP BY a.batch_id, a.source_system, a.endpoint_name
                          )
                          SELECT *
                          FROM batch_rows
                          ORDER BY last_created_at_utc DESC
                          LIMIT 100")]
                    ws-params))
        status-of (fn [row]
                    (let [run-status (some-> (:run_status row) str string/lower-case)]
                      (cond
                        (:archived row) "archived"
                        (#{"queued" "leased" "running" "recovering_orphan"} run-status) "preparing"
                        (#{"failed" "timed_out"} run-status) "rolled_back"
                        :else "committed")))
        counts (reduce (fn [acc row]
                         (update acc (status-of row) (fnil inc 0)))
                       {"pending_checkpoint" 0}
                       rows)]
    {:committed (long (get counts "committed" 0))
     :preparing (long (get counts "preparing" 0))
     :pending_checkpoint (long (get counts "pending_checkpoint" 0))
     :rolled_back (long (get counts "rolled_back" 0))
     :archived (long (get counts "archived" 0))
     :batches (mapv (fn [row]
                      {:manifest_id (:batch_id row)
                       :batch_id (:batch_id row)
                       :source (source-key (:source_system row) (:endpoint_name row))
                       :status (status-of row)
                       :rows (as-long (:artifact_count row))
                       :bad 0
                       :size "artifact store"
                       :created_at (:last_created_at_utc row)
                       :available_actions {:inspect true
                                           :replay true
                                           :rollback (not (:archived row))
                                           :archive (not (:archived row))}})
                    rows)}))

;; ---------------------------------------------------------------------------
;; 5. batch-detail
;; ---------------------------------------------------------------------------

(defn batch-detail
  "Single batch detail from the artifact store."
  [batch-id]
  (safe-query-one
   db/ds
   [(str "SELECT *
          FROM " artifact-store-table
         " WHERE batch_id = ?
          ORDER BY created_at_utc DESC")
    (str batch-id)]))

;; ---------------------------------------------------------------------------
;; 6. batch-artifacts
;; ---------------------------------------------------------------------------

(defn batch-artifacts
  "All artifacts for a given batch."
  [batch-id]
  (safe-query
   db/ds
   [(str "SELECT artifact_id, artifact_path, artifact_kind, run_id,
                 source_system, endpoint_name, batch_id,
                 artifact_checksum, archived_at_utc, created_at_utc
          FROM " artifact-store-table
         " WHERE batch_id = ?
          ORDER BY created_at_utc DESC")
    (str batch-id)]))

;; ---------------------------------------------------------------------------
;; 7. current-checkpoints
;; ---------------------------------------------------------------------------

(defn current-checkpoints
  "For each source in freshness table, return latest checkpoint info."
  [{:keys [workspace-key]}]
  (let [ws-clause (when workspace-key " WHERE workspace_key = ?")
        ws-params (when workspace-key [workspace-key])]
    (mapv
     (fn [row]
       {:graph_id (:graph_id row)
        :node_id (:api_node_id row)
        :source_kind (name (infer-source-kind (:source_system row)))
        :source_system (:source_system row)
        :endpoint_name (:endpoint_name row)
        :source_key (source-key (:source_system row) (:endpoint_name row))
        :checkpoint_type "watermark"
        :checkpoint_key (:endpoint_name row)
        :current_value (or (:max_watermark row) (:last_success_run_id row) "")
        :last_updated (or (:last_success_at_utc row) (:updated_at_utc row))
        :stale (and (:freshness_sla_seconds row)
                    (:freshness_lag_seconds row)
                    (> (as-long (:freshness_lag_seconds row))
                       (as-long (:freshness_sla_seconds row))))})
     (safe-query
      db/ds
      (into [(str "SELECT graph_id,
                          api_node_id,
                          source_system,
                          endpoint_name,
                          max_watermark,
                          last_success_at_utc,
                          last_success_run_id,
                          rows_written,
                          freshness_sla_seconds,
                          GREATEST(0, CAST(EXTRACT(EPOCH FROM (now() - COALESCE(last_success_at_utc, updated_at_utc))) AS BIGINT)) AS freshness_lag_seconds,
                          updated_at_utc
                   FROM " endpoint-freshness-table
                  ws-clause
                  " ORDER BY updated_at_utc DESC")]
            ws-params)))))

;; ---------------------------------------------------------------------------
;; 8. checkpoint-history
;; ---------------------------------------------------------------------------

(defn checkpoint-history
  "Query execution_run history for a source, showing watermark progression."
  [{:keys [workspace-key source-key limit]}]
  (let [limit (max 1 (long (or limit 50)))
        [source-system endpoint-name] (string/split (str (or source-key "")) #"::" 2)
        ws-clause (when workspace-key " AND workspace_key = ?")
        ws-params (when workspace-key [workspace-key])
        rows (safe-query
              db/ds
              (into [(str "SELECT run_id, graph_id, node_id, source_system, endpoint_name,
                                  status,
                                  started_at_utc, finished_at_utc,
                                  result_json
                           FROM " execution-run-table
                          " WHERE source_system = ?
                            AND endpoint_name = ?"
                          ws-clause
                          " ORDER BY started_at_utc DESC
                           LIMIT ?")
                     source-system
                     endpoint-name]
                    (concat ws-params [limit])))]
    (mapv (fn [row]
            (let [result (parse-json-safe (:result_json row))]
              {:timestamp (or (:finished_at_utc row) (:started_at_utc row))
               :action (:status row)
               :old_value nil
               :new_value (or (get result :max_watermark) (get result :rows_written))
               :actor "system"
               :batch_id (or (get result :replay_source_batch_id)
                             (some-> (get result :replay_source_batch_ids) first))
               :run_id (:run_id row)}))
          rows)))

;; ---------------------------------------------------------------------------
;; 9. active-replays
;; ---------------------------------------------------------------------------

(defn active-replays
  "Execution runs that are replays currently in progress."
  [{:keys [workspace-key]}]
  (let [ws-clause (when workspace-key " AND workspace_key = ?")
        ws-params (when workspace-key [workspace-key])]
    (mapv (fn [row]
            (let [result (parse-json-safe (:result_json row))]
              {:replay_id (:run_id row)
               :source (source-key (:source_system row) (:endpoint_name row))
               :status (:status row)
               :from_batch (or (get result :replay_source_batch_id)
                               (some-> (get result :replay_source_batch_ids) first))
               :started_at (:started_at_utc row)
               :progress (when (some? (get result :rows_written))
                           (str (as-long (get result :rows_written)) " rows"))}))
          (safe-query
           db/ds
           (into [(str "SELECT run_id, graph_id, node_id, source_system, endpoint_name,
                               status, workload_class, started_at_utc,
                               result_json
                        FROM " execution-run-table
                       " WHERE workload_class = 'replay'
                         AND status IN ('leased','running')"
                       ws-clause
                       " ORDER BY started_at_utc DESC")]
                 ws-params)))))

;; ---------------------------------------------------------------------------
;; 10. bad-record-summary
;; ---------------------------------------------------------------------------

(defn bad-record-summary
  "Return a stable bad-record payload shape even when global bad-record indexing
   is not available."
  [{:keys [workspace-key]}]
  (ensure-bad-record-action-table!)
  (let [ws-clause (when workspace-key " AND er.workspace_key = ?")
        ws-params (when workspace-key [workspace-key])
        total-row (safe-query-one
                   db/ds
                   (into [(str "SELECT COUNT(*) AS cnt
                                FROM " artifact-store-table " a
                                LEFT JOIN " execution-run-table " er ON er.run_id::text = a.run_id
                                WHERE a.artifact_kind = 'bad_record'"
                              ws-clause)]
                         ws-params))
        rows (safe-query
              db/ds
              (into [(str "SELECT a.artifact_id AS record_id,
                                  a.batch_id,
                                  a.run_id,
                                  a.source_system,
                                  a.endpoint_name,
                                  a.artifact_path,
                                  a.payload_json,
                                  a.created_at_utc,
                                  act.state AS action_state,
                                  act.updated_at_utc AS action_updated_at_utc
                           FROM " artifact-store-table " a
                           LEFT JOIN " execution-run-table " er ON er.run_id::text = a.run_id
                           LEFT JOIN " bad-record-action-table " act ON act.artifact_id = a.artifact_id
                           WHERE a.artifact_kind = 'bad_record'"
                          ws-clause
                          " ORDER BY a.created_at_utc DESC
                            LIMIT 200")]
                    ws-params))
        counts (reduce (fn [acc row]
                         (case (:action_state row)
                           "ignored" (update acc :ignored inc)
                           "replayed" (update acc :replayed inc)
                           (update acc :pending inc)))
                       {:pending 0 :replayed 0 :ignored 0}
                       rows)]
    {:total (as-long (:cnt total-row))
     :pending (long (:pending counts))
     :replayed (long (:replayed counts))
     :ignored (long (:ignored counts))
     :records (mapv (fn [row]
                      (let [pj (try (json/parse-string (or (:artifact_path row) "{}") true)
                                    (catch Exception _ nil))
                            pj2 (try (json/parse-string (or (:payload_json row) "{}") true)
                                     (catch Exception _ nil))
                            fc  (or (:failure_class pj2) (:failure_class pj) "bad_record")
                            msg (or (:error_message pj2) (:error_message pj) (:artifact_path row) "bad_record")]
                        {:record_id     (:record_id row)
                         :source        (source-key (:source_system row) (:endpoint_name row))
                         :batch_id      (:batch_id row)
                         :failure_class fc
                         :reason        msg
                         :status        (or (:action_state row) "pending")
                         :timestamp     (:created_at_utc row)}))
                    rows)}))

;; ---------------------------------------------------------------------------
;; 11. bad-record-payload
;; ---------------------------------------------------------------------------

(defn bad-record-payload
  "Single bad record detail from the artifact store."
  [record-id]
  (when-let [row (safe-query-one
                  db/ds
                  [(str "SELECT *
                         FROM " artifact-store-table
                        " WHERE artifact_id = ?")
                   (long record-id)])]
    (let [pj (try (json/parse-string (or (:payload_json row) "{}") true)
                  (catch Exception _ nil))]
      {:record_id      (:artifact_id row)
       :source         (source-key (:source_system row) (:endpoint_name row))
       :source_system  (:source_system row)
       :endpoint_name  (:endpoint_name row)
       :batch_id       (:batch_id row)
       :run_id         (:run_id row)
       :failure_class  (or (:failure_class pj) "bad_record")
       :error_message  (or (:error_message pj) (:artifact_path row))
       :payload        (if (:record pj)
                         (json/generate-string (:record pj) {:pretty true})
                         (:payload_json row))
       :created_at_utc (:created_at_utc row)})))

;; ---------------------------------------------------------------------------
;; 12. freshness-chain
;; ---------------------------------------------------------------------------

(defn freshness-chain
  "Bronze freshness from operations table, joined with Silver/Gold model_proposal status."
  [{:keys [workspace-key]}]
  (let [ws-clause (when workspace-key " WHERE f.workspace_key = ?")
        ws-params (when workspace-key [workspace-key])]
    (safe-query
     db/ds
     (into [(str "SELECT f.graph_id,
                         f.source_system,
                         f.endpoint_name,
                         f.target_table AS bronze_table,
                         f.status AS bronze_status,
                         f.last_success_at_utc AS bronze_last_success,
                         GREATEST(0, CAST(EXTRACT(EPOCH FROM (now() - COALESCE(f.last_success_at_utc, f.updated_at_utc))) AS BIGINT)) AS bronze_lag_seconds,
                         f.freshness_sla_seconds AS bronze_sla_seconds,
                         mp.target_model AS silver_table,
                         mp.status AS silver_status,
                         mp.created_at_utc AS silver_updated_at
                  FROM " endpoint-freshness-table " f
                  LEFT JOIN " model-proposal-table " mp
                    ON mp.source_graph_id = f.graph_id
                    AND mp.source_endpoint_name = f.endpoint_name
                    AND mp.status != 'superseded'"
                 ws-clause
                 " ORDER BY bronze_lag_seconds DESC, f.updated_at_utc DESC")]
           ws-params))))

;; ---------------------------------------------------------------------------
;; 13. schema-drift
;; ---------------------------------------------------------------------------

(defn schema-drift
  "Query schema profile snapshots for drift entries."
  [{:keys [workspace-key]}]
  (let [ws-clause (when workspace-key
                    (str " AND graph_id IN (SELECT DISTINCT graph_id FROM "
                         endpoint-freshness-table " WHERE workspace_key = ?)"))
        ws-params (when workspace-key [workspace-key])]
    (safe-query
     db/ds
     (into [(str "SELECT graph_id, source_system, endpoint_name,
                         profile_json, field_count,
                         created_at_utc AS captured_at_utc
                  FROM " schema-profile-table
                 " WHERE profile_json IS NOT NULL"
                 ws-clause
                 " ORDER BY created_at_utc DESC
                   LIMIT 200")]
           ws-params))))

;; ---------------------------------------------------------------------------
;; 14. medallion-releases
;; ---------------------------------------------------------------------------

(defn medallion-releases
  "Recent model releases ordered by release time."
  [{:keys [workspace-key limit]}]
  (let [limit (max 1 (long (or limit 50)))
        ws-clause (when workspace-key
                    (str " WHERE graph_id IN (SELECT DISTINCT graph_id FROM "
                         endpoint-freshness-table " WHERE workspace_key = ?)"))
        ws-params (when workspace-key [workspace-key])]
    (safe-query
     db/ds
     (into [(str "SELECT release_id, proposal_id, graph_artifact_id,
                         target_model AS source_key, layer,
                         version AS schema_version, status,
                         created_by AS released_by,
                         COALESCE(published_at_utc, created_at_utc) AS released_at_utc,
                         active AS active_release
                  FROM " model-release-table
                  (if ws-clause
                    ws-clause
                    "")
                  " ORDER BY COALESCE(published_at_utc, created_at_utc) DESC
                    LIMIT ?")]
           (concat ws-params [limit])))))

;; ---------------------------------------------------------------------------
;; 15. replay-from-checkpoint!
;; ---------------------------------------------------------------------------

(defn replay-from-checkpoint!
  "Create a replay execution request through the normal execution queue."
  [{:keys [workspace-key source-key source-kind graph-id node-id endpoint-name from-batch operator dry-run]}]
  (when-not from-batch
    (throw (ex-info "from_batch is required" {:status 400 :error "bad_request"})))
  (let [artifact-row (safe-query-one
                      db/ds
                      [(str "SELECT batch_id, run_id, source_system, endpoint_name
                             FROM " artifact-store-table
                            " WHERE batch_id = ?
                            ORDER BY created_at_utc DESC
                            LIMIT 1")
                       (str from-batch)])
        [source-system' endpoint-name'] (if (and (seq source-key) (string/includes? source-key "::"))
                                          (let [[ss ep] (string/split source-key #"::" 2)]
                                            [ss ep])
                                          [(:source_system artifact-row) endpoint-name])
        freshness-row (when (or (and source-system' endpoint-name') (and graph-id node-id))
                        (safe-query-one
                         db/ds
                         (if (and graph-id node-id)
                           [(str "SELECT graph_id, api_node_id, source_system, endpoint_name
                                  FROM " endpoint-freshness-table
                                 " WHERE graph_id = ? AND api_node_id = ?
                                 LIMIT 1")
                            graph-id
                            node-id]
                           [(str "SELECT graph_id, api_node_id, source_system, endpoint_name
                                  FROM " endpoint-freshness-table
                                 " WHERE source_system = ? AND endpoint_name = ?
                                   AND workspace_key = ?
                                 LIMIT 1")
                            source-system'
                            endpoint-name'
                            (or workspace-key "default")])))
        graph-id (or graph-id (:graph_id freshness-row))
        node-id (or node-id (:api_node_id freshness-row))
        source-system (or source-system' (:source_system freshness-row))
        endpoint-name (or endpoint-name' (:endpoint_name freshness-row))
        source-kind (keyword (or (some-> source-kind name)
                                 (name (infer-source-kind source-system))))
        request-params (cond-> {:replay_source_run_id (:run_id artifact-row)
                                :replay_source_batch_ids [(str from-batch)]}
                         endpoint-name (assoc :endpoint-name endpoint-name))]
    (when-not (and graph-id node-id endpoint-name)
      (throw (ex-info "Unable to resolve replay source from batch/source parameters"
                      {:status 404 :error "not_found" :from_batch from-batch :source_key source-key})))
    (if dry-run
      {:dry_run true
       :graph_id graph-id
       :node_id node-id
       :source_kind (name source-kind)
       :endpoint_name endpoint-name
       :request_params request-params}
      (case source-kind
        :api (ingest-execution/enqueue-api-request! graph-id node-id
                                                    {:workspace-key workspace-key
                                                     :endpoint-name endpoint-name
                                                     :trigger-type "manual"
                                                     :request-params request-params})
        :kafka (ingest-execution/enqueue-kafka-request! graph-id node-id
                                                        {:workspace-key workspace-key
                                                         :endpoint-name endpoint-name
                                                         :trigger-type "manual"
                                                         :request-params request-params})
        :file (ingest-execution/enqueue-file-request! graph-id node-id
                                                      {:workspace-key workspace-key
                                                       :endpoint-name endpoint-name
                                                       :trigger-type "manual"
                                                       :request-params request-params})
        (throw (ex-info "Unsupported source kind for replay"
                        {:status 400 :error "bad_request" :source_kind source-kind}))))))

;; ---------------------------------------------------------------------------
;; 16. replay-bad-records!
;; ---------------------------------------------------------------------------

(defn replay-bad-records!
  "Replay selected bad records. Marks them as replayed in the action ledger
   and returns a success response."
  [{:keys [record-ids workspace-key operator dry-run]}]
  (ensure-bad-record-action-table!)
  (let [ids (->> record-ids
                 (map #(try
                         (Long/parseLong (str %))
                         (catch Exception _
                           nil)))
                 (remove nil?)
                 distinct
                 vec)]
    (when-not (seq ids)
      (throw (ex-info "record_ids must contain at least one numeric id"
                      {:status 400
                       :error "bad_request"})))
    (let [rows (safe-query
                db/ds
                [(str "SELECT artifact_id, batch_id, source_system, endpoint_name
                       FROM " artifact-store-table "
                       WHERE artifact_kind = 'bad_record'
                         AND artifact_id = ANY(?)
                       ORDER BY created_at_utc DESC")
                 (into-array Long ids)])
          batches (->> rows (map :batch_id) (remove nil?) distinct vec)
          updated (safe-query
                   db/ds
                   [(str "INSERT INTO " bad-record-action-table "
                         (artifact_id, state, updated_by, workspace_key, updated_at_utc)
                         SELECT a.artifact_id, 'replayed', ?, ?, now()
                         FROM " artifact-store-table " a
                         WHERE a.artifact_kind = 'bad_record'
                           AND a.artifact_id = ANY(?)
                         ON CONFLICT (artifact_id) DO UPDATE
                           SET state = 'replayed',
                               updated_by = EXCLUDED.updated_by,
                               workspace_key = EXCLUDED.workspace_key,
                               updated_at_utc = now()
                         RETURNING artifact_id")
                    (or operator "operator")
                    workspace-key
                    (into-array Long ids)])]
      {:status "queued"
       :operator (or operator "operator")
       :requested_records (count ids)
       :unique_batches (count batches)
       :results (mapv (fn [b] {:batch_id b :status "queued"}) batches)})))

;; ---------------------------------------------------------------------------
;; 17. bulk-ignore-bad-records!
;; ---------------------------------------------------------------------------

(defn bulk-ignore-bad-records!
  "Mark selected bad records as ignored in the ops action ledger."
  [{:keys [record-ids operator workspace-key]}]
  (ensure-bad-record-action-table!)
  (let [ids (->> record-ids
                 (map #(try
                         (Long/parseLong (str %))
                         (catch Exception _
                           nil)))
                 (remove nil?)
                 distinct
                 vec)]
    (when-not (seq ids)
      (throw (ex-info "record_ids must contain at least one numeric id"
                      {:status 400
                       :error "bad_request"})))
    (let [ws-clause (when workspace-key " AND er.workspace_key = ?")
          ws-params (when workspace-key [workspace-key])
          rows (safe-query
                db/ds
                (into [(str "INSERT INTO " bad-record-action-table "
                            (artifact_id, state, updated_by, workspace_key, updated_at_utc)
                            SELECT a.artifact_id, 'ignored', ?, ?, now()
                            FROM " artifact-store-table " a
                            LEFT JOIN " execution-run-table " er ON er.run_id::text = a.run_id
                            WHERE a.artifact_kind = 'bad_record'
                              AND a.artifact_id = ANY(?)"
                           ws-clause
                           " ON CONFLICT (artifact_id) DO UPDATE
                              SET state = 'ignored',
                                  updated_by = EXCLUDED.updated_by,
                                  workspace_key = EXCLUDED.workspace_key,
                                  updated_at_utc = now()
                            RETURNING artifact_id")
                      (or operator "operator")
                      workspace-key
                      (into-array Long ids)]
                      ws-params))]
      {:status "updated"
       :requested (count ids)
       :updated (count rows)
       :record_ids (mapv :artifact_id rows)})))

;; ---------------------------------------------------------------------------
;; 18. export-bad-records
;; ---------------------------------------------------------------------------

(defn export-bad-records
  "Export bad records as a collection for download."
  [{:keys [workspace-key format limit]}]
  (ensure-bad-record-action-table!)
  (let [limit (max 1 (long (or limit 1000)))
        format (some-> (or format "json") name string/lower-case)
        ws-clause (when workspace-key " AND er.workspace_key = ?")
        ws-params (when workspace-key [workspace-key])
        rows (safe-query
              db/ds
              (into [(str "SELECT a.artifact_id AS record_id,
                                  a.batch_id,
                                  a.run_id,
                                  a.source_system,
                                  a.endpoint_name,
                                  a.artifact_path,
                                  a.created_at_utc,
                                  COALESCE(act.state, 'pending') AS action_state
                           FROM " artifact-store-table " a
                           LEFT JOIN " execution-run-table " er ON er.run_id::text = a.run_id
                           LEFT JOIN " bad-record-action-table " act ON act.artifact_id = a.artifact_id
                           WHERE a.artifact_kind = 'bad_record'"
                          ws-clause
                          " ORDER BY a.created_at_utc DESC
                            LIMIT ?")]
                    (concat ws-params [limit])))
        records (mapv (fn [row]
                        {:record_id (:record_id row)
                         :source_system (:source_system row)
                         :endpoint_name (:endpoint_name row)
                         :source_key (source-key (:source_system row) (:endpoint_name row))
                         :batch_id (:batch_id row)
                         :run_id (:run_id row)
                         :status (:action_state row)
                         :artifact_path (:artifact_path row)
                         :created_at_utc (:created_at_utc row)})
                      rows)]
    (if (= "csv" format)
      (let [header ["record_id" "source_key" "batch_id" "run_id" "status" "artifact_path" "created_at_utc"]
            csv-lines (cons (string/join "," header)
                            (map (fn [row]
                                   (string/join ","
                                                (map csv-escape
                                                     [(:record_id row)
                                                      (:source_key row)
                                                      (:batch_id row)
                                                      (:run_id row)
                                                      (:status row)
                                                      (:artifact_path row)
                                                      (:created_at_utc row)])))
                                 records))]
        (string/join "\n" csv-lines))
      {:format "json"
       :workspace_key workspace-key
       :records records})))
