(ns bitool.ops.timeseries
  (:require [bitool.db :as db]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

;; ---------------------------------------------------------------------------
;; Time-series rollup storage for sparklines
;;
;; Table: ops_timeseries_rollup
;;   metric_key     TEXT          -- e.g. "throughput.api_ingest"
;;   bucket         TIMESTAMPTZ  -- truncated to 5-min intervals
;;   workspace_key  TEXT          -- partition / filter dimension
;;   value          DOUBLE PRECISION
;;   sample_count   INT
;;
;; Metrics are recorded via record-metric! which INSERT ... ON CONFLICT
;; accumulates value and increments sample_count.
;; ---------------------------------------------------------------------------

(def ^:private rollup-table "ops_timeseries_rollup")
(defonce ^:private timeseries-ready? (atom false))

(def ^:private request-table "execution_request")
(def ^:private run-table "execution_run")

(defn- db-opts
  [conn]
  (jdbc/with-options conn {:builder-fn rs/as-unqualified-lower-maps}))

;; ---------------------------------------------------------------------------
;; Table bootstrap
;; ---------------------------------------------------------------------------

(defn ensure-timeseries-tables!
  "Create the ops_timeseries_rollup table and indexes if they do not already
   exist. Uses a double-check locking pattern so concurrent callers only run
   the DDL once per JVM lifetime."
  []
  (when-not @timeseries-ready?
    (locking timeseries-ready?
      (when-not @timeseries-ready?
        (doseq [sql-str
                [(str "CREATE TABLE IF NOT EXISTS " rollup-table " ("
                      "metric_key     TEXT NOT NULL, "
                      "bucket         TIMESTAMPTZ NOT NULL, "
                      "workspace_key  TEXT NOT NULL, "
                      "value          DOUBLE PRECISION NOT NULL, "
                      "sample_count   INT NOT NULL DEFAULT 1, "
                      "PRIMARY KEY (metric_key, bucket, workspace_key))")
                 (str "CREATE INDEX IF NOT EXISTS idx_ops_ts_query "
                      "ON " rollup-table "(workspace_key, metric_key, bucket DESC)")]]
          (jdbc/execute! db/ds [sql-str]))
        (reset! timeseries-ready? true))))
  nil)

;; ---------------------------------------------------------------------------
;; Metric key validation
;; ---------------------------------------------------------------------------

(def allowed-metric-prefixes
  "Set of recognised metric-key prefixes.  Any metric_key must start with one
   of these tokens (before the first dot)."
  #{"throughput" "error_rate" "lag" "bad_records" "queue_depth" "freshness_lag"})

(defn validate-metric-key
  "Return the metric-key string if its prefix is in `allowed-metric-prefixes`,
   otherwise throw."
  [k]
  (let [k (str k)
        prefix (first (string/split k #"\." 2))]
    (when-not (contains? allowed-metric-prefixes prefix)
      (throw (ex-info "Invalid metric key prefix"
                      {:metric_key k
                       :prefix     prefix
                       :allowed    allowed-metric-prefixes
                       :status     400})))
    k))

;; ---------------------------------------------------------------------------
;; record-metric!
;; ---------------------------------------------------------------------------

(defn record-metric!
  "Record a single metric data-point.

   Truncates the current time to the nearest 5-minute bucket and performs an
   INSERT ... ON CONFLICT that accumulates value and sample_count."
  [{:keys [metric-key workspace-key value]}]
  (ensure-timeseries-tables!)
  (let [metric-key    (validate-metric-key metric-key)
        workspace-key (or workspace-key "default")
        value         (double (or value 0.0))]
    (try
      (jdbc/execute!
       db/ds
       [(str "INSERT INTO " rollup-table "
              (metric_key, bucket, workspace_key, value, sample_count)
              VALUES (?,
                      date_trunc('hour', now())
                        + (EXTRACT(minute FROM now())::int / 5 * 5 || ' minutes')::interval,
                      ?, ?, 1)
              ON CONFLICT (metric_key, bucket, workspace_key) DO UPDATE
              SET value        = " rollup-table ".value + EXCLUDED.value,
                  sample_count = " rollup-table ".sample_count + 1")
        metric-key
        workspace-key
        value])
      (catch Exception e
        (log/warn e "record-metric! failed" {:metric_key metric-key
                                              :workspace_key workspace-key})))))

;; ---------------------------------------------------------------------------
;; sparkline-data
;; ---------------------------------------------------------------------------

(defn sparkline-data
  "Return time-series buckets for a given metric, workspace, and look-back
   window in hours.  Results are ordered by bucket ASC (oldest first) so they
   map directly to a sparkline array."
  [{:keys [metric-key workspace-key hours]
    :or   {hours 24}}]
  (ensure-timeseries-tables!)
  (let [metric-key    (validate-metric-key metric-key)
        workspace-key (or workspace-key "default")
        hours         (max 1 (long hours))]
    (try
      (jdbc/execute!
       (db-opts db/ds)
       [(str "SELECT metric_key, bucket, workspace_key, value, sample_count
              FROM " rollup-table "
              WHERE workspace_key = ?
                AND metric_key = ?
                AND bucket >= now() - (? || ' hours')::interval
              ORDER BY bucket ASC")
        workspace-key
        metric-key
        (str hours)])
      (catch Exception e
        (log/warn e "sparkline-data query failed" {:metric_key metric-key
                                                    :workspace_key workspace-key})
        []))))

;; ---------------------------------------------------------------------------
;; delta-from-yesterday
;; ---------------------------------------------------------------------------

(defn delta-from-yesterday
  "Compare today's aggregate metric value vs yesterday's.

   Returns:
     {:current   <double>
      :yesterday <double>
      :delta     <double>   ; current - yesterday
      :direction \"up\" | \"down\" | \"flat\"}"
  [{:keys [metric-key workspace-key]}]
  (ensure-timeseries-tables!)
  (let [metric-key    (validate-metric-key metric-key)
        workspace-key (or workspace-key "default")]
    (try
      (let [row (jdbc/execute-one!
                 (db-opts db/ds)
                 [(str "SELECT
                           COALESCE(SUM(CASE WHEN bucket >= date_trunc('day', now())
                                             THEN value END), 0) AS current_sum,
                           COALESCE(SUM(CASE WHEN bucket >= date_trunc('day', now()) - interval '1 day'
                                              AND bucket <  date_trunc('day', now())
                                             THEN value END), 0) AS yesterday_sum
                        FROM " rollup-table "
                        WHERE workspace_key = ?
                          AND metric_key = ?
                          AND bucket >= date_trunc('day', now()) - interval '1 day'")
                  workspace-key
                  metric-key])
            current   (double (or (:current_sum row) 0.0))
            yesterday (double (or (:yesterday_sum row) 0.0))
            delta     (- current yesterday)]
        {:current   current
         :yesterday yesterday
         :delta     delta
         :direction (cond
                      (> delta 0.0) "up"
                      (< delta 0.0) "down"
                      :else         "flat")})
      (catch Exception e
        (log/warn e "delta-from-yesterday failed" {:metric_key metric-key
                                                    :workspace_key workspace-key})
        {:current 0.0 :yesterday 0.0 :delta 0.0 :direction "flat"}))))

;; ---------------------------------------------------------------------------
;; record-rollup!
;; ---------------------------------------------------------------------------

(defn record-rollup!
  "Collect operational metrics from execution tables and record each as a
   time-series data-point.  Intended to be called periodically (e.g. every
   5 minutes) by the scheduler tick.

   Metrics collected:
     - throughput.runs_completed   (succeeded runs in the last 5 min)
     - error_rate.runs_failed      (failed runs in the last 5 min)
     - queue_depth.pending         (queued + leased requests right now)"
  []
  (ensure-timeseries-tables!)
  (try
    ;; -- throughput: completed runs in the last 5 minutes per workspace --
    (let [completed-rows
          (jdbc/execute!
           (db-opts db/ds)
           [(str "SELECT workspace_key, COUNT(*) AS cnt
                  FROM " run-table "
                  WHERE status = 'succeeded'
                    AND finished_at_utc >= now() - interval '5 minutes'
                  GROUP BY workspace_key")])]
      (doseq [row completed-rows]
        (record-metric! {:metric-key    "throughput.runs_completed"
                         :workspace-key (:workspace_key row)
                         :value         (double (long (:cnt row)))})))

    ;; -- error_rate: failed runs in the last 5 minutes per workspace --
    (let [failed-rows
          (jdbc/execute!
           (db-opts db/ds)
           [(str "SELECT workspace_key, COUNT(*) AS cnt
                  FROM " run-table "
                  WHERE status = 'failed'
                    AND finished_at_utc >= now() - interval '5 minutes'
                  GROUP BY workspace_key")])]
      (doseq [row failed-rows]
        (record-metric! {:metric-key    "error_rate.runs_failed"
                         :workspace-key (:workspace_key row)
                         :value         (double (long (:cnt row)))})))

    ;; -- queue_depth: pending requests right now per workspace --
    (let [pending-rows
          (jdbc/execute!
           (db-opts db/ds)
           [(str "SELECT workspace_key, COUNT(*) AS cnt
                  FROM " request-table "
                  WHERE status IN ('queued', 'leased')
                  GROUP BY workspace_key")])]
      (doseq [row pending-rows]
        (record-metric! {:metric-key    "queue_depth.pending"
                         :workspace-key (:workspace_key row)
                         :value         (double (long (:cnt row)))})))

    (log/debug "record-rollup! completed successfully")
    :ok
    (catch Exception e
      (log/warn e "record-rollup! failed (some metrics may be missing)")
      :error)))

;; ---------------------------------------------------------------------------
;; cleanup-old-rollups!
;; ---------------------------------------------------------------------------

(defn cleanup-old-rollups!
  "Delete time-series rollup data older than 30 days.  Safe to call from a
   periodic maintenance job."
  []
  (ensure-timeseries-tables!)
  (try
    (let [result (jdbc/execute!
                  db/ds
                  [(str "DELETE FROM " rollup-table "
                         WHERE bucket < now() - interval '30 days'")])]
      (let [deleted (long (or (:next.jdbc/update-count (first result)) 0))]
        (when (pos? deleted)
          (log/info "cleanup-old-rollups! deleted" deleted "rows"))
        deleted))
    (catch Exception e
      (log/warn e "cleanup-old-rollups! failed")
      0)))
