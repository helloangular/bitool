(ns bitool.operations
  (:require [bitool.config :as config]
            [bitool.control-plane :as control-plane]
            [bitool.db :as db]
            [bitool.ingest.execution :as execution]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [mount.core :as mount]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

(def ^:private endpoint-freshness-table "operations_endpoint_freshness_status")
(def ^:private usage-meter-table "operations_usage_meter_daily")
(defonce ^:private operations-ready? (atom false))

(defn- db-opts
  [conn]
  (jdbc/with-options conn {:builder-fn rs/as-unqualified-lower-maps}))

(defn- now-utc []
  (java.time.Instant/now))

(defn- ->sql-timestamp
  [value]
  (when value
    (java.sql.Timestamp/from value)))

(defn- ->uuid-safe
  [value field]
  (when value
    (try
      (java.util.UUID/fromString (str value))
      (catch IllegalArgumentException e
        (log/warn e "Ignoring malformed UUID while recording endpoint freshness"
                  {:field field
                   :value (str value)})
        nil))))

(defn- parse-usage-date
  [usage-date]
  (when usage-date
    (try
      (java.sql.Date/valueOf (str usage-date))
      (catch IllegalArgumentException _
        (throw (ex-info "usage_date must be YYYY-MM-DD"
                        {:usage_date usage-date
                         :status 400})))))) 

(defn ensure-operations-tables!
  []
  (when-not @operations-ready?
    (locking operations-ready?
      (when-not @operations-ready?
        (jdbc/execute!
         db/ds
         [(str "CREATE TABLE IF NOT EXISTS " endpoint-freshness-table " ("
               "graph_id INTEGER NOT NULL, "
               "api_node_id INTEGER NOT NULL, "
               "tenant_key VARCHAR(128) NOT NULL, "
               "workspace_key VARCHAR(128) NOT NULL, "
               "source_system TEXT NOT NULL, "
               "endpoint_name TEXT NOT NULL, "
               "target_table TEXT NULL, "
               "run_id UUID NULL, "
               "status VARCHAR(32) NOT NULL, "
               "last_success_at_utc TIMESTAMPTZ NULL, "
               "last_success_run_id UUID NULL, "
               "max_watermark TEXT NULL, "
               "rows_written BIGINT NOT NULL DEFAULT 0, "
               "freshness_sla_seconds INTEGER NOT NULL DEFAULT 3600, "
               "updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
               "PRIMARY KEY (graph_id, api_node_id, endpoint_name))")])
        (jdbc/execute!
         db/ds
         [(str "CREATE INDEX IF NOT EXISTS idx_operations_endpoint_freshness_workspace "
               "ON " endpoint-freshness-table " (workspace_key, updated_at_utc DESC)")])
        (jdbc/execute!
         db/ds
         [(str "CREATE TABLE IF NOT EXISTS " usage-meter-table " ("
               "usage_date DATE NOT NULL, "
               "tenant_key VARCHAR(128) NOT NULL, "
               "workspace_key VARCHAR(128) NOT NULL, "
               "request_kind VARCHAR(32) NOT NULL, "
               "workload_class VARCHAR(64) NOT NULL, "
               "queue_partition VARCHAR(128) NOT NULL, "
               "status VARCHAR(32) NOT NULL, "
               "request_count BIGINT NOT NULL DEFAULT 0, "
               "rows_written BIGINT NOT NULL DEFAULT 0, "
               "duration_ms_total BIGINT NOT NULL DEFAULT 0, "
               "retries_total BIGINT NOT NULL DEFAULT 0, "
               "updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
               "PRIMARY KEY (usage_date, tenant_key, workspace_key, request_kind, workload_class, queue_partition, status))")])
        (jdbc/execute!
         db/ds
         [(str "CREATE INDEX IF NOT EXISTS idx_operations_usage_meter_workspace "
               "ON " usage-meter-table " (workspace_key, usage_date DESC, request_kind)")])
        (reset! operations-ready? true))))
  nil)

(defn record-endpoint-freshness!
  [{:keys [graph-id api-node-id tenant-key workspace-key source-system endpoint-name target-table
           run-id status max-watermark rows-written freshness-sla-seconds finished-at]}]
  (ensure-operations-tables!)
  (let [success? (#{"success" "partial_success"} status)
        finished-at (or finished-at (now-utc))
        run-id-uuid (->uuid-safe run-id :run_id)
        freshness-sla-seconds (max 60 (long (or freshness-sla-seconds 3600)))]
    (jdbc/execute!
     db/ds
     [(str "INSERT INTO " endpoint-freshness-table "
            (graph_id, api_node_id, tenant_key, workspace_key, source_system, endpoint_name, target_table,
             run_id, status, last_success_at_utc, last_success_run_id, max_watermark, rows_written,
             freshness_sla_seconds, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (graph_id, api_node_id, endpoint_name) DO UPDATE
            SET tenant_key = EXCLUDED.tenant_key,
                workspace_key = EXCLUDED.workspace_key,
                source_system = EXCLUDED.source_system,
                target_table = EXCLUDED.target_table,
                run_id = EXCLUDED.run_id,
                status = EXCLUDED.status,
                last_success_at_utc = COALESCE(EXCLUDED.last_success_at_utc, " endpoint-freshness-table ".last_success_at_utc),
                last_success_run_id = COALESCE(EXCLUDED.last_success_run_id, " endpoint-freshness-table ".last_success_run_id),
                max_watermark = COALESCE(EXCLUDED.max_watermark, " endpoint-freshness-table ".max_watermark),
                rows_written = EXCLUDED.rows_written,
                freshness_sla_seconds = EXCLUDED.freshness_sla_seconds,
                updated_at_utc = EXCLUDED.updated_at_utc")
      graph-id
      api-node-id
      (or tenant-key "default")
      (or workspace-key "default")
      source-system
      endpoint-name
      target-table
      run-id-uuid
      status
      (when success? (->sql-timestamp finished-at))
      (when success? run-id-uuid)
      max-watermark
      (long (or rows-written 0))
      freshness-sla-seconds
      (->sql-timestamp finished-at)])))

(defn freshness-dashboard
  ([] (freshness-dashboard {}))
  ([{:keys [graph-id workspace-key tenant-key only-alerts? limit]
     :or {limit 200}}]
   (ensure-operations-tables!)
   (let [clauses (cond-> []
                   graph-id (conj "graph_id = ?")
                   workspace-key (conj "workspace_key = ?")
                   tenant-key (conj "tenant_key = ?"))
         params (cond-> []
                  graph-id (conj graph-id)
                  workspace-key (conj workspace-key)
                  tenant-key (conj tenant-key))
         rows (jdbc/execute!
               (db-opts db/ds)
               (into [(str "SELECT *,
                                   GREATEST(0, CAST(EXTRACT(EPOCH FROM (now() - COALESCE(last_success_at_utc, updated_at_utc))) AS BIGINT)) AS freshness_lag_seconds
                            FROM " endpoint-freshness-table
                            (when (seq clauses)
                              (str " WHERE " (string/join " AND " clauses)))
                            " ORDER BY freshness_lag_seconds DESC, updated_at_utc DESC
                              LIMIT ?")]
                     (conj params (max 1 (long limit)))))
         rows' (mapv (fn [row]
                       (assoc row
                              :freshness_lag_seconds (long (:freshness_lag_seconds row))
                              :overdue? (> (long (:freshness_lag_seconds row))
                                           (long (:freshness_sla_seconds row)))))
                     rows)]
     (if only-alerts?
       (filterv :overdue? rows')
       rows'))))

(defn freshness-alerts
  ([] (freshness-alerts {}))
  ([opts]
   (freshness-dashboard (assoc opts :only-alerts? true))))

(defn record-execution-usage!
  [{:keys [tenant-key workspace-key request-kind workload-class queue-partition status rows-written
           retry-count started-at finished-at]}]
  (ensure-operations-tables!)
  (let [finished-at    (or finished-at (now-utc))
        started-at     (or started-at finished-at)
        usage-date     (java.sql.Date/valueOf (.toLocalDate (.atZone finished-at (java.time.ZoneOffset/UTC))))
        duration-ms    (max 0 (long (.toMillis (java.time.Duration/between started-at finished-at))))]
    (jdbc/execute!
     db/ds
     [(str "INSERT INTO " usage-meter-table "
            (usage_date, tenant_key, workspace_key, request_kind, workload_class, queue_partition, status,
             request_count, rows_written, duration_ms_total, retries_total, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, now())
            ON CONFLICT (usage_date, tenant_key, workspace_key, request_kind, workload_class, queue_partition, status) DO UPDATE
            SET request_count = " usage-meter-table ".request_count + 1,
                rows_written = " usage-meter-table ".rows_written + EXCLUDED.rows_written,
                duration_ms_total = " usage-meter-table ".duration_ms_total + EXCLUDED.duration_ms_total,
                retries_total = " usage-meter-table ".retries_total + EXCLUDED.retries_total,
                updated_at_utc = now()")
      usage-date
      (or tenant-key "default")
      (or workspace-key "default")
      (or request-kind "unknown")
      (or workload-class "default")
      (or queue-partition "default")
      (or status "unknown")
      (long (or rows-written 0))
      duration-ms
      (long (or retry-count 0))])))

(defn usage-dashboard
  ([] (usage-dashboard {}))
  ([{:keys [tenant-key workspace-key request-kind workload-class usage-date limit]
     :or {limit 200}}]
   (let [usage-date* (parse-usage-date usage-date)]
     (ensure-operations-tables!)
     (let [clauses (cond-> []
                     tenant-key (conj "tenant_key = ?")
                     workspace-key (conj "workspace_key = ?")
                     request-kind (conj "request_kind = ?")
                     workload-class (conj "workload_class = ?")
                     usage-date* (conj "usage_date = ?"))
           params  (cond-> []
                     tenant-key (conj tenant-key)
                     workspace-key (conj workspace-key)
                     request-kind (conj request-kind)
                     workload-class (conj workload-class)
                     usage-date* (conj usage-date*))]
       (jdbc/execute!
        (db-opts db/ds)
        (into [(str "SELECT *
                     FROM " usage-meter-table
                     (when (seq clauses)
                       (str " WHERE " (string/join " AND " clauses)))
                     " ORDER BY usage_date DESC, request_count DESC, rows_written DESC
                       LIMIT ?")]
              (conj params (max 1 (long limit)))))))))

(defn replay-execution-run!
  [run-id]
  (let [run (execution/get-execution-run run-id)]
    (when-not run
      (throw (ex-info "Execution run not found"
                      {:run_id run-id
                       :status 404})))
    (case (:request_kind run)
      "api"
      (execution/enqueue-api-request!
       (:graph_id run)
       (:node_id run)
       {:endpoint-name (:endpoint_name run)
        :trigger-type "replay"
        :request-params {:replay_source_run_id (str run-id)
                         :replay_mode "deterministic"
                         :replay_source_graph_version (:graph_version run)
                         :replay_source_status (:status run)}})

      "scheduler"
      (execution/enqueue-scheduler-request!
       (:graph_id run)
       (:node_id run)
       {:trigger-type "replay"
        :request-params {:replay_source_run_id (str run-id)}})

      (throw (ex-info "Unsupported execution run kind for replay"
                      {:run_id run-id
                       :request_kind (:request_kind run)})))))

(defn graph-lineage
  [graph-id]
  (control-plane/lineage-view graph-id))

(mount/defstate ^{:on-reload :noop} operations-metadata
  :start
  (when (or (config/enabled-role? :api)
            (config/enabled-role? :scheduler)
            (config/enabled-role? :worker))
    (ensure-operations-tables!)
    {:ready? true})
  :stop nil)
