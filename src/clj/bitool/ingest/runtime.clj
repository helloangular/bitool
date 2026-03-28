(ns bitool.ingest.runtime
  (:require [bitool.config :as config :refer [env]]
            [bitool.connector.api :as api]
            [bitool.connector.file :as file-connector]
            [bitool.connector.kafka :as kafka]
            [bitool.control-plane :as control-plane]
            [bitool.ingest.databricks-control-plane :as dbx-control]
            [bitool.databricks.jobs :as dbx-jobs]
            [bitool.db :as db]
            [bitool.graph2 :as g2]
            [bitool.ingest.bronze :as bronze]
            [bitool.ingest.checkpoint :as checkpoint]
            [bitool.ingest.grain-planner :as grain-planner]
            [bitool.ingest.schema-infer :as schema-infer]
            [bitool.operations :as operations]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [mount.core :as mount]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import [java.security MessageDigest]
           [java.math BigInteger]
           [java.nio.file Files]
           [java.util HexFormat UUID]))

(def endpoint-run-detail-columns
  [{:column_name "run_id"          :data_type "STRING" :is_nullable "NO"}
   {:column_name "source_system"   :data_type "STRING" :is_nullable "NO"}
   {:column_name "endpoint_name"   :data_type "STRING" :is_nullable "NO"}
   {:column_name "started_at_utc"  :data_type "STRING" :is_nullable "NO"}
   {:column_name "finished_at_utc" :data_type "STRING" :is_nullable "YES"}
   {:column_name "status"          :data_type "STRING" :is_nullable "NO"}
   {:column_name "http_status_code" :data_type "INT"   :is_nullable "YES"}
   {:column_name "pages_fetched"   :data_type "INT"    :is_nullable "YES"}
   {:column_name "rows_extracted"  :data_type "INT"    :is_nullable "YES"}
   {:column_name "rows_written"    :data_type "INT"    :is_nullable "YES"}
   {:column_name "retry_count"     :data_type "INT"    :is_nullable "YES"}
   {:column_name "error_summary"   :data_type "STRING" :is_nullable "YES"}])

(def batch-manifest-columns
  [{:column_name "batch_id"            :data_type "STRING"    :is_nullable "NO"}
   {:column_name "run_id"              :data_type "STRING"    :is_nullable "NO"}
   {:column_name "source_system"       :data_type "STRING"    :is_nullable "NO"}
   {:column_name "endpoint_name"       :data_type "STRING"    :is_nullable "NO"}
   {:column_name "table_name"          :data_type "STRING"    :is_nullable "NO"}
   {:column_name "batch_seq"           :data_type "INT"       :is_nullable "NO"}
   {:column_name "status"              :data_type "STRING"    :is_nullable "NO"}
   {:column_name "row_count"           :data_type "INT"       :is_nullable "NO"}
   {:column_name "bad_record_count"    :data_type "INT"       :is_nullable "NO"}
   {:column_name "byte_count"          :data_type "BIGINT"    :is_nullable "NO"}
   {:column_name "page_count"          :data_type "INT"       :is_nullable "NO"}
   {:column_name "partition_dates_json" :data_type "STRING"   :is_nullable "YES"}
   {:column_name "source_bad_record_ids_json" :data_type "STRING" :is_nullable "YES"}
   {:column_name "max_watermark"       :data_type "STRING"    :is_nullable "YES"}
   {:column_name "next_cursor"         :data_type "STRING"    :is_nullable "YES"}
   {:column_name "artifact_path"       :data_type "STRING"    :is_nullable "YES"}
   {:column_name "artifact_checksum"   :data_type "STRING"    :is_nullable "YES"}
   {:column_name "active"              :data_type "BOOLEAN"   :is_nullable "NO"}
   {:column_name "rollback_reason"     :data_type "STRING"    :is_nullable "YES"}
   {:column_name "rolled_back_by"      :data_type "STRING"    :is_nullable "YES"}
   {:column_name "rolled_back_at_utc"  :data_type "STRING"    :is_nullable "YES"}
   {:column_name "archived_at_utc"     :data_type "STRING"    :is_nullable "YES"}
   {:column_name "started_at_utc"      :data_type "STRING"    :is_nullable "NO"}
   {:column_name "committed_at_utc"    :data_type "STRING"    :is_nullable "YES"}])

(defn- ingest-summary-logging-enabled? []
  (contains? #{"true" "1" "yes" "on"}
             (some-> (get env :bitool_ingest_summary_logs) str string/lower-case)))

(def endpoint-schema-snapshot-columns
  [{:column_name "graph_id"                :data_type "INT"      :is_nullable "NO"}
   {:column_name "api_node_id"             :data_type "INT"      :is_nullable "NO"}
   {:column_name "graph_version_id"        :data_type "BIGINT"   :is_nullable "YES"}
   {:column_name "graph_version"           :data_type "INT"      :is_nullable "YES"}
   {:column_name "source_system"           :data_type "STRING"   :is_nullable "NO"}
   {:column_name "endpoint_name"           :data_type "STRING"   :is_nullable "NO"}
   {:column_name "schema_mode"             :data_type "STRING"   :is_nullable "NO"}
   {:column_name "schema_enforcement_mode" :data_type "STRING"   :is_nullable "NO"}
   {:column_name "sample_record_count"     :data_type "INT"      :is_nullable "NO"}
   {:column_name "inferred_fields_json"    :data_type "STRING"   :is_nullable "NO"}
   {:column_name "schema_drift_json"       :data_type "STRING"   :is_nullable "YES"}
   {:column_name "captured_at_utc"         :data_type "STRING"   :is_nullable "NO"}])

(def endpoint-schema-approval-columns
  [{:column_name "graph_id"                :data_type "INT"      :is_nullable "NO"}
   {:column_name "api_node_id"             :data_type "INT"      :is_nullable "NO"}
   {:column_name "endpoint_name"           :data_type "STRING"   :is_nullable "NO"}
   {:column_name "schema_hash"             :data_type "STRING"   :is_nullable "NO"}
   {:column_name "review_state"            :data_type "STRING"   :is_nullable "NO"}
   {:column_name "review_notes"            :data_type "STRING"   :is_nullable "YES"}
   {:column_name "reviewed_by"             :data_type "STRING"   :is_nullable "YES"}
   {:column_name "reviewed_at_utc"         :data_type "STRING"   :is_nullable "NO"}
   {:column_name "promoted"                :data_type "BOOLEAN"  :is_nullable "NO"}
   {:column_name "promoted_at_utc"         :data_type "STRING"   :is_nullable "YES"}
   {:column_name "inferred_fields_json"    :data_type "STRING"   :is_nullable "NO"}
   {:column_name "field_decisions"         :data_type "STRING"   :is_nullable "YES"}])

(def ^:private artifact-store-table "ingest_batch_artifact_store")
(def ^:private local-table-confirmation-cache-table "ingest_table_confirmation_cache")
(def ^:private local-checkpoint-cache-table "ingest_checkpoint_row_cache")
(def ^:private local-schema-snapshot-cache-table "ingest_schema_snapshot_cache")
(defonce ^:private artifact-store-ready? (atom false))
(defonce ^:private local-ingest-cache-ready? (atom false))
(defonce ^:private manifest-columns-ready? (atom #{}))
(defonce ^:private checkpoint-columns-ready? (atom #{}))
(defonce ^:private bad-record-columns-ready? (atom #{}))
(defonce ^:private schema-approval-columns-ready? (atom #{}))
(defonce ^:private checkpoint-row-cache (atom {}))
(defonce ^:private schema-snapshot-latest-cache (atom {}))
(defonce ^:private adaptive-backpressure-state (atom {}))
(defonce ^:private source-circuit-breaker-state (atom {}))

(declare schema-enforcement-mode
         schema-snapshot-tracking-required?
         replay-endpoint-config-hash
         artifact-endpoint-config
         safe-path-segment
         non-blank-str
         validated-qualified-table-name
         cache-local-checkpoint-row!
         delete-local-checkpoint-row!
         cache-local-latest-schema-snapshot-row!
         delete-local-latest-schema-snapshot-row!
         null-backfill-needed?
         db-opts
         sql-opts
         connection-dbtype
         find-downstream-target
         abort-preparing-batches!
         mark-manifest-row!
         apply-api-retention!
         flush-bad-record-replay-batch!
         process-source-stream!)

(defn- now-utc [] (java.time.Instant/now))

(defn- checkpoint-cache-key
  [conn-id table-name source-system endpoint-name]
  [conn-id
   (validated-qualified-table-name table-name)
   (str source-system)
   (str endpoint-name)])

(defn- cache-checkpoint-row!
  [conn-id table-name source-system endpoint-name row]
  (swap! checkpoint-row-cache assoc
         (checkpoint-cache-key conn-id table-name source-system endpoint-name)
         row)
  (cache-local-checkpoint-row! conn-id table-name source-system endpoint-name row)
  row)

(defn- invalidate-checkpoint-row-cache!
  [conn-id table-name source-system endpoint-name]
  (swap! checkpoint-row-cache dissoc
         (checkpoint-cache-key conn-id table-name source-system endpoint-name))
  (when (and source-system endpoint-name)
    (delete-local-checkpoint-row! conn-id table-name source-system endpoint-name))
  nil)

(defn- schema-snapshot-cache-key
  [conn-id schema-snapshot-table graph-id api-node-id endpoint-name]
  [conn-id
   (validated-qualified-table-name schema-snapshot-table)
   graph-id
   api-node-id
   (str endpoint-name)])

(defn- cache-latest-schema-snapshot-row!
  [conn-id schema-snapshot-table graph-id api-node-id endpoint-name row]
  (swap! schema-snapshot-latest-cache assoc
         (schema-snapshot-cache-key conn-id schema-snapshot-table graph-id api-node-id endpoint-name)
         row)
  (cache-local-latest-schema-snapshot-row! conn-id schema-snapshot-table graph-id api-node-id endpoint-name row)
  row)

(defn- invalidate-latest-schema-snapshot-row-cache!
  [conn-id schema-snapshot-table graph-id api-node-id endpoint-name]
  (swap! schema-snapshot-latest-cache dissoc
         (schema-snapshot-cache-key conn-id schema-snapshot-table graph-id api-node-id endpoint-name))
  (delete-local-latest-schema-snapshot-row! conn-id schema-snapshot-table graph-id api-node-id endpoint-name)
  nil)

(defn- ensure-local-ingest-cache-tables!
  []
  (when-not @local-ingest-cache-ready?
    (locking local-ingest-cache-ready?
      (when-not @local-ingest-cache-ready?
        (doseq [sql-str
                [(str "CREATE TABLE IF NOT EXISTS " local-table-confirmation-cache-table " ("
                      "conn_id INTEGER NOT NULL, "
                      "table_name TEXT NOT NULL, "
                      "confirmed_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                      "PRIMARY KEY (conn_id, table_name))")
                 (str "CREATE TABLE IF NOT EXISTS " local-checkpoint-cache-table " ("
                      "conn_id INTEGER NOT NULL, "
                      "table_name TEXT NOT NULL, "
                      "source_system TEXT NOT NULL, "
                      "endpoint_name TEXT NOT NULL, "
                      "row_json TEXT NOT NULL, "
                      "updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                      "PRIMARY KEY (conn_id, table_name, source_system, endpoint_name))")
                 (str "CREATE TABLE IF NOT EXISTS " local-schema-snapshot-cache-table " ("
                      "conn_id INTEGER NOT NULL, "
                      "table_name TEXT NOT NULL, "
                      "graph_id INTEGER NOT NULL, "
                      "api_node_id INTEGER NOT NULL, "
                      "endpoint_name TEXT NOT NULL, "
                      "row_json TEXT NOT NULL, "
                      "updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                      "PRIMARY KEY (conn_id, table_name, graph_id, api_node_id, endpoint_name))")]]
          (jdbc/execute! db/ds [sql-str]))
        (reset! local-ingest-cache-ready? true))))
  nil)

(defn- local-table-confirmed?
  [conn-id table-name]
  (ensure-local-ingest-cache-tables!)
  (some?
   (jdbc/execute-one!
    (db-opts db/ds)
    [(str "SELECT 1 AS present FROM " local-table-confirmation-cache-table
          " WHERE conn_id = ? AND table_name = ?")
     conn-id
     (validated-qualified-table-name table-name)])))

(defn- cache-local-table-confirmed!
  [conn-id table-name]
  (ensure-local-ingest-cache-tables!)
  (jdbc/execute!
   (db-opts db/ds)
   [(str "INSERT INTO " local-table-confirmation-cache-table
         " (conn_id, table_name, confirmed_at_utc) VALUES (?, ?, now()) "
         "ON CONFLICT (conn_id, table_name) DO UPDATE SET confirmed_at_utc = excluded.confirmed_at_utc")
    conn-id
    (validated-qualified-table-name table-name)])
  nil)

(defn- delete-local-table-confirmed!
  [conn-id table-name]
  (ensure-local-ingest-cache-tables!)
  (jdbc/execute!
   (db-opts db/ds)
   [(str "DELETE FROM " local-table-confirmation-cache-table
         " WHERE conn_id = ? AND table_name = ?")
    conn-id
    (validated-qualified-table-name table-name)])
  nil)

(defn- local-checkpoint-row
  [conn-id table-name source-system endpoint-name]
  (ensure-local-ingest-cache-tables!)
  (some-> (jdbc/execute-one!
           (db-opts db/ds)
           [(str "SELECT row_json FROM " local-checkpoint-cache-table
                 " WHERE conn_id = ? AND table_name = ? AND source_system = ? AND endpoint_name = ?")
            conn-id
            (validated-qualified-table-name table-name)
            (str source-system)
            (str endpoint-name)])
          :row_json
          (json/parse-string true)))

(defn- cache-local-checkpoint-row!
  [conn-id table-name source-system endpoint-name row]
  (when row
    (ensure-local-ingest-cache-tables!)
    (jdbc/execute!
     (db-opts db/ds)
     [(str "INSERT INTO " local-checkpoint-cache-table
           " (conn_id, table_name, source_system, endpoint_name, row_json, updated_at_utc) "
           "VALUES (?, ?, ?, ?, ?, now()) "
           "ON CONFLICT (conn_id, table_name, source_system, endpoint_name) DO UPDATE "
           "SET row_json = excluded.row_json, updated_at_utc = excluded.updated_at_utc")
      conn-id
      (validated-qualified-table-name table-name)
      (str source-system)
      (str endpoint-name)
      (json/generate-string row)]))
  row)

(defn- delete-local-checkpoint-row!
  [conn-id table-name source-system endpoint-name]
  (ensure-local-ingest-cache-tables!)
  (jdbc/execute!
   (db-opts db/ds)
   [(str "DELETE FROM " local-checkpoint-cache-table
         " WHERE conn_id = ? AND table_name = ? AND source_system = ? AND endpoint_name = ?")
    conn-id
    (validated-qualified-table-name table-name)
    (str source-system)
    (str endpoint-name)])
  nil)

(defn- local-latest-schema-snapshot-row
  [conn-id table-name graph-id api-node-id endpoint-name]
  (ensure-local-ingest-cache-tables!)
  (some-> (jdbc/execute-one!
           (db-opts db/ds)
           [(str "SELECT row_json FROM " local-schema-snapshot-cache-table
                 " WHERE conn_id = ? AND table_name = ? AND graph_id = ? AND api_node_id = ? AND endpoint_name = ?")
            conn-id
            (validated-qualified-table-name table-name)
            graph-id
            api-node-id
            (str endpoint-name)])
          :row_json
          (json/parse-string true)))

(defn- cache-local-latest-schema-snapshot-row!
  [conn-id table-name graph-id api-node-id endpoint-name row]
  (when row
    (ensure-local-ingest-cache-tables!)
    (jdbc/execute!
     (db-opts db/ds)
     [(str "INSERT INTO " local-schema-snapshot-cache-table
           " (conn_id, table_name, graph_id, api_node_id, endpoint_name, row_json, updated_at_utc) "
           "VALUES (?, ?, ?, ?, ?, ?, now()) "
           "ON CONFLICT (conn_id, table_name, graph_id, api_node_id, endpoint_name) DO UPDATE "
           "SET row_json = excluded.row_json, updated_at_utc = excluded.updated_at_utc")
      conn-id
      (validated-qualified-table-name table-name)
      graph-id
      api-node-id
      (str endpoint-name)
      (json/generate-string row)]))
  row)

(defn- delete-local-latest-schema-snapshot-row!
  [conn-id table-name graph-id api-node-id endpoint-name]
  (ensure-local-ingest-cache-tables!)
  (jdbc/execute!
   (db-opts db/ds)
   [(str "DELETE FROM " local-schema-snapshot-cache-table
         " WHERE conn_id = ? AND table_name = ? AND graph_id = ? AND api_node_id = ? AND endpoint_name = ?")
    conn-id
    (validated-qualified-table-name table-name)
    graph-id
    api-node-id
    (str endpoint-name)])
  nil)

(defn- parse-int-env
  [k default-value]
  (try
    (if-some [value (get env k)]
      (Integer/parseInt (str value))
      default-value)
    (catch Exception _
      default-value)))

(defn- parse-bool-env
  [k default-value]
  (let [raw (get env k)]
    (if (nil? raw)
      default-value
      (contains? #{"1" "true" "yes" "on"}
                 (-> raw str string/trim string/lower-case)))))

(defn- parse-double-env
  [k default-value]
  (try
    (if-some [value (get env k)]
      (Double/parseDouble (str value))
      default-value)
    (catch Exception _
      default-value)))

(defn- databricks-target?
  "Returns true if the target connection is Databricks."
  [conn-id]
  (= "databricks" (connection-dbtype conn-id)))

(defn- databricks-query-int
  [conn-id value]
  (let [value (long value)]
    (if (databricks-target? conn-id)
      (int value)
      value)))

(defn- databricks-col-type
  "Converts Postgres column type syntax to Databricks-compatible syntax."
  [col-type]
  (-> col-type
      (string/replace #"(?i)TIMESTAMPTZ" "TIMESTAMP")
      (string/replace #"(?i)VARCHAR\(\d+\)" "STRING")
      (string/replace #"(?i)\s+NOT NULL\s+DEFAULT\s+.*" "")
      (string/replace #"(?i)\s+NULL\b" "")
      (string/replace #"(?i)\bTEXT\b" "STRING")))

(defn- add-column-ddl
  "Generates ALTER TABLE ADD COLUMN DDL, dialect-aware.
   Databricks does not support IF NOT EXISTS on ALTER TABLE ADD COLUMN."
  [table col-name col-type conn-id]
  (if (databricks-target? conn-id)
    (str "ALTER TABLE " table " ADD COLUMN " col-name " " (databricks-col-type col-type))
    (str "ALTER TABLE " table " ADD COLUMN IF NOT EXISTS " col-name " " col-type)))

(defn- now-nanos []
  (System/nanoTime))

(defn- elapsed-ms
  [started-nanos]
  (long (/ (- (System/nanoTime) started-nanos) 1000000)))

(defn- merge-timing-maps
  [& timing-maps]
  (apply merge-with
         (fn [a b] (+ (long (or a 0)) (long (or b 0))))
         (filter seq timing-maps)))

(defn- exec-add-column!
  "Executes an ALTER TABLE ADD COLUMN, silently ignoring 'column already exists' on Databricks."
  [conn-id table col-name col-type]
  (let [ddl (add-column-ddl table col-name col-type conn-id)]
    (try
      (jdbc/execute! (sql-opts conn-id) [ddl])
      (catch java.sql.SQLException e
        (if (and (databricks-target? conn-id)
                 (re-find #"(?i)already exists|COLUMN_ALREADY_EXISTS" (.getMessage e)))
          nil ;; column already exists, safe to ignore
          (throw e))))))

(defn- ensure-artifact-store-table!
  []
  (when-not @artifact-store-ready?
    (locking artifact-store-ready?
      (when-not @artifact-store-ready?
        (doseq [sql-str
                [(str "CREATE TABLE IF NOT EXISTS " artifact-store-table " ("
                      "artifact_id BIGSERIAL PRIMARY KEY, "
                      "artifact_path TEXT NOT NULL UNIQUE, "
                      "artifact_kind VARCHAR(64) NOT NULL, "
                      "run_id TEXT NOT NULL, "
                      "source_system TEXT NOT NULL, "
                 "endpoint_name TEXT NOT NULL, "
                 "batch_id TEXT NOT NULL, "
                 "payload_json TEXT NOT NULL, "
                 "artifact_checksum VARCHAR(128) NOT NULL, "
                 "archived_at_utc TIMESTAMPTZ NULL, "
                 "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                 (str "CREATE INDEX IF NOT EXISTS idx_ingest_batch_artifact_store_run_batch "
                      "ON " artifact-store-table " (run_id, endpoint_name, batch_id, created_at_utc DESC)")
                 (str "ALTER TABLE " artifact-store-table
                      " ADD COLUMN IF NOT EXISTS archived_at_utc TIMESTAMPTZ NULL")]]
          (jdbc/execute! db/ds [sql-str]))
        (reset! artifact-store-ready? true))))
  nil)

(defn- ensure-batch-manifest-columns!
  [conn-id manifest-table]
  (let [manifest-table (validated-qualified-table-name manifest-table)
        key [conn-id manifest-table]]
    (when-not (contains? @manifest-columns-ready? key)
      (locking manifest-columns-ready?
        (when-not (contains? @manifest-columns-ready? key)
          (try
            (doseq [[col-name col-type] [["active" "BOOLEAN NOT NULL DEFAULT TRUE"]
                                       ["rollback_reason" "TEXT NULL"]
                                       ["rolled_back_by" "TEXT NULL"]
                                       ["rolled_back_at_utc" "TIMESTAMPTZ NULL"]
                                       ["archived_at_utc" "TIMESTAMPTZ NULL"]
                                       ["source_bad_record_ids_json" "TEXT NULL"]]]
              (exec-add-column! conn-id manifest-table col-name col-type))
            (when-not (databricks-target? conn-id)
              (jdbc/execute! (sql-opts conn-id)
                [(str "CREATE INDEX IF NOT EXISTS "
                      (safe-path-segment (string/replace (str "idx_" manifest-table "_status_active") "." "_"))
                      " ON " manifest-table " (endpoint_name, status, active, committed_at_utc DESC)")]))
            (swap! manifest-columns-ready? conj key)
            (catch Exception e
              (throw (ex-info "Failed to migrate batch manifest columns"
                              {:conn_id conn-id
                               :manifest_table manifest-table
                               :failure_class "manifest_migration"}
                              e)))))))
    nil))

(defn- ensure-checkpoint-columns!
  [conn-id checkpoint-table]
  (let [checkpoint-table (validated-qualified-table-name checkpoint-table)
        key [conn-id checkpoint-table]]
    (when-not (contains? @checkpoint-columns-ready? key)
      (locking checkpoint-columns-ready?
        (when-not (contains? @checkpoint-columns-ready? key)
          (try
            (doseq [[col-name col-type] [["last_successful_batch_id" "TEXT NULL"]
                                       ["last_successful_batch_seq" "INT NULL"]]]
              (exec-add-column! conn-id checkpoint-table col-name col-type))
            (swap! checkpoint-columns-ready? conj key)
            (catch Exception e
              (throw (ex-info "Failed to migrate ingestion checkpoint columns"
                              {:conn_id conn-id
                               :checkpoint_table checkpoint-table
                               :failure_class "checkpoint_migration"}
                              e)))))))
    nil))

(defn- ensure-bad-record-columns!
  [conn-id bad-records-table]
  (let [bad-records-table (validated-qualified-table-name bad-records-table)
        key [conn-id bad-records-table]]
    (when-not (contains? @bad-record-columns-ready? key)
      (locking bad-record-columns-ready?
        (when-not (contains? @bad-record-columns-ready? key)
          (try
            (doseq [[col-name col-type] [["bad_record_id" "TEXT NULL"]
                                       ["row_json" "TEXT NULL"]
                                       ["replay_status" "VARCHAR(32) NULL"]
                                       ["replayed_run_id" "TEXT NULL"]
                                       ["replayed_at_utc" "TIMESTAMPTZ NULL"]
                                       ["replay_error_message" "TEXT NULL"]
                                       ["payload_archive_ref" "TEXT NULL"]
                                       ["payload_archived_at_utc" "TIMESTAMPTZ NULL"]]]
              (exec-add-column! conn-id bad-records-table col-name col-type))
            (let [bad-record-id-update
                  (when (null-backfill-needed? conn-id bad-records-table "bad_record_id")
                    (if (databricks-target? conn-id)
                      (str "UPDATE " bad-records-table
                           " SET bad_record_id = COALESCE(bad_record_id, md5(CONCAT(COALESCE(payload_json, ''), COALESCE(CAST(created_at_utc AS STRING), ''), COALESCE(error_message, ''))))")
                      (str "UPDATE " bad-records-table
                           " SET bad_record_id = COALESCE(bad_record_id, md5(COALESCE(payload_json, '') || COALESCE(created_at_utc, '') || COALESCE(error_message, '')))")))
                  replay-status-update
                  (when (null-backfill-needed? conn-id bad-records-table "replay_status")
                    (str "UPDATE " bad-records-table
                         " SET replay_status = COALESCE(replay_status, 'pending')"))
                  update-stmts (cond-> []
                                 bad-record-id-update (conj bad-record-id-update)
                                 replay-status-update (conj replay-status-update))]
              (doseq [sql-str update-stmts]
                (jdbc/execute! (sql-opts conn-id) [sql-str])))
            (when-not (databricks-target? conn-id)
              (doseq [sql-str
                      [(str "CREATE INDEX IF NOT EXISTS "
                            (safe-path-segment (string/replace (str "idx_" bad-records-table "_replay") "." "_"))
                            " ON " bad-records-table " (endpoint_name, replay_status, created_at_utc DESC)")
                       (str "CREATE INDEX IF NOT EXISTS "
                            (safe-path-segment (string/replace (str "idx_" bad-records-table "_retention") "." "_"))
                            " ON " bad-records-table " (endpoint_name, created_at_utc ASC)")]]
                (jdbc/execute! (sql-opts conn-id) [sql-str])))
            (swap! bad-record-columns-ready? conj key)
            (catch Exception e
              (throw (ex-info "Failed to migrate bad record columns"
                              {:conn_id conn-id
                               :bad_records_table bad-records-table
                               :failure_class "bad_record_migration"}
                              e)))))))
    nil))

(defn- ensure-schema-approval-columns!
  [conn-id schema-approval-table]
  (let [schema-approval-table (validated-qualified-table-name schema-approval-table)
        key [conn-id schema-approval-table]]
    (when-not (contains? @schema-approval-columns-ready? key)
      (locking schema-approval-columns-ready?
        (when-not (contains? @schema-approval-columns-ready? key)
          (swap! schema-approval-columns-ready? conj key))))
    nil))

(defn- sha256-hex
  [value]
  (let [digest (.digest (doto (MessageDigest/getInstance "SHA-256")
                          (.update (.getBytes (str value) "UTF-8"))))]
    (.formatHex (HexFormat/of) digest)))

(defn- canonicalize-json-value
  [value]
  (cond
    (map? value)
    (into (sorted-map)
          (map (fn [[k v]]
                 [(str k) (canonicalize-json-value v)]))
          value)

    (vector? value)
    (mapv canonicalize-json-value value)

    (sequential? value)
    (mapv canonicalize-json-value value)

    (set? value)
    (->> value
         (map canonicalize-json-value)
         sort
         vec)

    :else value))

(defn- stable-json-hash
  [value]
  (sha256-hex (json/generate-string (canonicalize-json-value value))))

(defn- connection-dbtype [conn-id]
  (some-> conn-id db/create-dbspec-from-id :dbtype))

(defn- audit-table
  [target conn-id table-name]
  (db/fully-qualified-table-name
    {:catalog (when (= "databricks" (connection-dbtype conn-id))
                (or (:catalog target) "sheetz_telematics"))
     :schema  (or (:audit_schema target) "audit")}
    table-name))

(defn- auto-endpoint-table-name
  [endpoint]
  (let [seed (or (non-blank-str (:endpoint_name endpoint))
                 (non-blank-str (:endpoint_url endpoint))
                 (non-blank-str (:topic_name endpoint))
                 (non-blank-str (:path endpoint))
                 "bronze_auto")
        sanitized (-> seed
                      string/lower-case
                      (string/replace #"[^a-z0-9_]+" "_")
                      (string/replace #"^_+|_+$" ""))]
    (cond
      (string/blank? sanitized) "bronze_auto"
      (re-matches #"^[a-z_].*" sanitized) sanitized
      :else (str "t_" sanitized))))

(defn- endpoint->table-name [target endpoint]
  (or (non-blank-str (:bronze_table_name endpoint))
      (when-let [target-table (non-blank-str (:table_name target))]
        (db/fully-qualified-table-name target target-table))
      (db/fully-qualified-table-name target (auto-endpoint-table-name endpoint))))

(defn- target-connection-id [target]
  (let [raw (or (:connection_id target) (:c target) (:connection target))]
    (cond
      (integer? raw) raw
      (string? raw) (let [trimmed (string/trim raw)]
                      (cond
                        (string/blank? trimmed) nil
                        (re-matches #"\d+" trimmed) (Integer/parseInt trimmed)
                        :else nil))
      (number? raw) (int raw)
      :else nil)))

(defn- resolved-target-connection-id
  [target]
  (let [conn-id (target-connection-id target)]
    (when (and conn-id (connection-dbtype conn-id))
      conn-id)))

(defn- require-target-connection-id!
  [target message ex-data]
  (if-let [conn-id (resolved-target-connection-id target)]
    conn-id
    (throw (ex-info message
                    (merge ex-data
                           (when-let [configured-connection-id (target-connection-id target)]
                             {:configured_connection_id configured-connection-id}))))))

(defn- target-partition-columns
  [conn-id target endpoint]
  (let [dbtype (some-> (connection-dbtype conn-id) string/lower-case)
        configured (->> (or (:partition_columns endpoint)
                            (:partition_columns target))
                        (map non-blank-str)
                        (remove nil?)
                        distinct
                        vec)]
    (cond
      (= dbtype "databricks")
      (let [columns (if (seq configured) configured ["partition_date"])]
        (when-not (some #{"partition_date"} columns)
          (throw (ex-info "Databricks Bronze partition policy requires partition_date"
                          {:failure_class "config_error"
                           :partition_columns columns
                           :endpoint_name (:endpoint_name endpoint)})))
        columns)

      (contains? #{"postgresql" "mysql"} dbtype)
      []

      :else
      (if (seq configured) configured ["partition_date"]))))

(defn- enabled-endpoints
  [api-node]
  (->> (:endpoint_configs api-node)
       (filter #(not= false (:enabled %)))))

(defn- enabled-source-configs
  [source-node config-key]
  (->> (get source-node config-key)
       (filter #(not= false (:enabled %)))))

(defn- select-source-configs!
  [source-node config-key endpoint-name empty-message]
  (let [configs (vec (enabled-source-configs source-node config-key))]
    (cond
      (and endpoint-name
           (some #(= endpoint-name (:endpoint_name %)) configs))
      [(first (filter #(= endpoint-name (:endpoint_name %)) configs))]

      endpoint-name
      (throw (ex-info (str "No enabled source config found for endpoint_name '" endpoint-name "'")
                      {:endpoint_name endpoint-name
                       :status 404}))

      (seq configs)
      configs

      :else
      (throw (ex-info empty-message {:status 409})))))

(defn- select-source-config!
  [source-node config-key endpoint-name empty-message source-label]
  (let [configs (vec (enabled-source-configs source-node config-key))]
    (cond
      (and endpoint-name
           (some #(= endpoint-name (:endpoint_name %)) configs))
      (first (filter #(= endpoint-name (:endpoint_name %)) configs))

      endpoint-name
      (throw (ex-info (str "No enabled source config found for endpoint_name '" endpoint-name "'")
                      {:endpoint_name endpoint-name
                       :status 404}))

      (= 1 (count configs))
      (first configs)

      (empty? configs)
      (throw (ex-info empty-message {:status 409}))

      :else
      (throw (ex-info (str "endpoint_name is required when a " source-label " node has multiple enabled source configs")
                      {:endpoint_names (mapv :endpoint_name configs)
                       :status 409})))))

(defn- parse-json-cursor
  [cursor]
  (try
    (when (seq (non-blank-str cursor))
      ;; Cursor payloads use dynamic keys such as file paths and partition ids,
      ;; so preserving string keys keeps checkpoint lookups stable.
      (json/parse-string cursor))
    (catch Exception _
      nil)))

(defn- select-endpoint!
  [api-node endpoint-name]
  (let [endpoints (vec (enabled-endpoints api-node))]
    (cond
      (and endpoint-name
           (some #(= endpoint-name (:endpoint_name %)) endpoints))
      (first (filter #(= endpoint-name (:endpoint_name %)) endpoints))

      endpoint-name
      (throw (ex-info (str "No enabled endpoint config found for endpoint_name '" endpoint-name "'")
                      {:endpoint_name endpoint-name}))

      (= 1 (count endpoints))
      (first endpoints)

      (empty? endpoints)
      (throw (ex-info "API node has no enabled endpoint configs"
                      {:api_node_id (:id api-node)}))

      :else
      (throw (ex-info "endpoint_name is required when an API node has multiple enabled endpoints"
                      {:endpoint_names (mapv :endpoint_name endpoints)})))))

(defn- non-blank-str [v]
  (let [s (some-> v str string/trim)]
    (when (seq s) s)))

(defn- truthy-db-bool?
  [value]
  (cond
    (true? value) true
    (false? value) false
    (number? value) (not (zero? (long value)))
    :else (contains? #{"true" "t" "1" "yes" "on"}
                     (some-> value str string/trim string/lower-case))))

(def ^:private sql-identifier-pattern #"^[A-Za-z_][A-Za-z0-9_]*$")

(defn- validated-sql-identifier [identifier]
  (let [identifier (cond
                     (keyword? identifier) (name identifier)
                     :else (non-blank-str identifier))]
    (when-not (and identifier (re-matches sql-identifier-pattern identifier))
      (throw (ex-info "Column name must be a valid identifier"
                      {:column_name identifier})))
    identifier))

(defn- validated-qualified-table-name [table-name]
  (let [raw table-name
        table-name (non-blank-str raw)]
    (when-not table-name
      (throw (ex-info "Table name must not be blank"
                      {:table_name raw})))
    (let [parts (string/split table-name #"\.")]
      (when (or (> (count parts) 3)
                (some #(not (re-matches sql-identifier-pattern %)) parts))
        (throw (ex-info "Table name must be a valid qualified identifier"
                        {:table_name table-name}))))
    table-name))

(defn- null-backfill-needed?
  [conn-id table-name column-name]
  (let [table-name  (validated-qualified-table-name table-name)
        column-name (validated-sql-identifier column-name)
        row         (jdbc/execute-one!
                     (sql-opts conn-id)
                     [(str "SELECT 1 AS needs_backfill FROM " table-name
                           " WHERE " column-name " IS NULL LIMIT 1")])]
    (boolean row)))

(defn- join-url [base path]
  (let [base (str base)
        path (str path)]
    (if (or (string/ends-with? base "/")
            (string/starts-with? path "/"))
      (str base path)
      (str base "/" path))))

(defn- resolve-secret-ref
  ([secret-ref] (resolve-secret-ref secret-ref "env"))
  ([secret-ref secret-backend]
  (let [raw secret-ref
        secret-ref (non-blank-str raw)]
    (when-not secret-ref
      (throw (ex-info "Secret reference must not be blank"
                      {:secret_ref raw})))
    (let [secret-backend (-> (or secret-backend "env") str string/trim string/lower-case)
          secret-val     (if (= secret-backend "db")
                           (control-plane/resolve-managed-secret secret-ref)
                           (let [env-name (-> secret-ref
                                              str
                                              string/upper-case
                                              (string/replace #"[^A-Z0-9]+" "_"))]
                             (or (System/getenv env-name)
                                 (throw (ex-info "Secret reference could not be resolved from environment"
                                                 {:secret_ref secret-ref
                                                  :env_name env-name})))))]
      (when-not secret-val
        (throw (ex-info "Secret reference could not be resolved from managed secret store"
                        {:secret_ref secret-ref
                         :secret_backend secret-backend})))
      secret-val))))

(defn- resolve-auth-ref [auth-ref]
  (let [auth-ref   (or auth-ref {})
        auth-type  (some-> (:type auth-ref) str string/trim string/lower-case)
        secret-ref (non-blank-str (:secret_ref auth-ref))
        secret-val (when secret-ref
                     (resolve-secret-ref secret-ref (:secret_backend auth-ref)))]
    (cond
      (= auth-type "bearer")
      {:type :bearer :token (or (:token auth-ref) secret-val)}

      (= auth-type "api-key")
      {:type :api-key
       :key (or (:key auth-ref) secret-val)
       :location (keyword (or (:location auth-ref) "header"))
       :param-name (or (:param_name auth-ref) "api_key")
       :header-name (or (:header_name auth-ref) "X-API-Key")}

      ;; No auth type specified — return nil so merge-auth skips auth
      :else nil)))

(def ^:dynamic *batch-sql-opts* nil)
(def ^:dynamic *row-load-context* nil)

(defn- pagination-config [endpoint]
  (let [strategy    (:pagination_strategy endpoint)
        base-query  (or (:query_params endpoint) {})
        base-body   (or (:body_params endpoint) {})
        page-size   (or (:page_size endpoint) 100)]
    (case strategy
      "none"
      {:pagination    :offset
       :query-builder base-query
       :body-builder  base-body
       :initial-state {:offset 0 :limit page-size :is-last true}
       :state-key-map {}
       :out-key-map  {}}

      "page"
      {:pagination    :page
       :query-builder base-query
       :body-builder  base-body
       :initial-state {:page 1 :page-size page-size}
       :state-key-map {(keyword (or (:page_field endpoint) "page")) :page
                       (keyword (or (:total_pages_field endpoint) "total_pages")) :total-pages}
       :out-key-map  {:page (keyword (or (:page_param endpoint) "page"))
                      :page-size (keyword (or (:size_param endpoint) "page_size"))}}

      "cursor"
      {:pagination   :cursor
       :query-builder base-query
       :body-builder  base-body
       :initial-state {}
       :state-key-map {(keyword (:cursor_field endpoint)) :last-cursor}
       :out-key-map  {:cursor (keyword (or (:cursor_param endpoint) "cursor"))}}

      "token"
      {:pagination    :token
       :query-builder base-query
       :body-builder  base-body
       :initial-state {}
       :state-key-map {(keyword (or (:token_field endpoint) "nextPageToken")) :next-token}
       :out-key-map  {:page_token (keyword (or (:token_param endpoint) "page_token"))}}

      "time"
      {:pagination    :time
       :query-builder base-query
       :body-builder  base-body
       :initial-state {:window-size (* 60 (or (:time_window_minutes endpoint) 60))}
       :state-key-map {(keyword (or (:time_field endpoint) (:watermark_column endpoint))) :last-updated
                       (keyword (or (:window_size_param endpoint) "window_size")) :window-size}
       :out-key-map  {:updated_since (keyword (or (:time_param endpoint) "updated_since"))
                      :window-size (keyword (or (:window_size_param endpoint) "window_size"))}}

      "link-header"
      {:pagination    :link-header
       :query-builder base-query
       :body-builder  base-body
       :initial-state {}}

      "offset"
      {:pagination    :offset
       :query-builder (merge base-query {(keyword (or (:offset_param endpoint) "offset")) 0
                                         (keyword (or (:limit_param endpoint) "limit")) page-size})
       :body-builder  base-body
       :initial-state {:offset 0 :limit page-size}
       :state-key-map {(keyword (or (:offset_field endpoint) "offset")) :offset
                       (keyword (or (:limit_field endpoint) "limit")) :limit
                       (keyword (or (:total_field endpoint) "total")) :total
                       (keyword (or (:is_last_field endpoint) "isLast")) :is-last}
       :out-key-map  {:offset (keyword (or (:offset_param endpoint) "offset"))
                      :limit  (keyword (or (:limit_param endpoint) "limit"))}}

      (throw (ex-info (str "Unsupported pagination strategy for ingestion runtime: " strategy)
                      {:field :pagination_strategy
                       :value strategy})))))

(defn- checkpoint-cursor-query-params [checkpoint-row endpoint]
  (if (= "cursor" (:pagination_strategy endpoint))
    (if-let [cursor (non-blank-str (:last_successful_cursor checkpoint-row))]
      {(keyword (or (:cursor_param endpoint) "cursor")) cursor}
      {})
    {}))

(defn- checkpoint-cursor-initial-state [checkpoint-row endpoint]
  (if (= "cursor" (:pagination_strategy endpoint))
    (if-let [cursor (non-blank-str (:last_successful_cursor checkpoint-row))]
      {:cursor cursor :last-cursor cursor}
      {})
    {}))

(defn- checkpoint-next-cursor [page-result]
  (or (some-> page-result :final-state :last-cursor non-blank-str)
      (some-> page-result :final-state :cursor non-blank-str)))

(defn- run-status [errors pages-fetched]
  (cond
    (and (seq errors) (zero? pages-fetched)) "failed"
    (seq errors) "partial_success"
    :else "success"))

(defn- endpoint-base-per-page-ms
  [endpoint]
  (long (max 0 (or (some-> endpoint :rate_limit :per-page-ms)
                   (:rate_limit_per_page_ms endpoint)
                   (:rate_limit_per_page_ms (or (:rate_limit endpoint) {}))
                   (parse-int-env :ingest-default-per-page-ms 0)))))

(defn- endpoint-adaptive-key
  [source-system endpoint-name]
  (str (or source-system "api") "::" endpoint-name))

(defn- endpoint-circuit-breaker-key
  [source-system endpoint]
  (endpoint-adaptive-key source-system (:endpoint_name endpoint)))

(defn- endpoint-circuit-breaker-config
  [endpoint]
  {:enabled? (if (contains? endpoint :circuit_breaker_enabled)
               (boolean (:circuit_breaker_enabled endpoint))
               (parse-bool-env :ingest-circuit-breaker-enabled true))
   :failure-threshold (max 1 (long (or (:circuit_breaker_failure_threshold endpoint)
                                       (parse-int-env :ingest-circuit-breaker-failure-threshold 5))))
   :window-seconds (max 1 (long (or (:circuit_breaker_window_seconds endpoint)
                                    (parse-int-env :ingest-circuit-breaker-window-seconds 300))))
   :reset-timeout-seconds (max 1 (long (or (:circuit_breaker_reset_timeout_seconds endpoint)
                                           (parse-int-env :ingest-circuit-breaker-reset-timeout-seconds 300))))
   :half-open-max-requests (max 1 (long (or (:circuit_breaker_half_open_max_requests endpoint)
                                            (parse-int-env :ingest-circuit-breaker-half-open-max-requests 1))))})

(defn- prune-circuit-events
  [events now-ms window-seconds]
  (let [cutoff (- now-ms (* 1000 window-seconds))]
    (->> (or events [])
         (filter #(>= (long (:at_ms % 0)) cutoff))
         vec)))

(defn- circuit-state-summary
  [source-system endpoint]
  (let [key    (endpoint-circuit-breaker-key source-system endpoint)
        config (endpoint-circuit-breaker-config endpoint)
        now-ms (System/currentTimeMillis)
        state  (get @source-circuit-breaker-state key)
        events (prune-circuit-events (:events state) now-ms (:window-seconds config))
        failures (count (filter :failure? events))]
    {:key key
     :enabled (:enabled? config)
     :state (name (or (:state state) :closed))
     :failure_count_window failures
     :failure_threshold (:failure-threshold config)
     :window_seconds (:window-seconds config)
     :reset_timeout_seconds (:reset-timeout-seconds config)
     :half_open_max_requests (:half-open-max-requests config)
     :open_until_epoch_ms (:open-until-ms state)
     :last_failure_class (:last-failure-class state)
     :last_failure_status (:last-failure-status state)
     :last_failure_at_utc (:last-failure-at-utc state)
     :last_success_at_utc (:last-success-at-utc state)}))

(defn- circuit-open-exception
  [source-system endpoint summary]
  (ex-info "Source circuit breaker is open"
           {:failure_class "rate_limited"
            :status 429
            :source_system source-system
            :endpoint_name (:endpoint_name endpoint)
            :circuit_breaker summary}))

(defn- begin-source-circuit-request!
  [source-system endpoint]
  (let [config (endpoint-circuit-breaker-config endpoint)]
    (when (:enabled? config)
      (let [key    (endpoint-circuit-breaker-key source-system endpoint)
            now-ms (System/currentTimeMillis)
            result (atom nil)]
        (swap! source-circuit-breaker-state
               (fn [state]
                 (let [current       (or (get state key) {})
                       events        (prune-circuit-events (:events current) now-ms (:window-seconds config))
                       current-state (keyword (or (:state current) :closed))
                       inflight      (long (or (:half-open-inflight current) 0))
                       open-until-ms (long (or (:open-until-ms current) 0))]
                   (cond
                     (and (= current-state :open)
                          (> open-until-ms now-ms))
                     (do
                       (reset! result {:allowed? false
                                       :summary (merge (circuit-state-summary source-system endpoint)
                                                       {:state "open"
                                                        :open_until_epoch_ms open-until-ms})})
                       (assoc state key (assoc current :events events)))

                     (= current-state :open)
                     (if (< inflight (:half-open-max-requests config))
                       (let [next-state (assoc current
                                               :state :half_open
                                               :events events
                                               :half-open-inflight (inc inflight)
                                               :last-half-open-at-utc (str (now-utc)))]
                         (reset! result {:allowed? true
                                         :key key
                                         :half-open-probe? true})
                         (assoc state key next-state))
                       (do
                         (reset! result {:allowed? false
                                         :summary (merge (circuit-state-summary source-system endpoint)
                                                         {:state "half_open"})})
                         (assoc state key (assoc current :events events))))

                     (= current-state :half_open)
                     (if (< inflight (:half-open-max-requests config))
                       (let [next-state (assoc current
                                               :events events
                                               :half-open-inflight (inc inflight))]
                         (reset! result {:allowed? true
                                         :key key
                                         :half-open-probe? true})
                         (assoc state key next-state))
                       (do
                         (reset! result {:allowed? false
                                         :summary (merge (circuit-state-summary source-system endpoint)
                                                         {:state "half_open"})})
                         (assoc state key (assoc current :events events))))

                     :else
                     (do
                       (reset! result {:allowed? true
                                       :key key
                                       :half-open-probe? false})
                       (assoc state key (assoc current :state :closed :events events)))))))
        (when-not (:allowed? @result)
          (throw (circuit-open-exception source-system endpoint (:summary @result))))
        @result))))

(defn- circuit-breaker-failure?
  [{:keys [errors final-http-status error]}]
  (let [failure-class (:failure_class (ex-data error))
        status        (or (:status (ex-data error)) final-http-status)]
    (boolean
     (or (= 429 status)
         (contains? #{"rate_limited" "transient_network"} failure-class)
         (and status (<= 500 (long status) 599))
         (some #(or (= :rate-limit (:type %))
                    (= :transport (:type %))
                    (= :server-error (:type %))
                    (= "rate_limited" (:failure_class %))
                    (= "transient_network" (:failure_class %))
                    (= 429 (:status %))
                    (and (:status %) (<= 500 (long (:status %)) 599)))
               errors)))))

(defn- complete-source-circuit-request!
  [source-system endpoint admission {:keys [errors final-http-status error]}]
  (when-let [key (:key admission)]
    (let [config   (endpoint-circuit-breaker-config endpoint)
          now      (now-utc)
          now-ms   (System/currentTimeMillis)
          failed?  (circuit-breaker-failure? {:errors errors
                                              :final-http-status final-http-status
                                              :error error})]
      (swap! source-circuit-breaker-state
             (fn [state]
               (let [current       (or (get state key) {})
                     events        (prune-circuit-events (:events current) now-ms (:window-seconds config))
                     current-state (keyword (or (:state current) :closed))
                     next-events   (conj events {:at_ms now-ms :failure? failed?})
                     inflight      (max 0 (dec (long (or (:half-open-inflight current) 0))))
                     failure-count (count (filter :failure? next-events))]
                 (cond
                   failed?
                   (let [open? (or (= current-state :half_open)
                                   (>= failure-count (:failure-threshold config)))]
                     (assoc state key
                            (cond-> (assoc current
                                           :events next-events
                                           :half-open-inflight inflight
                                           :last-failure-at-utc (str now)
                                           :last-failure-class (or (:failure_class (ex-data error))
                                                                   (when (= 429 final-http-status) "rate_limited")
                                                                   (when (and final-http-status
                                                                              (<= 500 (long final-http-status) 599))
                                                                     "transient_network"))
                                           :last-failure-status final-http-status)
                              open?
                              (assoc :state :open
                                     :open-until-ms (+ now-ms (* 1000 (:reset-timeout-seconds config)))))))

                   (= current-state :half_open)
                   (assoc state key
                          {:state :closed
                           :events []
                           :half-open-inflight 0
                           :last-success-at-utc (str now)
                           :last-failure-at-utc (:last-failure-at-utc current)
                           :last-failure-class (:last-failure-class current)
                           :last-failure-status (:last-failure-status current)})

                   :else
                   (assoc state key
                          (assoc current
                                 :state :closed
                                 :events next-events
                                 :half-open-inflight inflight
                                 :last-success-at-utc (str now))))))))))

(defn- load-adaptive-backpressure-ms
  [source-system endpoint]
  (long (or (get @adaptive-backpressure-state (endpoint-adaptive-key source-system (:endpoint_name endpoint)))
            (endpoint-base-per-page-ms endpoint)
            0)))

(defn- backpressure-rate-limited?
  [errors final-http-status]
  (boolean
   (or (= 429 final-http-status)
       (some #(or (= :rate-limit (:type %))
                  (= "rate_limited" (:failure_class %))
                  (= 429 (:status %)))
             errors))))

(defn- update-adaptive-backpressure!
  [source-system endpoint errors final-http-status]
  (let [key (endpoint-adaptive-key source-system (:endpoint_name endpoint))
        base-ms (endpoint-base-per-page-ms endpoint)]
    (swap! adaptive-backpressure-state
           (fn [state]
             (let [current-ms (long (or (get state key) base-ms 0))
                   rate-limited? (backpressure-rate-limited? errors final-http-status)
                   next-ms (cond
                             rate-limited? (min 60000 (max 100 (* 2 (max current-ms (max base-ms 50)))))
                             (> current-ms base-ms) (max base-ms (long (Math/floor (* 0.8 current-ms))))
                             :else base-ms)]
               (assoc state key next-ms))))))

(defn- aggregate-endpoint-status
  [results]
  (let [statuses (set (map :status results))]
    (cond
      (empty? statuses) "failed"
      (= #{"success"} statuses) "success"
      (= #{"failed"} statuses) "failed"
      :else "partial_success")))

(defn- schema-inference-enabled? [endpoint]
  (#{"infer" "hybrid"} (or (:schema_mode endpoint) "manual")))

(defn- preview-error
  [message response]
  (let [body (:body response)
        preview (cond
                  (string? body) (subs body 0 (min 200 (count body)))
                  (map? body) (json/generate-string (select-keys body (take 5 (keys body))))
                  :else (str body))]
    (throw (ex-info message
                    {:status (:status response)
                     :body_preview preview}))))

(defn- ensure-previewable-response! [response]
  (let [status (:status response)
        body   (:body response)]
    (when (or (nil? status) (not (<= 200 status 299)))
      (preview-error (str "Schema preview request failed"
                          (when status (str " with HTTP " status)))
                     response))
    (when-not (or (map? body) (sequential? body))
      (preview-error "Schema preview requires a JSON object or array response" response))
    response))

(defn- infer-fields-by-path [fields]
  (into {} (map (juxt :path identity) fields)))

(defn- inferred-effective-type
  [field]
  (or (not-empty (:override_type field))
      (:type field)
      "STRING"))

(def ^:private widening-targets
  {"BOOLEAN" #{"BOOLEAN" "STRING"}
   "INT" #{"INT" "BIGINT" "DOUBLE" "STRING"}
   "BIGINT" #{"BIGINT" "DOUBLE" "STRING"}
   "DOUBLE" #{"DOUBLE" "STRING"}
   "DATE" #{"DATE" "TIMESTAMP" "STRING"}
   "TIMESTAMP" #{"TIMESTAMP" "STRING"}
   "STRING" #{"STRING"}})

(defn- widening-type-change?
  [current-type inferred-type]
  (contains? (get widening-targets (or current-type "STRING") #{(or current-type "STRING")})
             (or inferred-type current-type "STRING")))

(defn- merge-inferred-field
  [existing inferred]
  (let [existing-type (inferred-effective-type existing)
        inferred-type (inferred-effective-type inferred)]
    (merge inferred
           (select-keys existing [:column_name :enabled :nullable :override_type :notes :source_kind])
           {:type (if (seq (:override_type existing))
                    (:type existing)
                    (if (widening-type-change? existing-type inferred-type)
                      inferred-type
                      (:type existing)))})))

(defn- compute-schema-drift [current inferred]
  (let [current-by-path  (infer-fields-by-path current)
        inferred-by-path (infer-fields-by-path inferred)
        current-paths    (set (keys current-by-path))
        inferred-paths   (set (keys inferred-by-path))
        new-fields       (->> (set/difference inferred-paths current-paths)
                              (mapv inferred-by-path))
        missing-fields   (->> (set/difference current-paths inferred-paths)
                              (mapv current-by-path))
        type-changes     (->> (set/intersection current-paths inferred-paths)
                              (keep (fn [path]
                                      (let [current-field  (get current-by-path path)
                                            inferred-field (get inferred-by-path path)
                                            current-type   (or (not-empty (:override_type current-field))
                                                               (:type current-field))
                                            inferred-type  (:type inferred-field)]
                                        (when (not= current-type inferred-type)
                                          {:path path
                                           :current_type current-type
                                           :inferred_type inferred-type}))))
                              vec)]
    (when (or (seq new-fields) (seq missing-fields) (seq type-changes))
      {:new_fields new-fields
       :missing_fields missing-fields
       :type_changes type-changes})))

(defn- schema-enforcement-mode
  [endpoint]
  (or (non-blank-str (:schema_enforcement_mode endpoint))
      (case (or (:schema_evolution_mode endpoint) "advisory")
        "additive" "additive"
        "strict" "strict"
        "permissive" "permissive"
        "advisory" "permissive"
        "none" "permissive"
        "permissive")))

(defn- merge-inferred-fields-additively
  [current-fields inferred-fields]
  (let [current-by-path  (infer-fields-by-path current-fields)
        inferred-by-path (infer-fields-by-path inferred-fields)]
    (->> inferred-fields
         (mapv (fn [field]
                 (if-let [existing (get current-by-path (:path field))]
                   (merge-inferred-field existing field)
                   field)))
         (concat (remove #(contains? inferred-by-path (:path %)) current-fields))
         vec)))

(defn- incompatible-type-changes
  [type-changes]
  (->> type-changes
       (remove (fn [{:keys [current_type inferred_type]}]
                 (widening-type-change? current_type inferred_type)))
       vec))

(defn- fail-schema-drift!
  [endpoint mode drift reason]
  (throw (ex-info reason
                  {:failure_class "schema_drift"
                   :endpoint_name (:endpoint_name endpoint)
                   :schema_enforcement_mode mode
                   :schema_drift drift})))

(defn- apply-schema-enforcement
  [endpoint inferred-fields]
  (let [current-fields (vec (or (:inferred_fields endpoint) []))
        enforcement-mode (schema-enforcement-mode endpoint)
        drift            (compute-schema-drift current-fields inferred-fields)]
    (cond
      (empty? current-fields)
      (assoc endpoint
             :schema_enforcement_mode enforcement-mode
             :inferred_fields inferred-fields
             :schema_drift drift)

      (nil? drift)
      (assoc endpoint
             :schema_enforcement_mode enforcement-mode
             :inferred_fields (merge-inferred-fields-additively current-fields inferred-fields)
             :schema_drift nil)

      (= enforcement-mode "strict")
      (fail-schema-drift! endpoint enforcement-mode drift
                          "Schema drift detected under strict enforcement")

      (= enforcement-mode "additive")
      (let [rejected-type-changes (incompatible-type-changes (:type_changes drift))
            additive-drift        (assoc drift :type_changes rejected-type-changes)]
        (when (seq rejected-type-changes)
          (fail-schema-drift! endpoint enforcement-mode additive-drift
                              "Schema drift requires an incompatible type change under additive enforcement"))
        (assoc endpoint
               :schema_enforcement_mode enforcement-mode
               :inferred_fields (merge-inferred-fields-additively current-fields inferred-fields)
               :schema_drift drift
               :_pre_drift_fields current-fields))

      :else
      (assoc endpoint
             :schema_enforcement_mode enforcement-mode
             :inferred_fields current-fields
             :schema_drift drift
             :_pre_drift_fields current-fields))))

(defn- maybe-infer-endpoint-fields [endpoint pages]
  (if (schema-inference-enabled? endpoint)
    (apply-schema-enforcement endpoint (schema-infer/infer-fields-from-pages pages endpoint))
    endpoint))

(defn- ensure-unique-field-column-names!
  [endpoint]
  (let [column-names (->> (schema-infer/effective-field-descriptors endpoint)
                          (filter #(not= false (:enabled %)))
                          (map :column_name)
                          vec)
        duplicates   (->> column-names
                          frequencies
                          (keep (fn [[column-name n]]
                                  (when (> n 1) column-name)))
                          vec)
        reserved     (->> column-names
                          (filter bronze/bronze-reserved-column-names)
                          distinct
                          vec)]
    (when (seq duplicates)
      (throw (ex-info "Endpoint field selection contains duplicate enabled column_name values"
                      {:failure_class "schema_drift"
                       :endpoint_name (:endpoint_name endpoint)
                       :columns duplicates})))
    (when (seq reserved)
      (throw (ex-info "Endpoint field selection uses reserved Bronze column_name values"
                      {:failure_class "schema_drift"
                       :endpoint_name (:endpoint_name endpoint)
                       :columns reserved})))
    endpoint))

(defn- checkpoint-row-for-failure [existing-row failure]
  (merge (select-keys existing-row [:last_successful_watermark
                                    :last_successful_cursor
                                    :last_successful_run_id
                                    :last_successful_batch_id
                                    :last_successful_batch_seq
                                    :rows_ingested])
         failure))

(declare changed-partition-dates
         replace-row!
         update-checkpoint-row!
         update-manifest-row!
         query-schema-snapshot-rows
         schema-fields-hash)

(defonce ^:private tables-confirmed-per-run (atom #{}))
(defonce ^:private current-run-id-for-cache (atom nil))

(defn- ensure-table! [conn-id table-name columns opts]
  (when-not conn-id
    (throw (ex-info "Target connection_id is missing or invalid"
                    {:table_name table-name
                     :status 400})))
  (let [qualified-table-name (validated-qualified-table-name table-name)
        cache-key [conn-id qualified-table-name]]
    (when-not (contains? @tables-confirmed-per-run cache-key)
      (if (local-table-confirmed? conn-id qualified-table-name)
        (swap! tables-confirmed-per-run conj cache-key)
        (do
          (db/create-table! conn-id nil qualified-table-name columns opts)
          (swap! tables-confirmed-per-run conj cache-key)
          (cache-local-table-confirmed! conn-id qualified-table-name))))))

(defn clear-confirmed-table-cache!
  []
  (reset! tables-confirmed-per-run #{})
  nil)

(defn reset-table-cache-for-run!
  "Track the current run id without clearing warm table-confirmation cache.
   Table cache now persists across runs and is invalidated only on structural failures."
  [run-id]
  (when (not= run-id @current-run-id-for-cache)
    (reset! current-run-id-for-cache run-id)))

(defn- key->col [row]
  (zipmap (keys row)
          (map (fn [k]
                 (cond
                   (keyword? k) (name k)
                   (string? k) k
                   :else (str k)))
               (keys row))))

(defn- key-name
  [k]
  (cond
    (keyword? k) (name k)
    (string? k) k
    :else (str k)))

(defn- sql-opts
  [conn-id]
  (or *batch-sql-opts*
      (db/get-opts conn-id nil)))

(defn- databricks-bind-value-local
  [value]
  (cond
    (nil? value) nil
    (instance? java.sql.Date value) (str (.toLocalDate ^java.sql.Date value))
    (instance? java.sql.Timestamp value) (str (.toInstant ^java.sql.Timestamp value))
    (instance? java.util.Date value) (str (.toInstant ^java.util.Date value))
    (instance? java.time.Instant value) (str value)
    (instance? java.time.OffsetDateTime value) (str value)
    (instance? java.time.ZonedDateTime value) (str value)
    (instance? java.time.LocalDate value) (str value)
    (instance? java.time.LocalDateTime value) (str value)
    (instance? java.util.UUID value) (str value)
    (map? value) (json/generate-string value)
    (vector? value) (json/generate-string value)
    :else value))

(defn- databricks-sql-literal-local
  [value]
  (let [value (databricks-bind-value-local value)]
    (cond
      (nil? value) "NULL"
      (string? value) (str "'" (string/replace value "'" "''") "'")
      (instance? java.lang.Boolean value) (if value "TRUE" "FALSE")
      (number? value) (str value)
      :else (str "'" (string/replace (str value) "'" "''") "'"))))

(defn- db-opts
  [conn]
  (jdbc/with-options conn {:builder-fn rs/as-unqualified-lower-maps}))

(defn- insert-rows-in-current-opts!
  [opts table-name rows]
  (when (seq rows)
    (let [table-name    (validated-qualified-table-name table-name)
          row-keys      (vec (keys (first rows)))
          column-names  (mapv (comp validated-sql-identifier key-name) row-keys)
          row-count     (count rows)
          col-count     (count column-names)
          row-values-sql (str "(" (string/join ", " (repeat col-count "?")) ")")
          sql-str       (str "INSERT INTO " table-name
                             " (" (string/join ", " column-names) ") VALUES "
                             (string/join ", " (repeat row-count row-values-sql)))
          flat-params   (vec (mapcat (fn [row] (mapv #(get row %) row-keys)) rows))]
      (jdbc/execute! opts (into [sql-str] flat-params)))))

(defn- load-rows! [conn-id table-name rows]
  (when (seq rows)
    (if (and *row-load-context*
             (= table-name (:primary-table-name *row-load-context*))
             (= :file (:source-kind *row-load-context*))
             (= "databricks" (some-> (connection-dbtype conn-id) string/lower-case))
             (= "copy_into" (some-> (get-in *row-load-context* [:target :write_mode]) str string/lower-case)))
      (let [copy-into-ran? (or (:copy-into-ran? *row-load-context*)
                               (atom false))
            copy-into-opts (merge (or (get-in *row-load-context* [:target :options :copy_into]) {})
                                  (or (get-in *row-load-context* [:target :options "copy_into"]) {}))
            source-uri     (or (:source_uri copy-into-opts)
                               (:source-uri copy-into-opts)
                               (:base_path (:config *row-load-context*)))]
        (if (compare-and-set! copy-into-ran? false true)
          (db/load-rows-databricks-copy-into!
           conn-id
           nil
           table-name
           {:source_uri source-uri
            :file_format (or (:file_format copy-into-opts)
                             (:file-format copy-into-opts)
                             "JSON")
            :files (vec (or (:files copy-into-opts) []))
            :pattern (:pattern copy-into-opts)
            :format_options (or (:format_options copy-into-opts)
                                (:format-options copy-into-opts)
                                {})
            :copy_options (or (:copy_options copy-into-opts)
                              (:copy-options copy-into-opts)
                              {})
            :credential (:credential copy-into-opts)})
          {:status :ok
           :load_method :databricks_copy_into
           :already_loaded true
           :table_name table-name}))
      (if (and *row-load-context*
             (= table-name (:primary-table-name *row-load-context*))
             (= :file (:source-kind *row-load-context*))
             (= "snowflake" (some-> (connection-dbtype conn-id) string/lower-case))
             (= "stage_copy" (some-> (get-in *row-load-context* [:target :sf_load_method]) str string/lower-case)))
      (db/load-rows-snowflake-stage-copy!
       conn-id
       nil
       table-name
       rows
       (key->col (first rows))
       {:sf_stage_name (get-in *row-load-context* [:target :sf_stage_name])
        :sf_on_error (get-in *row-load-context* [:target :sf_on_error])
        :sf_purge (get-in *row-load-context* [:target :sf_purge])})
      (if *batch-sql-opts*
      (insert-rows-in-current-opts! (sql-opts conn-id) table-name rows)
      (db/load-rows! conn-id nil table-name rows (key->col (first rows))))))))

(defn- fetch-checkpoint [conn-id table-name source-system endpoint-name]
  (let [table-name (validated-qualified-table-name table-name)
        cache-key  (checkpoint-cache-key conn-id table-name source-system endpoint-name)]
    (if (contains? @checkpoint-row-cache cache-key)
      (get @checkpoint-row-cache cache-key)
      (if-some [local-row (local-checkpoint-row conn-id table-name source-system endpoint-name)]
        (cache-checkpoint-row! conn-id table-name source-system endpoint-name local-row)
        (let [row (jdbc/execute-one!
                   (db/get-opts conn-id nil)
                   [(str "SELECT * FROM " table-name " WHERE source_system = ? AND endpoint_name = ?")
                    source-system endpoint-name])]
          (cache-checkpoint-row! conn-id table-name source-system endpoint-name row))))))

(defn- replace-checkpoint-row!
  [conn-id checkpoint-table row]
  (let [source-system (:source_system row)
        endpoint-name (:endpoint_name row)]
    (replace-row! conn-id checkpoint-table [:source_system :endpoint_name] row)
    (if (and source-system endpoint-name)
      (cache-checkpoint-row! conn-id checkpoint-table source-system endpoint-name row)
      (invalidate-checkpoint-row-cache! conn-id checkpoint-table source-system endpoint-name))
    row))

(defn- replace-row! [conn-id table-name key-cols row]
  (let [table-name  (validated-qualified-table-name table-name)
        where-sql  (string/join " AND " (map #(str (validated-sql-identifier %) " = ?") key-cols))
        where-vals (mapv row key-cols)
        opts       (sql-opts conn-id)]
    (jdbc/execute! opts (into [(str "DELETE FROM " table-name " WHERE " where-sql)] where-vals))
    (load-rows! conn-id table-name [row])))

(defn- update-row-by-key!
  [conn-id table-name key-cols row]
  (let [table-name   (validated-qualified-table-name table-name)
        dbtype       (some-> (connection-dbtype conn-id) string/lower-case)
        set-keys     (vec (remove (set key-cols) (keys row)))
        key-vals     (mapv row key-cols)]
    (when (seq set-keys)
      (if (= "databricks" dbtype)
        (let [set-sql   (string/join ", "
                                     (map (fn [k]
                                            (str (validated-sql-identifier k)
                                                 " = "
                                                 (databricks-sql-literal-local (row k))))
                                          set-keys))
              where-sql (string/join " AND "
                                     (map (fn [k]
                                            (str (validated-sql-identifier k)
                                                 " = "
                                                 (databricks-sql-literal-local (row k))))
                                          key-cols))]
          (jdbc/execute! (sql-opts conn-id)
                         [(str "UPDATE " table-name " SET " set-sql " WHERE " where-sql)]))
        (replace-row! conn-id table-name key-cols row)))))

(defn- delete-rows-by-column!
  [conn-id table-name column-name value]
  (let [table-name  (validated-qualified-table-name table-name)
        column-name (validated-sql-identifier column-name)]
    (try
      (jdbc/execute!
       (sql-opts conn-id)
       [(str "DELETE FROM " table-name " WHERE " column-name " = ?")
        value])
      (catch java.sql.SQLException e
        ;; Table may not exist yet (first run or dropped externally) — safe to skip
        (if (re-find #"(?i)TABLE_OR_VIEW_NOT_FOUND|does not exist|42P01" (.getMessage e))
          (do (swap! tables-confirmed-per-run disj [conn-id table-name])
              (delete-local-table-confirmed! conn-id table-name)
              nil)
          (throw e))))))

(defn- logical-record-count
  [endpoint page]
  (count (schema-infer/logical-records-from-body
          (:body page)
          (or (get-in endpoint [:json_explode_rules 0 :path]) ""))))

(defn- sample-record-count
  [endpoint sample-pages]
  (reduce + 0 (map #(logical-record-count endpoint %) sample-pages)))

(defn- persist-endpoint-schema-snapshot!
  [conn-id table-name {:keys [graph-id api-node-id graph-version-id graph-version
                              source-system endpoint sample-pages captured-at]}]
  (let [inferred-fields (vec (or (:inferred_fields endpoint) []))
        inferred-fields-json (json/generate-string inferred-fields)
        drift (:schema_drift endpoint)
        drift-json (when drift (json/generate-string drift))
        row {:graph_id graph-id
             :api_node_id api-node-id
             :graph_version_id graph-version-id
             :graph_version graph-version
             :source_system source-system
             :endpoint_name (:endpoint_name endpoint)
             :schema_mode (or (:schema_mode endpoint) "manual")
             :schema_enforcement_mode (schema-enforcement-mode endpoint)
             :sample_record_count (sample-record-count endpoint sample-pages)
             :inferred_fields_json inferred-fields-json
             :schema_drift_json drift-json
             :captured_at_utc (str captured-at)}
        workspace-key (delay
                        (when graph-id
                          (:workspace_key (control-plane/graph-workspace-context graph-id))))]
    (load-rows! conn-id table-name [row])
    (cache-latest-schema-snapshot-row! conn-id table-name graph-id api-node-id (:endpoint_name endpoint) row)
    ;; Persist schema drift event for alerting pipeline
    (when drift
      (try
        (let [persist-drift! (requiring-resolve 'bitool.ops.schema-drift/persist-schema-drift-event!)
              pre-fields-json (json/generate-string (vec (or (:_pre_drift_fields endpoint) [])))
              hash-before (stable-json-hash (vec (or (:_pre_drift_fields endpoint) [])))
              hash-after  (stable-json-hash inferred-fields)]
          (persist-drift!
           {:workspace-key (or @workspace-key 0)
            :graph-id graph-id
            :api-node-id api-node-id
            :endpoint-name (:endpoint_name endpoint)
            :source-system source-system
            :run-id nil
            :drift drift
            :enforcement-mode (schema-enforcement-mode endpoint)
            :schema-hash-before hash-before
            :schema-hash-after hash-after}))
        (catch Exception e
          (log/debug e "Schema drift event persistence skipped"))))))

(defn- latest-schema-snapshot-row
  [conn-id schema-snapshot-table graph-id api-node-id endpoint-name]
  (let [cache-key (schema-snapshot-cache-key conn-id schema-snapshot-table graph-id api-node-id endpoint-name)]
    (if (contains? @schema-snapshot-latest-cache cache-key)
      (get @schema-snapshot-latest-cache cache-key)
      (if-some [local-row (local-latest-schema-snapshot-row conn-id schema-snapshot-table graph-id api-node-id endpoint-name)]
        (cache-latest-schema-snapshot-row! conn-id schema-snapshot-table graph-id api-node-id endpoint-name local-row)
        (try
          (let [row (first (query-schema-snapshot-rows conn-id
                                                       schema-snapshot-table
                                                       {:graph-id graph-id
                                                        :api-node-id api-node-id
                                                        :endpoint-name endpoint-name
                                                        :limit 1}))]
            (cache-latest-schema-snapshot-row! conn-id schema-snapshot-table graph-id api-node-id endpoint-name row))
          (catch Exception e
            (log/debug e "Skipping latest schema snapshot lookup; defaulting to snapshot write"
                       {:graph_id graph-id
                        :api_node_id api-node-id
                        :endpoint_name endpoint-name})
            nil))))))

(defn- schema-snapshot-write-needed?
  [conn-id schema-snapshot-table {:keys [graph-id api-node-id source-system endpoint sample-pages]}]
  (let [sample-count   (sample-record-count endpoint sample-pages)]
    (if (or (zero? sample-count)
            (not (schema-snapshot-tracking-required? endpoint)))
      false
      (let [endpoint-name (:endpoint_name endpoint)
            latest-row    (latest-schema-snapshot-row conn-id schema-snapshot-table graph-id api-node-id endpoint-name)
            latest-hash   (some-> latest-row :inferred_fields_json schema-fields-hash)
            current-hash  (schema-fields-hash (vec (or (:inferred_fields endpoint) [])))
            latest-mode   (some-> latest-row :schema_mode str)
            current-mode  (or (:schema_mode endpoint) "manual")
            latest-enf    (some-> latest-row :schema_enforcement_mode str)
            current-enf   (schema-enforcement-mode endpoint)
            latest-source (some-> latest-row :source_system str)]
        (or (nil? latest-row)
            (not= latest-hash current-hash)
            (not= latest-mode current-mode)
            (not= latest-enf current-enf)
            (not= latest-source source-system)
            (some? (:schema_drift endpoint)))))))

(defn- schema-fields-hash
  [inferred-fields-json]
  (let [fields (if (string? inferred-fields-json)
                 (json/parse-string inferred-fields-json true)
                 inferred-fields-json)]
    (stable-json-hash (vec (or fields [])))))

(defn- schema-approval-required?
  [endpoint]
  (boolean
   (or (= "required" (some-> (:schema_review_state endpoint) str string/trim string/lower-case))
       (true? (:require_schema_approval endpoint))
       (parse-bool-env :ingest-require-schema-approval false))))

(defn- schema-snapshot-tracking-required?
  [endpoint]
  (let [schema-mode      (some-> (or (:schema_mode endpoint) "manual") str string/lower-case)
        enforcement-mode (some-> (schema-enforcement-mode endpoint) str string/lower-case)
        evolution-mode   (some-> (or (:schema_evolution_mode endpoint) "advisory") str string/lower-case)]
    (boolean
     (or (schema-approval-required? endpoint)
         (some? (:schema_drift endpoint))
         (not= "infer" schema-mode)
         (not= "permissive" enforcement-mode)
         (not= "advisory" evolution-mode)))))

(defn- collect-schema-sample!
  [pages-ch endpoint]
  (if-not (schema-inference-enabled? endpoint)
    {:sample-pages [] :terminal-msg nil}
    (let [sample-limit (max 1 (long (or (:sample_records endpoint) 100)))]
      (loop [sample-pages []
             sampled-records 0]
        (if (>= sampled-records sample-limit)
          {:sample-pages sample-pages :terminal-msg nil}
          (if-let [msg (async/<!! pages-ch)]
            (if (:body msg)
              (recur (conj sample-pages msg)
                     (+ sampled-records (logical-record-count endpoint msg)))
              {:sample-pages sample-pages
               :terminal-msg msg})
            {:sample-pages sample-pages :terminal-msg nil}))))))

(defn- new-batch-buffer []
  {:rows []
   :bad-records []
   :page-artifacts []
   :page-count 0
   :byte-count 0
   :last-state nil
   :last-http-status nil})

(defn- estimate-payload-bytes [rows]
  (reduce (fn [total row]
            (+ total (count (str (or (:payload_json row) "")))))
          0
          rows))

(defn- add-page-to-batch
  [buffer page-out page]
  (let [rows        (:rows page-out)
        bad-records (:bad-records page-out)
        page-bytes  (+ (estimate-payload-bytes rows)
                       (estimate-payload-bytes bad-records))]
    (-> buffer
        (update :rows into rows)
        (update :bad-records into bad-records)
        (update :page-artifacts conj {:page (:page page)
                                      :state (:state page)
                                      :response {:status (get-in page [:response :status])
                                                 :retry_count (get-in page [:response :retry-count])}
                                      :body (:body page)})
        (update :page-count inc)
        (update :byte-count + page-bytes)
        (assoc :last-state (:state page)
               :last-http-status (get-in page [:response :status])))))

(defn- batch-empty?
  [buffer]
  (and (empty? (:rows buffer))
       (empty? (:bad-records buffer))
       (zero? (:page-count buffer))))

(defn- should-flush-batch?
  [buffer]
  (or (>= (count (:rows buffer)) (parse-int-env :worker-batch-rows 1000))
      (>= (long (:byte-count buffer))
          (long (parse-int-env :worker-max-batch-bytes 52428800)))))

(defn- page-state-next-cursor
  [state]
  (or (some-> state :last-cursor non-blank-str)
      (some-> state :cursor non-blank-str)
      (some-> state :next-token non-blank-str)
      (some-> state :next-url non-blank-str)))

(defn- rows-max-watermark
  [rows]
  (->> rows
       (map :event_time_utc)
       (remove string/blank?)
       sort
       last))

(defn- merge-watermark
  [current next-value]
  (last (sort (remove string/blank? [current next-value]))))

(defn- artifact-root-dir []
  (or (non-blank-str (get env :ingest-artifact-root))
      "tmp/ingest-artifacts"))

(defn- artifact-archive-root-dir []
  (or (non-blank-str (get env :ingest-artifact-archive-root))
      "tmp/ingest-artifacts-archive"))

(defn- cutoff-instant
  [days]
  (.minusSeconds (now-utc) (* 86400 (long (max 0 days)))))

(defn- artifact-store-mode []
  (keyword (or (non-blank-str (get env :ingest-artifact-store))
               "local")))

(defn- safe-path-segment [value]
  (let [value (-> (str value)
                  string/lower-case
                  (string/replace #"[^a-z0-9._-]+" "_")
                  (string/replace #"_+" "_")
                  (string/replace #"^_+" "")
                  (string/replace #"_+$" ""))]
    (if (string/blank? value) "segment" value)))

(defn- persist-batch-artifact-local!
  ([run-id endpoint-name batch-id artifact-data]
   (persist-batch-artifact-local! run-id endpoint-name batch-id artifact-data "batch_pages"))
  ([run-id endpoint-name batch-id artifact-data _artifact-kind]
   (let [dir      (io/file (artifact-root-dir)
                           (safe-path-segment endpoint-name)
                           (safe-path-segment run-id))
         _        (.mkdirs dir)
         payload  (json/generate-string artifact-data)
         file     (io/file dir (str (safe-path-segment batch-id) ".json"))
         checksum (sha256-hex payload)]
     (try
       (spit file payload)
       {:artifact_path (.getPath file)
        :artifact_checksum checksum}
       (catch Exception e
         (log/warn e "Failed to persist batch artifact"
                   {:run_id run-id
                    :endpoint_name endpoint-name
                    :batch_id batch-id})
         {:artifact_path nil
          :artifact_checksum checksum})))))

(defn- persist-batch-artifact-db!
  ([run-id source-system endpoint-name batch-id artifact-data]
   (persist-batch-artifact-db! run-id source-system endpoint-name batch-id artifact-data "batch_pages"))
  ([run-id source-system endpoint-name batch-id artifact-data artifact-kind]
   (ensure-artifact-store-table!)
   (let [payload  (json/generate-string artifact-data)
         checksum (sha256-hex payload)
         path     (str "db://ingest-batch-artifact/" (safe-path-segment run-id) "/" (safe-path-segment batch-id))]
     (jdbc/execute-one!
      (jdbc/with-options db/ds {:builder-fn rs/as-unqualified-lower-maps})
      [(str "INSERT INTO " artifact-store-table "
             (artifact_path, artifact_kind, run_id, source_system, endpoint_name, batch_id, payload_json, artifact_checksum)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (artifact_path) DO UPDATE
               SET artifact_kind = EXCLUDED.artifact_kind,
                   payload_json = EXCLUDED.payload_json,
                   artifact_checksum = EXCLUDED.artifact_checksum
             RETURNING artifact_path, artifact_checksum")
       path
       artifact-kind
       run-id
       source-system
       endpoint-name
       batch-id
       payload
       checksum]))))

(defn- persist-batch-artifact-http!
  ([run-id endpoint-name batch-id artifact-data]
   (persist-batch-artifact-http! run-id endpoint-name batch-id artifact-data "batch_pages"))
  ([run-id endpoint-name batch-id artifact-data _artifact-kind]
   (let [endpoint-url (non-blank-str (get env :ingest-artifact-http-endpoint))]
     (when-not endpoint-url
       (throw (ex-info "HTTP artifact store requires ingest-artifact-http-endpoint"
                       {:batch_id batch-id
                        :run_id run-id
                        :endpoint_name endpoint-name})))
     (let [payload  (json/generate-string artifact-data)
           response (api/do-request {:method :post
                                     :url endpoint-url
                                     :base-headers {"content-type" "application/json"}
                                     :query-params {}
                                     :body-params payload
                                     :auth nil
                                     :retry-policy {:max-retries 1 :base-backoff-ms 1000}})
           status   (:status response)
           body     (:body response)
           checksum (sha256-hex payload)]
       (when (or (nil? status) (not (<= 200 status 299)))
         (throw (ex-info "HTTP artifact store write failed"
                         {:status status
                          :batch_id batch-id
                          :run_id run-id
                          :endpoint_name endpoint-name})))
       {:artifact_path (or (when (map? body) (non-blank-str (:artifact_path body)))
                           (str endpoint-url "/" (safe-path-segment batch-id)))
        :artifact_checksum (or (when (map? body) (non-blank-str (:artifact_checksum body)))
                               checksum)}))))

(defn- persist-batch-artifact!
  ([run-id source-system endpoint-name batch-id artifact-data]
   (persist-batch-artifact! run-id source-system endpoint-name batch-id artifact-data "batch_pages"))
  ([run-id source-system endpoint-name batch-id artifact-data artifact-kind]
   (case (artifact-store-mode)
     :local (persist-batch-artifact-local! run-id endpoint-name batch-id artifact-data artifact-kind)
     :http (persist-batch-artifact-http! run-id endpoint-name batch-id artifact-data artifact-kind)
     :db (persist-batch-artifact-db! run-id source-system endpoint-name batch-id artifact-data artifact-kind)
     :none {:artifact_path nil
            :artifact_checksum (sha256-hex (json/generate-string artifact-data))}
     (throw (ex-info "Unsupported ingest artifact store mode"
                     {:artifact_store (artifact-store-mode)})))))

(defn- artifact-read-payload
  [response]
  (let [status (:status response)
        body   (:body response)]
    (when (or (nil? status) (not (<= 200 status 299)))
      (throw (ex-info "Artifact read failed"
                      {:failure_class "artifact_read_failed"
                       :status status
                       :response_body body})))
    (cond
      (map? body) body
      (string? body) (json/parse-string body true)
      :else (throw (ex-info "Artifact read returned unsupported body"
                            {:failure_class "artifact_read_failed"})))))

(defn- read-batch-artifact-db!
  [artifact-path artifact-checksum]
  (ensure-artifact-store-table!)
  (let [row (jdbc/execute-one!
             (jdbc/with-options db/ds {:builder-fn rs/as-unqualified-lower-maps})
             [(str "SELECT payload_json, artifact_checksum FROM " artifact-store-table " WHERE artifact_path = ?")
              artifact-path])]
    (when-not row
      (throw (ex-info "Stored replay artifact not found"
                      {:failure_class "artifact_missing"
                       :artifact_path artifact-path})))
    (when (and (seq artifact-checksum) (not= artifact-checksum (:artifact_checksum row)))
      (throw (ex-info "Stored replay artifact checksum mismatch"
                      {:failure_class "artifact_checksum_mismatch"
                       :artifact_path artifact-path})))
    (json/parse-string (:payload_json row) true)))

(defn- artifact-root-file
  []
  (.getCanonicalFile (io/file (artifact-root-dir))))

(defn- artifact-archive-root-file
  []
  (.getCanonicalFile (io/file (artifact-archive-root-dir))))

(defn- managed-artifact-roots
  []
  [(artifact-root-file) (artifact-archive-root-file)])

(defn- assert-artifact-path-within-root!
  [artifact-path]
  (let [root-path   (.toPath (artifact-root-file))
        target-file (.getCanonicalFile (io/file artifact-path))
        target-path (.toPath target-file)]
    (when-not (.startsWith target-path root-path)
      (throw (ex-info "Artifact path escapes artifact root"
                      {:failure_class "artifact_invalid_path"
                       :artifact_path artifact-path
                       :artifact_root (.getPath (artifact-root-file))})))
    target-file))

(defn- assert-artifact-path-within-managed-roots!
  [artifact-path]
  (let [target-file (.getCanonicalFile (io/file artifact-path))
        target-path (.toPath target-file)
        root-paths  (map #(.toPath %) (managed-artifact-roots))]
    (when-not (some #(.startsWith target-path %) root-paths)
      (throw (ex-info "Artifact path escapes artifact root"
                      {:failure_class "artifact_invalid_path"
                       :artifact_path artifact-path
                       :artifact_roots (mapv #(.getPath %) (managed-artifact-roots))})))
    target-file))

(defn- local-artifact-relative-path
  [file root]
  (str (.relativize (.toPath (.getCanonicalFile root))
                    (.toPath (.getCanonicalFile file)))))

(defn- move-local-artifact-to-archive!
  [file]
  (let [root        (artifact-root-file)
        archive-root (artifact-archive-root-file)
        relative    (local-artifact-relative-path file root)
        archive-file (io/file archive-root relative)]
    (.mkdirs (.getParentFile archive-file))
    (io/copy file archive-file)
    (when-not (.delete file)
      (throw (ex-info "Failed to delete local artifact after archive copy"
                      {:artifact_path (.getPath file)
                       :archive_path (.getPath archive-file)})))
    archive-file))

(defn- parse-instant-safe
  [value]
  (when-let [value (non-blank-str value)]
    (try
      (checkpoint/parse-instant value)
      (catch Exception _
        nil))))

(defn- coerce-instant
  [value]
  (cond
    (nil? value) nil
    (instance? java.time.Instant value) value
    (instance? java.time.OffsetDateTime value) (.toInstant ^java.time.OffsetDateTime value)
    (instance? java.time.ZonedDateTime value) (.toInstant ^java.time.ZonedDateTime value)
    (instance? java.sql.Timestamp value) (.toInstant ^java.sql.Timestamp value)
    :else (parse-instant-safe value)))

(defn- read-batch-artifact-local!
  [artifact-path artifact-checksum]
  (let [payload (slurp (assert-artifact-path-within-managed-roots! artifact-path))
        checksum (sha256-hex payload)]
    (when (and (seq artifact-checksum) (not= artifact-checksum checksum))
      (throw (ex-info "Local replay artifact checksum mismatch"
                      {:failure_class "artifact_checksum_mismatch"
                       :artifact_path artifact-path})))
    (json/parse-string payload true)))

(defn- read-batch-artifact-http!
  [artifact-path artifact-checksum]
  (let [read-endpoint (non-blank-str (get env :ingest-artifact-http-read-endpoint))
        request       (if read-endpoint
                        {:method :get
                         :url read-endpoint
                         :base-headers {}
                         :query-params {:artifact_path artifact-path}
                         :auth nil
                         :retry-policy {:max-retries 1 :base-backoff-ms 1000}}
                        {:method :get
                         :url artifact-path
                         :base-headers {}
                         :query-params {}
                         :auth nil
                         :retry-policy {:max-retries 1 :base-backoff-ms 1000}})
        body          (artifact-read-payload (api/do-request request))
        checksum      (sha256-hex (json/generate-string body))]
    (when (and (seq artifact-checksum) (not= artifact-checksum checksum))
      (throw (ex-info "HTTP replay artifact checksum mismatch"
                      {:failure_class "artifact_checksum_mismatch"
                       :artifact_path artifact-path})))
    body))

(defn- read-batch-artifact!
  [artifact-path artifact-checksum]
  (cond
    (string/blank? (str artifact-path))
    (throw (ex-info "Replay batch is missing artifact_path"
                    {:failure_class "artifact_missing"
                     :artifact_path artifact-path}))

    (string/starts-with? artifact-path "db://")
    (read-batch-artifact-db! artifact-path artifact-checksum)

    (or (string/starts-with? artifact-path "http://")
        (string/starts-with? artifact-path "https://"))
    (read-batch-artifact-http! artifact-path artifact-checksum)

    :else
    (read-batch-artifact-local! artifact-path artifact-checksum)))

(defn- archive-batch-artifact-db!
  [artifact-path artifact-checksum]
  (ensure-artifact-store-table!)
  (jdbc/execute-one!
   (jdbc/with-options db/ds {:builder-fn rs/as-unqualified-lower-maps})
   [(str "UPDATE " artifact-store-table
         " SET archived_at_utc = COALESCE(archived_at_utc, now())"
         " WHERE artifact_path = ?"
         " RETURNING artifact_path, artifact_checksum")
    artifact-path])
  {:artifact_path artifact-path
   :artifact_checksum artifact-checksum})

(defn- delete-batch-artifact-db!
  [artifact-path]
  (ensure-artifact-store-table!)
  (:next.jdbc/update-count
   (jdbc/execute-one!
    (jdbc/with-options db/ds {:builder-fn rs/as-unqualified-lower-maps})
    [(str "DELETE FROM " artifact-store-table " WHERE artifact_path = ?")
     artifact-path])))

(defn- artifact-http-lifecycle!
  [endpoint-key artifact-path artifact-checksum]
  (let [endpoint-url (non-blank-str (get env endpoint-key))]
    (when-not endpoint-url
      (throw (ex-info "HTTP artifact lifecycle endpoint is not configured"
                      {:failure_class "artifact_lifecycle_unsupported"
                       :endpoint_key endpoint-key
                       :artifact_path artifact-path})))
    (let [response (api/do-request {:method :post
                                    :url endpoint-url
                                    :base-headers {"content-type" "application/json"}
                                    :query-params {}
                                    :body-params {:artifact_path artifact-path}
                                    :auth nil
                                    :retry-policy {:max-retries 1 :base-backoff-ms 1000}})
          status   (:status response)
          body     (:body response)]
      (when (or (nil? status) (not (<= 200 status 299)))
        (throw (ex-info "HTTP artifact lifecycle request failed"
                        {:failure_class "artifact_lifecycle_failed"
                         :endpoint_key endpoint-key
                         :artifact_path artifact-path
                         :http_status status
                         :response_body body})))
      {:artifact_path (or (when (map? body) (non-blank-str (:artifact_path body)))
                          artifact-path)
       :artifact_checksum (or (when (map? body) (non-blank-str (:artifact_checksum body)))
                              artifact-checksum)})))

(defn- archive-batch-artifact!
  [artifact-path artifact-checksum]
  (cond
    (string/blank? (str artifact-path))
    {:artifact_path nil
     :artifact_checksum nil}

    (string/starts-with? artifact-path "db://")
    (archive-batch-artifact-db! artifact-path artifact-checksum)

    (or (string/starts-with? artifact-path "http://")
        (string/starts-with? artifact-path "https://"))
    (artifact-http-lifecycle! :ingest-artifact-http-archive-endpoint artifact-path artifact-checksum)

    :else
    (let [file (assert-artifact-path-within-managed-roots! artifact-path)]
      (if (.startsWith (.toPath (.getCanonicalFile file))
                       (.toPath (artifact-archive-root-file)))
        {:artifact_path (.getPath file)
         :artifact_checksum artifact-checksum}
        (let [archive-file (move-local-artifact-to-archive! file)]
          {:artifact_path (.getPath archive-file)
           :artifact_checksum artifact-checksum})))))

(defn- delete-batch-artifact!
  [artifact-path]
  (cond
    (string/blank? (str artifact-path))
    0

    (string/starts-with? artifact-path "db://")
    (long (or (delete-batch-artifact-db! artifact-path) 0))

    (or (string/starts-with? artifact-path "http://")
        (string/starts-with? artifact-path "https://"))
    (do
      (artifact-http-lifecycle! :ingest-artifact-http-delete-endpoint artifact-path nil)
      1)

    :else
    (let [file (assert-artifact-path-within-managed-roots! artifact-path)]
      (if (and (.exists file) (.delete file)) 1 0))))

(defn- default-artifact-archive-days
  []
  (long (max 0
             (parse-int-env :ingest-artifact-archive-days
                            (parse-int-env :ingest-local-artifact-archive-days 30)))))

(defn- default-artifact-retention-days
  [archive-days]
  (long (max archive-days
             (parse-int-env :ingest-artifact-retention-days
                            (parse-int-env :ingest-local-artifact-retention-days 365)))))

(defn- default-bad-record-payload-archive-days
  []
  (long (max 0 (parse-int-env :ingest-bad-record-payload-archive-days 30))))

(defn- default-bad-record-retention-days
  [archive-days]
  (long (max archive-days
             (parse-int-env :ingest-bad-record-retention-days 90))))

(defn- parse-retention-target
  [value]
  (when-let [value (non-blank-str value)]
    (let [[graph-id api-node-id endpoint-name] (string/split value #":" 3)]
      (when (and graph-id api-node-id)
        (try
          {:graph-id (Integer/parseInt graph-id)
           :api-node-id (Integer/parseInt api-node-id)
           :endpoint-name (non-blank-str endpoint-name)}
          (catch Exception _
            nil))))))

(defn- graph-api-node-ids
  [graph]
  (->> (:n graph)
       (keep (fn [[node-id node]]
               (when (= "Ap" (get-in node [:na :btype]))
                 node-id)))))

(defn- retention-ready-api-node-ids
  [graph-id graph]
  (->> (graph-api-node-ids graph)
       (keep (fn [api-node-id]
               (let [target  (find-downstream-target graph api-node-id)
                     conn-id (resolved-target-connection-id target)]
                 (when-not conn-id
                   (log/debug "Skipping API node during manifest retention discovery without downstream target connection"
                              {:graph_id graph-id
                               :api_node_id api-node-id
                               :configured_connection_id (target-connection-id target)}))
                 (when conn-id
                   api-node-id))))))

(defn- discovered-retention-targets
  []
  (let [configured (some-> (get env :bitool-ingest-retention-targets)
                           str
                           string/trim)]
    (if (seq configured)
      (->> (string/split configured #",")
           (map string/trim)
           (remove string/blank?)
           (map parse-retention-target)
           (remove nil?)
           vec)
      (->> (db/list-graph-ids)
           (mapcat (fn [graph-id]
                     (try
                       (let [graph (db/getGraph graph-id)]
                         (map (fn [api-node-id]
                                {:graph-id graph-id
                                 :api-node-id api-node-id
                                 :endpoint-name nil})
                              (retention-ready-api-node-ids graph-id graph)))
                       (catch Exception e
                         (log/warn e "Skipping graph during manifest retention discovery"
                                   {:graph_id graph-id})
                         []))))
           vec))))

(defn cleanup-ingest-retention!
  []
  (let [archive-days       (default-artifact-archive-days)
        retention-days     (default-artifact-retention-days archive-days)
        artifact-store     (name (artifact-store-mode))
        manifest-mode?     (parse-bool-env :bitool-ingest-manifest-retention-enabled true)]
    (if-not manifest-mode?
      {:artifact_store artifact-store
       :maintenance_mode "disabled_without_manifest_coordination"
       :manifest_retention_enabled false
       :unsafe_cleanup_enabled false
       :unsafe_cleanup_supported false
       :targets_discovered 0
       :targets_processed 0
       :archived_count 0
       :deleted_count 0
       :bad_record_payload_archived_count 0
       :bad_record_metadata_deleted_count 0
       :errors []
       :target_summaries []
       :archive_days archive-days
       :retention_days retention-days}
      (let [targets          (discovered-retention-targets)
            per-target-limit (parse-int-env :bitool-ingest-retention-batch-limit 500)
            summary          (reduce
                              (fn [acc {:keys [graph-id api-node-id endpoint-name] :as target}]
                                (try
                                  (let [result (apply-api-retention!
                                                graph-id
                                                api-node-id
                                                {:endpoint-name endpoint-name
                                                 :archive-days archive-days
                                                 :retention-days retention-days
                                                 :limit per-target-limit
                                                 :archived-by "maintenance"})]
                                    (-> acc
                                        (update :archived_count + (long (:archived_count result)))
                                        (update :deleted_count + (long (:deleted_count result)))
                                        (update :bad_record_payload_archived_count + (long (or (:bad_record_payload_archived_count result) 0)))
                                        (update :bad_record_metadata_deleted_count + (long (or (:bad_record_metadata_deleted_count result) 0)))
                                        (update :targets_processed inc)
                                        (update :target_summaries conj (assoc result :target target))))
                                  (catch Exception e
                                    (log/error e "Manifest-aware retention maintenance target failed" target)
                                    (-> acc
                                        (update :targets_processed inc)
                                        (update :errors conj (merge target {:error (.getMessage e)}))))))
                              {:archived_count 0
                               :deleted_count 0
                               :bad_record_payload_archived_count 0
                               :bad_record_metadata_deleted_count 0
                               :targets_processed 0
                               :target_summaries []
                               :errors []}
                              targets)]
        {:artifact_store artifact-store
         :maintenance_mode "manifest_aware"
         :manifest_retention_enabled true
         :unsafe_cleanup_enabled false
         :unsafe_cleanup_supported false
         :targets_discovered (count targets)
         :targets_processed (:targets_processed summary)
         :archived_count (:archived_count summary)
         :deleted_count (:deleted_count summary)
         :bad_record_payload_archived_count (:bad_record_payload_archived_count summary)
         :bad_record_metadata_deleted_count (:bad_record_metadata_deleted_count summary)
         :errors (:errors summary)
         :target_summaries (:target_summaries summary)
         :archive_days archive-days
         :retention_days retention-days}))))

(defn- transactional-batch-commit-dbtype?
  [dbtype]
  (contains? #{"postgresql" "mysql"} (some-> dbtype str string/lower-case)))

(defn- atomic-batch-commit?
  [conn-id]
  (and (parse-bool-env :ingest-atomic-batch-commit true)
       (transactional-batch-commit-dbtype? (connection-dbtype conn-id))))

(defn- with-batch-commit
  [conn-id f]
  (if (atomic-batch-commit? conn-id)
    (let [db-spec (db/create-dbspec-from-id conn-id)
          ds      (jdbc/get-datasource db-spec)]
      (jdbc/with-transaction [tx ds]
        (binding [*batch-sql-opts* tx]
          (f))))
    (f)))

(defn- committed-manifest-row
  [{:keys [batch-id run-id source-system endpoint-name table-name batch-seq row-count
           bad-record-count byte-count page-count partition-dates max-watermark next-cursor
           source-bad-record-ids artifact-path artifact-checksum started-at committed-at active rollback-reason
           rolled-back-by rolled-back-at archived-at status]}]
  {:batch_id batch-id
   :run_id run-id
   :source_system source-system
   :endpoint_name endpoint-name
   :table_name table-name
   :batch_seq batch-seq
   :status (or status "committed")
   :row_count row-count
   :bad_record_count bad-record-count
   :byte_count byte-count
   :page_count page-count
   :partition_dates_json (when (seq partition-dates) (json/generate-string partition-dates))
   :source_bad_record_ids_json (when (seq source-bad-record-ids) (json/generate-string source-bad-record-ids))
   :max_watermark max-watermark
   :next_cursor next-cursor
   :artifact_path artifact-path
   :artifact_checksum artifact-checksum
   :active (if (nil? active) true active)
   :rollback_reason rollback-reason
   :rolled_back_by rolled-back-by
   :rolled_back_at_utc (some-> rolled-back-at str)
   :archived_at_utc (some-> archived-at str)
   :started_at_utc (str started-at)
   :committed_at_utc (str committed-at)})

(defn- preparing-manifest-row
  [{:keys [batch-id run-id source-system endpoint-name table-name batch-seq row-count
           bad-record-count byte-count page-count partition-dates max-watermark next-cursor
           source-bad-record-ids artifact-path artifact-checksum started-at]}]
  (committed-manifest-row
   {:batch-id batch-id
    :run-id run-id
    :source-system source-system
    :endpoint-name endpoint-name
    :table-name table-name
    :batch-seq batch-seq
    :row-count row-count
    :bad-record-count bad-record-count
    :byte-count byte-count
    :page-count page-count
    :partition-dates partition-dates
    :source-bad-record-ids source-bad-record-ids
    :max-watermark max-watermark
    :next-cursor next-cursor
    :artifact-path artifact-path
    :artifact-checksum artifact-checksum
    :started-at started-at
    :committed-at started-at
    :active false
    :status "preparing"}))

(defn- pending-checkpoint-manifest-row
  [{:keys [batch-id run-id source-system endpoint-name table-name batch-seq row-count
           bad-record-count byte-count page-count partition-dates max-watermark next-cursor
           source-bad-record-ids artifact-path artifact-checksum started-at committed-at]}]
  (committed-manifest-row
   {:batch-id batch-id
    :run-id run-id
    :source-system source-system
    :endpoint-name endpoint-name
    :table-name table-name
    :batch-seq batch-seq
    :row-count row-count
    :bad-record-count bad-record-count
    :byte-count byte-count
    :page-count page-count
    :partition-dates partition-dates
    :source-bad-record-ids source-bad-record-ids
    :max-watermark max-watermark
    :next-cursor next-cursor
    :artifact-path artifact-path
    :artifact-checksum artifact-checksum
    :started-at started-at
    :committed-at committed-at
    :active false
    :status "pending_checkpoint"}))

(defn- committed-manifest-rows
  ([conn-id manifest-table run-id endpoint-name]
   (committed-manifest-rows conn-id manifest-table run-id nil endpoint-name))
  ([conn-id manifest-table run-id source-system endpoint-name]
   (jdbc/execute!
    (sql-opts conn-id)
    (cond-> [(str "SELECT * FROM " (validated-qualified-table-name manifest-table)
                  " WHERE run_id = ?"
                  (when source-system " AND source_system = ?")
                  " AND endpoint_name = ? AND status = 'committed' AND active = TRUE"
                  " AND artifact_path IS NOT NULL"
                  " ORDER BY batch_seq ASC")
              run-id]
      source-system (conj source-system)
      true (conj endpoint-name)))))

(defn- query-manifest-rows
  [conn-id manifest-table {:keys [source-system endpoint-name run-id status active-only? replayable-only? archived-only? limit offset order]}]
  (let [clauses ["1=1"]
        params  []
        [clauses params] (if source-system
                           [(conj clauses "source_system = ?") (conj params source-system)]
                           [clauses params])
        [clauses params] (if endpoint-name
                           [(conj clauses "endpoint_name = ?") (conj params endpoint-name)]
                           [clauses params])
        [clauses params] (if run-id
                           [(conj clauses "run_id = ?") (conj params run-id)]
                           [clauses params])
        [clauses params] (if status
                           [(conj clauses "status = ?") (conj params status)]
                           [clauses params])
        [clauses params] (if active-only?
                           [(conj clauses "active = TRUE") params]
                           [clauses params])
        [clauses params] (if replayable-only?
                           [(conj clauses "status = 'committed'" "active = TRUE" "artifact_path IS NOT NULL")
                            params]
                           [clauses params])
        [clauses params] (cond
                           (true? archived-only?)
                           [(conj clauses "archived_at_utc IS NOT NULL") params]

                           (false? archived-only?)
                           [(conj clauses "archived_at_utc IS NULL") params]

                           :else
                           [clauses params])
        limit           (databricks-query-int conn-id (max 1 (min 500 (or limit 100))))
        offset          (databricks-query-int conn-id (max 0 (or offset 0)))
        order-direction (if (= :asc order) "ASC" "DESC")]
    (jdbc/execute!
     (sql-opts conn-id)
     (into [(str "SELECT * FROM " (validated-qualified-table-name manifest-table)
                 " WHERE " (string/join " AND " clauses)
                 " ORDER BY COALESCE(committed_at_utc, started_at_utc) " order-direction
                 ", batch_seq " order-direction
                 " LIMIT ? OFFSET ?")]
           (concat params [limit offset])))))

(defn- manifest-row-by-batch-id
  [conn-id manifest-table batch-id]
  (jdbc/execute-one!
   (sql-opts conn-id)
   [(str "SELECT * FROM " (validated-qualified-table-name manifest-table)
         " WHERE batch_id = ?")
    batch-id]))

(defn- query-schema-snapshot-rows
  [conn-id schema-snapshot-table {:keys [graph-id api-node-id endpoint-name limit]}]
  (jdbc/execute!
   (sql-opts conn-id)
   [(str "SELECT *
          FROM " (validated-qualified-table-name schema-snapshot-table) "
          WHERE graph_id = ? AND api_node_id = ? AND endpoint_name = ?
          ORDER BY captured_at_utc DESC
          LIMIT ?")
    graph-id
    api-node-id
    endpoint-name
    (long (max 1 (min 500 (or limit 100))))]))

(defn- query-schema-approval-rows
  [conn-id schema-approval-table {:keys [graph-id api-node-id endpoint-name promoted-only limit]}]
  (let [clauses ["graph_id = ?" "api_node_id = ?" "endpoint_name = ?"]
        params  [graph-id api-node-id endpoint-name]
        [clauses params] (if promoted-only
                           [(conj clauses "promoted = TRUE") params]
                           [clauses params])]
    (jdbc/execute!
     (sql-opts conn-id)
     (into [(str "SELECT *
                  FROM " (validated-qualified-table-name schema-approval-table) "
                  WHERE " (string/join " AND " clauses) "
                  ORDER BY reviewed_at_utc DESC
                  LIMIT ?")]
           (concat params [(long (max 1 (min 500 (or limit 100))))])))))

(defn- latest-promoted-schema-approval
  [conn-id schema-approval-table graph-id api-node-id endpoint-name]
  (first (query-schema-approval-rows conn-id schema-approval-table
                                     {:graph-id graph-id
                                      :api-node-id api-node-id
                                      :endpoint-name endpoint-name
                                      :promoted-only true
                                      :limit 1})))

(defn- with-schema-promotion-lock
  [conn-id graph-id api-node-id endpoint-name f]
  (let [dbtype (some-> (connection-dbtype conn-id) string/lower-case)
        lock-key (format "schema-promotion:%s:%s:%s" graph-id api-node-id endpoint-name)]
    (cond
      (= "postgresql" dbtype)
      (do
        (jdbc/execute! (sql-opts conn-id)
                       ["SELECT pg_advisory_xact_lock(hashtext(?))" lock-key])
        (f))

      (= "mysql" dbtype)
      (let [lock-row (jdbc/execute-one! (sql-opts conn-id)
                                        ["SELECT GET_LOCK(?, 30) AS locked" lock-key])]
        (when-not (contains? #{1 true "1"} (:locked lock-row))
          (throw (ex-info "Could not acquire schema promotion lock"
                          {:status 409
                           :failure_class "schema_promotion_lock_timeout"
                           :endpoint_name endpoint-name})))
        (try
          (f)
          (finally
            (jdbc/execute-one! (sql-opts conn-id)
                               ["SELECT RELEASE_LOCK(?)" lock-key]))))

      :else
      (f))))

(defn- schema-approval-summary
  [row]
  (let [inferred-fields-json (:inferred_fields_json row)
        inferred-fields (if (string? inferred-fields-json)
                          (json/parse-string inferred-fields-json true)
                          inferred-fields-json)
        field-decisions-raw (:field_decisions row)
        field-decisions (when field-decisions-raw
                          (if (string? field-decisions-raw)
                            (try (json/parse-string field-decisions-raw true) (catch Exception _ nil))
                            field-decisions-raw))]
    (cond-> {:graph_id (:graph_id row)
             :api_node_id (:api_node_id row)
             :endpoint_name (:endpoint_name row)
             :schema_hash (:schema_hash row)
             :review_state (or (:review_state row) "pending")
             :review_notes (:review_notes row)
             :reviewed_by (:reviewed_by row)
             :reviewed_at_utc (:reviewed_at_utc row)
             :promoted (truthy-db-bool? (:promoted row))
             :promoted_at_utc (:promoted_at_utc row)
             :inferred_fields inferred-fields}
      field-decisions (assoc :field_decisions field-decisions))))

(defn- preparing-manifest-rows
  ([conn-id manifest-table endpoint-name]
   (preparing-manifest-rows conn-id manifest-table nil endpoint-name))
  ([conn-id manifest-table source-system endpoint-name]
   (jdbc/execute!
    (sql-opts conn-id)
    (cond-> [(str "SELECT * FROM " (validated-qualified-table-name manifest-table)
                  " WHERE endpoint_name = ?"
                  (when source-system " AND source_system = ?")
                  " AND status = 'preparing'"
                  " ORDER BY batch_seq ASC")
              endpoint-name]
      source-system (conj source-system)))))

(defn- incomplete-manifest-rows
  ([conn-id manifest-table endpoint-name]
   (incomplete-manifest-rows conn-id manifest-table nil endpoint-name))
  ([conn-id manifest-table source-system endpoint-name]
   (jdbc/execute!
    (sql-opts conn-id)
    (cond-> [(str "SELECT * FROM " (validated-qualified-table-name manifest-table)
                  " WHERE endpoint_name = ?"
                  (when source-system " AND source_system = ?")
                  " AND status IN ('preparing', 'pending_checkpoint')"
                  " ORDER BY batch_seq ASC")
              endpoint-name]
      source-system (conj source-system)))))

(defn- query-bad-record-rows
  [conn-id bad-records-table {:keys [source-system endpoint-name run-id batch-id limit include-succeeded?]}]
  (let [clauses ["1=1"]
        params  []
        [clauses params] (if source-system
                           [(conj clauses "source_system = ?") (conj params source-system)]
                           [clauses params])
        [clauses params] (if endpoint-name
                           [(conj clauses "endpoint_name = ?") (conj params endpoint-name)]
                           [clauses params])
        [clauses params] (if run-id
                           [(conj clauses "run_id = ?") (conj params run-id)]
                           [clauses params])
        [clauses params] (if batch-id
                           [(conj clauses "batch_id = ?") (conj params batch-id)]
                           [clauses params])
        [clauses params] (if include-succeeded?
                           [clauses params]
                           [(conj clauses "COALESCE(replay_status, 'pending') <> 'succeeded'") params])
        limit           (databricks-query-int conn-id (max 1 (min 5000 (or limit 1000))))]
    (jdbc/execute!
     (sql-opts conn-id)
     (into [(str "SELECT * FROM " (validated-qualified-table-name bad-records-table)
                 " WHERE " (string/join " AND " clauses)
                 " ORDER BY created_at_utc ASC LIMIT ?")]
           (concat params [limit])))))

(defn- query-bad-record-retention-rows
  [conn-id bad-records-table {:keys [source-system endpoint-name limit offset]}]
  (jdbc/execute!
   (sql-opts conn-id)
   (cond-> [(str "SELECT bad_record_id, run_id, batch_id, source_system, endpoint_name, payload_json, row_json,
                         error_message, replay_status, created_at_utc, payload_archive_ref, payload_archived_at_utc
                  FROM " (validated-qualified-table-name bad-records-table) "
                  WHERE endpoint_name = ?"
                 (when source-system " AND source_system = ?")
                 " ORDER BY created_at_utc ASC
                   LIMIT ? OFFSET ?")
              endpoint-name]
     source-system (conj source-system)
     true (conj (databricks-query-int conn-id (max 1 (min 5000 (or limit 1000))))
                (databricks-query-int conn-id (max 0 (or offset 0)))))))

(defn- mark-bad-record-payload-archived!
  [conn-id bad-records-table bad-record-id archive-ref archived-at]
  (jdbc/execute!
   (sql-opts conn-id)
   [(str "UPDATE " (validated-qualified-table-name bad-records-table) "
          SET payload_archive_ref = ?,
              payload_archived_at_utc = ?,
              payload_json = NULL,
              row_json = NULL
          WHERE bad_record_id = ?")
    archive-ref
    (str archived-at)
    bad-record-id]))

(defn- delete-bad-record!
  [conn-id bad-records-table bad-record-id]
  (:next.jdbc/update-count
   (jdbc/execute-one!
    (sql-opts conn-id)
    [(str "DELETE FROM " (validated-qualified-table-name bad-records-table)
          " WHERE bad_record_id = ?")
     bad-record-id])))

(defn- query-committed-replay-manifests
  [conn-id manifest-table source-system endpoint-name]
  (jdbc/execute!
   (sql-opts conn-id)
   [(str "SELECT batch_id, source_bad_record_ids_json FROM " (validated-qualified-table-name manifest-table)
         " WHERE source_system = ?"
         " AND endpoint_name = ?"
         " AND status = 'committed'"
         " AND active = TRUE"
         " AND source_bad_record_ids_json IS NOT NULL"
         " ORDER BY committed_at_utc DESC")
    source-system
    endpoint-name]))

(defn- manifest-source-bad-record-ids
  [manifest]
  (let [raw (:source_bad_record_ids_json manifest)
        parsed (try
                 (cond
                   (nil? raw) []
                   (string? raw) (json/parse-string raw true)
                   (sequential? raw) raw
                   :else [])
                 (catch Exception e
                   (log/warn e "Ignoring malformed source_bad_record_ids_json on manifest"
                             {:batch_id (:batch_id manifest)
                              :endpoint_name (:endpoint_name manifest)})
                   []))]
    (->> parsed
         (map non-blank-str)
         (remove nil?)
         distinct
         vec)))

(defn- committed-replayed-source-bad-record-ids
  [conn-id manifest-table source-system endpoint-name]
  (->> (query-committed-replay-manifests conn-id manifest-table source-system endpoint-name)
       (mapcat manifest-source-bad-record-ids)
       set))

(defn- query-manifest-closure-summary
  [conn-id manifest-table {:keys [source-system endpoint-name limit]}]
  (let [manifest-table (validated-qualified-table-name manifest-table)
        limit (long (max 1 (min 500 (or limit 500))))
        count-row (jdbc/execute-one!
                   (sql-opts conn-id)
                   [(str "SELECT COUNT(*) AS cnt
                          FROM " manifest-table "
                          WHERE source_system = ?
                            AND endpoint_name = ?")
                    source-system
                    endpoint-name])
        incomplete (jdbc/execute!
                    (sql-opts conn-id)
                    [(str "SELECT batch_id
                           FROM " manifest-table "
                           WHERE source_system = ?
                             AND endpoint_name = ?
                             AND status IN ('preparing', 'pending_checkpoint')
                           ORDER BY COALESCE(committed_at_utc, started_at_utc) DESC, batch_seq DESC
                           LIMIT ?")
                     source-system
                     endpoint-name
                     limit])
        active-non-committed (jdbc/execute!
                              (sql-opts conn-id)
                              [(str "SELECT batch_id
                                     FROM " manifest-table "
                                     WHERE source_system = ?
                                       AND endpoint_name = ?
                                       AND active = TRUE
                                       AND status <> 'committed'
                                     ORDER BY COALESCE(committed_at_utc, started_at_utc) DESC, batch_seq DESC
                                     LIMIT ?")
                               source-system
                               endpoint-name
                               limit])]
    {:manifest-count (long (or (:cnt count-row) 0))
     :incomplete-batch-ids (mapv :batch_id incomplete)
     :active-non-committed-batch-ids (mapv :batch_id active-non-committed)}))

(defn- mark-bad-record-replay-statuses!
  [conn-id bad-records-table bad-record-ids status replay-run-id replay-error]
  (let [bad-record-ids (->> bad-record-ids
                            (map #(some-> % str non-blank-str))
                            (remove nil?)
                            distinct
                            vec)]
    (when (seq bad-record-ids)
      (let [placeholders (string/join ", " (repeat (count bad-record-ids) "?"))]
        (jdbc/execute!
         (sql-opts conn-id)
         (into [(str "UPDATE " (validated-qualified-table-name bad-records-table) "
                     SET replay_status = ?,
                         replayed_run_id = ?,
                         replayed_at_utc = now(),
                         replay_error_message = ?
                     WHERE bad_record_id IN (" placeholders ")")
                status
                (when replay-run-id (str replay-run-id))
                replay-error]
               bad-record-ids))))))

(defn- replay-batch-id
  [endpoint-name source-bad-record-ids]
  (let [source-bad-record-ids (->> source-bad-record-ids (map str) sort vec)
        hash-prefix           (subs (stable-json-hash {:endpoint_name endpoint-name
                                                       :source_bad_record_ids source-bad-record-ids})
                                    0
                                    20)]
    (str "badreplay-" (safe-path-segment endpoint-name) "-" hash-prefix)))

(defn- checkpoint-covers-manifest?
  [checkpoint-row manifest]
  (let [checkpoint-run-id (non-blank-str (:last_successful_run_id checkpoint-row))
        manifest-run-id   (non-blank-str (:run_id manifest))
        checkpoint-batch-id (non-blank-str (:last_successful_batch_id checkpoint-row))
        manifest-batch-id   (non-blank-str (:batch_id manifest))
        checkpoint-batch-seq (some-> (:last_successful_batch_seq checkpoint-row) long)
        manifest-batch-seq   (some-> (:batch_seq manifest) long)
        checkpoint-cursor (non-blank-str (:last_successful_cursor checkpoint-row))
        manifest-cursor   (non-blank-str (:next_cursor manifest))
        checkpoint-watermark (non-blank-str (:last_successful_watermark checkpoint-row))
        manifest-watermark   (non-blank-str (:max_watermark manifest))
        explicit-position-match? (or checkpoint-cursor
                                     manifest-cursor
                                     checkpoint-watermark
                                     manifest-watermark)]
    (boolean
     (and checkpoint-row
          checkpoint-run-id
          manifest-run-id
          (= checkpoint-run-id manifest-run-id)
          (if (or checkpoint-batch-id
                  checkpoint-batch-seq)
            (and checkpoint-batch-id
                 manifest-batch-id
                 (some? checkpoint-batch-seq)
                 (some? manifest-batch-seq)
                 (= checkpoint-batch-id manifest-batch-id)
                 (= checkpoint-batch-seq manifest-batch-seq))
            (and explicit-position-match?
                 (= checkpoint-cursor manifest-cursor)
                 (= checkpoint-watermark manifest-watermark)))))))

(defn- api-endpoint-runtime-context
  [graph-id api-node-id endpoint-name]
  (let [g                 (db/getGraph graph-id)
        api-node          (g2/getData g api-node-id)
        target            (find-downstream-target g api-node-id)
        conn-id           (require-target-connection-id! target
                                                         "No downstream target connection found for API node"
                                                         {:graph_id graph-id
                                                          :api_node_id api-node-id})
        endpoint          (when endpoint-name
                            (select-endpoint! api-node endpoint-name))
        bad-records-table (when conn-id
                            (validated-qualified-table-name (audit-table target conn-id "bad_records")))
        manifest-table    (when conn-id
                            (validated-qualified-table-name (audit-table target conn-id "run_batch_manifest")))
        schema-snapshot-table (when conn-id
                                (validated-qualified-table-name (audit-table target conn-id "endpoint_schema_snapshot")))
        schema-approval-table (when conn-id
                                (validated-qualified-table-name (audit-table target conn-id "endpoint_schema_approval")))]
    (ensure-table! conn-id manifest-table batch-manifest-columns {})
    (ensure-batch-manifest-columns! conn-id manifest-table)
    (ensure-table! conn-id bad-records-table bronze/bad-record-columns {})
    (ensure-bad-record-columns! conn-id bad-records-table)
    (ensure-table! conn-id schema-snapshot-table endpoint-schema-snapshot-columns {})
    (ensure-table! conn-id schema-approval-table endpoint-schema-approval-columns {})
    (ensure-schema-approval-columns! conn-id schema-approval-table)
    {:graph graph-id
     :api-node-id api-node-id
     :api-node api-node
     :target target
     :conn-id conn-id
     :source-system (or (:source_system api-node) "samara")
     :endpoint endpoint
     :bad-records-table bad-records-table
     :manifest-table manifest-table
     :schema-snapshot-table schema-snapshot-table
     :schema-approval-table schema-approval-table}))

(defn schema-drift-target-context
  [graph-id api-node-id endpoint-name]
  (let [{:keys [conn-id target endpoint] :as ctx} (api-endpoint-runtime-context graph-id api-node-id endpoint-name)
        workspace-context (control-plane/graph-workspace-context graph-id)
        endpoint-name' (or endpoint-name (:endpoint_name endpoint))]
    {:workspace_key (:workspace_key workspace-context)
     :graph_id graph-id
     :api_node_id api-node-id
     :endpoint_name endpoint-name'
     :conn_id conn-id
     :warehouse (some-> (connection-dbtype conn-id) str string/lower-case)
     :target_table (validated-qualified-table-name (endpoint->table-name target endpoint))
     :schema_snapshot_table (:schema-snapshot-table ctx)
     :schema_approval_table (:schema-approval-table ctx)}))

(defn resolve-api-schema-approval
  [graph-id api-node-id {:keys [endpoint-name schema-hash promoted-only]}]
  (let [{:keys [conn-id schema-approval-table endpoint]} (api-endpoint-runtime-context graph-id api-node-id endpoint-name)
        endpoint-name' (or endpoint-name (:endpoint_name endpoint))
        approvals (query-schema-approval-rows conn-id schema-approval-table
                                              {:graph-id graph-id
                                               :api-node-id api-node-id
                                               :endpoint-name endpoint-name'
                                               :promoted-only promoted-only
                                               :limit 500})
        selected (if-let [schema-hash' (non-blank-str schema-hash)]
                   (first (filter #(= schema-hash' (:schema_hash %)) approvals))
                   (first approvals))]
    (when selected
      (schema-approval-summary selected))))

(defn- source-kind-config-key
  [source-kind]
  (case source-kind
    :kafka :topic_configs
    :file :file_configs
    (throw (ex-info "Unsupported Bronze source kind"
                    {:source_kind source-kind
                     :status 400}))))

(defn- source-kind-node-btype
  [source-kind]
  (case source-kind
    :kafka "Kf"
    :file "Fs"
    nil))

(defn- source-kind-label
  [source-kind]
  (case source-kind
    :kafka "Kafka source"
    :file "File source"
    "Bronze source"))

(defn- bronze-source-runtime-context
  [graph-id node-id source-kind endpoint-name]
  (let [g                 (db/getGraph graph-id)
        source-node       (g2/getData g node-id)
        expected-btype    (source-kind-node-btype source-kind)
        _                 (when-not (= expected-btype (:btype source-node))
                            (throw (ex-info "Node does not match the requested Bronze source kind"
                                            {:graph_id graph-id
                                             :node_id node-id
                                             :source_kind source-kind
                                             :btype (:btype source-node)
                                             :status 400})))
        config-key        (source-kind-config-key source-kind)
        config            (select-source-config! source-node
                                                 config-key
                                                 endpoint-name
                                                 (str (source-kind-label source-kind) " node has no enabled source configs")
                                                 (source-kind-label source-kind))
        target            (find-downstream-target g node-id)
        conn-id           (require-target-connection-id! target
                                                         (str "No downstream target connection found for " (source-kind-label source-kind) " node")
                                                         {:graph_id graph-id
                                                          :node_id node-id
                                                          :source_kind source-kind})
        source-system     (or (:source_system source-node)
                              (name source-kind))
        checkpoint-table  (when conn-id
                            (validated-qualified-table-name (audit-table target conn-id "ingestion_checkpoint")))
        run-detail-table  (when conn-id
                            (validated-qualified-table-name (audit-table target conn-id "endpoint_run_detail")))
        bad-records-table (when conn-id
                            (validated-qualified-table-name (audit-table target conn-id "bad_records")))
        manifest-table    (when conn-id
                            (validated-qualified-table-name (audit-table target conn-id "run_batch_manifest")))]
    (ensure-table! conn-id checkpoint-table checkpoint/ingestion-checkpoint-columns {})
    (ensure-checkpoint-columns! conn-id checkpoint-table)
    (ensure-table! conn-id run-detail-table endpoint-run-detail-columns {})
    (ensure-table! conn-id manifest-table batch-manifest-columns {})
    (ensure-batch-manifest-columns! conn-id manifest-table)
    (ensure-table! conn-id bad-records-table bronze/bad-record-columns {})
    (ensure-bad-record-columns! conn-id bad-records-table)
    {:graph-id graph-id
     :node-id node-id
     :graph g
     :source-kind source-kind
     :source-node source-node
     :target target
     :conn-id conn-id
     :source-system source-system
     :endpoint config
     :checkpoint-table checkpoint-table
     :run-detail-table run-detail-table
     :bad-records-table bad-records-table
     :manifest-table manifest-table}))

(defn- manifest-committed-instant
  [manifest]
  (or (parse-instant-safe (:committed_at_utc manifest))
      (parse-instant-safe (:started_at_utc manifest))))

(defn- manifest-artifact-state
  [manifest]
  (cond
    (seq (:artifact_path manifest))
    (if (:archived_at_utc manifest) "archived" "active")

    (:archived_at_utc manifest) "deleted"
    :else "missing"))

(defn- manifest-replayable?
  [manifest]
  (boolean
   (and (= "committed" (:status manifest))
        (truthy-db-bool? (:active manifest))
        (seq (:artifact_path manifest)))))

(defn- manifest-available-actions
  [manifest]
  {:rollback (not (#{"rolled_back" "aborted"} (:status manifest)))
   :archive (and (seq (:artifact_path manifest))
                 (nil? (:archived_at_utc manifest)))
   :replay (manifest-replayable? manifest)})

(defn- manifest-summary
  [manifest]
  (let [partition-dates (when-let [raw (:partition_dates_json manifest)]
                          (if (string? raw)
                            (json/parse-string raw true)
                            raw))
        source-bad-record-ids (manifest-source-bad-record-ids manifest)]
    {:batch_id (:batch_id manifest)
     :run_id (:run_id manifest)
     :endpoint_name (:endpoint_name manifest)
     :table_name (:table_name manifest)
     :batch_seq (:batch_seq manifest)
     :status (:status manifest)
     :active (truthy-db-bool? (:active manifest))
     :row_count (long (or (:row_count manifest) 0))
     :bad_record_count (long (or (:bad_record_count manifest) 0))
     :byte_count (long (or (:byte_count manifest) 0))
     :page_count (long (or (:page_count manifest) 0))
     :partition_dates partition-dates
     :source_bad_record_ids source-bad-record-ids
     :max_watermark (:max_watermark manifest)
     :next_cursor (:next_cursor manifest)
     :artifact_path (:artifact_path manifest)
     :artifact_checksum (:artifact_checksum manifest)
     :artifact_state (manifest-artifact-state manifest)
     :replayable (manifest-replayable? manifest)
     :rollback_reason (:rollback_reason manifest)
     :rolled_back_by (:rolled_back_by manifest)
     :rolled_back_at_utc (:rolled_back_at_utc manifest)
     :archived_at_utc (:archived_at_utc manifest)
     :started_at_utc (:started_at_utc manifest)
     :committed_at_utc (:committed_at_utc manifest)
     :available_actions (manifest-available-actions manifest)}))

(defn list-api-batches
  [graph-id api-node-id {:keys [endpoint-name run-id status active-only replayable-only archived-only limit]}]
  (let [{:keys [conn-id manifest-table endpoint source-system]} (api-endpoint-runtime-context graph-id api-node-id endpoint-name)
        rows (query-manifest-rows conn-id
                                  manifest-table
                                  {:source-system source-system
                                   :endpoint-name (or endpoint-name (:endpoint_name endpoint))
                                   :run-id run-id
                                   :status status
                                   :active-only? active-only
                                   :replayable-only? replayable-only
                                   :archived-only? archived-only
                                   :limit limit})]
    {:graph_id graph-id
     :api_node_id api-node-id
     :endpoint_name (or endpoint-name (:endpoint_name endpoint))
     :batch_count (count rows)
     :batches (mapv manifest-summary rows)}))

(defn list-bronze-source-batches
  [graph-id node-id {:keys [source-kind endpoint-name run-id status active-only replayable-only archived-only limit]}]
  (let [{:keys [conn-id manifest-table endpoint source-system]} (bronze-source-runtime-context graph-id node-id source-kind endpoint-name)
        endpoint-name' (:endpoint_name endpoint)
        rows (query-manifest-rows conn-id
                                  manifest-table
                                  {:source-system source-system
                                   :endpoint-name endpoint-name'
                                   :run-id run-id
                                   :status status
                                   :active-only? active-only
                                   :replayable-only? replayable-only
                                   :archived-only? archived-only
                                   :limit limit})]
    {:graph_id graph-id
     :node_id node-id
     :source_kind (name source-kind)
     :endpoint_name endpoint-name'
     :batch_count (count rows)
     :batches (mapv manifest-summary rows)}))

(defn list-api-bad-records
  [graph-id api-node-id {:keys [endpoint-name run-id batch-id limit include-succeeded include-payloads]}]
  (let [{:keys [conn-id bad-records-table endpoint source-system]} (api-endpoint-runtime-context graph-id api-node-id endpoint-name)
        endpoint-name' (or endpoint-name (:endpoint_name endpoint))
        rows           (query-bad-record-rows conn-id bad-records-table
                                              {:source-system source-system
                                               :endpoint-name endpoint-name'
                                               :run-id run-id
                                               :batch-id batch-id
                                               :limit limit
                                               :include-succeeded? include-succeeded})
        rows           (mapv (fn [row]
                               (cond-> {:bad_record_id (:bad_record_id row)
                                        :run_id (:run_id row)
                                        :batch_id (:batch_id row)
                                        :source_system (:source_system row)
                                        :endpoint_name (:endpoint_name row)
                                        :error_message (:error_message row)
                                        :replay_status (or (:replay_status row) "pending")
                                        :replayed_run_id (:replayed_run_id row)
                                        :replayed_at_utc (:replayed_at_utc row)
                                        :replay_error_message (:replay_error_message row)
                                        :created_at_utc (:created_at_utc row)}
                                 include-payloads (assoc :payload_json (:payload_json row)
                                                         :row_json (:row_json row))))
                             rows)]
    {:graph_id graph-id
     :api_node_id api-node-id
     :endpoint_name endpoint-name'
     :bad_record_count (count rows)
     :bad_records rows}))

(defn list-api-schema-approvals
  [graph-id api-node-id {:keys [endpoint-name include-snapshots limit promoted-only]}]
  (let [{:keys [conn-id schema-approval-table schema-snapshot-table endpoint]} (api-endpoint-runtime-context graph-id api-node-id endpoint-name)
        endpoint-name' (or endpoint-name (:endpoint_name endpoint))
        approvals (query-schema-approval-rows conn-id schema-approval-table
                                              {:graph-id graph-id
                                               :api-node-id api-node-id
                                               :endpoint-name endpoint-name'
                                               :promoted-only promoted-only
                                               :limit limit})
        snapshots (when include-snapshots
                    (query-schema-snapshot-rows conn-id schema-snapshot-table
                                                {:graph-id graph-id
                                                 :api-node-id api-node-id
                                                 :endpoint-name endpoint-name'
                                                 :limit (or limit 100)}))]
    {:graph_id graph-id
     :api_node_id api-node-id
     :endpoint_name endpoint-name'
     :approval_count (count approvals)
     :approvals (mapv schema-approval-summary approvals)
     :snapshots (when include-snapshots
                  (mapv (fn [row]
                          {:captured_at_utc (:captured_at_utc row)
                           :schema_mode (:schema_mode row)
                           :schema_enforcement_mode (:schema_enforcement_mode row)
                           :sample_record_count (long (or (:sample_record_count row) 0))
                           :schema_hash (schema-fields-hash (:inferred_fields_json row))
                           :inferred_fields (if (string? (:inferred_fields_json row))
                                              (json/parse-string (:inferred_fields_json row) true)
                                              (:inferred_fields_json row))
                           :schema_drift (when-let [drift (:schema_drift_json row)]
                                           (if (string? drift) (json/parse-string drift true) drift))})
                        snapshots))}))

(defn review-api-schema!
  [graph-id api-node-id {:keys [endpoint-name schema-hash review-state review-notes reviewed-by promote? field-decisions]
                         :or {reviewed-by "system"}}]
  (let [{:keys [conn-id schema-approval-table schema-snapshot-table endpoint]} (api-endpoint-runtime-context graph-id api-node-id endpoint-name)
        endpoint-name' (or endpoint-name (:endpoint_name endpoint))
        review-state (-> (or (non-blank-str review-state) "pending") string/lower-case)
        snapshots (query-schema-snapshot-rows conn-id schema-snapshot-table
                                              {:graph-id graph-id
                                               :api-node-id api-node-id
                                               :endpoint-name endpoint-name'
                                               :limit 200})
        selected-snapshot (if-let [requested-hash (non-blank-str schema-hash)]
                            (first (filter #(= requested-hash (schema-fields-hash (:inferred_fields_json %))) snapshots))
                            (first snapshots))]
    (when-not (contains? #{"pending" "approved" "rejected"} review-state)
      (throw (ex-info "review_state must be pending, approved, or rejected"
                      {:status 400
                       :review_state review-state})))
    (when-not selected-snapshot
      (throw (ex-info "No schema snapshot found for endpoint review"
                      {:status 404
                       :graph_id graph-id
                       :api_node_id api-node-id
                       :endpoint_name endpoint-name'})))
    (let [inferred-fields-json (if (string? (:inferred_fields_json selected-snapshot))
                                 (:inferred_fields_json selected-snapshot)
                                 (json/generate-string (:inferred_fields_json selected-snapshot)))
          schema-hash (schema-fields-hash inferred-fields-json)
          promoted? (and (boolean promote?) (= "approved" review-state))
          review-time (str (now-utc))
          field-decisions-json (when field-decisions
                                (if (string? field-decisions)
                                  field-decisions
                                  (json/generate-string field-decisions)))
          row {:graph_id graph-id
               :api_node_id api-node-id
               :endpoint_name endpoint-name'
               :schema_hash schema-hash
               :review_state review-state
               :review_notes (non-blank-str review-notes)
               :reviewed_by reviewed-by
               :reviewed_at_utc review-time
               :promoted promoted?
               :promoted_at_utc (when promoted? review-time)
               :inferred_fields_json inferred-fields-json
               :field_decisions field-decisions-json}]
      (with-batch-commit
        conn-id
        (fn []
          (with-schema-promotion-lock
            conn-id
            graph-id
            api-node-id
            endpoint-name'
            (fn []
              (when promoted?
                (doseq [existing (query-schema-approval-rows conn-id schema-approval-table
                                                             {:graph-id graph-id
                                                              :api-node-id api-node-id
                                                              :endpoint-name endpoint-name'
                                                              :limit 500})]
                  (when (and (truthy-db-bool? (:promoted existing))
                             (not= (:schema_hash existing) schema-hash))
                    (replace-row! conn-id schema-approval-table
                                  [:graph_id :api_node_id :endpoint_name :schema_hash]
                                  (assoc existing :promoted false :promoted_at_utc nil)))))
              (replace-row! conn-id schema-approval-table
                            [:graph_id :api_node_id :endpoint_name :schema_hash]
                            row)))))
      (schema-approval-summary row))))

(defn promote-api-schema!
  [graph-id api-node-id {:keys [endpoint-name schema-hash reviewed-by review-notes]
                         :or {reviewed-by "system"}}]
  (review-api-schema! graph-id api-node-id
                      {:endpoint-name endpoint-name
                       :schema-hash schema-hash
                       :review-state "approved"
                       :review-notes review-notes
                       :reviewed-by reviewed-by
                       :promote? true}))

(defn verify-api-commit-closure
  [graph-id api-node-id {:keys [endpoint-name limit]}]
  (let [{:keys [conn-id manifest-table endpoint source-system]} (api-endpoint-runtime-context graph-id api-node-id endpoint-name)
        endpoint-name' (or endpoint-name (:endpoint_name endpoint))
        {:keys [manifest-count incomplete-batch-ids active-non-committed-batch-ids]}
        (query-manifest-closure-summary conn-id manifest-table
                                        {:source-system source-system
                                         :endpoint-name endpoint-name'
                                         :limit limit})]
    {:graph_id graph-id
     :api_node_id api-node-id
     :endpoint_name endpoint-name'
     :atomic_batch_commit (atomic-batch-commit? conn-id)
     :ready? (and (empty? incomplete-batch-ids) (empty? active-non-committed-batch-ids))
     :manifest_count manifest-count
     :incomplete_batch_ids incomplete-batch-ids
     :active_non_committed_batch_ids active-non-committed-batch-ids}))

(defn reset-api-checkpoint!
  [graph-id api-node-id {:keys [endpoint-name reset-to-cursor reset-to-watermark requested-by reason]
                         :or {requested-by "system"}}]
  (let [g               (db/getGraph graph-id)
        api-node        (g2/getData g api-node-id)
        target          (find-downstream-target g api-node-id)
        conn-id         (require-target-connection-id! target
                                                       "No downstream target connection found for API node"
                                                       {:graph_id graph-id
                                                        :api_node_id api-node-id})
        endpoint        (select-endpoint! api-node endpoint-name)
        source-system   (or (:source_system api-node) "samara")
        checkpoint-table (validated-qualified-table-name (audit-table target conn-id "ingestion_checkpoint"))
        manifest-table (validated-qualified-table-name (audit-table target conn-id "run_batch_manifest"))
        bad-records-table (validated-qualified-table-name (audit-table target conn-id "bad_records"))
        _               (ensure-table! conn-id checkpoint-table checkpoint/ingestion-checkpoint-columns {})
        _               (ensure-checkpoint-columns! conn-id checkpoint-table)
        _               (ensure-table! conn-id manifest-table batch-manifest-columns {})
        _               (ensure-batch-manifest-columns! conn-id manifest-table)
        _               (ensure-table! conn-id bad-records-table bronze/bad-record-columns {})
        _               (ensure-bad-record-columns! conn-id bad-records-table)
        current         (fetch-checkpoint conn-id checkpoint-table source-system (:endpoint_name endpoint))
        _               (abort-preparing-batches! conn-id {:manifest-table manifest-table
                                                           :checkpoint-table checkpoint-table
                                                           :bad-records-table bad-records-table
                                                           :source-system source-system
                                                           :endpoint-name (:endpoint_name endpoint)})
        incomplete      (incomplete-manifest-rows conn-id manifest-table source-system (:endpoint_name endpoint))]
    (when (seq incomplete)
      (throw (ex-info "Cannot reset checkpoint while endpoint has incomplete manifests"
                      {:status 409
                       :endpoint_name (:endpoint_name endpoint)
                       :incomplete_batch_ids (mapv :batch_id incomplete)})))
    (when-not (non-blank-str reason)
      (throw (ex-info "reason is required for checkpoint reset"
                      {:status 400})))
    (let [row {:source_system source-system
               :endpoint_name (:endpoint_name endpoint)
               :last_successful_watermark (non-blank-str reset-to-watermark)
               :last_attempted_watermark nil
               :last_successful_cursor (non-blank-str reset-to-cursor)
               :last_attempted_cursor nil
               :last_successful_run_id nil
               :last_successful_batch_id nil
               :last_successful_batch_seq nil
               :last_status "reset"
               :rows_ingested 0
               :updated_at_utc (str (now-utc))}]
      (replace-checkpoint-row! conn-id checkpoint-table row)
      {:graph_id graph-id
       :api_node_id api-node-id
       :endpoint_name (:endpoint_name endpoint)
       :requested_by requested-by
       :reason reason
       :previous_checkpoint current
       :checkpoint row})))

(defn api-observability-summary
  [graph-id api-node-id {:keys [endpoint-name]}]
  (let [g                 (db/getGraph graph-id)
        api-node          (g2/getData g api-node-id)
        target            (find-downstream-target g api-node-id)
        conn-id           (require-target-connection-id! target
                                                         "No downstream target connection found for API node"
                                                         {:graph_id graph-id
                                                          :api_node_id api-node-id})
        endpoint          (select-endpoint! api-node endpoint-name)
        source-system     (or (:source_system api-node) "samara")
        endpoint-name'    (:endpoint_name endpoint)
        checkpoint-table  (validated-qualified-table-name (audit-table target conn-id "ingestion_checkpoint"))
        run-detail-table  (validated-qualified-table-name (audit-table target conn-id "endpoint_run_detail"))
        manifest-table    (validated-qualified-table-name (audit-table target conn-id "run_batch_manifest"))
        bad-records-table (validated-qualified-table-name (audit-table target conn-id "bad_records"))
        checkpoint-row    (fetch-checkpoint conn-id checkpoint-table source-system endpoint-name')
        latest-run        (jdbc/execute-one!
                           (sql-opts conn-id)
                           [(str "SELECT * FROM " run-detail-table
                                 " WHERE source_system = ? AND endpoint_name = ?"
                                 " ORDER BY started_at_utc DESC LIMIT 1")
                            source-system
                            (or endpoint-name' (:endpoint_name endpoint))])
        manifests         (query-manifest-rows conn-id manifest-table
                                               {:source-system source-system
                                                :endpoint-name endpoint-name'
                                                :limit 200})
        committed-manifests (filter #(= "committed" (:status %)) manifests)
        latest-run-id     (:run_id latest-run)
        latest-run-manifests (if latest-run-id
                               (filter #(= (str latest-run-id) (str (:run_id %))) committed-manifests)
                               [])
        latest-bad-ratio  (let [rows (reduce + 0 (map #(long (or (:row_count %) 0)) latest-run-manifests))
                                bads (reduce + 0 (map #(long (or (:bad_record_count %) 0)) latest-run-manifests))
                                denom (+ rows bads)]
                            (if (pos? denom)
                              (/ (double bads) (double denom))
                              0.0))
        latency-samples   (->> committed-manifests
                               (keep (fn [m]
                                       (let [start (coerce-instant (:started_at_utc m))
                                             commit (coerce-instant (:committed_at_utc m))]
                                         (when (and start commit)
                                           (.toMillis (java.time.Duration/between start commit)))))))
        avg-commit-latency-ms (if (seq latency-samples)
                                (long (/ (reduce + 0 latency-samples) (count latency-samples)))
                                0)
        day-ago-str       (str (.minusSeconds (now-utc) 86400))
        retry-volume-24h  (long (or (:retry_volume
                                     (jdbc/execute-one!
                                      (sql-opts conn-id)
                                      [(str "SELECT COALESCE(SUM(retry_count), 0) AS retry_volume
                                            FROM " run-detail-table "
                                            WHERE source_system = ?
                                              AND endpoint_name = ?
                                              AND started_at_utc >= ?")
                                       source-system
                                       endpoint-name'
                                       day-ago-str]))
                                    0))
        bad-records-24h   (long (or (:cnt
                                     (jdbc/execute-one!
                                      (sql-opts conn-id)
                                      [(str "SELECT COUNT(*) AS cnt
                                            FROM " bad-records-table "
                                            WHERE source_system = ?
                                              AND endpoint_name = ?
                                              AND created_at_utc >= ?")
                                       source-system
                                       endpoint-name'
                                       day-ago-str]))
                                    0))
        replay-stats-7d   (jdbc/execute!
                           (db-opts db/ds)
                           [(str "SELECT status, COUNT(*) AS cnt
                                 FROM execution_run
                                 WHERE request_kind = 'api'
                                   AND trigger_type = 'replay'
                                   AND graph_id = ?
                                   AND node_id = ?
                                   AND endpoint_name = ?
                                   AND started_at_utc >= now() - interval '7 days'
                                 GROUP BY status")
                            graph-id
                            api-node-id
                            (or endpoint-name' (:endpoint_name endpoint))])
        replay-successes  (reduce + 0 (map (fn [row]
                                             (if (= "succeeded" (:status row))
                                               (long (:cnt row))
                                               0))
                                           replay-stats-7d))
        replay-failures   (reduce + 0 (map (fn [row]
                                             (if (#{ "failed" "dead_lettered"} (:status row))
                                               (long (:cnt row))
                                               0))
                                           replay-stats-7d))
        circuit-summary   (circuit-state-summary source-system endpoint)
        checkpoint-lag-seconds (let [updated (coerce-instant (:updated_at_utc checkpoint-row))]
                                 (if updated
                                   (long (max 0 (.toSeconds (java.time.Duration/between updated (now-utc)))))
                                   0))
        freshness-sla-seconds (long (or (:freshness_sla_seconds endpoint)
                                        (some-> (:freshness_sla_minutes endpoint) long (* 60))
                                        (parse-int-env :ingest-default-freshness-sla-seconds 3600)))
        checkpoint-lag-threshold (long (max (* 2 freshness-sla-seconds)
                                            (long (or (:checkpoint_alert_seconds endpoint)
                                                      (parse-int-env :ingest-checkpoint-alert-seconds (* 2 freshness-sla-seconds))))))
        bad-record-rate-threshold (double (or (:bad_record_alert_ratio endpoint)
                                              (parse-double-env :ingest-bad-record-alert-ratio 0.05)))
        retry-threshold (long (or (:retry_volume_alert_24h endpoint)
                                  (parse-int-env :ingest-retry-volume-alert-24h 50)))
        replay-failure-threshold (long (or (:replay_failure_alert_7d endpoint)
                                           (parse-int-env :ingest-replay-failure-alert-7d 3)))
        alerts (cond-> []
                 (> checkpoint-lag-seconds checkpoint-lag-threshold)
                 (conj {:code "stale_checkpoint"
                        :severity "high"
                        :checkpoint_lag_seconds checkpoint-lag-seconds
                        :threshold_seconds checkpoint-lag-threshold})
                 (> latest-bad-ratio bad-record-rate-threshold)
                 (conj {:code "high_bad_record_ratio"
                        :severity "medium"
                        :bad_record_ratio latest-bad-ratio
                        :threshold bad-record-rate-threshold})
                 (> retry-volume-24h retry-threshold)
                 (conj {:code "high_retry_volume"
                        :severity "medium"
                        :retry_volume_24h retry-volume-24h
                        :threshold retry-threshold})
                 (>= replay-failures replay-failure-threshold)
                 (conj {:code "replay_failures"
                        :severity "high"
                        :replay_failures_7d replay-failures
                        :threshold replay-failure-threshold})
                 (= "open" (:state circuit-summary))
                 (conj {:code "source_circuit_open"
                        :severity "high"
                        :failure_count_window (:failure_count_window circuit-summary)
                        :failure_threshold (:failure_threshold circuit-summary)
                        :open_until_epoch_ms (:open_until_epoch_ms circuit-summary)
                        :last_failure_class (:last_failure_class circuit-summary)}))]
    {:graph_id graph-id
     :api_node_id api-node-id
     :endpoint_name endpoint-name'
     :checkpoint_lag_seconds checkpoint-lag-seconds
     :freshness_sla_seconds freshness-sla-seconds
     :bad_record_ratio_latest_run latest-bad-ratio
     :bad_records_24h bad-records-24h
     :batch_commit_latency_ms_avg avg-commit-latency-ms
     :retry_volume_24h retry-volume-24h
     :replay_successes_7d replay-successes
     :replay_failures_7d replay-failures
     :circuit_breaker circuit-summary
     :alerts alerts
     :latest_run (when latest-run
                   {:run_id (:run_id latest-run)
                    :status (:status latest-run)
                    :started_at_utc (:started_at_utc latest-run)
                    :finished_at_utc (:finished_at_utc latest-run)
                    :rows_written (long (or (:rows_written latest-run) 0))
                    :retry_count (long (or (:retry_count latest-run) 0))})}))

(defn api-observability-alerts
  [graph-id api-node-id {:keys [endpoint-name]}]
  (let [summary (api-observability-summary graph-id api-node-id {:endpoint-name endpoint-name})]
    {:graph_id graph-id
     :api_node_id api-node-id
     :endpoint_name (:endpoint_name summary)
     :alert_count (count (:alerts summary))
     :alerts (:alerts summary)}))

(defn bronze-source-observability-summary
  [graph-id node-id {:keys [source-kind endpoint-name]}]
  (let [{:keys [conn-id checkpoint-table run-detail-table manifest-table bad-records-table endpoint source-system]}
        (bronze-source-runtime-context graph-id node-id source-kind endpoint-name)
        endpoint-name'         (:endpoint_name endpoint)
        checkpoint-row         (fetch-checkpoint conn-id checkpoint-table source-system endpoint-name')
        latest-run             (jdbc/execute-one!
                                (sql-opts conn-id)
                                [(str "SELECT * FROM " run-detail-table
                                      " WHERE source_system = ? AND endpoint_name = ?"
                                      " ORDER BY started_at_utc DESC LIMIT 1")
                                 source-system
                                 endpoint-name'])
        manifests              (query-manifest-rows conn-id manifest-table
                                                    {:source-system source-system
                                                     :endpoint-name endpoint-name'
                                                     :limit 200})
        committed-manifests    (filter #(= "committed" (:status %)) manifests)
        latest-run-id          (:run_id latest-run)
        latest-run-manifests   (if latest-run-id
                                 (filter #(= (str latest-run-id) (str (:run_id %))) committed-manifests)
                                 [])
        latest-bad-ratio       (let [rows (reduce + 0 (map #(long (or (:row_count %) 0)) latest-run-manifests))
                                     bads (reduce + 0 (map #(long (or (:bad_record_count %) 0)) latest-run-manifests))
                                     denom (+ rows bads)]
                                 (if (pos? denom)
                                   (/ (double bads) (double denom))
                                   0.0))
        latency-samples        (->> committed-manifests
                                    (keep (fn [m]
                                            (let [start (coerce-instant (:started_at_utc m))
                                                  commit (coerce-instant (:committed_at_utc m))]
                                              (when (and start commit)
                                                (.toMillis (java.time.Duration/between start commit)))))))
        avg-commit-latency-ms  (if (seq latency-samples)
                                 (long (/ (reduce + 0 latency-samples) (count latency-samples)))
                                 0)
        day-ago-str            (str (.minusSeconds (now-utc) 86400))
        retry-volume-24h       (long (or (:retry_volume
                                          (jdbc/execute-one!
                                           (sql-opts conn-id)
                                           [(str "SELECT COALESCE(SUM(retry_count), 0) AS retry_volume
                                                 FROM " run-detail-table "
                                                 WHERE source_system = ?
                                                   AND endpoint_name = ?
                                                   AND started_at_utc >= ?")
                                            source-system
                                            endpoint-name'
                                            day-ago-str]))
                                         0))
        bad-records-24h        (long (or (:cnt
                                          (jdbc/execute-one!
                                           (sql-opts conn-id)
                                           [(str "SELECT COUNT(*) AS cnt
                                                 FROM " bad-records-table "
                                                 WHERE source_system = ?
                                                   AND endpoint_name = ?
                                                   AND created_at_utc >= ?")
                                            source-system
                                            endpoint-name'
                                            day-ago-str]))
                                         0))
        replay-stats-7d        (jdbc/execute!
                                (db-opts db/ds)
                                [(str "SELECT status, COUNT(*) AS cnt
                                      FROM execution_run
                                      WHERE request_kind = ?
                                        AND trigger_type = 'replay'
                                        AND graph_id = ?
                                        AND node_id = ?
                                        AND endpoint_name = ?
                                        AND started_at_utc >= now() - interval '7 days'
                                      GROUP BY status")
                                 (name source-kind)
                                 graph-id
                                 node-id
                                 endpoint-name'])
        replay-successes       (reduce + 0 (map (fn [row]
                                                  (if (= "succeeded" (:status row))
                                                    (long (:cnt row))
                                                    0))
                                                replay-stats-7d))
        replay-failures        (reduce + 0 (map (fn [row]
                                                  (if (#{"failed" "dead_lettered"} (:status row))
                                                    (long (:cnt row))
                                                    0))
                                                replay-stats-7d))
        checkpoint-lag-seconds (let [updated (coerce-instant (:updated_at_utc checkpoint-row))]
                                 (if updated
                                   (long (max 0 (.toSeconds (java.time.Duration/between updated (now-utc)))))
                                   0))
        freshness-sla-seconds  (long (or (:freshness_sla_seconds endpoint)
                                         (some-> (:freshness_sla_minutes endpoint) long (* 60))
                                         (parse-int-env :ingest-default-freshness-sla-seconds 3600)))
        checkpoint-threshold   (long (max (* 2 freshness-sla-seconds)
                                          (long (or (:checkpoint_alert_seconds endpoint)
                                                    (parse-int-env :ingest-checkpoint-alert-seconds (* 2 freshness-sla-seconds))))))
        bad-record-threshold   (double (or (:bad_record_alert_ratio endpoint)
                                           (parse-double-env :ingest-bad-record-alert-ratio 0.05)))
        retry-threshold        (long (or (:retry_volume_alert_24h endpoint)
                                         (parse-int-env :ingest-retry-volume-alert-24h 50)))
        replay-threshold       (long (or (:replay_failure_alert_7d endpoint)
                                         (parse-int-env :ingest-replay-failure-alert-7d 3)))
        alerts                 (cond-> []
                                 (> checkpoint-lag-seconds checkpoint-threshold)
                                 (conj {:code "stale_checkpoint"
                                        :severity "high"
                                        :checkpoint_lag_seconds checkpoint-lag-seconds
                                        :threshold_seconds checkpoint-threshold})
                                 (> latest-bad-ratio bad-record-threshold)
                                 (conj {:code "high_bad_record_ratio"
                                        :severity "medium"
                                        :bad_record_ratio latest-bad-ratio
                                        :threshold bad-record-threshold})
                                 (> retry-volume-24h retry-threshold)
                                 (conj {:code "high_retry_volume"
                                        :severity "medium"
                                        :retry_volume_24h retry-volume-24h
                                        :threshold retry-threshold})
                                 (>= replay-failures replay-threshold)
                                 (conj {:code "replay_failures"
                                        :severity "high"
                                        :replay_failures_7d replay-failures
                                        :threshold replay-threshold}))]
    {:graph_id graph-id
     :node_id node-id
     :source_kind (name source-kind)
     :endpoint_name endpoint-name'
     :checkpoint_lag_seconds checkpoint-lag-seconds
     :freshness_sla_seconds freshness-sla-seconds
     :bad_record_ratio_latest_run latest-bad-ratio
     :bad_records_24h bad-records-24h
     :batch_commit_latency_ms_avg avg-commit-latency-ms
     :retry_volume_24h retry-volume-24h
     :replay_successes_7d replay-successes
     :replay_failures_7d replay-failures
     :alerts alerts
     :latest_run (when latest-run
                   {:run_id (:run_id latest-run)
                    :status (:status latest-run)
                    :started_at_utc (:started_at_utc latest-run)
                    :finished_at_utc (:finished_at_utc latest-run)
                    :rows_written (long (or (:rows_written latest-run) 0))
                    :retry_count (long (or (:retry_count latest-run) 0))})}))

(defn bronze-source-observability-alerts
  [graph-id node-id {:keys [source-kind endpoint-name]}]
  (let [summary (bronze-source-observability-summary graph-id node-id
                                                     {:source-kind source-kind
                                                      :endpoint-name endpoint-name})]
    {:graph_id graph-id
     :node_id node-id
     :source_kind (name source-kind)
     :endpoint_name (:endpoint_name summary)
     :alert_count (count (:alerts summary))
     :alerts (:alerts summary)}))

(defn archive-api-batch!
  [graph-id api-node-id batch-id {:keys [endpoint-name archived-by]
                                  :or {archived-by "system"}}]
  (let [{:keys [conn-id manifest-table endpoint]} (api-endpoint-runtime-context graph-id api-node-id endpoint-name)
        manifest (manifest-row-by-batch-id conn-id manifest-table batch-id)]
    (when-not manifest
      (throw (ex-info "Batch manifest not found" {:batch_id batch-id :status 404})))
    (when (and endpoint-name
               (not= (:endpoint_name manifest) (:endpoint_name endpoint)))
      (throw (ex-info "Batch manifest does not belong to the requested endpoint"
                      {:batch_id batch-id
                       :requested_endpoint (:endpoint_name endpoint)
                       :manifest_endpoint (:endpoint_name manifest)
                       :status 409})))
    (when-not (seq (:artifact_path manifest))
      (throw (ex-info "Batch artifact is not available for archive"
                      {:batch_id batch-id
                       :status 409})))
    (let [now (now-utc)
          archived-artifact (archive-batch-artifact! (:artifact_path manifest) (:artifact_checksum manifest))
          updated (assoc manifest
                         :artifact_path (:artifact_path archived-artifact)
                         :artifact_checksum (:artifact_checksum archived-artifact)
                         :archived_at_utc (str now)
                         :rolled_back_by (or (:rolled_back_by manifest) archived-by))]
      (mark-manifest-row! conn-id manifest-table updated)
      (assoc (manifest-summary updated)
             :graph_id graph-id
             :api_node_id api-node-id
             :archive_status "archived"))))

(defn apply-api-retention!
  [graph-id api-node-id {:keys [endpoint-name archive-days retention-days dry-run limit archived-by
                                bad-record-payload-archive-days bad-record-retention-days]
                         :or {archived-by "system"}}]
  (let [{:keys [conn-id manifest-table endpoint bad-records-table api-node]} (api-endpoint-runtime-context graph-id api-node-id endpoint-name)
        source-system (or (:source_system api-node) "api")
        archive-days (long (max 0 (or archive-days (default-artifact-archive-days))))
        retention-days (long (max archive-days (or retention-days (default-artifact-retention-days archive-days))))
        bad-record-payload-archive-days (long (max 0 (or bad-record-payload-archive-days
                                                        (default-bad-record-payload-archive-days))))
        bad-record-retention-days (long (max bad-record-payload-archive-days
                                             (or bad-record-retention-days
                                                 (default-bad-record-retention-days bad-record-payload-archive-days))))
        archive-cutoff (cutoff-instant archive-days)
        retention-cutoff (cutoff-instant retention-days)
        bad-record-payload-archive-cutoff (cutoff-instant bad-record-payload-archive-days)
        bad-record-retention-cutoff (cutoff-instant bad-record-retention-days)
        page-size (long (max 1 (min 1000 (or limit 500))))
        endpoint-name' (or endpoint-name (:endpoint_name endpoint))]
    (let [manifest-summary
          (loop [offset 0
                 acc {:graph_id graph-id
                      :api_node_id api-node-id
                      :endpoint_name endpoint-name'
                      :archive_days archive-days
                      :retention_days retention-days
                      :bad_record_payload_archive_days bad-record-payload-archive-days
                      :bad_record_retention_days bad-record-retention-days
                      :dry_run (boolean dry-run)
                      :archived_count 0
                      :deleted_count 0
                      :bad_record_payload_archived_count 0
                      :bad_record_metadata_deleted_count 0
                      :batch_ids []
                      :bad_record_ids []
                      :errors []}]
            (let [manifests (query-manifest-rows conn-id
                                                 manifest-table
                                                 {:source-system source-system
                                                  :endpoint-name endpoint-name'
                                                  :limit page-size
                                                  :offset offset
                                                  :order :asc})
                  eligible (remove #(contains? #{"preparing" "pending_checkpoint"} (:status %)) manifests)]
              (if (empty? manifests)
                acc
                (recur (+ offset (count manifests))
                       (reduce
                        (fn [acc manifest]
                          (let [committed-at (manifest-committed-instant manifest)
                                batch-id (:batch_id manifest)]
                            (cond
                              (nil? committed-at)
                              acc

                              (and (seq (:artifact_path manifest))
                                   (.isBefore committed-at retention-cutoff))
                              (try
                                (let [updated (assoc manifest
                                                     :artifact_path nil
                                                     :artifact_checksum nil
                                                     :archived_at_utc (or (:archived_at_utc manifest) (str (now-utc))))]
                                  (when-not dry-run
                                    (delete-batch-artifact! (:artifact_path manifest))
                                    (mark-manifest-row! conn-id manifest-table updated))
                                  (-> acc
                                      (update :deleted_count inc)
                                      (update :batch_ids conj batch-id)))
                                (catch Exception e
                                  (log/warn e "Failed to delete retained API batch artifact"
                                            {:graph_id graph-id
                                             :api_node_id api-node-id
                                             :endpoint_name endpoint-name'
                                             :batch_id batch-id})
                                  (update acc :errors conj {:batch_id batch-id
                                                            :action "delete"
                                                            :error (.getMessage e)})))

                              (and (seq (:artifact_path manifest))
                                   (nil? (:archived_at_utc manifest))
                                   (.isBefore committed-at archive-cutoff))
                              (try
                                (let [archived-artifact (when-not dry-run
                                                         (archive-batch-artifact! (:artifact_path manifest)
                                                                                  (:artifact_checksum manifest)))
                                      updated (assoc manifest
                                                     :artifact_path (or (some-> archived-artifact :artifact_path)
                                                                        (:artifact_path manifest))
                                                     :artifact_checksum (or (some-> archived-artifact :artifact_checksum)
                                                                            (:artifact_checksum manifest))
                                                     :archived_at_utc (str (now-utc))
                                                     :rolled_back_by (or (:rolled_back_by manifest) archived-by))]
                                  (when-not dry-run
                                    (mark-manifest-row! conn-id manifest-table updated))
                                  (-> acc
                                      (update :archived_count inc)
                                      (update :batch_ids conj batch-id)))
                                (catch Exception e
                                  (log/warn e "Failed to archive retained API batch artifact"
                                            {:graph_id graph-id
                                             :api_node_id api-node-id
                                             :endpoint_name endpoint-name'
                                             :batch_id batch-id})
                                  (update acc :errors conj {:batch_id batch-id
                                                            :action "archive"
                                                            :error (.getMessage e)})))

                              :else
                              acc)))
                        acc
                        eligible)))))]
      (loop [offset 0
             acc manifest-summary]
        (let [rows (query-bad-record-retention-rows conn-id bad-records-table
                                                     {:source-system source-system
                                                      :endpoint-name endpoint-name'
                                                      :limit page-size
                                                      :offset offset})]
          (if (empty? rows)
            acc
            (recur (+ offset (count rows))
                   (reduce
                    (fn [acc row]
                      (let [created-at (coerce-instant (:created_at_utc row))
                            bad-record-id (:bad_record_id row)
                            has-payload? (or (seq (:payload_json row))
                                             (seq (:row_json row)))]
                        (cond
                          (nil? created-at)
                          acc

                          (.isBefore created-at bad-record-retention-cutoff)
                          (try
                            (when-not dry-run
                              (delete-bad-record! conn-id bad-records-table bad-record-id))
                            (-> acc
                                (update :bad_record_metadata_deleted_count inc)
                                (update :bad_record_ids conj bad-record-id))
                            (catch Exception e
                              (log/warn e "Failed to delete retained bad-record metadata"
                                        {:graph_id graph-id
                                         :api_node_id api-node-id
                                         :endpoint_name endpoint-name'
                                         :bad_record_id bad-record-id})
                              (update acc :errors conj {:bad_record_id bad-record-id
                                                        :action "bad_record_delete"
                                                        :error (.getMessage e)})))

                          (and has-payload?
                               (nil? (:payload_archive_ref row))
                               (.isBefore created-at bad-record-payload-archive-cutoff))
                          (try
                            (let [artifact (when-not dry-run
                                             (persist-batch-artifact!
                                              (or (non-blank-str (:run_id row)) "bad-record-retention")
                                              (or (non-blank-str (:source_system row)) source-system)
                                              endpoint-name'
                                              (str "bad-record-" (safe-path-segment bad-record-id))
                                              {:bad_record_id bad-record-id
                                               :run_id (:run_id row)
                                               :batch_id (:batch_id row)
                                               :endpoint_name endpoint-name'
                                               :payload_json (:payload_json row)
                                               :row_json (:row_json row)
                                               :error_message (:error_message row)}
                                              "bad_record_payload"))
                                  artifact-path (or (some-> artifact :artifact_path)
                                                    (when dry-run
                                                      (str "dry-run://bad-record/" (safe-path-segment bad-record-id))))]
                              (when-not artifact-path
                                (throw (ex-info "Bad-record payload archive requires an artifact store path"
                                                {:failure_class "retention_payload_archive_unavailable"
                                                 :bad_record_id bad-record-id
                                                 :artifact_store (name (artifact-store-mode))})))
                              (when-not dry-run
                                (mark-bad-record-payload-archived! conn-id bad-records-table bad-record-id artifact-path (now-utc)))
                              (-> acc
                                  (update :bad_record_payload_archived_count inc)
                                  (update :bad_record_ids conj bad-record-id)))
                            (catch Exception e
                              (log/warn e "Failed to archive bad-record payload"
                                        {:graph_id graph-id
                                         :api_node_id api-node-id
                                         :endpoint_name endpoint-name'
                                         :bad_record_id bad-record-id})
                              (update acc :errors conj {:bad_record_id bad-record-id
                                                        :action "bad_record_payload_archive"
                                                        :error (.getMessage e)})))

                          :else
                          acc)))
                    acc
                    rows))))))))

(defn replay-api-bad-records!
  [graph-id api-node-id {:keys [endpoint-name batch-id source-run-id limit include-succeeded? replayed-by]
                         :or {replayed-by "system"}}]
  (let [endpoint-name (non-blank-str endpoint-name)]
    (when-not endpoint-name
      (throw (ex-info "endpoint_name is required for bad-record replay"
                      {:status 400})))
    (let [{:keys [conn-id target api-node endpoint bad-records-table manifest-table]} (api-endpoint-runtime-context graph-id api-node-id endpoint-name)
          endpoint        (or endpoint (select-endpoint! api-node endpoint-name))
          source-system   (or (:source_system api-node) "samara")
          run-id          (str (UUID/randomUUID))
          started-at      (now-utc)
          rows            (query-bad-record-rows conn-id bad-records-table
                                                 {:source-system source-system
                                                  :endpoint-name endpoint-name
                                                  :run-id source-run-id
                                                  :batch-id batch-id
                                                  :limit limit
                                                  :include-succeeded? include-succeeded?})
          already-replayed-ids (committed-replayed-source-bad-record-ids conn-id manifest-table source-system endpoint-name)
          skipped-already-replayed (->> rows
                                        (filter #(contains? already-replayed-ids (:bad_record_id %)))
                                        (mapv :bad_record_id))
          rows            (->> rows
                               (remove #(contains? already-replayed-ids (:bad_record_id %)))
                               vec)
          source-bad-record-ids (mapv :bad_record_id rows)]
      (when (and (empty? rows) (empty? skipped-already-replayed))
        (throw (ex-info "No bad records available for replay"
                        {:status 404
                         :endpoint_name endpoint-name
                         :batch_id batch-id
                         :run_id source-run-id})))
      (when (some nil? source-bad-record-ids)
        (throw (ex-info "Bad-record replay requires bad_record_id for all source rows"
                        {:status 409
                         :failure_class "bad_record_replay_invalid_source"
                         :endpoint_name endpoint-name
                         :run_id source-run-id})))
      (if (empty? rows)
        {:graph_id graph-id
         :api_node_id api-node-id
         :endpoint_name endpoint-name
         :run_id nil
         :status "noop_already_replayed"
         :source_bad_record_count 0
         :skipped_already_replayed_count (count skipped-already-replayed)
         :skipped_already_replayed_bad_record_ids skipped-already-replayed
         :rows_replayed 0
         :bad_records_remaining 0
         :manifest nil}
        (let [table-name-raw (endpoint->table-name target endpoint)
              _              (when (string/blank? (str table-name-raw))
                               (throw (ex-info "Endpoint is missing bronze_table_name and target table_name"
                                               {:endpoint_name endpoint-name
                                                :status 409})))
              table-name     (validated-qualified-table-name table-name-raw)
              partition-columns (target-partition-columns conn-id target endpoint)
              _              (ensure-table! conn-id table-name (bronze/bronze-columns endpoint)
                                            {:partition-columns partition-columns})
              replayed       (bronze/replay-bad-records->rows
                              rows
                              endpoint
                              {:run-id run-id
                               :source-system source-system
                               :now started-at
                               :request-url (join-url (:base_url api-node) (:endpoint_url endpoint))})
              manifest       (flush-bad-record-replay-batch!
                              conn-id
                              {:table-name table-name
                               :bad-records-table bad-records-table
                               :manifest-table manifest-table
                               :source-system source-system
                               :endpoint-name endpoint-name
                               :run-id run-id
                               :started-at started-at
                               :endpoint-config endpoint}
                              (:rows replayed)
                              (:bad-records replayed)
                              {:source-bad-record-ids source-bad-record-ids
                               :succeeded-source-bad-record-ids (:succeeded-source-bad-record-ids replayed)
                               :failed-source-bad-record-ids (:failed-source-bad-record-ids replayed)
                               :replay-run-id run-id
                               :failed-message "Replay coercion failed"})]
        (try
          (control-plane/record-audit-event!
           {:event_type "api.bad_record_replay"
            :actor replayed-by
            :graph_id graph-id
            :node_id api-node-id
            :run_id run-id
            :details {:endpoint_name endpoint-name
                      :batch_id batch-id
                      :source_run_id source-run-id
                      :source_bad_record_count (count rows)
                      :skipped_already_replayed_count (count skipped-already-replayed)
                      :replayed_rows (count (:rows replayed))
                      :remaining_bad_records (count (:bad-records replayed))}})
          (catch Exception e
            (log/warn e "Failed to persist bad-record replay audit event"
                      {:graph_id graph-id
                       :api_node_id api-node-id
                       :run_id run-id})))
        {:graph_id graph-id
         :api_node_id api-node-id
         :endpoint_name endpoint-name
         :run_id run-id
         :source_bad_record_count (count rows)
         :skipped_already_replayed_count (count skipped-already-replayed)
         :skipped_already_replayed_bad_record_ids skipped-already-replayed
         :rows_replayed (count (:rows replayed))
         :bad_records_remaining (count (:bad-records replayed))
         :partition_columns partition-columns
         :manifest (manifest-summary manifest)})))))

(defn- flush-batch!
  [conn-id {:keys [table-name bad-records-table manifest-table checkpoint-table
                   source-system endpoint-name run-id started-at endpoint-config
                   post-commit-fn]}
   buffer state]
  (if (batch-empty? buffer)
    state
    (let [flush-start              (now-nanos)
          batch-seq                (inc (:batch-seq state))
          batch-id                 (or (:batch-id-override state)
                                       (format "%s-b%06d" run-id batch-seq))
          committed-at             (now-utc)
          checkpoint-wm            (:checkpoint-watermark state)
          all-rows                 (mapv #(assoc % :batch_id batch-id) (:rows buffer))
          batch-max-wm             (rows-max-watermark all-rows)
          batch-rows               (if (and (seq checkpoint-wm)
                                           (seq batch-max-wm)
                                           (<= (compare (str batch-max-wm) (str checkpoint-wm)) 0))
                                     []
                                     all-rows)
          batch-bad-records        (mapv #(assoc % :batch_id batch-id) (:bad-records buffer))
          max-watermark            (rows-max-watermark batch-rows)
          next-cursor              (page-state-next-cursor (:last-state buffer))
          stale-noop?              (and (empty? batch-rows)
                                        (empty? batch-bad-records))
          requires-cleanup-delete? (some? (:batch-id-override state))]
      (if stale-noop?
        (-> state
            (assoc :next-cursor next-cursor
                   :last-http-status (:last-http-status buffer)
                   :max-watermark (merge-watermark (:max-watermark state) max-watermark)
                   :timings (merge-timing-maps
                             (:timings state)
                             {:batch_flush_count 1
                              :batch_flush_ms (elapsed-ms flush-start)
                              :stale_batch_skip_count 1})))
        (let [artifact-start  (now-nanos)
              artifact        (persist-batch-artifact!
                               run-id
                               source-system
                               endpoint-name
                               batch-id
                               {:batch_id batch-id
                                :run_id run-id
                                :endpoint_name endpoint-name
                                :source_system source-system
                                :endpoint_config endpoint-config
                                :endpoint_config_hash (replay-endpoint-config-hash endpoint-config)
                                :pages (:page-artifacts buffer)})
              artifact-ms     (elapsed-ms artifact-start)
              preparing-row   (preparing-manifest-row
                               {:batch-id batch-id
                                :run-id run-id
                                :source-system source-system
                                :endpoint-name endpoint-name
                                :table-name table-name
                                :batch-seq batch-seq
                                :row-count (count batch-rows)
                                :bad-record-count (count batch-bad-records)
                                :byte-count (:byte-count buffer)
                                :page-count (:page-count buffer)
                                :partition-dates (changed-partition-dates batch-rows)
                                :max-watermark max-watermark
                                :next-cursor next-cursor
                                :artifact-path (:artifact_path artifact)
                                :artifact-checksum (:artifact_checksum artifact)
                                :started-at started-at})
              pending-row     (pending-checkpoint-manifest-row
                               {:batch-id batch-id
                                :run-id run-id
                                :source-system source-system
                                :endpoint-name endpoint-name
                                :table-name table-name
                                :batch-seq batch-seq
                                :row-count (count batch-rows)
                                :bad-record-count (count batch-bad-records)
                                :byte-count (:byte-count buffer)
                                :page-count (:page-count buffer)
                                :partition-dates (changed-partition-dates batch-rows)
                                :max-watermark max-watermark
                                :next-cursor next-cursor
                                :artifact-path (:artifact_path artifact)
                                :artifact-checksum (:artifact_checksum artifact)
                                :started-at started-at
                                :committed-at committed-at})
              manifest-row    (committed-manifest-row
                               {:batch-id batch-id
                                :run-id run-id
                                :source-system source-system
                                :endpoint-name endpoint-name
                                :table-name table-name
                                :batch-seq batch-seq
                                :row-count (count batch-rows)
                                :bad-record-count (count batch-bad-records)
                                :byte-count (:byte-count buffer)
                                :page-count (:page-count buffer)
                                :partition-dates (changed-partition-dates batch-rows)
                                :max-watermark max-watermark
                                :next-cursor next-cursor
                                :artifact-path (:artifact_path artifact)
                                :artifact-checksum (:artifact_checksum artifact)
                                :started-at started-at
                                :committed-at committed-at})
              total-rows      (+ (:rows-written state) (count batch-rows))
              checkpoint-row' (checkpoint/success-row
                               {:source_system source-system
                                :endpoint_name endpoint-name
                                :run_id run-id
                                :batch_id batch-id
                                :batch_seq batch-seq
                                :rows_ingested total-rows
                                :max_watermark (merge-watermark (:max-watermark state) max-watermark)
                                :next_cursor next-cursor
                                :status "success"
                                :now committed-at})
              commit-start    (now-nanos)
              commit-fn       (if (atomic-batch-commit? conn-id)
                                (fn []
                                  (with-batch-commit
                                    conn-id
                                    (fn []
                                      (replace-row! conn-id manifest-table [:batch_id] preparing-row)
                                      (when requires-cleanup-delete?
                                        (delete-rows-by-column! conn-id table-name :batch_id batch-id))
                                      (when (seq batch-rows)
                                        (load-rows! conn-id table-name batch-rows))
                                      (when requires-cleanup-delete?
                                        (delete-rows-by-column! conn-id bad-records-table :batch_id batch-id))
                                      (when (seq batch-bad-records)
                                        (load-rows! conn-id bad-records-table batch-bad-records))
                                      (replace-checkpoint-row! conn-id checkpoint-table checkpoint-row')
                                      (replace-row! conn-id manifest-table [:batch_id] manifest-row))))
                                (fn []
                                  ;; For non-transactional targets, write the recovery marker once
                                  ;; as pending_checkpoint before data load. This preserves crash
                                  ;; recovery semantics while saving one manifest round trip.
                                  (replace-row! conn-id manifest-table [:batch_id] pending-row)
                                  (when requires-cleanup-delete?
                                    (delete-rows-by-column! conn-id table-name :batch_id batch-id))
                                  (when (seq batch-rows)
                                    (load-rows! conn-id table-name batch-rows))
                                  (when requires-cleanup-delete?
                                    (delete-rows-by-column! conn-id bad-records-table :batch_id batch-id))
                                  (when (seq batch-bad-records)
                                    (load-rows! conn-id bad-records-table batch-bad-records))
                                  (if (:checkpoint-row state)
                                    (do
                                      (update-checkpoint-row! conn-id checkpoint-table checkpoint-row')
                                      (cache-checkpoint-row! conn-id checkpoint-table source-system endpoint-name checkpoint-row'))
                                    (replace-checkpoint-row! conn-id checkpoint-table checkpoint-row'))
                                  (update-manifest-row! conn-id manifest-table manifest-row)))]
          (commit-fn)
          (let [commit-ms (elapsed-ms commit-start)
                flush-ms  (elapsed-ms flush-start)]
            (when post-commit-fn
              (try
                (post-commit-fn {:batch-id batch-id
                                 :batch-seq batch-seq
                                 :manifest manifest-row
                                 :checkpoint-row checkpoint-row'
                                 :buffer buffer})
                (catch Exception e
                  (log/warn e "Post-commit hook failed after Bronze batch commit"
                            {:batch_id batch-id
                             :endpoint_name endpoint-name
                             :source_system source-system}))))
            (-> state
                (assoc :batch-seq batch-seq
                       :batch-id-override nil
                       :checkpoint-row checkpoint-row'
                       :max-watermark (merge-watermark (:max-watermark state) max-watermark)
                       :next-cursor next-cursor
                       :rows-written total-rows
                       :bad-records-written (+ (:bad-records-written state) (count batch-bad-records))
                       :last-http-status (:last-http-status buffer)
                       :timings (merge-timing-maps
                                 (:timings state)
                                 {:batch_flush_count 1
                                  :batch_flush_ms flush-ms
                                  :artifact_persist_ms artifact-ms
                                  :batch_commit_ms commit-ms}))
                (update :changed-partition-dates into (changed-partition-dates batch-rows))
                (update :manifests conj manifest-row))))))))

(defn- flush-context
  [{:keys [table-name bad-records-table manifest-table checkpoint-table
           source-system endpoint-name run-id started-at endpoint-config post-commit-fn]}]
  {:table-name table-name
   :bad-records-table bad-records-table
   :manifest-table manifest-table
   :checkpoint-table checkpoint-table
   :source-system source-system
   :endpoint-name endpoint-name
   :run-id run-id
   :started-at started-at
   :endpoint-config endpoint-config
   :post-commit-fn post-commit-fn})

(defn- flush-bad-record-replay-batch!
  [conn-id {:keys [table-name bad-records-table manifest-table source-system endpoint-name run-id started-at endpoint-config]}
   rows bad-records {:keys [source-bad-record-ids succeeded-source-bad-record-ids failed-source-bad-record-ids replay-run-id failed-message]
                     :or {failed-message "Replay coercion failed"}}]
  (let [batch-id      (replay-batch-id endpoint-name source-bad-record-ids)
        committed-at  (now-utc)
        batch-rows    (mapv #(assoc % :batch_id batch-id) rows)
        batch-bad     (mapv #(assoc % :batch_id batch-id) bad-records)
        max-watermark (rows-max-watermark batch-rows)
        artifact      (persist-batch-artifact!
                       run-id
                       source-system
                       endpoint-name
                       batch-id
                       {:batch_id batch-id
                        :run_id run-id
                        :endpoint_name endpoint-name
                        :source_system source-system
                        :endpoint_config endpoint-config
                        :endpoint_config_hash (replay-endpoint-config-hash endpoint-config)
                        :pages []})
        manifest-base {:batch-id batch-id
                       :run-id run-id
                       :source-system source-system
                       :endpoint-name endpoint-name
                       :table-name table-name
                       :batch-seq 1
                       :row-count (count batch-rows)
                       :bad-record-count (count batch-bad)
                       :byte-count (+ (estimate-payload-bytes batch-rows)
                                      (estimate-payload-bytes batch-bad))
                       :page-count 0
                       :partition-dates (changed-partition-dates batch-rows)
                       :source-bad-record-ids source-bad-record-ids
                       :max-watermark max-watermark
                       :next-cursor nil
                       :artifact-path (:artifact_path artifact)
                       :artifact-checksum (:artifact_checksum artifact)
                       :started-at started-at}
        preparing-row (preparing-manifest-row manifest-base)
        pending-row   (pending-checkpoint-manifest-row (assoc manifest-base :committed-at committed-at))
        manifest-row  (committed-manifest-row
                       {:batch-id batch-id
                        :run-id run-id
                        :source-system source-system
                        :endpoint-name endpoint-name
                        :table-name table-name
                        :batch-seq 1
                        :row-count (count batch-rows)
                        :bad-record-count (count batch-bad)
                        :byte-count (+ (estimate-payload-bytes batch-rows)
                                       (estimate-payload-bytes batch-bad))
                        :page-count 0
                        :partition-dates (changed-partition-dates batch-rows)
                        :max-watermark max-watermark
                        :next-cursor nil
                        :artifact-path (:artifact_path artifact)
                        :artifact-checksum (:artifact_checksum artifact)
                        :started-at started-at
                        :committed-at committed-at})]
    (if (atomic-batch-commit? conn-id)
      (with-batch-commit
        conn-id
        (fn []
          (delete-rows-by-column! conn-id table-name :batch_id batch-id)
          (when (seq batch-rows)
            (load-rows! conn-id table-name batch-rows))
          (delete-rows-by-column! conn-id bad-records-table :batch_id batch-id)
          (when (seq batch-bad)
            (load-rows! conn-id bad-records-table batch-bad))
          (mark-bad-record-replay-statuses! conn-id bad-records-table succeeded-source-bad-record-ids "succeeded" replay-run-id nil)
          (mark-bad-record-replay-statuses! conn-id bad-records-table failed-source-bad-record-ids "failed" replay-run-id failed-message)
          (mark-manifest-row! conn-id manifest-table manifest-row)))
      (do
        ;; Non-transactional targets write pending_checkpoint up front so a crash before the
        ;; checkpoint update can still be reconciled, without paying an extra preparing write.
        (mark-manifest-row! conn-id manifest-table pending-row)
        (delete-rows-by-column! conn-id table-name :batch_id batch-id)
        (when (seq batch-rows)
          (load-rows! conn-id table-name batch-rows))
        (delete-rows-by-column! conn-id bad-records-table :batch_id batch-id)
        (when (seq batch-bad)
          (load-rows! conn-id bad-records-table batch-bad))
        (update-manifest-row! conn-id manifest-table manifest-row)
        ;; Run replay status updates only after manifest commit on non-transactional targets.
        (mark-bad-record-replay-statuses! conn-id bad-records-table succeeded-source-bad-record-ids "succeeded" replay-run-id nil)
        (mark-bad-record-replay-statuses! conn-id bad-records-table failed-source-bad-record-ids "failed" replay-run-id failed-message)))
    manifest-row))

(defn- mark-manifest-row!
  [conn-id manifest-table row]
  (replace-row! conn-id manifest-table [:batch_id] row))

(defn- update-checkpoint-row!
  [conn-id checkpoint-table row]
  (try
    (update-row-by-key! conn-id checkpoint-table [:source_system :endpoint_name] row)
    row
    (catch Exception e
      (log/debug e "Falling back to replace-checkpoint-row! for checkpoint update"
                 {:conn-id conn-id :table checkpoint-table})
      (replace-checkpoint-row! conn-id checkpoint-table row))))

(defn- update-manifest-row!
  [conn-id manifest-table row]
  (try
    (update-row-by-key! conn-id manifest-table [:batch_id] row)
    row
    (catch Exception e
      (log/debug e "Falling back to mark-manifest-row! for manifest update"
                 {:conn-id conn-id :table manifest-table})
      (mark-manifest-row! conn-id manifest-table row))))

(defn- reconcile-incomplete-manifest!
  [conn-id manifest-table checkpoint-table bad-records-table manifest]
  (let [checkpoint-row (fetch-checkpoint conn-id checkpoint-table
                                         (:source_system manifest)
                                         (:endpoint_name manifest))]
    (cond
      (= "pending_checkpoint" (:status manifest))
      (if (checkpoint-covers-manifest? checkpoint-row manifest)
        (mark-manifest-row! conn-id manifest-table
                            (assoc manifest
                                   :status "committed"
                                   :active true
                                   :committed_at_utc (str (now-utc))))
        (do
          (delete-rows-by-column! conn-id (:table_name manifest) :batch_id (:batch_id manifest))
          (delete-rows-by-column! conn-id bad-records-table :batch_id (:batch_id manifest))
          (mark-manifest-row! conn-id manifest-table
                              (assoc manifest
                                     :status "aborted"
                                     :active false
                                     :rollback_reason "automatic_abort_of_pending_checkpoint_batch"
                                     :rolled_back_by "system"
                                     :rolled_back_at_utc (str (now-utc))))))

      :else
      (do
        (delete-rows-by-column! conn-id (:table_name manifest) :batch_id (:batch_id manifest))
        (delete-rows-by-column! conn-id bad-records-table :batch_id (:batch_id manifest))
        (mark-manifest-row! conn-id manifest-table
                            (assoc manifest
                                   :status "aborted"
                                   :active false
                                   :rollback_reason "automatic_abort_of_preparing_batch"
                                   :rolled_back_by "system"
                                   :rolled_back_at_utc (str (now-utc))))))))

(defn- abort-preparing-batches!
  [conn-id {:keys [manifest-table checkpoint-table bad-records-table source-system endpoint-name]}]
  (try
    (doseq [manifest (incomplete-manifest-rows conn-id manifest-table source-system endpoint-name)]
      (reconcile-incomplete-manifest! conn-id manifest-table checkpoint-table bad-records-table manifest))
    (catch Exception e
      (throw (ex-info "Failed to reconcile incomplete batch manifests before API run"
                      {:conn_id conn-id
                       :manifest_table manifest-table
                       :checkpoint_table checkpoint-table
                       :source_system source-system
                       :endpoint_name endpoint-name
                       :failure_class "manifest_reconciliation"}
                      e)))))

(defn- artifact-pages->messages
  [artifact-data]
  (->> (or (:pages artifact-data) [])
       (mapv (fn [page]
               {:body (:body page)
                :page (:page page)
                :state (:state page)
                :response {:status (get-in page [:response :status])
                           :retry-count (or (get-in page [:response :retry_count])
                                            (get-in page [:response :retry-count]))}}))))

(defn- log-api-run-summary!
  [{:keys [graph-id api-node-id source-system status run-timings results]}]
  (when (ingest-summary-logging-enabled?)
    (let [rows-written (reduce + 0 (map #(long (or (:rows_written %) 0)) results))
          bad-records  (reduce + 0 (map #(long (or (:bad_records %) 0)) results))
          batches      (reduce + 0 (map #(long (or (:batch_count %) 0)) results))
          batch-commit (reduce + 0 (map #(long (or (get-in % [:timings :batch_commit_ms]) 0)) results))
          checkpoint   (reduce + 0 (map #(long (or (get-in % [:timings :checkpoint_lookup_ms]) 0)) results))
          snapshot     (reduce + 0 (map #(long (or (get-in % [:timings :schema_snapshot_ms]) 0)) results))
          endpoint-ms  (reduce + 0 (map #(long (or (get-in % [:timings :endpoint_total_ms]) 0)) results))
          stale-skips  (reduce + 0 (map #(long (or (get-in % [:timings :stale_batch_skip_count]) 0)) results))
          endpoint-names (mapv :endpoint_name results)]
      (log/debug "API ingest run summary"
                 {:graph_id graph-id
                  :api_node_id api-node-id
                  :source_system source-system
                  :status status
                  :run_total_ms (long (or (:run_total_ms run-timings) 0))
                  :endpoints_total_ms (long (or (:endpoints_total_ms run-timings) endpoint-ms))
                  :rows_written rows-written
                  :bad_records bad-records
                  :batch_count batches
                  :batch_commit_ms batch-commit
                  :checkpoint_lookup_ms checkpoint
                  :schema_snapshot_ms snapshot
                  :stale_batch_skip_count stale-skips
                  :endpoint_names endpoint-names}))))

(defn- apply-replay-pages
  [run-id source-system request-url started-at endpoint buffer state pages]
  (reduce (fn [[buf acc] page]
            (let [page-out (bronze/build-page-rows page endpoint {:run-id run-id
                                                                  :source-system source-system
                                                                  :now started-at
                                                                  :request-url request-url})]
              [(add-page-to-batch buf page-out page)
               (-> acc
                   (update :pages-fetched inc)
                   (update :retry-count + (long (or (get-in page [:response :retry-count]) 0)))
                   (update :rows-extracted + (count (:rows page-out)))
                   (update :bad-records-total + (count (:bad-records page-out)))
                   (assoc :last-http-status (get-in page [:response :status])))]))
          [buffer state]
          pages))

(defn- process-endpoint-replay!
  [conn-id {:keys [table-name bad-records-table manifest-table checkpoint-table
                   source-system endpoint-name run-id started-at request-url]}
   endpoint {:keys [manifests endpoint-config-hash]} replay-run-id]
  (let [flush-ctx (flush-context {:table-name table-name
                                  :bad-records-table bad-records-table
                                  :manifest-table manifest-table
                                  :checkpoint-table checkpoint-table
                                  :source-system source-system
                                  :endpoint-name endpoint-name
                                  :run-id run-id
                                  :started-at started-at
                                  :endpoint-config endpoint})]
    (loop [remaining manifests
           state {:batch-seq 0
                  :batch-id-override nil
                  :checkpoint-row nil
                  :max-watermark nil
                  :next-cursor nil
                  :rows-extracted 0
                  :rows-written 0
                  :bad-records-total 0
                  :bad-records-written 0
                  :pages-fetched 0
                  :retry-count 0
                  :last-http-status nil
                  :changed-partition-dates []
                  :manifests []
                  :timings {}}]
      (if-let [manifest (first remaining)]
        (let [artifact-data           (read-batch-artifact! (:artifact_path manifest)
                                                            (:artifact_checksum manifest))
              artifact-config-hash    (or (non-blank-str (:endpoint_config_hash artifact-data))
                                          (when-let [stored-endpoint (artifact-endpoint-config artifact-data)]
                                            (replay-endpoint-config-hash stored-endpoint)))
              _                       (when (and endpoint-config-hash
                                                artifact-config-hash
                                                (not= endpoint-config-hash artifact-config-hash))
                                        (throw (ex-info "Replay artifacts contain inconsistent endpoint config snapshots"
                                                        {:failure_class "artifact_invalid"
                                                         :replay_source_run_id replay-run-id
                                                         :endpoint_name endpoint-name
                                                         :artifact_path (:artifact_path manifest)})))
              replay-endpoint         (or (artifact-endpoint-config artifact-data) endpoint)
              pages                   (artifact-pages->messages artifact-data)
              [buffer replay-state]   (apply-replay-pages run-id
                                                          source-system
                                                          request-url
                                                          started-at
                                                          replay-endpoint
                                                          (new-batch-buffer)
                                                          state
                                                          pages)
              flushed-state           (if (batch-empty? buffer)
                                        replay-state
                                        (flush-batch! conn-id flush-ctx buffer
                                                      (assoc replay-state :batch-id-override (:batch_id manifest))))]
          (recur (next remaining) flushed-state))
        (assoc state
               :stop-reason :replay_artifact
               :final-state {:replay_source_run_id replay-run-id}
               :final-http-status (:last-http-status state))))))

(defn- artifact-endpoint-config
  [artifact-data]
  (or (:endpoint_config artifact-data)
      (when-let [endpoint-json (:endpoint_config_json artifact-data)]
        (if (string? endpoint-json)
          (json/parse-string endpoint-json true)
          endpoint-json))))

(defn- find-downstream-target [g start-id]
  (loop [queue (keys (get-in g [:n start-id :e]))
         visited #{}]
    (when-let [nid (first queue)]
      (if (contains? visited nid)
        (recur (rest queue) visited)
        (let [na (get-in g [:n nid :na])]
          (if (= "Tg" (:btype na))
            na
            (recur (concat (rest queue) (keys (get-in g [:n nid :e])))
                   (conj visited nid))))))))

(defn- collect-pages! [pages-ch]
  (loop [pages [] stop-reason nil final-state nil final-status nil total-retries 0]
    (if-let [msg (async/<!! pages-ch)]
      (if (:body msg)
        (recur (conj pages msg)
               stop-reason
               final-state
               final-status
               (+ total-retries (long (or (get-in msg [:response :retry-count]) 0))))
        (recur pages
               (:stop-reason msg)
               (:state msg)
               (:http-status msg)
               (+ total-retries (long (or (:retry-count msg)
                                          (get-in msg [:response :retry-count])
                                          0)))))
      {:pages pages
       :stop-reason stop-reason
       :final-state final-state
       :final-http-status final-status
       :retry-count total-retries})))

(defn- collect-errors! [errors-ch]
  (loop [errors []]
    (if-let [msg (async/<!! errors-ch)]
      (recur (conj errors msg))
      errors)))

(defn- drain-errors!
  ([errors-ch] (drain-errors! errors-ch 25 3))
  ([errors-ch idle-timeout-ms max-idle-timeouts]
   (loop [errors [] idle-timeouts 0]
     (let [[msg port] (async/alts!! [errors-ch (async/timeout idle-timeout-ms)])]
       (cond
         (= port errors-ch)
         (if (nil? msg)
           errors
           (recur (conj errors msg) 0))

         (< idle-timeouts max-idle-timeouts)
         (recur errors (inc idle-timeouts))

         :else errors)))))

(defn- cleanup-fetch-stream!
  [cancel errors-ch]
  (when cancel
    (try
      (cancel)
      (catch Exception e
        (log/warn e "Failed to cancel fetch stream during cleanup"))))
  (when errors-ch
    (try
      (drain-errors! errors-ch)
      (catch Exception e
        (log/warn e "Failed to drain fetch errors during cleanup")))))

(defn- endpoint-run-row
  [{:keys [run-id source-system endpoint-name started-at finished-at status http-status pages-fetched rows-extracted rows-written retry-count error-summary]}]
  {:run_id run-id
   :source_system source-system
   :endpoint_name endpoint-name
   :started_at_utc (str started-at)
   :finished_at_utc (str finished-at)
   :status status
   :http_status_code http-status
   :pages_fetched pages-fetched
   :rows_extracted rows-extracted
   :rows_written rows-written
   :retry_count retry-count
   :error_summary error-summary})

(defn- changed-partition-dates [rows]
  (->> rows
       (map :partition_date)
       (map #(when (some? %) (str %)))
       (remove string/blank?)
       distinct
       vec))

(defn- manifest-partition-dates
  [manifest]
  (let [raw (:partition_dates_json manifest)
        parsed (cond
                 (nil? raw) []
                 (string? raw) (json/parse-string raw true)
                 (sequential? raw) raw
                 :else [])]
    (->> parsed
         (map non-blank-str)
         (remove nil?)
         distinct
         vec)))

(defn- committed-manifest-partition-dates
  [manifests]
  (->> manifests
       (filter #(and (= "committed" (:status %))
                     (truthy-db-bool? (:active %))))
       (mapcat manifest-partition-dates)
       distinct
       vec))

(defn- ensure-final-run-manifest-invariants!
  [conn-id endpoint-name run-id manifests]
  (when-not (atomic-batch-commit? conn-id)
    (let [incomplete (->> manifests
                          (filter #(contains? #{"preparing" "pending_checkpoint"} (:status %)))
                          (mapv :batch_id))
          active-non-committed (->> manifests
                                    (filter #(and (truthy-db-bool? (:active %))
                                                  (not= "committed" (:status %))))
                                    (mapv :batch_id))]
      (when (seq incomplete)
        (throw (ex-info "Non-transactional manifest commit closure is incomplete for the endpoint run"
                        {:failure_class "manifest_commit_incomplete"
                         :run_id run-id
                         :endpoint_name endpoint-name
                         :incomplete_batch_ids incomplete})))
      (when (seq active-non-committed)
        (throw (ex-info "Non-transactional endpoint run has active manifests outside committed state"
                        {:failure_class "manifest_state_invariant"
                         :run_id run-id
                         :endpoint_name endpoint-name
                         :invalid_batch_ids active-non-committed})))))
  nil)

(defn- merge-job-params [target endpoint params]
  (merge {:source_system (:source_system params)
          :endpoint_name (:endpoint_name endpoint)
          :bronze_table (endpoint->table-name target endpoint)
          :silver_table (:silver_table_name endpoint)
          :run_id (:run_id params)
          :changed_partition_dates (:changed_partition_dates params)}
         params))

(defn- maybe-trigger-job! [conn-id job-id params]
  (when (seq (str job-id))
    (dbx-jobs/trigger-job! conn-id job-id params)))

(defn- target-option
  [target k]
  (or (get-in target [:options k])
      (get-in target [:options (name k)])
      (get target k)))

(defn- databricks-bronze-job-config
  [target]
  {:job-id (or (non-blank-str (:bronze_job_id target))
               (non-blank-str (target-option target :bronze_job_id)))
   :job-params (merge (or (:bronze_job_params target) {})
                      (or (target-option target :bronze_job_params) {}))
   :callback-url (or (non-blank-str (target-option target :bitool_callback_url))
                     (non-blank-str (some-> (get env :bitool-bronze-callback-url) str))
                     (non-blank-str (some-> (get env :bitool-public-base-url) str)))
   :callback-token (or (non-blank-str (target-option target :bitool_callback_token))
                       (non-blank-str (some-> (get env :bitool-bronze-callback-token) str)))})

(defn- databricks-run->status
  [response-body]
  (let [state        (or (:state response-body) {})
        lifecycle    (some-> (or (:life_cycle_state state)
                                 (:life_cycle_state response-body))
                             str
                             string/upper-case)
        result-state (some-> (or (:result_state state)
                                 (:result_state response-body))
                             str
                             string/upper-case)]
    (cond
      (and (= "TERMINATED" lifecycle) (= "SUCCESS" result-state)) "success"
      (and (= "TERMINATED" lifecycle)
           (contains? #{"FAILED" "TIMEDOUT" "CANCELED" "CANCELLED"} result-state)) "failed"
      (contains? #{"INTERNAL_ERROR" "SKIPPED"} lifecycle) "failed"
      (= "TERMINATED" lifecycle) "failed"
      :else "running")))

(defn- aggregate-control-plane-run-results
  [graph-id api-node-id endpoint-name started-at]
  (let [results (dbx-control/latest-run-results graph-id api-node-id {:endpoint-name endpoint-name
                                                                      :started-after started-at
                                                                      :limit 25})
        statuses (set (map #(some-> (:status %) str string/lower-case) results))
        overall (cond
                  (empty? results) nil
                  (= #{"success"} statuses) "success"
                  (contains? statuses "failed") (if (contains? statuses "success") "partial_success" "failed")
                  :else (first statuses))]
    (when (seq results)
      {:graph_id graph-id
       :api_node_id api-node-id
       :source_system (:source_system (first results))
       :status overall
       :results (vec (reverse results))
       :rows_written (reduce + 0 (map #(long (or (:rows_written %) 0)) results))
       :batch_count (count results)})))

(defn- databricks-task-run-id
  [poll-response]
  (or (some-> (get-in poll-response [:body :tasks]) first :run_id str)
      (some-> (get-in poll-response [:body "tasks"]) first :run_id str)
      (some-> (get-in poll-response [:body "tasks"]) first (get "run_id") str)
      (some-> (:run_id poll-response) str)))

(defn- parse-databricks-notebook-result
  [raw]
  (cond
    (map? raw) raw
    (string? raw) (try
                    (json/parse-string raw true)
                    (catch Exception _
                      nil))
    :else nil))

(defn- databricks-run-metrics
  [run-body]
  {:databricks_run_duration_ms (long (or (:run_duration run-body)
                                         (get run-body "run_duration")
                                         0))
   :databricks_setup_duration_ms (long (or (:setup_duration run-body)
                                           (get run-body "setup_duration")
                                           0))
   :databricks_execution_duration_ms (long (or (:execution_duration run-body)
                                               (get run-body "execution_duration")
                                               0))
   :databricks_cleanup_duration_ms (long (or (:cleanup_duration run-body)
                                             (get run-body "cleanup_duration")
                                             0))})

(defn- notebook-output->result
  [graph-id api-node-id source-system external-run-id run-body output-body]
  (when-let [result-payload (or (some-> output-body :notebook_output :result parse-databricks-notebook-result)
                                (some-> output-body (get "notebook_output") (get "result") parse-databricks-notebook-result))]
    (let [total-failed (long (or (:total_failed result-payload) 0))
          total-rows   (long (or (:total_rows result-payload) 0))]
    {:graph_id graph-id
     :api_node_id api-node-id
     :source_system (or (:source_system result-payload) source-system)
     :status (cond
               (zero? total-failed) "success"
               (zero? total-rows) "failed"
               :else "partial_success")
     :results (vec (or (:results result-payload) []))
     :rows_written (reduce + 0 (map #(long (or (:rows_written %) 0))
                                    (or (:results result-payload) [])))
     :batch_count (count (or (:results result-payload) []))
     :external_run_id external-run-id
     :job_output result-payload
     :job_metrics (databricks-run-metrics run-body)})))

(defn- direct-databricks-job-params
  [graph-id api-node-id target endpoint-name]
  (let [bootstrap (dbx-control/bootstrap graph-id api-node-id {:endpoint-name endpoint-name})
        endpoints (vec (or (:endpoints bootstrap) []))]
    {:source_system (:source_system bootstrap)
     :api_base_url (:api_base_url bootstrap)
     :catalog (:catalog bootstrap)
     :bronze_schema (:schema bootstrap)
     :endpoints (string/join "," (map :endpoint_name endpoints))
     :endpoint_configs_json (json/generate-string endpoints)}))

(defn- run-api-node-via-databricks-job!
  [graph-id api-node-id {:keys [endpoint-name] :as _opts}]
  (let [graph       (db/getGraph graph-id)
        api-node    (g2/getData graph api-node-id)
        target      (find-downstream-target graph api-node-id)
        conn-id     (require-target-connection-id! target
                                                   "No downstream target connection found for API node"
                                                   {:graph_id graph-id
                                                    :api_node_id api-node-id})
        dbtype      (some-> (connection-dbtype conn-id) str string/lower-case)
        {:keys [job-id job-params callback-url callback-token]} (databricks-bronze-job-config target)]
    (when-not (= "databricks" dbtype)
      (throw (ex-info "Databricks Bronze job requested for non-Databricks target"
                      {:graph_id graph-id
                       :api_node_id api-node-id
                       :connection_id conn-id
                       :dbtype dbtype})))
    (when-not job-id
      (throw (ex-info "Databricks Bronze target has no bronze_job_id configured"
                      {:graph_id graph-id
                       :api_node_id api-node-id
                       :connection_id conn-id
                       :status 409})))
    (let [triggered-at (now-utc)
          direct-params (direct-databricks-job-params graph-id api-node-id target endpoint-name)
          response     (dbx-jobs/trigger-job! conn-id
                                              job-id
                                              (merge job-params
                                                     direct-params
                                                     {:graph_id (str graph-id)
                                                      :api_node_id (str api-node-id)}
                                                     (when endpoint-name
                                                       {:endpoint_name endpoint-name})
                                                     (when callback-url
                                                       {:bitool_callback_url callback-url})
                                                     (when callback-token
                                                       {:bitool_callback_token callback-token})))
          run-id       (some-> (:run_id response) str)
          max-polls    (max 1 (parse-int-env :bitool-databricks-max-polls 180))]
      (loop [poll-count 0]
        (when (>= poll-count max-polls)
          (throw (ex-info "Databricks Bronze job poll timeout exceeded"
                          {:failure_class "transient_platform_error"
                           :graph_id graph-id
                           :api_node_id api-node-id
                           :poll_count poll-count
                           :max_polls max-polls
                           :external_run_id run-id})))
        (Thread/sleep (min 15000 (+ 2000 (* poll-count 500))))
        (let [poll-response (dbx-jobs/get-run! conn-id run-id)
              status        (databricks-run->status (:body poll-response))]
          (cond
            (= "success" status)
            (let [result (or (loop [attempt 0]
                               (let [result (aggregate-control-plane-run-results graph-id api-node-id endpoint-name triggered-at)]
                                 (cond
                                   result result
                                   (>= attempt 4) nil
                                   :else (do
                                           (Thread/sleep 1000)
                                           (recur (inc attempt))))))
                             (some->> (databricks-task-run-id poll-response)
                                      (dbx-jobs/get-run-output! conn-id)
                                      :body
                                      (notebook-output->result graph-id
                                                               api-node-id
                                                               (or (:source_system api-node) "api")
                                                               run-id
                                                               (:body poll-response)))
                             {:graph_id graph-id
                              :api_node_id api-node-id
                              :source_system (or (:source_system api-node) "api")
                              :status "success"
                              :external_run_id run-id
                              :job_trigger response
                              :job_metrics (databricks-run-metrics (:body poll-response))})]
              (if (= "failed" (:status result))
                (throw (ex-info "Databricks Bronze job failed"
                                {:failure_class "endpoint_run_failed"
                                 :graph_id graph-id
                                 :api_node_id api-node-id
                                 :external_run_id run-id
                                 :job_trigger response
                                 :poll_response poll-response
                                 :output_body (:job_output result)
                                 :error_message (or (some-> result :results first :error)
                                                    "Databricks Bronze job returned failed endpoint results")}))
                result))

            (= "failed" status)
            (let [task-run-id  (databricks-task-run-id poll-response)
                  output-body  (when task-run-id
                                 (try
                                   (some-> (dbx-jobs/get-run-output! conn-id task-run-id) :body)
                                   (catch Exception _
                                     nil)))
                  error-text   (or (some-> output-body :error)
                                   (some-> output-body (get "error"))
                                   (get-in poll-response [:body :state :state_message])
                                   (get-in poll-response [:body "state" "state_message"]))]
              (throw (ex-info "Databricks Bronze job failed"
                              {:failure_class "endpoint_run_failed"
                               :graph_id graph-id
                               :api_node_id api-node-id
                               :external_run_id run-id
                               :job_trigger response
                               :poll_response poll-response
                               :task_run_id task-run-id
                               :output_body output-body
                               :error_message error-text})))

            :else
            (recur (inc poll-count))))))))

(defn- trigger-downstream-jobs! [conn-id target endpoint params]
  (let [dbtype        (connection-dbtype conn-id)
        silver-job-id (or (:silver_job_id endpoint) (:silver_job_id target))
        gold-job-id   (or (:gold_job_id endpoint) (:gold_job_id target))
        silver-params (merge-job-params target endpoint
                                        (merge (or (:silver_job_params target) {})
                                               (or (:silver_job_params endpoint) {})
                                               params))
        gold-params   (merge-job-params target endpoint
                                        (merge (or (:gold_job_params target) {})
                                               (or (:gold_job_params endpoint) {})
                                               params))
        trigger-gold? (or (:trigger_gold_on_success endpoint)
                          (:trigger_gold_on_success target))]
    (cond-> {}
      (and (= "databricks" dbtype) silver-job-id)
      (assoc :silver (maybe-trigger-job! conn-id silver-job-id silver-params))
      (and (= "databricks" dbtype) trigger-gold? gold-job-id)
      (assoc :gold (maybe-trigger-job! conn-id gold-job-id gold-params)))))

(defn- replay-source-run-id
  [opts]
  (or (non-blank-str (:replay-source-run-id opts))
      (non-blank-str (:replay_source_run_id opts))))

(defn- replay-source-batch-ids
  [opts]
  (let [raw (or (:replay-source-batch-ids opts)
                (:replay_source_batch_ids opts)
                (:replay-source-batch-id opts)
                (:replay_source_batch_id opts))]
    (cond
      (nil? raw) nil
      (string? raw) (->> (string/split raw #",")
                         (map non-blank-str)
                         (remove nil?)
                         vec)
      (sequential? raw) (->> raw
                             (map non-blank-str)
                             (remove nil?)
                             vec)
      :else (when-let [single (non-blank-str raw)]
              [single]))))

(defn- replay-source-graph-version
  [opts]
  (or (:replay-source-graph-version opts)
      (:replay_source_graph_version opts)))

(defn- parse-replay-graph-version
  [value]
  (cond
    (nil? value) nil
    (number? value) (long value)
    :else
    (try
      (Long/parseLong (str value))
      (catch Exception _
        (throw (ex-info "Replay source graph version must be an integer"
                        {:failure_class "config_error"
                         :status 400
                         :replay_source_graph_version value}))))))

(defn- replay-endpoint-config-hash
  [endpoint]
  (stable-json-hash endpoint))

(defn- ensure-replay-compatible!
  [graph replay-opts]
  (when-let [source-version (some-> (replay-source-graph-version replay-opts)
                                    parse-replay-graph-version)]
    (let [current-version (get-in graph [:a :v])]
      (when (and (some? current-version)
                 (not= (long current-version) (long source-version))
                 (not (true? (or (:allow-replay-graph-drift replay-opts)
                                 (:allow_replay_graph_drift replay-opts)))))
        (throw (ex-info "Deterministic replay requires the source graph version to match the current graph version"
                        {:failure_class "config_error"
                         :current_graph_version current-version
                         :replay_source_graph_version source-version}))))))

(defn- ensure-schema-approved!
  [conn-id schema-approval-table graph-id api-node-id endpoint]
  (when (schema-approval-required? endpoint)
    (let [schema-hash (schema-fields-hash (vec (or (:inferred_fields endpoint) [])))
          approval    (latest-promoted-schema-approval conn-id
                                                       schema-approval-table
                                                       graph-id
                                                       api-node-id
                                                       (:endpoint_name endpoint))]
      (when-not approval
        (throw (ex-info "Schema approval is required before ingestion can run for this endpoint"
                        {:status 409
                         :failure_class "schema_approval_required"
                         :graph_id graph-id
                         :api_node_id api-node-id
                         :endpoint_name (:endpoint_name endpoint)
                         :schema_hash schema-hash})))
      (when-not (= schema-hash (:schema_hash approval))
        (throw (ex-info "Current inferred schema does not match the promoted approved schema"
                        {:status 409
                         :failure_class "schema_approval_mismatch"
                         :graph_id graph-id
                         :api_node_id api-node-id
                         :endpoint_name (:endpoint_name endpoint)
                         :schema_hash schema-hash
                         :approved_schema_hash (:schema_hash approval)}))))))

(defn- resolve-replay-plan!
  [conn-id manifest-table replay-run-id endpoint replay-opts]
  (let [requested-batch-ids (set (or (replay-source-batch-ids replay-opts) []))
        manifests (vec (committed-manifest-rows conn-id manifest-table replay-run-id (:endpoint_name endpoint)))
        manifests (if (seq requested-batch-ids)
                    (->> manifests
                         (filter #(contains? requested-batch-ids (str (:batch_id %))))
                         vec)
                    manifests)]
    (when-not (seq manifests)
      (throw (ex-info "No committed batch manifests found for replay source run"
                      {:failure_class "artifact_missing"
                       :replay_source_run_id replay-run-id
                       :endpoint_name (:endpoint_name endpoint)
                       :replay_source_batch_ids (vec requested-batch-ids)})))
    (let [first-artifact      (read-batch-artifact! (:artifact_path (first manifests))
                                                    (:artifact_checksum (first manifests)))
          stored-endpoint     (artifact-endpoint-config first-artifact)
          stored-config-hash  (or (non-blank-str (:endpoint_config_hash first-artifact))
                                  (when stored-endpoint
                                    (replay-endpoint-config-hash stored-endpoint)))
          current-config-hash (replay-endpoint-config-hash endpoint)
          replay-endpoint     (or stored-endpoint endpoint)]
      (when (and stored-config-hash
                 (not= stored-config-hash current-config-hash))
        (log/warn "Replaying API endpoint with stored endpoint config snapshot instead of current graph endpoint config"
                  {:endpoint_name (:endpoint_name endpoint)
                   :replay_source_run_id replay-run-id}))
      {:manifests manifests
       :endpoint replay-endpoint
       :endpoint-config-hash stored-config-hash})))

(defn- process-endpoint-stream!
  [conn-id {:keys [table-name bad-records-table manifest-table checkpoint-table
                   source-system endpoint-name run-id started-at request-url
                   checkpoint-watermark]}
   endpoint pages-ch initial-pages initial-terminal]
  (process-source-stream!
   conn-id
   {:table-name table-name
    :bad-records-table bad-records-table
    :manifest-table manifest-table
    :checkpoint-table checkpoint-table
    :source-system source-system
    :endpoint-name endpoint-name
    :run-id run-id
    :started-at started-at
    :endpoint-config endpoint
    :checkpoint-watermark checkpoint-watermark}
   pages-ch
   initial-pages
   initial-terminal
   (fn [page]
     (bronze/build-page-rows page endpoint {:run-id run-id
                                            :source-system source-system
                                            :now started-at
                                            :request-url request-url}))))

(defn- process-source-stream!
  [conn-id flush-ctx pages-ch initial-pages initial-terminal page->rows]
  (let [checkpoint-wm (:checkpoint-watermark flush-ctx)
        flush-ctx (flush-context flush-ctx)]
    (loop [pending-pages (seq initial-pages)
           terminal-msg  initial-terminal
           buffer        (new-batch-buffer)
           state         {:batch-seq 0
                          :checkpoint-row nil
                          :max-watermark nil
                          :next-cursor nil
                          :checkpoint-watermark checkpoint-wm
                          :rows-extracted 0
                          :rows-written 0
                          :bad-records-total 0
                          :bad-records-written 0
                          :pages-fetched 0
                          :retry-count 0
                          :last-http-status nil
                          :changed-partition-dates []
                          :manifests []
                          :timings {}}]
      (cond
        pending-pages
        (let [page      (first pending-pages)
              page-out  (page->rows page)
              buffer'   (add-page-to-batch buffer page-out page)
              state'    (-> state
                            (update :pages-fetched inc)
                            (update :retry-count + (long (or (get-in page [:response :retry-count]) 0)))
                            (update :rows-extracted + (count (:rows page-out)))
                            (update :bad-records-total + (count (:bad-records page-out)))
                            (assoc :last-http-status (get-in page [:response :status])))]
          (if (should-flush-batch? buffer')
            (recur (next pending-pages)
                   terminal-msg
                   (new-batch-buffer)
                   (flush-batch! conn-id flush-ctx buffer' state'))
            (recur (next pending-pages) terminal-msg buffer' state')))

        terminal-msg
        (let [state' (if (batch-empty? buffer)
                       state
                       (flush-batch! conn-id flush-ctx buffer state))]
          (assoc (update state'
                         :retry-count
                         + (long (or (:retry-count terminal-msg)
                                     (get-in terminal-msg [:response :retry-count])
                                     0)))
                 :stop-reason (:stop-reason terminal-msg)
                 :final-state (:state terminal-msg)
                 :final-http-status (:http-status terminal-msg)))

        :else
        (if-let [msg (async/<!! pages-ch)]
          (if (:body msg)
            (recur (list msg) nil buffer state)
            (recur nil msg buffer state))
          (let [state' (if (batch-empty? buffer)
                         state
                         (flush-batch! conn-id flush-ctx buffer state))]
            (assoc state'
                   :stop-reason nil
                   :final-state nil
                   :final-http-status (:last-http-status state'))))))))

(defn preview-endpoint-schema!
  [api-node endpoint]
  (let [endpoint       (g2/normalize-api-endpoint-config endpoint)
        auth           (try (resolve-auth-ref (:auth_ref api-node))
                            (catch Exception _ nil))
        pagination     (pagination-config endpoint)
        pagination-location (keyword (or (:pagination_location endpoint) "query"))
        query-builder  (:query-builder pagination)
        body-builder   (:body-builder pagination)
        response       (-> (api/do-request {:method (keyword (string/lower-case (or (:http_method endpoint) "GET")))
                                            :url (join-url (:base_url api-node) (:endpoint_url endpoint))
                                            :base-headers (or (:request_headers endpoint) {})
                                            :query-params query-builder
                                            :body-params body-builder
                                            :auth auth
                                            :retry-policy (or (:retry_policy endpoint) {})})
                           ensure-previewable-response!)
        configured-path (get-in endpoint [:json_explode_rules 0 :path])
        detected-path   (when-not (seq (string/trim (str configured-path)))
                          (schema-infer/detect-dominant-records-path (:body response)))
        endpoint'       (if (seq (string/trim (str detected-path)))
                          (assoc endpoint :json_explode_rules [{:path detected-path}])
                          endpoint)
        inferred       (schema-infer/infer-endpoint-fields endpoint' (:body response))
        recommendations (grain-planner/recommend-endpoint-config endpoint'
                                                                  inferred
                                                                  {:detected-records-path detected-path
                                                                   :configured-records-path configured-path})]
    {:endpoint_name (:endpoint_name endpoint)
     :http_status (:status response)
     :sampled_records (count (schema-infer/logical-records-from-body (:body response)
                                                                     (get-in endpoint' [:json_explode_rules 0 :path])))
     :detected_records_path detected-path
     :applied_json_explode_rules (vec (or (:json_explode_rules endpoint') []))
     :inferred_fields inferred
     :recommendations recommendations}))

(defn run-api-node!
  ([graph-id api-node-id] (run-api-node! graph-id api-node-id {}))
  ([graph-id api-node-id {:keys [endpoint-name] :as replay-opts}]
  (reset-table-cache-for-run! (str graph-id "-" api-node-id "-" (System/currentTimeMillis)))
  (let [run-start       (now-nanos)
         run-timings*   (volatile! {})
         run-time-step! (fn [k f]
                          (let [started (now-nanos)
                                result  (f)]
                            (vswap! run-timings* assoc k (elapsed-ms started))
                            result))
         g             (run-time-step! :graph_load_ms #(db/getGraph graph-id))
         api-node      (run-time-step! :api_node_load_ms #(g2/getData g api-node-id))
         target        (run-time-step! :target_resolution_ms #(find-downstream-target g api-node-id))
         conn-id       (run-time-step! :target_connection_resolution_ms
                                       #(require-target-connection-id! target
                                                                       "No downstream target connection found for API node"
                                                                       {:graph_id graph-id
                                                                        :api_node_id api-node-id}))
         source-system (or (:source_system api-node) "samara")
         auth          (run-time-step! :auth_resolution_ms #(resolve-auth-ref (:auth_ref api-node)))
         endpoints     (run-time-step! :endpoint_selection_ms
                                       (fn []
                                         (->> (:endpoint_configs api-node)
                                              (filter :enabled)
                                              (filter (fn [cfg]
                                                        (if endpoint-name
                                                          (= endpoint-name (:endpoint_name cfg))
                                                          true))))))
         checkpoint-table (validated-qualified-table-name (audit-table target conn-id "ingestion_checkpoint"))
         run-detail-table (validated-qualified-table-name (audit-table target conn-id "endpoint_run_detail"))
         bad-records-table (validated-qualified-table-name (audit-table target conn-id "bad_records"))
         manifest-table   (validated-qualified-table-name (audit-table target conn-id "run_batch_manifest"))
         schema-snapshot-table (validated-qualified-table-name (audit-table target conn-id "endpoint_schema_snapshot"))
         schema-approval-table (validated-qualified-table-name (audit-table target conn-id "endpoint_schema_approval"))
         continue-on-endpoint-failure? (and (nil? endpoint-name) (> (count endpoints) 1))]
     (run-time-step! :replay_compatibility_ms #(ensure-replay-compatible! g replay-opts))
     (when-not conn-id
       (throw (ex-info "No downstream target connection found for API node"
                       {:graph_id graph-id :api_node_id api-node-id})))
     (when-not (seq endpoints)
       (throw (ex-info (if endpoint-name
                         (str "No enabled endpoint config found for endpoint_name '" endpoint-name "'")
                        "API node has no enabled endpoint configs")
                       {:graph_id graph-id
                        :api_node_id api-node-id
                        :endpoint_name endpoint-name})))
     (run-time-step! :checkpoint_table_ensure_ms
                     #(ensure-table! conn-id checkpoint-table checkpoint/ingestion-checkpoint-columns {}))
     (run-time-step! :checkpoint_migration_ms
                     #(ensure-checkpoint-columns! conn-id checkpoint-table))
     (run-time-step! :run_detail_table_ensure_ms
                     #(ensure-table! conn-id run-detail-table endpoint-run-detail-columns {}))
     (run-time-step! :bad_records_table_ensure_ms
                     #(ensure-table! conn-id bad-records-table bronze/bad-record-columns {}))
     (run-time-step! :bad_record_migration_ms
                     #(ensure-bad-record-columns! conn-id bad-records-table))
     (run-time-step! :manifest_table_ensure_ms
                     #(ensure-table! conn-id manifest-table batch-manifest-columns {}))
     (run-time-step! :manifest_migration_ms
                     #(ensure-batch-manifest-columns! conn-id manifest-table))
     (run-time-step! :schema_snapshot_table_ensure_ms
                     #(ensure-table! conn-id schema-snapshot-table endpoint-schema-snapshot-columns {}))
     (run-time-step! :schema_approval_table_ensure_ms
                     #(ensure-table! conn-id schema-approval-table endpoint-schema-approval-columns {}))
     (run-time-step! :schema_approval_migration_ms
                     #(ensure-schema-approval-columns! conn-id schema-approval-table))
     (let [results
           (mapv
            (fn [endpoint]
         (let [run-id              (str (java.util.UUID/randomUUID))
               started-at          (now-utc)
               endpoint-start      (now-nanos)
               timings*            (volatile! {})
               time-step!          (fn [k f]
                                     (let [started (now-nanos)
                                           result  (f)]
                                       (vswap! timings* assoc k (elapsed-ms started))
                                       result))
               checkpoint-row      (time-step! :checkpoint_lookup_ms
                                               #(fetch-checkpoint conn-id checkpoint-table source-system (:endpoint_name endpoint)))
               pagination          (pagination-config endpoint)
               watermark-params    (checkpoint/watermark-query-params checkpoint-row endpoint started-at)
               cursor-params       (checkpoint-cursor-query-params checkpoint-row endpoint)
               pagination-location (keyword (or (:pagination_location endpoint) "query"))
               query-builder       (if (= pagination-location :body)
                                     (:query-builder pagination)
                                     (merge (:query-builder pagination)
                                            watermark-params
                                            cursor-params))
               body-builder        (if (= pagination-location :body)
                                     (merge (:body-builder pagination)
                                            watermark-params
                                            cursor-params)
                                     (:body-builder pagination))
               initial-state       (merge (:initial-state pagination)
                                          (checkpoint-cursor-initial-state checkpoint-row endpoint))
               table-name-raw      (endpoint->table-name target endpoint)
               request-url         (join-url (:base_url api-node) (:endpoint_url endpoint))
               replay-run-id       (replay-source-run-id replay-opts)
               adaptive-per-page-ms (time-step! :adaptive_rate_limit_lookup_ms
                                                #(load-adaptive-backpressure-ms source-system endpoint))
               pages*              (atom nil)
               errors-ch*          (atom nil)
               cancel*             (atom nil)
               circuit-admission*  (atom nil)
               circuit-completed?  (atom false)]
           (try
             (when-not replay-run-id
               (reset! circuit-admission* (begin-source-circuit-request! source-system endpoint)))
             (let [fetch-result         (when-not replay-run-id
                                         (time-step! :fetch_bootstrap_ms
                                                     #(api/fetch-paged-async
                                                       (merge pagination
                                                              {:base-url (:base_url api-node)
                                                               :endpoint (:endpoint_url endpoint)
                                                               :method (keyword (string/lower-case (or (:http_method endpoint) "GET")))
                                                               :headers (or (:request_headers endpoint) {})
                                                               :auth auth
                                                               :retry-policy (or (:retry_policy endpoint) {})
                                                               :pagination-location pagination-location
                                                               :initial-state initial-state
                                                               :rate-limit {:per-page-ms adaptive-per-page-ms}
                                                               :query-builder query-builder
                                                               :body-builder body-builder}))))
                   _                   (reset! pages* (:pages fetch-result))
                   _                   (reset! errors-ch* (:errors fetch-result))
                   _                   (reset! cancel* (:cancel fetch-result))
                   sample              (if replay-run-id
                                         {:sample-pages [] :terminal-msg nil}
                                         (time-step! :schema_sample_ms
                                                     #(collect-schema-sample! @pages* endpoint)))
                   replay-plan         (when replay-run-id
                                         (time-step! :replay_plan_ms
                                                     #(resolve-replay-plan! conn-id manifest-table replay-run-id endpoint replay-opts)))
                   endpoint'           (time-step! :field_resolution_ms
                                                   #(-> (if replay-run-id
                                                          (:endpoint replay-plan)
                                                          (maybe-infer-endpoint-fields endpoint (:sample-pages sample)))
                                                        ensure-unique-field-column-names!))
                   _                   (when (string/blank? (str table-name-raw))
                                         (throw (ex-info "Endpoint is missing bronze_table_name and target table_name"
                                                         {:endpoint_name (:endpoint_name endpoint)})))
                   table-name          (validated-qualified-table-name table-name-raw)
                   _                   (when (and (not replay-run-id)
                                                  (schema-inference-enabled? endpoint'))
                                         (when (schema-snapshot-write-needed?
                                                conn-id
                                                schema-snapshot-table
                                                {:graph-id graph-id
                                                 :api-node-id api-node-id
                                                 :source-system source-system
                                                 :endpoint endpoint'
                                                 :sample-pages (:sample-pages sample)})
                                           (time-step! :schema_snapshot_ms
                                                       #(persist-endpoint-schema-snapshot!
                                                         conn-id
                                                         schema-snapshot-table
                                                         {:graph-id graph-id
                                                          :api-node-id api-node-id
                                                          :graph-version-id nil
                                                          :graph-version (get-in g [:a :v])
                                                          :source-system source-system
                                                          :endpoint endpoint'
                                                          :sample-pages (:sample-pages sample)
                                                          :captured-at started-at}))))
                   _                   (time-step! :schema_approval_ms
                                                   #(ensure-schema-approved! conn-id
                                                                             schema-approval-table
                                                                             graph-id
                                                                             api-node-id
                                                                             endpoint'))
                   partition-columns   (time-step! :partition_columns_ms
                                                   #(target-partition-columns conn-id target endpoint'))
                   _                   (time-step! :bronze_table_ensure_ms
                                                   #(ensure-table! conn-id table-name (bronze/bronze-columns endpoint')
                                                                   {:partition-columns partition-columns}))
                   _                   (time-step! :manifest_recovery_ms
                                                   #(abort-preparing-batches! conn-id {:manifest-table manifest-table
                                                                                       :checkpoint-table checkpoint-table
                                                                                       :bad-records-table bad-records-table
                                                                                       :source-system source-system
                                                                                       :endpoint-name (:endpoint_name endpoint)}))
                   stream-state        (if replay-run-id
                                         (time-step! :stream_process_ms
                                                     #(process-endpoint-replay!
                                                       conn-id
                                                       {:table-name table-name
                                                        :bad-records-table bad-records-table
                                                        :manifest-table manifest-table
                                                        :checkpoint-table checkpoint-table
                                                        :source-system source-system
                                                        :endpoint-name (:endpoint_name endpoint)
                                                        :run-id run-id
                                                        :started-at started-at
                                                        :request-url request-url}
                                                       endpoint'
                                                       replay-plan
                                                       replay-run-id))
                                         (time-step! :stream_process_ms
                                                     #(process-endpoint-stream!
                                                       conn-id
                                                       {:table-name table-name
                                                        :bad-records-table bad-records-table
                                                        :manifest-table manifest-table
                                                        :checkpoint-table checkpoint-table
                                                        :source-system source-system
                                                        :endpoint-name (:endpoint_name endpoint)
                                                        :run-id run-id
                                                        :started-at started-at
                                                        :request-url request-url
                                                        :checkpoint-watermark (non-blank-str (:last_successful_watermark checkpoint-row))}
                                                       endpoint'
                                                       @pages*
                                                       (:sample-pages sample)
                                                       (:terminal-msg sample))))
                   errors              (if replay-run-id [] (time-step! :error_collection_ms #(collect-errors! @errors-ch*)))
                   finished-at         (now-utc)
                   status              (run-status errors (:pages-fetched stream-state))
                   _                   (when-not replay-run-id
                                         (do
                                           (update-adaptive-backpressure! source-system
                                                                          endpoint
                                                                          errors
                                                                          (or (:final-http-status stream-state)
                                                                              (:last-http-status stream-state)))
                                           (when-let [admission @circuit-admission*]
                                             (complete-source-circuit-request!
                                              source-system
                                              endpoint
                                              admission
                                              {:errors errors
                                               :final-http-status (or (:final-http-status stream-state)
                                                                      (:last-http-status stream-state))})
                                             (reset! circuit-completed? true))))
                   next-cursor         (or (:next-cursor stream-state)
                                           (some-> stream-state :final-state page-state-next-cursor))
                   retry-count         (:retry-count stream-state)
                   watermark-param     (checkpoint/watermark-query-param endpoint)
                   attempted-watermark (when (seq (str watermark-param))
                                         (get watermark-params (keyword watermark-param)))
                   checkpoint-row'     (when (and (not= status "failed")
                                                  (or (nil? (:checkpoint-row stream-state))
                                                      (and (zero? (long (:rows-written stream-state)))
                                                           (zero? (long (:bad-records-written stream-state)))))
                                                  (or (some? next-cursor)
                                                      (some? (:max-watermark stream-state))
                                                      (pos? (long (:pages-fetched stream-state)))))
                                         (checkpoint/success-row
                                          {:source_system source-system
                                           :endpoint_name (:endpoint_name endpoint)
                                           :run_id run-id
                                           :rows_ingested (:rows-written stream-state)
                                           :max_watermark (:max-watermark stream-state)
                                           :next_cursor next-cursor
                                           :now finished-at}))
                   _                   (when (= status "failed")
                                         (replace-checkpoint-row! conn-id checkpoint-table
                                                                  (checkpoint-row-for-failure
                                                                   checkpoint-row
                                                                   (checkpoint/failure-row
                                                                    {:source_system source-system
                                                                     :endpoint_name (:endpoint_name endpoint)
                                                                     :attempted_watermark attempted-watermark
                                                                     :attempted_cursor next-cursor
                                                                     :now finished-at}))))
                   run-row             (endpoint-run-row
                                        {:run-id run-id
                                         :source-system source-system
                                         :endpoint-name (:endpoint_name endpoint)
                                         :started-at started-at
                                         :finished-at finished-at
                                         :status status
                                         :http-status (or (:final-http-status stream-state)
                                                          (:last-http-status stream-state))
                                         :pages-fetched (:pages-fetched stream-state)
                                         :rows-extracted (:rows-extracted stream-state)
                                         :rows-written (:rows-written stream-state)
                                         :retry-count retry-count
                                         :error-summary (when (seq errors) (json/generate-string errors))})]
               ((if (and checkpoint-row' (atomic-batch-commit? conn-id))
                  (fn []
                    (with-batch-commit
                      conn-id
                      (fn []
                        (load-rows! conn-id run-detail-table [run-row])
                        (replace-checkpoint-row! conn-id checkpoint-table checkpoint-row'))))
                  (fn []
                    (load-rows! conn-id run-detail-table [run-row])
                    (when checkpoint-row'
                      (replace-checkpoint-row! conn-id checkpoint-table checkpoint-row')))))
               (time-step! :finalization_ms
                           #(try
                 (let [workspace-context (try
                                           (control-plane/graph-workspace-context graph-id)
                                           (catch Exception e
                                             (log/warn e "Skipping workspace-context lookup for endpoint freshness"
                                                       {:graph_id graph-id
                                                        :api_node_id api-node-id})
                                             nil))]
                 (operations/record-endpoint-freshness!
                  {:graph-id graph-id
                   :api-node-id api-node-id
                   :tenant-key (:tenant_key workspace-context)
                   :workspace-key (:workspace_key workspace-context)
                   :source-system source-system
                   :endpoint-name (:endpoint_name endpoint)
                   :target-table table-name
                   :run-id run-id
                   :status status
                   :max-watermark (:max-watermark stream-state)
                   :rows-written (:rows-written stream-state)
                   :freshness-sla-seconds (or (:freshness_sla_seconds endpoint)
                                              (some-> (:freshness_sla_minutes endpoint) long (* 60))
                                              (parse-int-env :ingest-default-freshness-sla-seconds 3600))
                   :finished-at finished-at}))
                 (catch Exception e
                   (log/warn e "Failed to record endpoint freshness metadata"
                             {:graph_id graph-id
                              :api_node_id api-node-id
                              :endpoint_name (:endpoint_name endpoint)
                              :run_id run-id
                              :status status}))))
               (let [_ (ensure-final-run-manifest-invariants!
                        conn-id
                        (:endpoint_name endpoint)
                        run-id
                        (:manifests stream-state))
                     committed-partition-dates (committed-manifest-partition-dates (:manifests stream-state))
                     job-results (when (not= status "failed")
                                   (try
                                     (trigger-downstream-jobs! conn-id
                                                               target
                                                               endpoint
                                                               {:source_system source-system
                                                                :run_id run-id
                                                                :changed_partition_dates committed-partition-dates})
                                     (catch Exception e
                                       {:trigger_error (.getMessage e)})))
                     timings (merge-timing-maps
                              @timings*
                              (:timings stream-state)
                              {:endpoint_total_ms (elapsed-ms endpoint-start)})]
                 {:endpoint_name (:endpoint_name endpoint)
                  :run_id run-id
                  :status status
                  :inferred_fields (when (schema-inference-enabled? endpoint')
                                     (:inferred_fields endpoint'))
                  :schema_drift (:schema_drift endpoint')
                  :rows_written (:rows-written stream-state)
                  :bad_records (:bad-records-total stream-state)
                  :batch_count (:batch-seq stream-state)
                  :manifests (:manifests stream-state)
                  :retry_count retry-count
                  :stop_reason (:stop-reason stream-state)
                  :adaptive_rate_limit_ms (load-adaptive-backpressure-ms source-system endpoint)
                  :partition_columns partition-columns
                  :replay_source_run_id replay-run-id
                  :job_triggers job-results
                  :errors (count errors)
                  :timings timings}))
             (catch Throwable t
               (when (and (not replay-run-id)
                          @circuit-admission*
                          (not @circuit-completed?))
                 (complete-source-circuit-request!
                  source-system
                  endpoint
                  @circuit-admission*
                  {:error t
                   :final-http-status (:status (ex-data t))})
                 (reset! circuit-completed? true))
               (if continue-on-endpoint-failure?
                 (let [finished-at      (now-utc)
                       error-message    (or (ex-message t) (.getMessage t) "Endpoint execution failed")
                       failure-class    (or (:failure_class (ex-data t)) "endpoint_run_error")
                       run-row          (endpoint-run-row
                                         {:run-id run-id
                                          :source-system source-system
                                          :endpoint-name (:endpoint_name endpoint)
                                          :started-at started-at
                                          :finished-at finished-at
                                          :status "failed"
                                          :http-status nil
                                          :pages-fetched 0
                                          :rows-extracted 0
                                          :rows-written 0
                                          :retry-count 0
                                          :error-summary (json/generate-string [{:failure_class failure-class
                                                                                :message error-message}])})]
                   (try
                     (load-rows! conn-id run-detail-table [run-row])
                     (replace-checkpoint-row! conn-id checkpoint-table
                                              (checkpoint-row-for-failure
                                               checkpoint-row
                                               (checkpoint/failure-row
                                                {:source_system source-system
                                                 :endpoint_name (:endpoint_name endpoint)
                                                 :attempted_watermark nil
                                                 :attempted_cursor nil
                                                 :run_id run-id
                                                 :error_message error-message
                                                 :now finished-at})))
                     (catch Exception persist-error
                       (log/error persist-error
                                  "Failed to persist endpoint-level failure details"
                                  {:graph_id graph-id
                                   :api_node_id api-node-id
                                   :endpoint_name (:endpoint_name endpoint)
                                   :run_id run-id})))
                   {:endpoint_name (:endpoint_name endpoint)
                    :run_id run-id
                    :status "failed"
                    :rows_written 0
                    :bad_records 0
                    :batch_count 0
                    :manifests []
                    :retry_count 0
                    :stop_reason nil
                    :replay_source_run_id replay-run-id
                    :job_triggers nil
                    :errors 1
                    :timings (merge-timing-maps
                              @timings*
                              {:endpoint_total_ms (elapsed-ms endpoint-start)})
                    :failure_class failure-class
                    :error error-message})
                 (throw t)))
             (finally
               (cleanup-fetch-stream! @cancel* @errors-ch*)))))
           endpoints)
           status (aggregate-endpoint-status results)
           run-timings (merge @run-timings*
                              {:run_total_ms (elapsed-ms run-start)
                               :endpoints_total_ms (reduce + 0 (map #(long (or (get-in % [:timings :endpoint_total_ms]) 0))
                                                                    results))})]
       (log-api-run-summary!
        {:graph-id graph-id
         :api-node-id api-node-id
         :source-system source-system
         :status status
         :run-timings run-timings
         :results results})
       {:graph_id graph-id
        :api_node_id api-node-id
       :source_system source-system
        :status status
        :run_timings run-timings
        :results results}))))

(defn execute-api-request!
  ([graph-id api-node-id] (execute-api-request! graph-id api-node-id {}))
  ([graph-id api-node-id {:keys [endpoint-name] :as opts}]
   (let [graph         (db/getGraph graph-id)
         target        (find-downstream-target graph api-node-id)
         conn-id       (when target (resolved-target-connection-id target))
         dbtype        (some-> conn-id connection-dbtype str string/lower-case)
         bronze-job-id (some-> target databricks-bronze-job-config :job-id)
         replay-run-id (replay-source-run-id opts)]
     (if (and (= "databricks" dbtype)
              (seq bronze-job-id)
              (nil? replay-run-id))
       (run-api-node-via-databricks-job! graph-id api-node-id {:endpoint-name endpoint-name})
       (run-api-node! graph-id api-node-id opts)))))

(defn- source-run-request-url
  [scheme config]
  (case scheme
    :kafka (str "kafka://" (:topic_name config))
    :file (str "file://" (or (:path config) (:endpoint_name config)))
    nil))

(defn- run-bronze-source-node!
  [graph-id node-id source-node config-key fetch-stream-fn {:keys [endpoint-name source-kind]}]
  (let [g                 (db/getGraph graph-id)
        target            (find-downstream-target g node-id)
        conn-id           (require-target-connection-id! target
                                                         "No downstream target connection found for source node"
                                                         {:graph_id graph-id
                                                          :node_id node-id
                                                          :source_kind source-kind})
        source-system     (or (:source_system source-node) (name source-kind))
        configs           (select-source-configs! source-node
                                                  config-key
                                                  endpoint-name
                                                  (str (string/capitalize (name source-kind))
                                                       " node has no enabled source configs"))
        checkpoint-table  (validated-qualified-table-name (audit-table target conn-id "ingestion_checkpoint"))
        run-detail-table  (validated-qualified-table-name (audit-table target conn-id "endpoint_run_detail"))
        bad-records-table (validated-qualified-table-name (audit-table target conn-id "bad_records"))
        manifest-table    (validated-qualified-table-name (audit-table target conn-id "run_batch_manifest"))
        continue-on-config-failure? (and (nil? endpoint-name) (> (count configs) 1))]
    (ensure-table! conn-id checkpoint-table checkpoint/ingestion-checkpoint-columns {})
    (ensure-checkpoint-columns! conn-id checkpoint-table)
    (ensure-table! conn-id run-detail-table endpoint-run-detail-columns {})
    (ensure-table! conn-id bad-records-table bronze/bad-record-columns {})
    (ensure-bad-record-columns! conn-id bad-records-table)
    (ensure-table! conn-id manifest-table batch-manifest-columns {})
    (ensure-batch-manifest-columns! conn-id manifest-table)
    (let [results
          (mapv
           (fn [config]
             (let [run-id         (str (UUID/randomUUID))
                   started-at     (now-utc)
                   endpoint-name  (:endpoint_name config)
                   request-url    (source-run-request-url source-kind config)
                   checkpoint-row (fetch-checkpoint conn-id checkpoint-table source-system endpoint-name)
                   table-name-raw (endpoint->table-name target config)
                   cancel*        (atom nil)
                   errors-ch*     (atom nil)]
               (try
                 (when (string/blank? (str table-name-raw))
                   (throw (ex-info "Source config is missing bronze_table_name and target table_name"
                                   {:endpoint_name endpoint-name
                                    :status 409})))
                 (let [table-name         (validated-qualified-table-name table-name-raw)
                       partition-columns (target-partition-columns conn-id target config)
                       _                 (ensure-table! conn-id table-name (bronze/bronze-columns config)
                                                        {:partition-columns partition-columns})
                       _                 (abort-preparing-batches! conn-id {:manifest-table manifest-table
                                                                            :checkpoint-table checkpoint-table
                                                                            :bad-records-table bad-records-table
                                                                            :source-system source-system
                                                                            :endpoint-name endpoint-name})
                       fetch-result      (fetch-stream-fn {:graph-id graph-id
                                                           :node-id node-id
                                                           :source-node source-node
                                                           :config config
                                                           :checkpoint-row checkpoint-row
                                                           :run-id run-id})
                       _                 (reset! cancel* (:cancel fetch-result))
                       _                 (reset! errors-ch* (:errors fetch-result))
                       stream-state      (binding [*row-load-context* {:primary-table-name table-name
                                                                       :source-kind source-kind
                                                                       :target target
                                                                       :config config
                                                                       :copy-into-ran? (atom false)}]
                                           (process-source-stream!
                                            conn-id
                                            {:table-name table-name
                                             :bad-records-table bad-records-table
                                             :manifest-table manifest-table
                                             :checkpoint-table checkpoint-table
                                             :source-system source-system
                                             :endpoint-name endpoint-name
                                             :run-id run-id
                                             :started-at started-at
                                             :endpoint-config config
                                             :post-commit-fn (:post-commit-fn fetch-result)}
                                            (:pages fetch-result)
                                            []
                                            nil
                                            (fn [page]
                                              (bronze/build-record-rows
                                               (:body page)
                                               config
                                               {:run-id run-id
                                                :source-system source-system
                                                :now started-at
                                                :request-url request-url
                                                :page-number (:page page)
                                                :cursor (page-state-next-cursor (:state page))
                                                :http-status (get-in page [:response :status])}))))
                       errors            (collect-errors! @errors-ch*)
                       finished-at       (now-utc)
                       status            (run-status errors (:pages-fetched stream-state))
                       next-cursor       (or (:next-cursor stream-state)
                                             (some-> stream-state :final-state page-state-next-cursor))
                       retry-count       (:retry-count stream-state)
                       checkpoint-row'   (when (and (not= status "failed")
                                                    (or (nil? (:checkpoint-row stream-state))
                                                        (zero? (long (:rows-written stream-state))))
                                                    (or (some? next-cursor)
                                                        (pos? (long (:pages-fetched stream-state)))))
                                           (checkpoint/success-row
                                            {:source_system source-system
                                             :endpoint_name endpoint-name
                                             :run_id run-id
                                             :rows_ingested (:rows-written stream-state)
                                             :max_watermark (:max-watermark stream-state)
                                             :next_cursor next-cursor
                                             :now finished-at}))
                       _                 (when (= status "failed")
                                           (replace-checkpoint-row! conn-id checkpoint-table
                                                                    (checkpoint-row-for-failure
                                                                     checkpoint-row
                                                                     (checkpoint/failure-row
                                                                      {:source_system source-system
                                                                       :endpoint_name endpoint-name
                                                                       :attempted_watermark nil
                                                                       :attempted_cursor next-cursor
                                                                       :now finished-at}))))
                       run-row           (endpoint-run-row
                                          {:run-id run-id
                                           :source-system source-system
                                           :endpoint-name endpoint-name
                                           :started-at started-at
                                           :finished-at finished-at
                                           :status status
                                           :http-status (or (:final-http-status stream-state)
                                                            (:last-http-status stream-state))
                                           :pages-fetched (:pages-fetched stream-state)
                                           :rows-extracted (:rows-extracted stream-state)
                                           :rows-written (:rows-written stream-state)
                                           :retry-count retry-count
                                           :error-summary (when (seq errors)
                                                            (json/generate-string errors))})]
                   ((if (and checkpoint-row' (atomic-batch-commit? conn-id))
                      (fn []
                        (with-batch-commit
                          conn-id
                          (fn []
                            (load-rows! conn-id run-detail-table [run-row])
                            (replace-checkpoint-row! conn-id checkpoint-table checkpoint-row'))))
                      (fn []
                        (load-rows! conn-id run-detail-table [run-row])
                        (when checkpoint-row'
                          (replace-checkpoint-row! conn-id checkpoint-table checkpoint-row')))))
                   (let [_ (ensure-final-run-manifest-invariants!
                            conn-id
                            endpoint-name
                            run-id
                            (:manifests stream-state))
                         committed-partition-dates (committed-manifest-partition-dates (:manifests stream-state))
                         job-results (when (not= status "failed")
                                       (try
                                         (trigger-downstream-jobs! conn-id
                                                                   target
                                                                   config
                                                                   {:source_system source-system
                                                                    :run_id run-id
                                                                    :changed_partition_dates committed-partition-dates})
                                         (catch Exception e
                                           {:trigger_error (.getMessage e)})))]
                     {:endpoint_name endpoint-name
                      :run_id run-id
                      :status status
                      :rows_written (:rows-written stream-state)
                      :bad_records (:bad-records-total stream-state)
                      :batch_count (:batch-seq stream-state)
                      :manifests (:manifests stream-state)
                      :retry_count retry-count
                      :stop_reason (:stop-reason stream-state)
                      :partition_columns partition-columns
                      :job_triggers job-results
                      :errors (count errors)}))
                 (catch Throwable t
                   (if continue-on-config-failure?
                     {:endpoint_name endpoint-name
                      :run_id run-id
                      :status "failed"
                      :rows_written 0
                      :bad_records 0
                      :batch_count 0
                      :manifests []
                      :retry_count 0
                      :stop_reason nil
                      :job_triggers nil
                      :errors 1
                      :failure_class (or (:failure_class (ex-data t)) "source_run_error")
                      :error (or (ex-message t) (.getMessage t) "Source execution failed")}
                     (throw t)))
                 (finally
                   (cleanup-fetch-stream! @cancel* @errors-ch*)))))
           configs)
          status (aggregate-endpoint-status results)]
      {:graph_id graph-id
       :node_id node-id
       :source_system source-system
       :status status
       :results results})))

(defn run-kafka-node!
  ([graph-id node-id] (run-kafka-node! graph-id node-id {}))
  ([graph-id node-id {:keys [endpoint-name poll-fn commit-fn close-fn consumer-ops-factory] :as opts}]
   (let [source-node (g2/getData (db/getGraph graph-id) node-id)]
     (run-bronze-source-node!
      graph-id
      node-id
      source-node
      :topic_configs
      (fn [{:keys [config checkpoint-row run-id]}]
        (let [initial-cursor (parse-json-cursor (:last_successful_cursor checkpoint-row))
              consumer-ops   (or (when consumer-ops-factory
                                   (consumer-ops-factory {:graph-id graph-id
                                                          :node-id node-id
                                                          :source-node source-node
                                                          :config config
                                                          :checkpoint-row checkpoint-row
                                                          :run-id run-id}))
                                 (when (or poll-fn commit-fn close-fn)
                                   {:poll-fn poll-fn
                                    :commit-fn commit-fn
                                    :close-fn close-fn})
                                 (kafka/native-consumer-ops {:source-node source-node
                                                             :topic-config config
                                                             :topic-name (:topic_name config)
                                                             :initial-cursor initial-cursor}))
              fetch-result   (kafka/fetch-kafka-async
                              {:topic-name (:topic_name config)
                               :topic-config config
                               :initial-cursor initial-cursor
                               :poll-timeout-ms (:poll-timeout-ms consumer-ops)
                               :rate-limit-ms (or (:rate_limit_per_poll_ms config) 0)
                               :poll-fn (or (:poll-fn consumer-ops)
                                            (:poll-fn opts)
                                            (fn [_]
                                              (throw (ex-info "Kafka runtime requires an injected poll-fn or native consumer support"
                                                              {:failure_class "unsupported"}))))
                               :commit-fn (or (:commit-fn consumer-ops) commit-fn)
                               :close-fn (or (:close-fn consumer-ops) close-fn)
                               :wakeup-fn (:wakeup-fn consumer-ops)})]
          (assoc fetch-result
                 :post-commit-fn (fn [{:keys [buffer]}]
                                   (when-let [commit! (:commit! fetch-result)]
                                     (when-let [offsets (get-in buffer [:last-state :offsets])]
                                       (commit! offsets)))))))
      {:endpoint-name endpoint-name
       :source-kind :kafka}))))

(defn run-file-node!
  ([graph-id node-id] (run-file-node! graph-id node-id {}))
  ([graph-id node-id {:keys [endpoint-name] :as _opts}]
   (let [source-node (g2/getData (db/getGraph graph-id) node-id)]
     (run-bronze-source-node!
      graph-id
      node-id
      source-node
      :file_configs
      (fn [{:keys [source-node config checkpoint-row]}]
        (let [cursor-map (or (parse-json-cursor (:last_successful_cursor checkpoint-row)) {})
              paths      (or (:paths config) [])
              changed    (->> paths
                              (filter (fn [path]
                                        (let [checksum (try
                                                         (file-connector/file-checksum source-node config path)
                                                         (catch Exception e
                                                           (log/warn e "Failed to compute file checksum; treating file as changed"
                                                                     {:path path
                                                                      :endpoint_name (:endpoint_name config)})
                                                           ::checksum-unavailable))]
                                          (or (= checksum ::checksum-unavailable)
                                              (not= (get cursor-map path) checksum)))))
                              vec)
              config'    (assoc config :paths (if (seq changed) changed paths))]
          (file-connector/fetch-files-async {:source-node source-node
                                             :file-config config'})))
      {:endpoint-name endpoint-name
       :source-kind :file}))))

(defn preview-copybook-schema
  [{:keys [copybook encoding]}]
  (let [field-specs (file-connector/parse-copybook copybook)]
    {:field_count (count field-specs)
     :encoding (or encoding "UTF-8")
     :field_specs field-specs}))

(defn rollback-api-batch!
  [graph-id api-node-id batch-id {:keys [endpoint-name rollback-reason rolled-back-by]
                                  :or {rolled-back-by "system"}}]
  (let [g                 (db/getGraph graph-id)
        api-node          (g2/getData g api-node-id)
        target            (find-downstream-target g api-node-id)
        conn-id           (require-target-connection-id! target
                                                         "No downstream target connection found for API node"
                                                         {:graph_id graph-id
                                                          :api_node_id api-node-id})
        endpoint          (select-endpoint! api-node endpoint-name)
        bad-records-table (validated-qualified-table-name (audit-table target conn-id "bad_records"))
        manifest-table    (validated-qualified-table-name (audit-table target conn-id "run_batch_manifest"))]
    (ensure-table! conn-id manifest-table batch-manifest-columns {})
    (ensure-batch-manifest-columns! conn-id manifest-table)
    (let [manifest (manifest-row-by-batch-id conn-id manifest-table batch-id)]
      (when-not manifest
        (throw (ex-info "Batch manifest not found" {:batch_id batch-id :status 404})))
      (when-not (= (:endpoint_name manifest) (:endpoint_name endpoint))
        (throw (ex-info "Batch manifest does not belong to the requested endpoint"
                        {:batch_id batch-id
                         :requested_endpoint (:endpoint_name endpoint)
                         :manifest_endpoint (:endpoint_name manifest)
                         :status 409})))
      (when (#{"preparing" "pending_checkpoint"} (:status manifest))
        (throw (ex-info "In-flight batches cannot be rolled back manually"
                        {:batch_id batch-id
                         :status 409
                         :manifest_status (:status manifest)})))
      (when (= "rolled_back" (:status manifest))
        (throw (ex-info "Batch has already been rolled back"
                        {:batch_id batch-id :status 409})))
      (with-batch-commit
        conn-id
        (fn []
          (delete-rows-by-column! conn-id (:table_name manifest) :batch_id batch-id)
          (delete-rows-by-column! conn-id bad-records-table :batch_id batch-id)
          (mark-manifest-row! conn-id manifest-table
                              (assoc manifest
                                     :status "rolled_back"
                                     :active false
                                     :rollback_reason (or rollback-reason "operator_requested")
                                     :rolled_back_by rolled-back-by
                                     :rolled_back_at_utc (str (now-utc))))
          {:graph_id graph-id
           :api_node_id api-node-id
           :endpoint_name (:endpoint_name endpoint)
           :batch_id batch-id
           :status "rolled_back"
           :checkpoint_behavior "unchanged"
           :replay_required_from_override true})))))

(mount/defstate ^{:on-reload :noop} ingest-retention-maintenance
  :start
  (when (and (or (config/enabled-role? :api)
                 (config/enabled-role? :worker))
             (parse-bool-env :bitool-enable-ingest-retention-maintenance true))
    (let [running? (atom true)
          poll-ms  (parse-int-env :bitool-ingest-retention-poll-ms 3600000)
          thread   (future
                     (while @running?
                       (try
                         (cleanup-ingest-retention!)
                        (catch Exception e
                          (log/error e "Ingest retention maintenance iteration failed")))
                       (loop [remaining (long poll-ms)]
                         (when (and @running? (pos? remaining))
                           (let [sleep-ms (long (min 1000 remaining))]
                             (Thread/sleep sleep-ms)
                             (recur (- remaining sleep-ms)))))))]
      {:running? running?
       :thread thread}))
  :stop
  (do
    (when-let [running? (:running? ingest-retention-maintenance)]
      (reset! running? false))
    (when-let [thread (:thread ingest-retention-maintenance)]
      (future-cancel thread))))
