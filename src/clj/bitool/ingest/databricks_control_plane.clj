(ns bitool.ingest.databricks-control-plane
  (:require [bitool.control-plane :as control-plane]
            [bitool.db :as db]
            [bitool.graph2 :as g2]
            [bitool.ingest.checkpoint :as checkpoint]
            [bitool.operations :as operations]
            [cheshire.core :as json]
            [clojure.string :as string]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

(def ^:private checkpoint-table "ingest_control_plane_checkpoint")
(def ^:private run-detail-table "ingest_control_plane_run_detail")
(def ^:private batch-manifest-table "ingest_control_plane_batch_manifest")
(defonce ^:private ready? (atom false))

(defn- db-opts
  [conn]
  (jdbc/with-options conn {:builder-fn rs/as-unqualified-lower-maps}))

(defn- non-blank-str
  [value]
  (let [value (some-> value str string/trim)]
    (when (seq value) value)))

(defn- now-utc []
  (java.time.Instant/now))

(declare current-checkpoint)

(defn- ->sql-ts
  [inst]
  (when inst
    (java.sql.Timestamp/from inst)))

(defn- ->instant
  [value]
  (cond
    (nil? value) nil
    (instance? java.time.Instant value) value
    (instance? java.sql.Timestamp value) (.toInstant ^java.sql.Timestamp value)
    (string? value) (checkpoint/parse-instant value)
    :else value))

(defn ensure-tables!
  []
  (when-not @ready?
    (locking ready?
      (when-not @ready?
        (doseq [sql-str
                [(str "CREATE TABLE IF NOT EXISTS " checkpoint-table " ("
                      "graph_id INTEGER NOT NULL, "
                      "api_node_id INTEGER NOT NULL, "
                      "tenant_key VARCHAR(128) NOT NULL, "
                      "workspace_key VARCHAR(128) NOT NULL, "
                      "source_system TEXT NOT NULL, "
                      "endpoint_name TEXT NOT NULL, "
                      "target_table TEXT NULL, "
                      "last_successful_watermark TEXT NULL, "
                      "last_attempted_watermark TEXT NULL, "
                      "last_successful_cursor TEXT NULL, "
                      "last_attempted_cursor TEXT NULL, "
                      "last_successful_run_id TEXT NULL, "
                      "last_successful_batch_id TEXT NULL, "
                      "last_successful_batch_seq INTEGER NULL, "
                      "last_status VARCHAR(32) NULL, "
                      "rows_ingested BIGINT NOT NULL DEFAULT 0, "
                      "last_success_at_utc TIMESTAMPTZ NULL, "
                      "updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                      "PRIMARY KEY (graph_id, api_node_id, endpoint_name))")
                 (str "CREATE INDEX IF NOT EXISTS idx_ingest_cp_checkpoint_workspace "
                      "ON " checkpoint-table " (workspace_key, updated_at_utc DESC)")
                 (str "CREATE TABLE IF NOT EXISTS " run-detail-table " ("
                      "id BIGSERIAL PRIMARY KEY, "
                      "graph_id INTEGER NOT NULL, "
                      "api_node_id INTEGER NOT NULL, "
                      "tenant_key VARCHAR(128) NOT NULL, "
                      "workspace_key VARCHAR(128) NOT NULL, "
                      "source_system TEXT NOT NULL, "
                      "endpoint_name TEXT NOT NULL, "
                      "target_table TEXT NULL, "
                      "run_id TEXT NOT NULL, "
                      "started_at_utc TIMESTAMPTZ NOT NULL, "
                      "finished_at_utc TIMESTAMPTZ NULL, "
                      "status VARCHAR(32) NOT NULL, "
                      "http_status_code INTEGER NULL, "
                      "pages_fetched INTEGER NULL, "
                      "rows_extracted BIGINT NULL, "
                      "rows_written BIGINT NULL, "
                      "retry_count INTEGER NULL, "
                      "max_watermark TEXT NULL, "
                      "next_cursor TEXT NULL, "
                      "error_summary_json TEXT NULL, "
                      "result_json TEXT NULL, "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                      "UNIQUE (graph_id, api_node_id, endpoint_name, run_id))")
                 (str "CREATE INDEX IF NOT EXISTS idx_ingest_cp_run_detail_workspace "
                      "ON " run-detail-table " (workspace_key, finished_at_utc DESC)")
                 (str "CREATE TABLE IF NOT EXISTS " batch-manifest-table " ("
                      "id BIGSERIAL PRIMARY KEY, "
                      "graph_id INTEGER NOT NULL, "
                      "api_node_id INTEGER NOT NULL, "
                      "tenant_key VARCHAR(128) NOT NULL, "
                      "workspace_key VARCHAR(128) NOT NULL, "
                      "source_system TEXT NOT NULL, "
                      "endpoint_name TEXT NOT NULL, "
                      "target_table TEXT NULL, "
                      "run_id TEXT NOT NULL, "
                      "batch_id TEXT NOT NULL, "
                      "batch_seq INTEGER NOT NULL, "
                      "status VARCHAR(32) NOT NULL, "
                      "row_count BIGINT NOT NULL DEFAULT 0, "
                      "bad_record_count BIGINT NOT NULL DEFAULT 0, "
                      "byte_count BIGINT NOT NULL DEFAULT 0, "
                      "page_count INTEGER NOT NULL DEFAULT 0, "
                      "partition_dates_json TEXT NULL, "
                      "source_bad_record_ids_json TEXT NULL, "
                      "max_watermark TEXT NULL, "
                      "next_cursor TEXT NULL, "
                      "artifact_path TEXT NULL, "
                      "artifact_checksum TEXT NULL, "
                      "active BOOLEAN NOT NULL DEFAULT TRUE, "
                      "rollback_reason TEXT NULL, "
                      "rolled_back_by TEXT NULL, "
                      "rolled_back_at_utc TIMESTAMPTZ NULL, "
                      "archived_at_utc TIMESTAMPTZ NULL, "
                      "started_at_utc TIMESTAMPTZ NOT NULL, "
                      "committed_at_utc TIMESTAMPTZ NULL, "
                      "result_json TEXT NULL, "
                      "updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                      "UNIQUE (graph_id, api_node_id, endpoint_name, run_id, batch_id))")
                 (str "CREATE INDEX IF NOT EXISTS idx_ingest_cp_manifest_workspace "
                      "ON " batch-manifest-table " (workspace_key, updated_at_utc DESC)")]]
          (jdbc/execute! db/ds [sql-str]))
        (reset! ready? true))))
  nil)

(defn- endpoint->table-name
  [target endpoint]
  (let [seed (or (non-blank-str (:bronze_table_name endpoint))
                 (non-blank-str (:table_name target))
                 (some-> (:endpoint_name endpoint)
                         string/lower-case
                         (string/replace #"[^a-z0-9_]+" "_")
                         (string/replace #"^_+|_+$" "")))]
    (db/fully-qualified-table-name target (or seed "bronze_auto"))))

(defn- target-connection-id
  [target]
  (let [raw (or (:connection_id target) (:c target) (:connection target))]
    (cond
      (integer? raw) raw
      (number? raw) (int raw)
      (string? raw) (let [trimmed (string/trim raw)]
                      (when (re-matches #"\d+" trimmed)
                        (Integer/parseInt trimmed)))
      :else nil)))

(defn- find-downstream-target
  [g start-id]
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

(defn- endpoint-run-bootstrap
  [target checkpoint-row endpoint]
  {:endpoint_name (:endpoint_name endpoint)
   :endpoint_url (:endpoint_url endpoint)
   :http_method (or (:http_method endpoint) "GET")
   :query_params (or (:query_params endpoint) {})
   :body_params (or (:body_params endpoint) {})
   :request_headers (or (:request_headers endpoint) {})
   :pagination_strategy (:pagination_strategy endpoint)
   :pagination_location (:pagination_location endpoint)
   :page_size (:page_size endpoint)
   :page_param (:page_param endpoint)
   :size_param (:size_param endpoint)
   :offset_param (:offset_param endpoint)
   :limit_param (:limit_param endpoint)
   :cursor_param (:cursor_param endpoint)
   :cursor_field (:cursor_field endpoint)
   :token_param (:token_param endpoint)
   :token_field (:token_field endpoint)
   :time_param (:time_param endpoint)
   :time_field (:time_field endpoint)
   :window_size_param (:window_size_param endpoint)
   :time_window_minutes (:time_window_minutes endpoint)
   :watermark_param (:watermark_param endpoint)
   :watermark_column (:watermark_column endpoint)
   :watermark_overlap_minutes (:watermark_overlap_minutes endpoint)
   :primary_key_fields (vec (or (:primary_key_fields endpoint) []))
   :json_explode_rules (vec (or (:json_explode_rules endpoint) []))
   :target_table (endpoint->table-name target endpoint)
   :checkpoint checkpoint-row})

(defn bootstrap
  [graph-id api-node-id {:keys [endpoint-name]}]
  (ensure-tables!)
  (let [graph (db/getGraph graph-id)
        api-node (g2/getData graph api-node-id)
        target (find-downstream-target graph api-node-id)
        conn-id (target-connection-id target)
        connection-row (first (db/get-connection conn-id))
        workspace-context (control-plane/graph-workspace-context graph-id)
        source-system (or (:source_system api-node) "samara")
        endpoints (->> (:endpoint_configs api-node)
                       (filter #(not= false (:enabled %)))
                       (filter (fn [endpoint]
                                 (if endpoint-name
                                   (= endpoint-name (:endpoint_name endpoint))
                                   true)))
                       vec)]
    (when-not api-node
      (throw (ex-info "API node not found"
                      {:status 404
                       :graph_id graph-id
                       :api_node_id api-node-id})))
    (when-not target
      (throw (ex-info "No downstream target found for API node"
                      {:status 404
                       :graph_id graph-id
                       :api_node_id api-node-id})))
    {:graph_id graph-id
     :api_node_id api-node-id
     :tenant_key (:tenant_key workspace-context)
     :workspace_key (:workspace_key workspace-context)
     :source_system source-system
     :api_base_url (:base_url api-node)
     :warehouse (some-> connection-row :dbtype str string/lower-case)
     :catalog (:catalog connection-row)
     :schema (:schema connection-row)
     :connection_id conn-id
     :endpoints (mapv (fn [endpoint]
                        (endpoint-run-bootstrap
                         target
                         (current-checkpoint graph-id api-node-id (:endpoint_name endpoint))
                         endpoint))
                      endpoints)}))

(defn current-checkpoint
  [graph-id api-node-id endpoint-name]
  (ensure-tables!)
  (jdbc/execute-one!
   (db-opts db/ds)
   [(str "SELECT * FROM " checkpoint-table
         " WHERE graph_id = ? AND api_node_id = ? AND endpoint_name = ?")
    graph-id
    api-node-id
    (str endpoint-name)]))

(defn reset-checkpoint!
  [{:keys [graph-id api-node-id endpoint-name requested-by reason reset-to-cursor reset-to-watermark]}]
  (ensure-tables!)
  (when-not (non-blank-str reason)
    (throw (ex-info "reason is required for checkpoint reset" {:status 400})))
  (let [graph (db/getGraph graph-id)
        api-node (g2/getData graph api-node-id)
        workspace-context (control-plane/graph-workspace-context graph-id)
        source-system (or (:source_system api-node) "samara")
        row {:graph_id graph-id
             :api_node_id api-node-id
             :tenant_key (or (:tenant_key workspace-context) "default")
             :workspace_key (or (:workspace_key workspace-context) "default")
             :endpoint_name (str endpoint-name)
             :last_successful_watermark (non-blank-str reset-to-watermark)
             :last_attempted_watermark nil
             :last_successful_cursor (non-blank-str reset-to-cursor)
             :last_attempted_cursor nil
             :last_successful_run_id nil
             :last_successful_batch_id nil
             :last_successful_batch_seq nil
             :last_status "reset"
             :rows_ingested 0
             :last_success_at_utc nil
             :updated_at_utc (now-utc)}]
    (jdbc/execute!
     db/ds
     [(str "INSERT INTO " checkpoint-table "
            (graph_id, api_node_id, tenant_key, workspace_key, source_system, endpoint_name, target_table,
             last_successful_watermark, last_attempted_watermark, last_successful_cursor, last_attempted_cursor,
             last_successful_run_id, last_successful_batch_id, last_successful_batch_seq, last_status,
             rows_ingested, last_success_at_utc, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (graph_id, api_node_id, endpoint_name) DO UPDATE
            SET tenant_key = EXCLUDED.tenant_key,
                workspace_key = EXCLUDED.workspace_key,
                last_successful_watermark = EXCLUDED.last_successful_watermark,
                last_attempted_watermark = EXCLUDED.last_attempted_watermark,
                last_successful_cursor = EXCLUDED.last_successful_cursor,
                last_attempted_cursor = EXCLUDED.last_attempted_cursor,
                last_successful_run_id = EXCLUDED.last_successful_run_id,
                last_successful_batch_id = EXCLUDED.last_successful_batch_id,
                last_successful_batch_seq = EXCLUDED.last_successful_batch_seq,
                last_status = EXCLUDED.last_status,
                rows_ingested = EXCLUDED.rows_ingested,
                last_success_at_utc = EXCLUDED.last_success_at_utc,
                updated_at_utc = EXCLUDED.updated_at_utc")
      graph-id
      api-node-id
      (or (:tenant_key workspace-context) "default")
      (or (:workspace_key workspace-context) "default")
      source-system
      (str endpoint-name)
      (non-blank-str reset-to-watermark)
      nil
      (non-blank-str reset-to-cursor)
      nil
      nil
      nil
      nil
      "reset"
      0
      nil
      (->sql-ts (:updated_at_utc row))])
    {:requested_by (or requested-by "system")
     :reason reason
     :checkpoint row}))

(defn- checkpoint-row-for-result
  [source-system endpoint-name run-id result finished-at]
  (let [status (some-> (:status result) str string/lower-case)
        max-watermark (non-blank-str (:max_watermark result))
        next-cursor (non-blank-str (:next_cursor result))
        batch-id (non-blank-str (:batch_id result))
        batch-seq (some-> (:batch_seq result) long)]
    (if (= status "failed")
      (checkpoint/failure-row
       {:source_system source-system
        :endpoint_name endpoint-name
        :attempted_watermark max-watermark
        :attempted_cursor next-cursor
        :now finished-at})
      (checkpoint/success-row
       {:source_system source-system
        :endpoint_name endpoint-name
        :run_id run-id
        :batch_id batch-id
        :batch_seq batch-seq
        :rows_ingested (long (or (:rows_written result) 0))
        :max_watermark max-watermark
        :next_cursor next-cursor
        :now finished-at
        :status status}))))

(defn- upsert-checkpoint!
  [workspace-context graph-id api-node-id source-system endpoint-name target-table result finished-at]
  (let [row (checkpoint-row-for-result source-system endpoint-name (:run_id result) result finished-at)
        success? (contains? #{"success" "partial_success"}
                            (some-> (:status result) str string/lower-case))]
    (jdbc/execute!
     db/ds
     [(str "INSERT INTO " checkpoint-table "
            (graph_id, api_node_id, tenant_key, workspace_key, source_system, endpoint_name, target_table,
             last_successful_watermark, last_attempted_watermark, last_successful_cursor, last_attempted_cursor,
             last_successful_run_id, last_successful_batch_id, last_successful_batch_seq, last_status,
             rows_ingested, last_success_at_utc, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (graph_id, api_node_id, endpoint_name) DO UPDATE
            SET tenant_key = EXCLUDED.tenant_key,
                workspace_key = EXCLUDED.workspace_key,
                source_system = EXCLUDED.source_system,
                target_table = EXCLUDED.target_table,
                last_successful_watermark = COALESCE(EXCLUDED.last_successful_watermark, " checkpoint-table ".last_successful_watermark),
                last_attempted_watermark = COALESCE(EXCLUDED.last_attempted_watermark, " checkpoint-table ".last_attempted_watermark),
                last_successful_cursor = COALESCE(EXCLUDED.last_successful_cursor, " checkpoint-table ".last_successful_cursor),
                last_attempted_cursor = COALESCE(EXCLUDED.last_attempted_cursor, " checkpoint-table ".last_attempted_cursor),
                last_successful_run_id = COALESCE(EXCLUDED.last_successful_run_id, " checkpoint-table ".last_successful_run_id),
                last_successful_batch_id = COALESCE(EXCLUDED.last_successful_batch_id, " checkpoint-table ".last_successful_batch_id),
                last_successful_batch_seq = COALESCE(EXCLUDED.last_successful_batch_seq, " checkpoint-table ".last_successful_batch_seq),
                last_status = EXCLUDED.last_status,
                rows_ingested = EXCLUDED.rows_ingested,
                last_success_at_utc = COALESCE(EXCLUDED.last_success_at_utc, " checkpoint-table ".last_success_at_utc),
                updated_at_utc = EXCLUDED.updated_at_utc")
      graph-id
      api-node-id
      (or (:tenant_key workspace-context) "default")
      (or (:workspace_key workspace-context) "default")
      source-system
      endpoint-name
      target-table
      (:last_successful_watermark row)
      (:last_attempted_watermark row)
      (:last_successful_cursor row)
      (:last_attempted_cursor row)
      (:last_successful_run_id row)
      (:last_successful_batch_id row)
      (:last_successful_batch_seq row)
      (:last_status row)
      (long (or (:rows_ingested row) 0))
      (when success? (->sql-ts finished-at))
      (->sql-ts finished-at)])))

(defn- insert-run-detail!
  [workspace-context graph-id api-node-id source-system endpoint-name target-table result started-at finished-at]
  (jdbc/execute!
   db/ds
   [(str "INSERT INTO " run-detail-table "
          (graph_id, api_node_id, tenant_key, workspace_key, source_system, endpoint_name, target_table,
           run_id, started_at_utc, finished_at_utc, status, http_status_code, pages_fetched,
           rows_extracted, rows_written, retry_count, max_watermark, next_cursor, error_summary_json, result_json)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT (graph_id, api_node_id, endpoint_name, run_id) DO UPDATE
          SET tenant_key = EXCLUDED.tenant_key,
              workspace_key = EXCLUDED.workspace_key,
              source_system = EXCLUDED.source_system,
              target_table = EXCLUDED.target_table,
              started_at_utc = EXCLUDED.started_at_utc,
              finished_at_utc = EXCLUDED.finished_at_utc,
              status = EXCLUDED.status,
              http_status_code = EXCLUDED.http_status_code,
              pages_fetched = EXCLUDED.pages_fetched,
              rows_extracted = EXCLUDED.rows_extracted,
              rows_written = EXCLUDED.rows_written,
              retry_count = EXCLUDED.retry_count,
              max_watermark = EXCLUDED.max_watermark,
              next_cursor = EXCLUDED.next_cursor,
              error_summary_json = EXCLUDED.error_summary_json,
              result_json = EXCLUDED.result_json")
    graph-id
    api-node-id
    (or (:tenant_key workspace-context) "default")
    (or (:workspace_key workspace-context) "default")
    source-system
    endpoint-name
    target-table
    (str (:run_id result))
    (->sql-ts started-at)
    (->sql-ts finished-at)
    (or (:status result) "unknown")
    (some-> (:http_status_code result) long)
    (some-> (:pages_fetched result) int)
    (some-> (or (:rows_extracted result) (:rows_written result)) long)
    (some-> (:rows_written result) long)
    (some-> (:retry_count result) int)
    (non-blank-str (:max_watermark result))
    (non-blank-str (:next_cursor result))
    (when-let [error (:error result)]
      (json/generate-string {:error error}))
    (json/generate-string result)]))

(defn- upsert-manifest!
  [workspace-context graph-id api-node-id source-system endpoint-name target-table run-id started-at finished-at manifest]
  (jdbc/execute!
   db/ds
   [(str "INSERT INTO " batch-manifest-table "
          (graph_id, api_node_id, tenant_key, workspace_key, source_system, endpoint_name, target_table,
           run_id, batch_id, batch_seq, status, row_count, bad_record_count, byte_count, page_count,
           partition_dates_json, source_bad_record_ids_json, max_watermark, next_cursor, artifact_path,
           artifact_checksum, active, rollback_reason, rolled_back_by, rolled_back_at_utc, archived_at_utc,
           started_at_utc, committed_at_utc, result_json, updated_at_utc)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
          ON CONFLICT (graph_id, api_node_id, endpoint_name, run_id, batch_id) DO UPDATE
          SET tenant_key = EXCLUDED.tenant_key,
              workspace_key = EXCLUDED.workspace_key,
              source_system = EXCLUDED.source_system,
              target_table = EXCLUDED.target_table,
              batch_seq = EXCLUDED.batch_seq,
              status = EXCLUDED.status,
              row_count = EXCLUDED.row_count,
              bad_record_count = EXCLUDED.bad_record_count,
              byte_count = EXCLUDED.byte_count,
              page_count = EXCLUDED.page_count,
              partition_dates_json = EXCLUDED.partition_dates_json,
              source_bad_record_ids_json = EXCLUDED.source_bad_record_ids_json,
              max_watermark = EXCLUDED.max_watermark,
              next_cursor = EXCLUDED.next_cursor,
              artifact_path = EXCLUDED.artifact_path,
              artifact_checksum = EXCLUDED.artifact_checksum,
              active = EXCLUDED.active,
              rollback_reason = EXCLUDED.rollback_reason,
              rolled_back_by = EXCLUDED.rolled_back_by,
              rolled_back_at_utc = EXCLUDED.rolled_back_at_utc,
              archived_at_utc = EXCLUDED.archived_at_utc,
              started_at_utc = EXCLUDED.started_at_utc,
              committed_at_utc = EXCLUDED.committed_at_utc,
              result_json = EXCLUDED.result_json,
              updated_at_utc = now()")
    graph-id
    api-node-id
    (or (:tenant_key workspace-context) "default")
    (or (:workspace_key workspace-context) "default")
    source-system
    endpoint-name
    target-table
    (str run-id)
    (or (non-blank-str (:batch_id manifest))
        (str run-id "-b000001"))
    (int (or (:batch_seq manifest) 1))
    (or (:status manifest) "committed")
    (long (or (:row_count manifest) 0))
    (long (or (:bad_record_count manifest) 0))
    (long (or (:byte_count manifest) 0))
    (int (or (:page_count manifest) 0))
    (when-let [partition-dates (:partition_dates manifest)]
      (json/generate-string partition-dates))
    (when-let [ids (:source_bad_record_ids manifest)]
      (json/generate-string ids))
    (non-blank-str (:max_watermark manifest))
    (non-blank-str (:next_cursor manifest))
    (non-blank-str (:artifact_path manifest))
    (non-blank-str (:artifact_checksum manifest))
    (not= false (:active manifest))
    (non-blank-str (:rollback_reason manifest))
    (non-blank-str (:rolled_back_by manifest))
    (some-> (:rolled_back_at_utc manifest) ->instant ->sql-ts)
    (some-> (:archived_at_utc manifest) ->instant ->sql-ts)
    (->sql-ts (or (some-> (:started_at_utc manifest) ->instant) started-at))
    (->sql-ts (or (some-> (:committed_at_utc manifest) ->instant) finished-at))
    (json/generate-string manifest)]))

(defn record-callback!
  [{:keys [graph_id api_node_id source_system completed_at_utc results]}]
  (ensure-tables!)
  (when-not (some? graph_id)
    (throw (ex-info "graph_id is required" {:status 400})))
  (when-not (some? api_node_id)
    (throw (ex-info "api_node_id is required" {:status 400})))
  (let [graph-id (int graph_id)
        api-node-id (int api_node_id)
        workspace-context (control-plane/graph-workspace-context graph-id)
        finished-at (or (some-> completed_at_utc ->instant) (now-utc))
        source-system (or (non-blank-str source_system) "samara")
        results (vec (or results []))]
    (when-not (seq results)
      (throw (ex-info "results is required" {:status 400})))
    (doseq [result results]
      (let [endpoint-name (or (non-blank-str (:endpoint_name result))
                              (non-blank-str (:endpoint result)))
            target-table (or (non-blank-str (:target_table result))
                             (non-blank-str (:table result)))
            started-at (or (some-> (:started_at_utc result) ->instant) finished-at)
            run-id (or (non-blank-str (:run_id result))
                       (throw (ex-info "result.run_id is required"
                                       {:status 400
                                        :endpoint_name endpoint-name})))]
        (when-not endpoint-name
          (throw (ex-info "result.endpoint_name is required"
                          {:status 400})))
        (insert-run-detail! workspace-context graph-id api-node-id source-system endpoint-name target-table result started-at finished-at)
        (upsert-checkpoint! workspace-context graph-id api-node-id source-system endpoint-name target-table (assoc result :run_id run-id) finished-at)
        (doseq [manifest (or (:manifests result)
                             [{:batch_id (:batch_id result)
                               :batch_seq (or (:batch_seq result) 1)
                               :status (if (= "failed" (some-> (:status result) str string/lower-case))
                                         "failed"
                                         "committed")
                               :row_count (or (:rows_written result) 0)
                               :page_count (or (:pages_fetched result) 0)
                               :max_watermark (:max_watermark result)
                               :next_cursor (:next_cursor result)}])]
          (upsert-manifest! workspace-context graph-id api-node-id source-system endpoint-name target-table run-id started-at finished-at manifest))
        (operations/record-endpoint-freshness!
         {:graph-id graph-id
          :api-node-id api-node-id
          :tenant-key (:tenant_key workspace-context)
          :workspace-key (:workspace_key workspace-context)
          :source-system source-system
          :endpoint-name endpoint-name
          :target-table target-table
          :run-id run-id
          :status (or (:status result) "unknown")
          :max-watermark (:max_watermark result)
          :rows-written (:rows_written result)
          :finished-at finished-at})))
    {:graph_id graph-id
     :api_node_id api-node-id
     :source_system source-system
     :result_count (count results)
     :completed_at_utc (str finished-at)}))

(defn latest-run-results
  [graph-id api-node-id {:keys [endpoint-name started-after limit]
                         :or {limit 50}}]
  (ensure-tables!)
  (let [started-after (some-> started-after ->instant)
        rows (jdbc/execute!
              (db-opts db/ds)
              (cond-> [(str "SELECT *
                             FROM " run-detail-table "
                            WHERE graph_id = ? AND api_node_id = ?"
                            (when endpoint-name " AND endpoint_name = ?")
                            (when started-after " AND started_at_utc >= ?")
                            " ORDER BY started_at_utc DESC
                              LIMIT ?")
                       graph-id
                       api-node-id]
                endpoint-name (conj (str endpoint-name))
                started-after (conj (->sql-ts started-after))
                true (conj (max 1 (long limit)))))]
    (mapv (fn [row]
            (merge {:run_id (:run_id row)
                    :endpoint_name (:endpoint_name row)
                    :target_table (:target_table row)
                    :status (:status row)
                    :started_at_utc (:started_at_utc row)
                    :finished_at_utc (:finished_at_utc row)
                    :rows_written (long (or (:rows_written row) 0))
                    :rows_extracted (long (or (:rows_extracted row) 0))
                    :pages_fetched (long (or (:pages_fetched row) 0))
                    :max_watermark (:max_watermark row)
                    :next_cursor (:next_cursor row)}
                   (when-let [result-json (:result_json row)]
                     (if (string? result-json)
                       (json/parse-string result-json true)
                       result-json))))
          rows)))
