(ns bitool.ingest.execution
  (:require [bitool.config :as config :refer [env]]
            [bitool.control-plane :as control-plane]
            [bitool.db :as db]
            [bitool.graph2 :as g2]
            [bitool.platform.plugins :as plugins]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [mount.core :as mount]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import [java.security MessageDigest]
           [java.util HexFormat UUID]))

(def ^:private graph-version-table "graph_version")
(def ^:private graph-release-table "graph_release")
(def ^:private execution-request-table "execution_request")
(def ^:private execution-run-table "execution_run")
(def ^:private node-run-table "node_run")
(def ^:private execution-lease-heartbeat-table "execution_lease_heartbeat")
(def ^:private execution-dlq-table "execution_dlq")
(def ^:private execution-orphan-recovery-event-table "execution_orphan_recovery_event")

(def ^:private default-graph-release-table graph-release-table)
(def ^:private default-execution-request-table execution-request-table)
(def ^:private default-execution-run-table execution-run-table)
(def ^:private default-node-run-table node-run-table)
(def ^:private default-execution-lease-heartbeat-table execution-lease-heartbeat-table)
(def ^:private default-execution-orphan-recovery-event-table execution-orphan-recovery-event-table)
(defonce ^:private execution-table-init-cache (atom #{}))
(defonce ^:private builtin-execution-handlers-registered? (atom false))
(def ^:private dedupe-active-statuses ["queued" "leased" "running" "recovering_orphan"])

(def ^:private retryable-failure-classes
  #{"transient_network" "rate_limited" "target_conflict" "worker_orphaned"})

(defn- db-opts
  [conn]
  (jdbc/with-options conn {:builder-fn rs/as-unqualified-lower-maps}))

(defn- update-count
  [result]
  (long (or (:next.jdbc/update-count (first result)) 0)))

(defn- now-utc []
  (java.time.Instant/now))

(defn- parse-int-env
  [k default-value]
  (try
    (if-some [value (get env k)]
      (Integer/parseInt (str value))
      default-value)
    (catch Exception _
      default-value)))

(defn- sha256-hex
  [value]
  (let [digest (.digest (doto (MessageDigest/getInstance "SHA-256")
                          (.update (.getBytes (str value) "UTF-8"))))]
    (.formatHex (HexFormat/of) digest)))

(defn- derived-index-name
  [base table-name default-table-name]
  (if (= table-name default-table-name)
    base
    (str base "_" (subs (sha256-hex table-name) 0 12))))

(defn- parse-json-safe
  [value]
  (cond
    (nil? value) nil
    (map? value) value
    (vector? value) value
    (string? value) (try
                      (json/parse-string value true)
                      (catch Exception _
                        nil))
    :else nil))

(defn- parse-csv-env-set
  [k]
  (let [raw (some-> (get env k) str string/trim)]
    (when (and (seq raw) (not= "*" raw) (not= "all" (string/lower-case raw)))
      (->> (string/split raw #",")
           (map #(some-> % string/trim string/lower-case))
           (remove string/blank?)
           set))))

(defn- stable-hash
  [value]
  (bit-and 0x7fffffff (hash (str value))))

(defn- queue-partition-for
  [request-key workspace-key]
  (let [partition-count (max 1 (parse-int-env :execution-queue-partition-count 1))]
    (if (= 1 partition-count)
      "default"
      (format "p%02d" (mod (stable-hash (or workspace-key request-key)) partition-count)))))

(defn- request-workload-class
  [request-kind trigger-type request-params]
  (let [plugin-handler (plugins/resolve-execution-handler request-kind)]
    (or (when-let [classifier (:workload-classifier plugin-handler)]
          (some-> (classifier {:request-kind request-kind
                               :trigger-type trigger-type
                               :request-params request-params})
                  str
                  string/lower-case))
        (cond
          (= "scheduler" (name request-kind)) "scheduled"
          (or (= "replay" trigger-type)
              (some? (:replay_source_run_id request-params))
              (some? (:replay-source-run-id request-params))) "replay"
          (= "manual" trigger-type) "interactive"
          :else (name request-kind)))))

(defn- scheduled-for-utc
  [request-params]
  (or (:scheduled-for-utc request-params)
      (:scheduled_for_utc request-params)))

(defn ensure-execution-tables!
  []
  (let [table-key [graph-version-table
                   graph-release-table
                   execution-request-table
                   execution-run-table
                   node-run-table
                   execution-lease-heartbeat-table
                   execution-dlq-table
                   execution-orphan-recovery-event-table]]
    (when-not (contains? @execution-table-init-cache table-key)
      (locking execution-table-init-cache
        (when-not (contains? @execution-table-init-cache table-key)
          (let [graph-release-active-index
                (derived-index-name "idx_graph_release_active" graph-release-table default-graph-release-table)
                execution-request-active-index
                (derived-index-name "idx_execution_request_active_v2" execution-request-table default-execution-request-table)
                execution-request-claim-index
                (derived-index-name "idx_execution_request_claim" execution-request-table default-execution-request-table)
                execution-request-lease-expiry-index
                (derived-index-name "idx_execution_request_lease_expiry" execution-request-table default-execution-request-table)
                execution-run-graph-index
                (derived-index-name "idx_execution_run_graph" execution-run-table default-execution-run-table)
                node-run-run-id-index
                (derived-index-name "idx_node_run_run_id" node-run-table default-node-run-table)
                execution-lease-heartbeat-request-index
                (derived-index-name "idx_execution_lease_heartbeat_request"
                                    execution-lease-heartbeat-table
                                    default-execution-lease-heartbeat-table)
                execution-orphan-recovery-event-created-index
                (derived-index-name "idx_execution_orphan_recovery_event_created"
                                    execution-orphan-recovery-event-table
                                    default-execution-orphan-recovery-event-table)]
            (doseq [sql-str
                    [(str "CREATE TABLE IF NOT EXISTS " graph-version-table " ("
                          "id BIGSERIAL PRIMARY KEY, "
                          "graph_id INTEGER NOT NULL, "
                          "graph_version INTEGER NOT NULL, "
                          "graph_name TEXT NULL, "
                          "graph_definition TEXT NOT NULL, "
                          "definition_checksum VARCHAR(64) NOT NULL, "
                          "published_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                          "UNIQUE (graph_id, graph_version))")
                     (str "CREATE TABLE IF NOT EXISTS " graph-release-table " ("
                          "id BIGSERIAL PRIMARY KEY, "
                          "graph_id INTEGER NOT NULL, "
                          "environment VARCHAR(64) NOT NULL, "
                          "graph_version_id BIGINT NOT NULL REFERENCES " graph-version-table "(id), "
                          "graph_version INTEGER NOT NULL, "
                          "status VARCHAR(32) NOT NULL, "
                          "published_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                          "activated_at_utc TIMESTAMPTZ NULL)")
                     (str "CREATE UNIQUE INDEX IF NOT EXISTS " graph-release-active-index " "
                          "ON " graph-release-table " (graph_id, environment) WHERE status = 'active'")
                     (str "CREATE TABLE IF NOT EXISTS " execution-request-table " ("
                          "request_id UUID PRIMARY KEY, "
                          "request_key TEXT NOT NULL, "
                          "request_kind VARCHAR(32) NOT NULL, "
                          "tenant_key VARCHAR(128) NOT NULL DEFAULT 'default', "
                          "workspace_key VARCHAR(128) NOT NULL DEFAULT 'default', "
                          "graph_id INTEGER NOT NULL, "
                          "graph_version_id BIGINT NOT NULL REFERENCES " graph-version-table "(id), "
                          "graph_version INTEGER NOT NULL, "
                          "environment VARCHAR(64) NOT NULL, "
                          "node_id INTEGER NOT NULL, "
                          "trigger_type VARCHAR(64) NOT NULL, "
                          "endpoint_name TEXT NULL, "
                          "source_system VARCHAR(128) NULL, "
                          "credential_ref VARCHAR(255) NULL, "
                          "source_max_concurrency INTEGER NULL, "
                          "credential_max_concurrency INTEGER NULL, "
                          "request_params TEXT NULL, "
                          "queue_partition VARCHAR(128) NOT NULL DEFAULT 'default', "
                          "workload_class VARCHAR(64) NOT NULL DEFAULT 'api', "
                          "status VARCHAR(32) NOT NULL, "
                          "requested_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                          "available_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                          "started_at_utc TIMESTAMPTZ NULL, "
                          "finished_at_utc TIMESTAMPTZ NULL, "
                          "worker_id TEXT NULL, "
                          "lease_expires_at_utc TIMESTAMPTZ NULL, "
                          "retry_count INTEGER NOT NULL DEFAULT 0, "
                          "max_retries INTEGER NOT NULL DEFAULT 0, "
                          "error_message TEXT NULL, "
                          "failure_class VARCHAR(64) NULL, "
                          "last_heartbeat_at_utc TIMESTAMPTZ NULL)")
                     (str "ALTER TABLE " execution-request-table
                          " ADD COLUMN IF NOT EXISTS tenant_key VARCHAR(128) NOT NULL DEFAULT 'default'")
                     (str "ALTER TABLE " execution-request-table
                          " ADD COLUMN IF NOT EXISTS workspace_key VARCHAR(128) NOT NULL DEFAULT 'default'")
                     (str "ALTER TABLE " execution-request-table
                          " ADD COLUMN IF NOT EXISTS failure_class VARCHAR(64) NULL")
                     (str "ALTER TABLE " execution-request-table
                          " ADD COLUMN IF NOT EXISTS last_heartbeat_at_utc TIMESTAMPTZ NULL")
                     (str "ALTER TABLE " execution-request-table
                          " ADD COLUMN IF NOT EXISTS queue_partition VARCHAR(128) NOT NULL DEFAULT 'default'")
                     (str "ALTER TABLE " execution-request-table
                          " ADD COLUMN IF NOT EXISTS workload_class VARCHAR(64) NOT NULL DEFAULT 'api'")
                     (str "ALTER TABLE " execution-request-table
                          " ADD COLUMN IF NOT EXISTS source_system VARCHAR(128) NULL")
                     (str "ALTER TABLE " execution-request-table
                          " ADD COLUMN IF NOT EXISTS credential_ref VARCHAR(255) NULL")
                     (str "ALTER TABLE " execution-request-table
                          " ADD COLUMN IF NOT EXISTS source_max_concurrency INTEGER NULL")
                     (str "ALTER TABLE " execution-request-table
                          " ADD COLUMN IF NOT EXISTS credential_max_concurrency INTEGER NULL")
                     (str "CREATE UNIQUE INDEX IF NOT EXISTS " execution-request-active-index " "
                          "ON " execution-request-table " (request_key) "
                          "WHERE status IN ('queued', 'leased', 'running', 'recovering_orphan')")
                     (str "CREATE INDEX IF NOT EXISTS " execution-request-claim-index " "
                          "ON " execution-request-table " (status, available_at_utc, requested_at_utc)")
                     (str "CREATE INDEX IF NOT EXISTS " execution-request-lease-expiry-index " "
                          "ON " execution-request-table " (status, lease_expires_at_utc)")
                     (str "CREATE INDEX IF NOT EXISTS idx_execution_request_partition_claim "
                          "ON " execution-request-table " (queue_partition, workload_class, status, available_at_utc)")
                     (str "CREATE INDEX IF NOT EXISTS idx_execution_request_workspace_status "
                          "ON " execution-request-table " (workspace_key, status, requested_at_utc)")
                     (str "CREATE INDEX IF NOT EXISTS idx_execution_request_source_status "
                          "ON " execution-request-table " (source_system, status, available_at_utc)")
                     (str "CREATE INDEX IF NOT EXISTS idx_execution_request_credential_status "
                          "ON " execution-request-table " (credential_ref, status, available_at_utc)")
                     (str "CREATE TABLE IF NOT EXISTS " execution-run-table " ("
                          "run_id UUID PRIMARY KEY, "
                          "request_id UUID NOT NULL UNIQUE REFERENCES " execution-request-table "(request_id), "
                          "tenant_key VARCHAR(128) NOT NULL DEFAULT 'default', "
                          "workspace_key VARCHAR(128) NOT NULL DEFAULT 'default', "
                          "graph_id INTEGER NOT NULL, "
                          "graph_version_id BIGINT NOT NULL REFERENCES " graph-version-table "(id), "
                          "graph_version INTEGER NOT NULL, "
                          "environment VARCHAR(64) NOT NULL, "
                          "request_kind VARCHAR(32) NOT NULL, "
                          "queue_partition VARCHAR(128) NOT NULL DEFAULT 'default', "
                          "workload_class VARCHAR(64) NOT NULL DEFAULT 'api', "
                          "source_system VARCHAR(128) NULL, "
                          "credential_ref VARCHAR(255) NULL, "
                          "node_id INTEGER NOT NULL, "
                          "trigger_type VARCHAR(64) NOT NULL, "
                          "endpoint_name TEXT NULL, "
                          "status VARCHAR(32) NOT NULL, "
                          "started_at_utc TIMESTAMPTZ NULL, "
                          "finished_at_utc TIMESTAMPTZ NULL, "
                          "result_json TEXT NULL, "
                          "error_message TEXT NULL, "
                          "failure_class VARCHAR(64) NULL)")
                     (str "ALTER TABLE " execution-run-table
                          " ADD COLUMN IF NOT EXISTS tenant_key VARCHAR(128) NOT NULL DEFAULT 'default'")
                     (str "ALTER TABLE " execution-run-table
                          " ADD COLUMN IF NOT EXISTS workspace_key VARCHAR(128) NOT NULL DEFAULT 'default'")
                     (str "ALTER TABLE " execution-run-table
                          " ADD COLUMN IF NOT EXISTS failure_class VARCHAR(64) NULL")
                     (str "ALTER TABLE " execution-run-table
                          " ADD COLUMN IF NOT EXISTS queue_partition VARCHAR(128) NOT NULL DEFAULT 'default'")
                     (str "ALTER TABLE " execution-run-table
                          " ADD COLUMN IF NOT EXISTS workload_class VARCHAR(64) NOT NULL DEFAULT 'api'")
                     (str "ALTER TABLE " execution-run-table
                          " ADD COLUMN IF NOT EXISTS source_system VARCHAR(128) NULL")
                     (str "ALTER TABLE " execution-run-table
                          " ADD COLUMN IF NOT EXISTS credential_ref VARCHAR(255) NULL")
                     (str "CREATE INDEX IF NOT EXISTS " execution-run-graph-index " "
                          "ON " execution-run-table " (graph_id, started_at_utc DESC)")
                     (str "CREATE TABLE IF NOT EXISTS " node-run-table " ("
                          "id BIGSERIAL PRIMARY KEY, "
                          "run_id UUID NOT NULL REFERENCES " execution-run-table "(run_id), "
                          "node_id INTEGER NOT NULL, "
                          "node_type VARCHAR(64) NOT NULL, "
                          "status VARCHAR(32) NOT NULL, "
                          "started_at_utc TIMESTAMPTZ NULL, "
                          "finished_at_utc TIMESTAMPTZ NULL, "
                          "detail_json TEXT NULL)")
                     (str "CREATE INDEX IF NOT EXISTS " node-run-run-id-index " "
                          "ON " node-run-table " (run_id, node_id)")
                     (str "CREATE TABLE IF NOT EXISTS " execution-lease-heartbeat-table " ("
                          "id BIGSERIAL PRIMARY KEY, "
                          "request_id UUID NOT NULL REFERENCES " execution-request-table "(request_id), "
                          "run_id UUID NULL REFERENCES " execution-run-table "(run_id), "
                          "worker_id TEXT NOT NULL, "
                          "heartbeat_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                     (str "CREATE INDEX IF NOT EXISTS " execution-lease-heartbeat-request-index " "
                          "ON " execution-lease-heartbeat-table " (request_id, heartbeat_at_utc DESC)")
                     (str "CREATE TABLE IF NOT EXISTS " execution-dlq-table " ("
                          "id BIGSERIAL PRIMARY KEY, "
                          "request_id UUID NOT NULL UNIQUE REFERENCES " execution-request-table "(request_id), "
                          "run_id UUID NULL REFERENCES " execution-run-table "(run_id), "
                          "graph_id INTEGER NOT NULL, "
                          "node_id INTEGER NOT NULL, "
                          "request_kind VARCHAR(32) NOT NULL, "
                          "endpoint_name TEXT NULL, "
                          "failure_class VARCHAR(64) NOT NULL, "
                          "error_message TEXT NULL, "
                          "request_params TEXT NULL, "
                          "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                     (str "CREATE TABLE IF NOT EXISTS " execution-orphan-recovery-event-table " ("
                          "id BIGSERIAL PRIMARY KEY, "
                          "request_id UUID NOT NULL REFERENCES " execution-request-table "(request_id), "
                          "run_id UUID NULL REFERENCES " execution-run-table "(run_id), "
                          "metric_name VARCHAR(32) NOT NULL, "
                          "failure_class VARCHAR(64) NULL, "
                          "error_message TEXT NULL, "
                          "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                     (str "CREATE INDEX IF NOT EXISTS " execution-orphan-recovery-event-created-index " "
                          "ON " execution-orphan-recovery-event-table " (created_at_utc DESC, metric_name)")]]
              (jdbc/execute! db/ds [sql-str]))
            (swap! execution-table-init-cache conj table-key)))))))

(defn orphan-recovery-metrics
  ([] (orphan-recovery-metrics {}))
  ([{:keys [since-hours]
     :or {since-hours 24}}]
   (let [rows (jdbc/execute!
               (db-opts db/ds)
               [(str "SELECT metric_name, COUNT(*) AS cnt
                      FROM " execution-orphan-recovery-event-table "
                      WHERE created_at_utc >= now() - (? || ' hours')::interval
                      GROUP BY metric_name")
                (str (max 1 (long since-hours)))])]
     (reduce (fn [acc row]
               (assoc acc (keyword (:metric_name row)) (long (:cnt row))))
             {:claimed 0
              :timed_out 0
              :failed 0
              :queued 0
              :stale 0}
             rows))))

(defn- latest-graph-row
  [conn graph-id]
  (jdbc/execute-one!
   (db-opts conn)
   ["SELECT id, version, name, definition
     FROM graph
     WHERE id = ?
     ORDER BY version DESC
     LIMIT 1" graph-id]))

(defn- load-active-release
  [conn graph-id environment]
  (jdbc/execute-one!
   (db-opts conn)
   [(str "SELECT * FROM " graph-release-table
         " WHERE graph_id = ? AND environment = ? AND status = 'active'
            ORDER BY published_at_utc DESC LIMIT 1")
    graph-id environment]))

(defn- ensure-graph-version-row!
  [conn graph-id]
  (let [graph-row (latest-graph-row conn graph-id)]
    (when-not graph-row
      (throw (ex-info "Graph not found" {:graph_id graph-id})))
    (let [existing (jdbc/execute-one!
                    (db-opts conn)
                    [(str "SELECT * FROM " graph-version-table " WHERE graph_id = ? AND graph_version = ?")
                     graph-id (:version graph-row)])]
      (or existing
          (jdbc/execute-one!
           (db-opts conn)
           [(str "INSERT INTO " graph-version-table
                 " (graph_id, graph_version, graph_name, graph_definition, definition_checksum)
                    VALUES (?, ?, ?, ?, ?)
                    RETURNING *")
            graph-id
            (:version graph-row)
            (:name graph-row)
            (:definition graph-row)
            (sha256-hex (:definition graph-row))])))))

(defn- ensure-active-release!
  [conn graph-id environment]
  (or (load-active-release conn graph-id environment)
      (let [graph-version-row (ensure-graph-version-row! conn graph-id)]
        (jdbc/execute-one!
         (db-opts conn)
         [(str "INSERT INTO " graph-release-table
               " (graph_id, environment, graph_version_id, graph_version, status, activated_at_utc)
                  VALUES (?, ?, ?, ?, 'active', now())
                  RETURNING *")
          graph-id
          environment
          (:id graph-version-row)
          (:graph_version graph-version-row)]))))

(defn- active-request-by-key
  [conn request-key]
  (jdbc/execute-one!
   (db-opts conn)
   [(str "SELECT r.*, er.run_id
          FROM " execution-request-table " r
          JOIN " execution-run-table " er ON er.request_id = r.request_id
          WHERE r.request_key = ?
            AND r.status IN ('queued', 'leased', 'running', 'recovering_orphan')
          LIMIT 1")
    request-key]))

(defn- normalize-endpoint-name
  [endpoint-name]
  (some-> endpoint-name str string/trim not-empty))

(defn- normalize-credential-ref
  [auth-ref]
  (or (some-> auth-ref :secret_ref str string/trim not-empty)
      (some-> auth-ref :key str sha256-hex (subs 0 24))
      (some-> auth-ref :token str sha256-hex (subs 0 24))))

(defn- api-node-endpoint
  [api-node endpoint-name]
  (when endpoint-name
    (->> (:endpoint_configs api-node)
         (filter #(not= false (:enabled %)))
         (filter #(= endpoint-name (:endpoint_name %)))
         first)))

(defn- api-request-concurrency-context
  [graph-id node-id endpoint-name]
  (let [graph      (db/getGraph graph-id)
        api-node   (g2/getData graph node-id)
        endpoint   (api-node-endpoint api-node endpoint-name)
        auth-ref   (or (:auth_ref endpoint) (:auth_ref api-node))
        source-max (max 1 (long (or (:source_max_concurrency endpoint)
                                    (:source_max_concurrency api-node)
                                    (parse-int-env :execution-api-source-max-concurrency 8))))
        credential-ref (normalize-credential-ref auth-ref)
        credential-max (when credential-ref
                         (max 1 (long (or (:credential_max_concurrency endpoint)
                                          (:credential_max_concurrency api-node)
                                          (parse-int-env :execution-api-credential-max-concurrency 1)))))]
    {:source-system (or (:source_system api-node) "api")
     :credential-ref credential-ref
     :source-max-concurrency source-max
     :credential-max-concurrency credential-max}))

(defn- api-request-scope-key
  [graph-id node-id environment]
  (string/join "::" ["api-scope" graph-id node-id environment]))

(defn- source-request-scope-key
  [request-kind graph-id node-id environment]
  (string/join "::" [(str (name request-kind) "-scope") graph-id node-id environment]))

(defn- active-source-request-overlap
  [conn request-kind graph-id node-id environment endpoint-name]
  (let [endpoint-name (normalize-endpoint-name endpoint-name)
        request-kind  (name request-kind)
        [sql-str params] (if endpoint-name
                           [(str "SELECT r.*, er.run_id
                                  FROM " execution-request-table " r
                                  JOIN " execution-run-table " er ON er.request_id = r.request_id
                                  WHERE r.request_kind = ?
                                    AND r.graph_id = ?
                                    AND r.node_id = ?
                                    AND r.environment = ?
                                    AND r.status IN ('queued', 'leased', 'running', 'recovering_orphan')
                                    AND (r.endpoint_name IS NULL OR r.endpoint_name = ?)
                                  ORDER BY CASE WHEN r.endpoint_name = ? THEN 0 ELSE 1 END,
                                           r.requested_at_utc ASC
                                  LIMIT 1")
                            [request-kind graph-id node-id environment endpoint-name endpoint-name]]
                           [(str "SELECT r.*, er.run_id
                                  FROM " execution-request-table " r
                                  JOIN " execution-run-table " er ON er.request_id = r.request_id
                                  WHERE r.request_kind = ?
                                    AND r.graph_id = ?
                                    AND r.node_id = ?
                                    AND r.environment = ?
                                    AND r.status IN ('queued', 'leased', 'running', 'recovering_orphan')
                                  ORDER BY r.requested_at_utc ASC
                                  LIMIT 1")
                            [request-kind graph-id node-id environment]])]
    (jdbc/execute-one! (db-opts conn) (into [sql-str] params))))

(defn- request->response
  [row]
  (when row
    (-> row
        (update :request_id str)
        (update :run_id str)
        (update :request_params parse-json-safe)
        (assoc :created? false))))

(defn- default-max-retries
  [request-kind]
  (case request-kind
    :api (parse-int-env :execution-api-max-retries 3)
    :kafka (parse-int-env :execution-kafka-max-retries 3)
    :file (parse-int-env :execution-file-max-retries 3)
    :scheduler (parse-int-env :execution-scheduler-max-retries 0)
    0))

(defn- enqueue-request!
  [request-kind graph-id node-id {:keys [environment endpoint-name trigger-type request-params max-retries workspace-key]}]
  (let [environment         (or environment "default")
        endpoint-name       (normalize-endpoint-name endpoint-name)
        trigger-type        (or trigger-type "manual")
        request-key         (string/join "::" [(name request-kind) graph-id node-id environment (or endpoint-name "")])
        request-id          (UUID/randomUUID)
        run-id              (UUID/randomUUID)
        dependency-blockers (control-plane/dependency-blockers graph-id)
        workspace-context   (if-let [workspace-key (some-> workspace-key str string/trim not-empty)]
                              (control-plane/workspace-context workspace-key {:required? true})
                              (control-plane/graph-workspace-context graph-id))
        request-params      (or request-params {})
        params-json         (json/generate-string request-params)
        max-retries         (max 0 (int (or max-retries (default-max-retries request-kind))))]
    (when (seq dependency-blockers)
      (throw (ex-info "Graph has unmet upstream dependencies"
                      {:graph_id graph-id
                       :dependency_blockers dependency-blockers
                       :status 409})))
    (when (false? (:tenant_active workspace-context))
      (throw (ex-info "Tenant is inactive"
                      {:graph_id graph-id
                       :workspace_key (:workspace_key workspace-context)
                       :tenant_key (:tenant_key workspace-context)
                       :status 409})))
    (when (false? (:active workspace-context))
      (throw (ex-info "Workspace is inactive"
                      {:graph_id graph-id
                       :workspace_key (:workspace_key workspace-context)
                       :tenant_key (:tenant_key workspace-context)
                       :status 409})))
    (let [{:keys [source-system credential-ref source-max-concurrency credential-max-concurrency]}
          (when (= request-kind :api)
            (api-request-concurrency-context graph-id node-id endpoint-name))
          queue-partition (queue-partition-for request-key (:workspace_key workspace-context))
          workload-class  (request-workload-class request-kind trigger-type request-params)]
      (jdbc/with-transaction [tx db/ds]
        (when (contains? #{:api :kafka :file} request-kind)
          (jdbc/execute! tx ["SELECT pg_advisory_xact_lock(hashtext(?))"
                             (source-request-scope-key request-kind graph-id node-id environment)]))
        (jdbc/execute! tx ["SELECT pg_advisory_xact_lock(hashtext(?))" request-key])
        (jdbc/execute! tx ["SELECT pg_advisory_xact_lock(hashtext(?))" (:tenant_key workspace-context)])
        (jdbc/execute! tx ["SELECT pg_advisory_xact_lock(hashtext(?))" (:workspace_key workspace-context)])
        (if-let [existing (active-request-by-key tx request-key)]
          (request->response existing)
          (if-let [overlap (when (contains? #{:api :kafka :file} request-kind)
                             (active-source-request-overlap tx request-kind graph-id node-id environment endpoint-name))]
            (if (nil? endpoint-name)
              (throw (ex-info (str (string/upper-case (name request-kind))
                                   " node already has an active endpoint-scoped ingestion request; cannot start an all-endpoints run")
                              {:graph_id graph-id
                               :node_id node-id
                               :environment environment
                               :active_request_id (str (:request_id overlap))
                               :active_run_id (str (:run_id overlap))
                               :active_endpoint_name (:endpoint_name overlap)
                               :status 409}))
              (request->response overlap))
          (let [workspace-queued-count (-> (jdbc/execute-one!
                                            (db-opts tx)
                                            [(str "SELECT COUNT(*) AS cnt FROM " execution-request-table "
                                                   WHERE workspace_key = ?
                                                     AND status = 'queued'")
                                             (:workspace_key workspace-context)])
                                           :cnt
                                           long)
                tenant-queued-count (-> (jdbc/execute-one!
                                         (db-opts tx)
                                         [(str "SELECT COUNT(*) AS cnt FROM " execution-request-table "
                                                WHERE tenant_key = ?
                                                  AND status = 'queued'")
                                          (:tenant_key workspace-context)])
                                        :cnt
                                        long)
                _ (when (>= workspace-queued-count (long (:max_queued_requests workspace-context)))
                    (throw (ex-info "Workspace queue is at capacity"
                                    {:graph_id graph-id
                                     :workspace_key (:workspace_key workspace-context)
                                     :tenant_key (:tenant_key workspace-context)
                                     :max_queued_requests (:max_queued_requests workspace-context)
                                     :status 429})))
                _ (when (>= tenant-queued-count (long (or (:tenant_max_queued_requests workspace-context) 1000)))
                    (throw (ex-info "Tenant queue is at capacity"
                                    {:graph_id graph-id
                                     :workspace_key (:workspace_key workspace-context)
                                     :tenant_key (:tenant_key workspace-context)
                                     :tenant_max_queued_requests (:tenant_max_queued_requests workspace-context)
                                     :status 429})))
                release-row (ensure-active-release! tx graph-id environment)
                request-row (jdbc/execute-one!
                             (db-opts tx)
                             [(str "INSERT INTO " execution-request-table
                                   " (request_id, request_key, request_kind, tenant_key, workspace_key, graph_id, graph_version_id, graph_version,
                                      environment, node_id, trigger_type, endpoint_name, source_system, credential_ref,
                                      source_max_concurrency, credential_max_concurrency,
                                      request_params, queue_partition, workload_class, status, max_retries)
                                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)
                                      RETURNING *")
                              request-id
                              request-key
                              (name request-kind)
                              (:tenant_key workspace-context)
                              (:workspace_key workspace-context)
                              graph-id
                              (:graph_version_id release-row)
                              (:graph_version release-row)
                              environment
                              node-id
                              trigger-type
                              endpoint-name
                              source-system
                              credential-ref
                              source-max-concurrency
                              credential-max-concurrency
                              params-json
                              queue-partition
                              workload-class
                              max-retries])
                run-row (jdbc/execute-one!
                         (db-opts tx)
                         [(str "INSERT INTO " execution-run-table
                               " (run_id, request_id, tenant_key, workspace_key, graph_id, graph_version_id, graph_version, environment,
                                  request_kind, queue_partition, workload_class, source_system, credential_ref,
                                  node_id, trigger_type, endpoint_name, status)
                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued')
                                  RETURNING *")
                          run-id
                          request-id
                          (:tenant_key workspace-context)
                          (:workspace_key workspace-context)
                          graph-id
                          (:graph_version_id release-row)
                          (:graph_version release-row)
                          environment
                          (name request-kind)
                          queue-partition
                          workload-class
                          source-system
                          credential-ref
                          node-id
                          trigger-type
                          endpoint-name])]
            (assoc (request->response (merge request-row {:run_id (:run_id run-row)}))
                   :created? true))))))))

(defn enqueue-api-request!
  [graph-id api-node-id {:keys [endpoint-name] :as opts}]
  (enqueue-request! :api graph-id api-node-id
                    {:environment (:environment opts)
                     :endpoint-name endpoint-name
                     :trigger-type (:trigger-type opts)
                     :workspace-key (:workspace-key opts)
                     :max-retries (:max-retries opts)
                     :request-params (merge (select-keys opts [:endpoint-name])
                                            (:request-params opts))}))

(defn enqueue-scheduler-request!
  [graph-id scheduler-node-id {:keys [scheduled-for-utc] :as opts}]
  (enqueue-request! :scheduler graph-id scheduler-node-id
                    {:environment (:environment opts)
                     :trigger-type (or (:trigger-type opts) "scheduler")
                     :workspace-key (:workspace-key opts)
                     :max-retries (:max-retries opts)
                     :request-params (merge (cond-> {}
                                              scheduled-for-utc (assoc :scheduled-for-utc (str scheduled-for-utc)))
                                            (:request-params opts))}))

(defn enqueue-kafka-request!
  [graph-id kafka-node-id {:keys [endpoint-name] :as opts}]
  (enqueue-request! :kafka graph-id kafka-node-id
                    {:environment (:environment opts)
                     :endpoint-name endpoint-name
                     :trigger-type (:trigger-type opts)
                     :workspace-key (:workspace-key opts)
                     :max-retries (:max-retries opts)
                     :request-params (merge (select-keys opts [:endpoint-name])
                                            (:request-params opts))}))

(defn enqueue-file-request!
  [graph-id file-node-id {:keys [endpoint-name] :as opts}]
  (enqueue-request! :file graph-id file-node-id
                    {:environment (:environment opts)
                     :endpoint-name endpoint-name
                     :trigger-type (:trigger-type opts)
                     :workspace-key (:workspace-key opts)
                     :max-retries (:max-retries opts)
                     :request-params (merge (select-keys opts [:endpoint-name])
                                            (:request-params opts))}))

(defn- sql-in-clause
  [values]
  (str "(" (string/join ", " (repeat (count values) "?")) ")"))

(defn- claim-next-request!
  [worker-id lease-seconds {:keys [allowed-partitions allowed-workloads]}]
  (let [partition-clause (when (seq allowed-partitions)
                           (str " AND r.queue_partition IN " (sql-in-clause allowed-partitions)))
        workload-clause  (when (seq allowed-workloads)
                           (str " AND r.workload_class IN " (sql-in-clause allowed-workloads)))
        sql-str          (str "UPDATE " execution-request-table "
                              SET status = 'leased',
                                  started_at_utc = COALESCE(started_at_utc, now()),
                                  worker_id = ?,
                                  lease_expires_at_utc = now() + (? || ' seconds')::interval,
                                  last_heartbeat_at_utc = now()
                              WHERE request_id = (
                                SELECT candidate.request_id
                                FROM " execution-request-table " candidate
                                WHERE candidate.request_id IN (
                                  SELECT r.request_id
                                  FROM " execution-request-table " r
                                  LEFT JOIN control_plane_workspace w ON w.workspace_key = r.workspace_key
                                  LEFT JOIN control_plane_tenant t ON t.tenant_key = r.tenant_key
                                  LEFT JOIN (
                                    SELECT workspace_key, COUNT(*) AS active_count
                                    FROM " execution-request-table "
                                    WHERE status IN ('leased', 'running', 'recovering_orphan')
                                    GROUP BY workspace_key
                                  ) workspace_active ON workspace_active.workspace_key = r.workspace_key
                                  LEFT JOIN (
                                    SELECT tenant_key, COUNT(*) AS active_count
                                    FROM " execution-request-table "
                                    WHERE status IN ('leased', 'running', 'recovering_orphan')
                                    GROUP BY tenant_key
                                  ) tenant_active ON tenant_active.tenant_key = r.tenant_key
                                  LEFT JOIN (
                                    SELECT source_system, COUNT(*) AS active_count
                                    FROM " execution-request-table "
                                    WHERE status IN ('leased', 'running', 'recovering_orphan')
                                      AND source_system IS NOT NULL
                                    GROUP BY source_system
                                  ) source_active ON source_active.source_system = r.source_system
                                  LEFT JOIN (
                                    SELECT credential_ref, COUNT(*) AS active_count
                                    FROM " execution-request-table "
                                    WHERE status IN ('leased', 'running', 'recovering_orphan')
                                      AND credential_ref IS NOT NULL
                                    GROUP BY credential_ref
                                  ) credential_active ON credential_active.credential_ref = r.credential_ref
                                  WHERE r.status = 'queued'
                                    AND r.available_at_utc <= now()
                                    AND COALESCE(w.active, TRUE) = TRUE
                                    AND COALESCE(t.active, TRUE) = TRUE
                                    AND COALESCE(workspace_active.active_count, 0) < GREATEST(COALESCE(w.max_concurrent_requests, 2), 1)
                                    AND COALESCE(tenant_active.active_count, 0) < GREATEST(COALESCE(t.max_concurrent_requests, 10), 1)
                                    AND (r.source_max_concurrency IS NULL OR COALESCE(source_active.active_count, 0) < GREATEST(r.source_max_concurrency, 1))
                                    AND (r.credential_ref IS NULL OR r.credential_max_concurrency IS NULL
                                         OR COALESCE(credential_active.active_count, 0) < GREATEST(r.credential_max_concurrency, 1))"
                              partition-clause
                              workload-clause
                              " ORDER BY
                                    (COALESCE(tenant_active.active_count, 0)::DOUBLE PRECISION / GREATEST(COALESCE(t.weight, 1), 1)) ASC,
                                    (COALESCE(workspace_active.active_count, 0)::DOUBLE PRECISION / GREATEST(COALESCE(w.weight, 1), 1)) ASC,
                                    r.requested_at_utc ASC
                                  LIMIT 10
                                )
                                ORDER BY candidate.requested_at_utc ASC
                                LIMIT 1
                                FOR UPDATE OF candidate SKIP LOCKED
                              )
                              RETURNING *")
        params           (vec (concat [sql-str worker-id (str lease-seconds)]
                                      allowed-partitions
                                      allowed-workloads))]
    (jdbc/execute-one! (db-opts db/ds) params)))

(defn- find-run-id
  [request-id]
  (some-> (jdbc/execute-one!
           (db-opts db/ds)
           [(str "SELECT run_id FROM " execution-run-table " WHERE request_id = ?")
            request-id])
          :run_id))

(defn- guarded-request-update!
  [conn set-sql set-params request-id {:keys [expected-statuses expected-worker-id]}]
  (let [status-placeholders (when (seq expected-statuses)
                              (string/join ", " (repeat (count expected-statuses) "?")))
        sql-str             (str "UPDATE " execution-request-table " "
                                 set-sql
                                 " WHERE request_id = ?"
                                 (when (seq expected-statuses)
                                   (str " AND status IN (" status-placeholders ")"))
                                 (when (some? expected-worker-id)
                                   " AND worker_id = ?"))
        params              (vec (concat [sql-str]
                                         set-params
                                         [request-id]
                                         expected-statuses
                                         (when (some? expected-worker-id)
                                           [expected-worker-id])))]
    (pos? (update-count (jdbc/execute! conn params)))))

(defn- mark-request-running!
  [conn request-id run-id worker-id]
  (when (guarded-request-update!
         conn
         "SET status = 'running'"
         []
         request-id
         {:expected-statuses ["leased"]
          :expected-worker-id worker-id})
    (jdbc/execute!
     conn
     [(str "UPDATE " execution-run-table "
            SET status = 'running',
                started_at_utc = COALESCE(started_at_utc, now()),
                finished_at_utc = NULL,
                error_message = NULL,
                failure_class = NULL
            WHERE run_id = ?")
      run-id])
    true))

(defn- mark-run-status!
  [conn request-id run-id status result error-message failure-class {:keys [expected-statuses expected-worker-id]}]
  (when (guarded-request-update!
         conn
         "SET status = ?, finished_at_utc = now(), lease_expires_at_utc = NULL,
              worker_id = NULL, error_message = ?, failure_class = ?, last_heartbeat_at_utc = NULL"
         [status error-message failure-class]
         request-id
         {:expected-statuses expected-statuses
          :expected-worker-id expected-worker-id})
    (jdbc/execute!
     conn
     [(str "UPDATE " execution-run-table "
            SET status = ?, started_at_utc = COALESCE(started_at_utc, now()), finished_at_utc = now(),
                result_json = ?, error_message = ?, failure_class = ?
            WHERE run_id = ?")
      status
      (when result (json/generate-string result))
      error-message
      failure-class
      run-id])
    true))

(defn- insert-node-run!
  [conn run-id node-id node-type status detail]
  (jdbc/execute-one!
   (db-opts conn)
   [(str "INSERT INTO " node-run-table "
          (run_id, node_id, node_type, status, started_at_utc, finished_at_utc, detail_json)
          VALUES (?, ?, ?, ?, now(), now(), ?)")
    run-id
    node-id
    node-type
    status
    (when detail (json/generate-string detail))]))

(defn- renew-request-lease!
  [request-id run-id worker-id lease-seconds]
  (let [updated (jdbc/execute-one!
                 (db-opts db/ds)
                 [(str "UPDATE " execution-request-table "
                        SET lease_expires_at_utc = now() + (? || ' seconds')::interval,
                            last_heartbeat_at_utc = now()
                        WHERE request_id = ?
                          AND worker_id = ?
                          AND status IN ('leased', 'running')
                        RETURNING request_id")
                  (str lease-seconds)
                  request-id
                  worker-id])]
    (when updated
      (jdbc/execute!
       db/ds
       [(str "INSERT INTO " execution-lease-heartbeat-table
             " (request_id, run_id, worker_id) VALUES (?, ?, ?)")
        request-id
        run-id
        worker-id]))
    (boolean updated)))

(defn- start-lease-heartbeat!
  [request-id run-id worker-id lease-seconds heartbeat-seconds]
  (let [running?        (atom true)
        sleep-slice-ms 100
        sleep-iterations (max 1 (int (Math/ceil (/ (* 1000.0 (double heartbeat-seconds))
                                                   sleep-slice-ms))))
        future   (future
                   (while @running?
                     (dotimes [_ sleep-iterations]
                       (when @running?
                         (Thread/sleep sleep-slice-ms)))
                     (when @running?
                       (try
                         (renew-request-lease! request-id run-id worker-id lease-seconds)
                         (catch Exception e
                           (log/warn e "Failed to renew execution lease"
                                     {:request-id request-id
                                      :run-id run-id
                                      :worker-id worker-id}))))))]
    {:stop (fn []
             (reset! running? false)
             @future)}))

(defn- classify-failure
  [error]
  (let [data    (ex-data error)
        status  (:status data)
        message (or (ex-message error) (.getMessage error) "Execution failed")
        lower   (string/lower-case message)]
    (cond
      (string? (:failure_class data)) (:failure_class data)
      (= status 429) "rate_limited"
      (#{401 403} status) "auth_expired"
      (re-find #"missing bronze_table_name|must not be blank|unsupported|graph not found|no downstream target|valid identifier" lower)
      "config_error"
      (or (:schema_drift data)
          (re-find #"schema drift|duplicate enabled column_name" lower))
      "schema_drift"
      (re-find #"\bconflict\b|\bdeadlock\b|duplicate key|already exists" lower)
      "target_conflict"
      (re-find #"\bpayload\b|invalid json|\bparse error\b|\bcoerc(?:e|ion)?\b|cannot coerce" lower)
      "poison_payload"
      (or (and status (<= 500 (long status) 599))
          (re-find #"\btimeout\b|timed out|connection reset|broken pipe|\beof\b|\btransport\b|(?:^|[^[:alnum:]_])i/o(?:$|[^[:alnum:]_])|\brefused\b" lower))
      "transient_network"
      :else "unknown")))

(defn- retryable-failure?
  [failure-class]
  (contains? retryable-failure-classes failure-class))

(defn- retry-decision
  [request-row failure-class]
  (let [retry-count (long (or (:retry_count request-row) 0))
        max-retries (long (or (:max_retries request-row) 0))]
    {:retryable_failure? (retryable-failure? failure-class)
     :retry_count retry-count
     :max_retries max-retries
     :retry? (and (retryable-failure? failure-class)
                  (< retry-count max-retries))}))

(defn- retry-delay-seconds
  [request-row failure-class]
  (let [base (case failure-class
               "rate_limited" (parse-int-env :execution-rate-limit-retry-base-seconds 60)
               (parse-int-env :execution-retry-base-seconds 30))
        attempt (inc (long (or (:retry_count request-row) 0)))]
    (long (min 3600 (* base (long (Math/pow 2.0 (double (max 0 (dec attempt))))))))))

(defn- schedule-retry!
  [conn request-row run-id failure-class error-message request-guard]
  (let [delay-seconds (retry-delay-seconds request-row failure-class)]
    (if (guarded-request-update!
         conn
         "SET status = 'queued',
              available_at_utc = now() + (? || ' seconds')::interval,
              retry_count = retry_count + 1,
              lease_expires_at_utc = NULL,
              worker_id = NULL,
              error_message = ?,
              failure_class = ?,
              last_heartbeat_at_utc = NULL"
         [(str delay-seconds) error-message failure-class]
         (:request_id request-row)
         request-guard)
      (do
        (jdbc/execute!
         conn
         [(str "UPDATE " execution-run-table "
                SET status = 'queued',
                    finished_at_utc = NULL,
                    error_message = ?,
                    failure_class = ?
                WHERE run_id = ?")
          error-message
          failure-class
          run-id])
        (insert-node-run! conn run-id (:node_id request-row) (:request_kind request-row) "failed_retryable"
                          {:failure_class failure-class
                           :error error-message
                           :retry_count (inc (long (or (:retry_count request-row) 0)))
                           :available_in_seconds delay-seconds})
        {:status "queued"
         :failure_class failure-class
         :retry_scheduled_in_seconds delay-seconds})
      {:status "stale"
       :failure_class failure-class
       :error error-message})))

(defn- write-dlq!
  [conn request-row run-id failure-class error-message]
  (jdbc/execute!
   conn
   [(str "INSERT INTO " execution-dlq-table
         " (request_id, run_id, graph_id, node_id, request_kind, endpoint_name, failure_class, error_message, request_params)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (request_id) DO UPDATE
              SET run_id = EXCLUDED.run_id,
                  failure_class = EXCLUDED.failure_class,
                  error_message = EXCLUDED.error_message,
                  request_params = EXCLUDED.request_params")
    (:request_id request-row)
    run-id
    (:graph_id request-row)
    (:node_id request-row)
    (:request_kind request-row)
    (:endpoint_name request-row)
    failure-class
    error-message
    (:request_params request-row)]))

(defn- maybe-complete-scheduler-slot!
  [request-row status details]
  (when (= "scheduler" (:request_kind request-row))
    (when-let [scheduled-at (scheduled-for-utc (parse-json-safe (:request_params request-row)))]
      ((requiring-resolve 'bitool.ingest.scheduler/complete-enqueued-slot!)
       (:graph_id request-row)
       (:node_id request-row)
       scheduled-at
       status
       details))))

(defn- finalize-terminal-failure!
  [conn request-row run-id final-status failure-class error-message request-guard]
  (when (mark-run-status! conn
                          (:request_id request-row)
                          run-id
                          final-status
                          nil
                          error-message
                          failure-class
                          request-guard)
    (insert-node-run! conn run-id (:node_id request-row) (:request_kind request-row) final-status
                      {:failure_class failure-class
                       :error error-message
                       :retry_count (:retry_count request-row)})
    (write-dlq! conn request-row run-id failure-class error-message)
    true))

(defn- handle-request-failure!
  ([request-row run-id error]
   (handle-request-failure! request-row run-id error {:expected-statuses ["leased" "running"]}))
  ([request-row run-id error request-guard]
  (let [failure-class (classify-failure error)
        error-message (or (ex-message error) (.getMessage error) "Execution failed")
        can-retry?    (:retry? (retry-decision request-row failure-class))]
    (jdbc/with-transaction [tx db/ds]
      (if can-retry?
        (schedule-retry! tx request-row run-id failure-class error-message request-guard)
        (let [final-status (if (= failure-class "worker_orphaned") "timed_out" "failed")]
          (if (finalize-terminal-failure! tx request-row run-id final-status failure-class error-message request-guard)
            {:status final-status
             :failure_class failure-class
             :error error-message}
            {:status "stale"
             :failure_class failure-class
             :error error-message})))))))

(defn- claim-expired-request!
  []
  (jdbc/execute-one!
   (db-opts db/ds)
   [(str "WITH expired AS (
            SELECT request_id
            FROM " execution-request-table "
            WHERE status IN ('leased', 'running')
              AND lease_expires_at_utc IS NOT NULL
              AND lease_expires_at_utc <= now()
            ORDER BY lease_expires_at_utc ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
          )
          UPDATE " execution-request-table " r
          SET status = 'recovering_orphan',
              lease_expires_at_utc = NULL,
              worker_id = NULL,
              last_heartbeat_at_utc = NULL
          FROM expired
          WHERE r.request_id = expired.request_id
          RETURNING r.*")]))

(defn- record-orphan-recovery-event!
  [conn request-id run-id metric-name failure-class error-message]
  (jdbc/execute!
   conn
   [(str "INSERT INTO " execution-orphan-recovery-event-table "
          (request_id, run_id, metric_name, failure_class, error_message)
          VALUES (?, ?, ?, ?, ?)")
    request-id
    run-id
    metric-name
    failure-class
    error-message]))

(defn- register-builtin-execution-handlers!
  []
  (when-not @builtin-execution-handlers-registered?
    (locking builtin-execution-handlers-registered?
      (when-not @builtin-execution-handlers-registered?
        (plugins/register-execution-handler!
         :api
         {:description "Built-in API ingestion request handler"
         :workload-classifier (fn [{:keys [trigger-type request-params]}]
                                 (cond
                                   (or (= "replay" trigger-type)
                                       (:replay_source_run_id request-params)
                                       (:replay-source-run-id request-params)) "replay"
                                   (= "manual" trigger-type) "interactive"
                                   :else "api"))
          :execute (fn [request-row _]
                     (let [result ((requiring-resolve 'bitool.ingest.runtime/run-api-node!)
                                   (:graph_id request-row)
                                   (:node_id request-row)
                                   (merge {:endpoint-name (:endpoint_name request-row)}
                                          (parse-json-safe (:request_params request-row))))]
                       (when (= "failed" (:status result))
                         (throw (ex-info "API node execution failed for all endpoints"
                                         {:failure_class "endpoint_run_failed"
                                          :graph_id (:graph_id request-row)
                                          :node_id (:node_id request-row)
                                          :request_id (:request_id request-row)
                                          :run_result result})))
                       result))})
        (plugins/register-execution-handler!
         :scheduler
         {:description "Built-in scheduler trigger request handler"
          :workload-classifier (fn [_] "scheduled")
          :execute (fn [request-row request-params]
                     (let [scheduler-fn (requiring-resolve 'bitool.ingest.scheduler/execute-scheduler-node!)
                           result       (scheduler-fn (:graph_id request-row) (:node_id request-row))]
                       (when-let [scheduled-at (scheduled-for-utc request-params)]
                         ((requiring-resolve 'bitool.ingest.scheduler/complete-enqueued-slot!)
                          (:graph_id request-row)
                          (:node_id request-row)
                          scheduled-at
                          "success"
                          result))
                       result))})
        (plugins/register-execution-handler!
         :kafka
         {:description "Built-in Kafka ingestion request handler"
          :workload-classifier (fn [_] "kafka")
          :execute (fn [request-row request-params]
                     ((requiring-resolve 'bitool.ingest.kafka-runtime/run-kafka-node!)
                      (:graph_id request-row)
                      (:node_id request-row)
                      (merge {:endpoint-name (:endpoint_name request-row)}
                             request-params)))})
        (plugins/register-execution-handler!
         :file
         {:description "Built-in file ingestion request handler"
          :workload-classifier (fn [_] "file")
          :execute (fn [request-row request-params]
                     ((requiring-resolve 'bitool.ingest.file-runtime/run-file-node!)
                      (:graph_id request-row)
                      (:node_id request-row)
                      (merge {:endpoint-name (:endpoint_name request-row)}
                             request-params)))})
        (reset! builtin-execution-handlers-registered? true)))))

(defn- execute-request*!
  [request-row]
  (register-builtin-execution-handlers!)
  (let [request-params (parse-json-safe (:request_params request-row))
        request-kind  (:request_kind request-row)
        handler       (plugins/resolve-execution-handler request-kind)]
    (when-not handler
      (throw (ex-info "Unsupported execution request kind" {:request_kind request-kind
                                                            :failure_class "config_error"})))
    ((:execute handler) request-row request-params)))

(defn- sweep-expired-leases!
  ([] (sweep-expired-leases! (parse-int-env :execution-lease-sweeper-batch-size 50)))
  ([max-claims]
   (dotimes [_ (max 0 (long max-claims))]
    (when-let [request-row (claim-expired-request!)]
      (when-let [run-id (find-run-id (:request_id request-row))]
        (record-orphan-recovery-event! db/ds (:request_id request-row) run-id "claimed" nil nil)
        (log/warn "Claimed expired execution lease for orphan recovery"
                  {:request-id (:request_id request-row)
                   :run-id run-id
                   :graph-id (:graph_id request-row)
                   :node-id (:node_id request-row)})
        (let [result (handle-request-failure!
                      request-row
                      run-id
                      (ex-info "Execution lease expired before completion"
                               {:failure_class "worker_orphaned"
                                :request_id (:request_id request-row)})
                      {:expected-statuses ["recovering_orphan"]})]
          (record-orphan-recovery-event! db/ds
                                         (:request_id request-row)
                                         run-id
                                         (:status result)
                                         (:failure_class result)
                                         (:error result))
          (log/info "Completed orphan recovery decision"
                    {:request-id (:request_id request-row)
                     :run-id run-id
                     :status (:status result)
                     :failure-class (:failure_class result)})
          (when (#{"failed" "timed_out"} (:status result))
            (maybe-complete-scheduler-slot! request-row "failed"
                                            {:failure_class (:failure_class result)
                                             :error (:error result)}))))))))

(defn- worker-lease-config
  []
  (let [lease-seconds     (parse-int-env :execution-worker-lease-seconds 300)
        heartbeat-seconds (max 1 (parse-int-env :execution-worker-heartbeat-seconds
                                                (max 1 (quot lease-seconds 3))))]
    (when (<= lease-seconds heartbeat-seconds)
      (log/warn "Execution worker heartbeat interval is not safely below lease duration"
                {:lease_seconds lease-seconds
                 :heartbeat_seconds heartbeat-seconds}))
    {:lease-seconds lease-seconds
     :heartbeat-seconds heartbeat-seconds}))

(defn- worker-claim-profile
  []
  {:allowed-partitions (parse-csv-env-set :bitool-worker-queue-partitions)
   :allowed-workloads (parse-csv-env-set :bitool-worker-workload-classes)})

(defn- result-rows-written
  [result]
  (cond
    (map? result)
    (long (or (:rows_written result)
              (when (vector? (:results result))
                (reduce + 0 (map #(long (or (:rows_written %) 0)) (:results result))))
              0))

    :else 0))

(defn- maybe-record-usage!
  [request-row run-id status failure-class result]
  (try
    (let [workspace-context (control-plane/workspace-context (:workspace_key request-row) {:required? false})]
      (when (not= false (:metering_enabled workspace-context))
        (when-let [record-usage! (requiring-resolve 'bitool.operations/record-execution-usage!)]
          (record-usage!
           {:request-id (:request_id request-row)
            :run-id run-id
            :tenant-key (:tenant_key request-row)
            :workspace-key (:workspace_key request-row)
            :graph-id (:graph_id request-row)
            :node-id (:node_id request-row)
            :request-kind (:request_kind request-row)
            :queue-partition (:queue_partition request-row)
            :workload-class (:workload_class request-row)
            :status status
            :failure-class failure-class
            :rows-written (result-rows-written result)
            :retry-count (:retry_count request-row)
            :started-at (or (:started_at_utc request-row) (now-utc))
            :finished-at (now-utc)}))))
    (catch Exception e
      (log/warn e "Failed to record execution usage meter"
                {:request-id (:request_id request-row)
                 :run-id run-id
                 :status status}))))

(defn process-next-request!
  []
  (sweep-expired-leases! (parse-int-env :execution-lease-sweeper-max-per-poll 5))
  (let [worker-id         (or (get env :bitool-worker-id) (str "worker-" (UUID/randomUUID)))
        {:keys [lease-seconds heartbeat-seconds]} (worker-lease-config)
        claim-profile      (worker-claim-profile)]
    (when-let [request-row (claim-next-request! worker-id lease-seconds claim-profile)]
      (let [run-id    (find-run-id (:request_id request-row))
            heartbeat (start-lease-heartbeat! (:request_id request-row)
                                              run-id
                                              worker-id
                                              lease-seconds
                                              heartbeat-seconds)]
        (if-not (jdbc/with-transaction [tx db/ds]
                 (mark-request-running! tx (:request_id request-row) run-id worker-id))
          (do
            ((:stop heartbeat))
            {:processed? false
             :request_id (str (:request_id request-row))
             :run_id (str run-id)
             :status "stale"})
          (try
            (let [result (execute-request*! request-row)
                  completed? (jdbc/with-transaction
                               [tx db/ds]
                               (when (mark-run-status! tx
                                                       (:request_id request-row)
                                                       run-id
                                                       "succeeded"
                                                       result
                                                       nil
                                                       nil
                                                       {:expected-statuses ["leased" "running"]
                                                        :expected-worker-id worker-id})
                                 (insert-node-run! tx run-id (:node_id request-row) (:request_kind request-row) "succeeded" result)
                                 true))]
              ((:stop heartbeat))
              (when completed?
                (maybe-record-usage! request-row run-id "succeeded" nil result))
              {:processed? completed?
               :request_id (str (:request_id request-row))
               :run_id (str run-id)
               :status (if completed? "succeeded" "stale")})
            (catch Exception e
              ((:stop heartbeat))
              (let [result (handle-request-failure! request-row
                                                    run-id
                                                    e
                                                    {:expected-statuses ["leased" "running"]
                                                     :expected-worker-id worker-id})]
                (when (#{"failed" "timed_out"} (:status result))
                  (maybe-complete-scheduler-slot! request-row "failed"
                                                  {:failure_class (:failure_class result)
                                                   :error (:error result)}))
                (when (not= "stale" (:status result))
                  (maybe-record-usage! request-row run-id (:status result) (:failure_class result) nil))
                (when (not= "stale" (:status result))
                  (log/error e "Execution request failed"
                             {:request-id (:request_id request-row)
                              :run-id run-id
                              :failure-class (:failure_class result)}))
                {:processed? (not= "stale" (:status result))
                 :request_id (str (:request_id request-row))
                 :run_id (str run-id)
                 :status (:status result)
                 :failure_class (:failure_class result)
                 :error (:error result)}))))))))

(defn list-execution-runs
  ([] (list-execution-runs {}))
  ([{:keys [graph-id status limit workspace-key tenant-key endpoint-name request-kind workload-class queue-partition]}]
   (ensure-execution-tables!)
   (let [limit (max 1 (min 200 (or limit 50)))
         clauses (cond-> []
                   graph-id (conj "graph_id = ?")
                   status (conj "status = ?")
                   workspace-key (conj "workspace_key = ?")
                   tenant-key (conj "tenant_key = ?")
                   endpoint-name (conj "endpoint_name = ?")
                   request-kind (conj "request_kind = ?")
                   workload-class (conj "workload_class = ?")
                   queue-partition (conj "queue_partition = ?"))
         params (cond-> []
                  graph-id (conj graph-id)
                  status (conj status)
                  workspace-key (conj workspace-key)
                  tenant-key (conj tenant-key)
                  endpoint-name (conj endpoint-name)
                  request-kind (conj request-kind)
                  workload-class (conj workload-class)
                  queue-partition (conj queue-partition))
         sql-str (str "SELECT * FROM " execution-run-table
                      (when (seq clauses)
                        (str " WHERE " (string/join " AND " clauses)))
                      " ORDER BY COALESCE(started_at_utc, finished_at_utc) DESC NULLS LAST LIMIT ?")]
     (mapv
      (fn [row]
        (-> row
            (update :run_id str)
            (update :request_id str)
            (update :result_json parse-json-safe)))
      (jdbc/execute! (db-opts db/ds) (into [sql-str] (conj params limit)))))))

(defn execution-demand-snapshot
  []
  (ensure-execution-tables!)
  (mapv
   (fn [row]
     (-> row
         (update :queued_count long)
         (update :active_count long)
         (update :oldest_queued_age_seconds #(some-> % long))))
   (jdbc/execute!
    (db-opts db/ds)
    [(str "SELECT tenant_key,
                  workspace_key,
                  queue_partition,
                  workload_class,
                  COUNT(*) FILTER (WHERE status = 'queued') AS queued_count,
                  COUNT(*) FILTER (WHERE status IN ('leased', 'running', 'recovering_orphan')) AS active_count,
                  CAST(MAX(EXTRACT(EPOCH FROM (now() - requested_at_utc))) FILTER (WHERE status = 'queued') AS BIGINT) AS oldest_queued_age_seconds
           FROM " execution-request-table "
           WHERE status IN ('queued', 'leased', 'running', 'recovering_orphan')
           GROUP BY tenant_key, workspace_key, queue_partition, workload_class
           ORDER BY queued_count DESC, active_count DESC, tenant_key ASC, workspace_key ASC")])))

(defn execution-request-status-counts
  ([] (execution-request-status-counts {}))
  ([{:keys [scope since-hours]
     :or {scope :active
          since-hours 24}}]
   (let [clauses (case scope
                   :active ["status IN ('queued', 'leased', 'running', 'recovering_orphan')"]
                   :recent ["COALESCE(finished_at_utc, started_at_utc, requested_at_utc) >= now() - (? || ' hours')::interval"]
                   :all [])
         params  (cond-> []
                   (= scope :recent) (conj (str (max 1 (long since-hours)))))
         rows    (jdbc/execute!
                  (db-opts db/ds)
                  (into [(str "SELECT status, COUNT(*) AS cnt
                               FROM " execution-request-table
                               (when (seq clauses)
                                 (str " WHERE " (string/join " AND " clauses)))
                               " GROUP BY status")]
                        params))]
    (reduce (fn [acc row]
              (assoc acc (:status row) (long (:cnt row))))
            {}
            rows))))

(defn get-execution-run
  [run-id]
  (ensure-execution-tables!)
  (when-let [run-row (jdbc/execute-one!
                      (db-opts db/ds)
                      [(str "SELECT * FROM " execution-run-table " WHERE run_id = ?") (UUID/fromString (str run-id))])]
    (assoc (-> run-row
               (update :run_id str)
               (update :request_id str)
               (update :result_json parse-json-safe))
           :node_runs
           (mapv
            (fn [node-row]
              (update node-row :detail_json parse-json-safe))
           (jdbc/execute!
             (db-opts db/ds)
             [(str "SELECT * FROM " node-run-table " WHERE run_id = ? ORDER BY id ASC")
              (UUID/fromString (str run-id))])))))

(mount/defstate ^{:on-reload :noop} execution-metadata
  :start
  (when (or (config/enabled-role? :api)
            (config/enabled-role? :scheduler)
            (config/enabled-role? :worker))
    (register-builtin-execution-handlers!)
    (ensure-execution-tables!)
    {:ready? true})
  :stop nil)

(mount/defstate ^{:on-reload :noop} execution-worker
  :start
  (when (config/enabled-role? :worker)
    (let [running? (atom true)
          poll-ms  (parse-int-env :execution-worker-poll-ms 2000)]
      (ensure-execution-tables!)
      {:running? running?
       :future
       (future
         (while @running?
           (try
             (or (process-next-request!)
                 (Thread/sleep poll-ms))
             (catch Exception e
               (log/error e "Execution worker loop failed")
               (Thread/sleep poll-ms)))))}))
  :stop
  (when-let [running? (:running? execution-worker)]
    (reset! running? false)))
