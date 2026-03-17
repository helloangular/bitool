(ns bitool.control-plane
  (:require [bitool.config :as config]
            [bitool.db :as db]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [mount.core :as mount]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import [java.security SecureRandom]
           [java.util Base64]
           [javax.crypto Cipher]
           [javax.crypto.spec GCMParameterSpec SecretKeySpec]))

(def ^:private tenant-table "control_plane_tenant")
(def ^:private workspace-table "control_plane_workspace")
(def ^:private graph-workspace-table "control_plane_graph_workspace")
(def ^:private graph-dependency-table "control_plane_graph_dependency")
(def ^:private secret-store-table "control_plane_secret_store")
(def ^:private audit-event-table "control_plane_audit_event")
(def ^:private api-bronze-signoff-table "control_plane_api_bronze_signoff")
(def ^:dynamic execution-run-table "execution_run")

(def ^:private default-tenant-key "default")
(def ^:private default-workspace-key "default")
(defonce ^:private control-plane-ready? (atom false))

(defn- db-opts
  [conn]
  (jdbc/with-options conn {:builder-fn rs/as-unqualified-lower-maps}))

(defn- non-blank-str
  [value]
  (let [value (some-> value str string/trim)]
    (when (seq value) value)))

(defn- now-utc []
  (java.time.Instant/now))

(defn- parse-bool-env
  [k default-value]
  (let [raw (get config/env k)]
    (if (nil? raw)
      default-value
      (contains? #{"1" "true" "yes" "on"}
                 (some-> raw str string/trim string/lower-case)))))

(defn- secret-encryption-key-bytes
  []
  (when-let [encoded (non-blank-str (or (get config/env :bitool-secret-encryption-key-b64)
                                         (get config/env :bitool-secret-encryption-key)))]
    (try
      (.decode (Base64/getDecoder) encoded)
      (catch Exception _
        nil))))

(defn- secret-encryption-key
  []
  (when-let [key-bytes (secret-encryption-key-bytes)]
    (when (contains? #{16 24 32} (alength key-bytes))
      (SecretKeySpec. key-bytes "AES"))))

(defn- encrypt-managed-secret
  [secret-value]
  (if-let [key (secret-encryption-key)]
    (let [iv     (byte-array 12)
          _      (.nextBytes (SecureRandom.) iv)
          cipher (doto (Cipher/getInstance "AES/GCM/NoPadding")
                   (.init Cipher/ENCRYPT_MODE key (GCMParameterSpec. 128 iv)))
          bytes  (.doFinal cipher (.getBytes (str secret-value) "UTF-8"))
          iv-b64 (.encodeToString (Base64/getEncoder) iv)
          ct-b64 (.encodeToString (Base64/getEncoder) bytes)]
      {:encoding "aes_gcm_v1"
       :stored-value (str "v1:" iv-b64 ":" ct-b64)})
    {:encoding "plaintext"
     :stored-value (str secret-value)}))

(defn- decrypt-managed-secret
  [stored-value encoding]
  (let [stored-value (str stored-value)
        encoding (or (non-blank-str encoding) "plaintext")]
    (case encoding
      "aes_gcm_v1"
      (if-let [key (secret-encryption-key)]
        (let [[version iv-b64 ct-b64] (string/split stored-value #":" 3)]
          (when-not (= "v1" version)
            (throw (ex-info "Unsupported managed secret ciphertext version"
                            {:encoding encoding})))
          (let [iv     (.decode (Base64/getDecoder) iv-b64)
                cipher (.decode (Base64/getDecoder) ct-b64)
                aes    (doto (Cipher/getInstance "AES/GCM/NoPadding")
                         (.init Cipher/DECRYPT_MODE key (GCMParameterSpec. 128 iv)))]
            (String. (.doFinal aes cipher) "UTF-8")))
        (throw (ex-info "Managed secret encryption key is not configured"
                        {:encoding encoding
                         :status 500})))
      stored-value)))

(def ^:private workspace-context-select
  (str "SELECT w.workspace_key,
               w.tenant_key,
               w.max_concurrent_requests,
               w.max_queued_requests,
               w.weight,
               w.active,
               t.max_concurrent_requests AS tenant_max_concurrent_requests,
               t.max_queued_requests AS tenant_max_queued_requests,
               t.weight AS tenant_weight,
               t.active AS tenant_active,
               t.metering_enabled
        FROM " workspace-table " w
        JOIN " tenant-table " t ON t.tenant_key = w.tenant_key
        WHERE w.workspace_key = ?"))

(def ^:private graph-workspace-context-select
  (str "SELECT gw.graph_id, gw.workspace_key, w.tenant_key, w.max_concurrent_requests, w.max_queued_requests, w.weight, w.active,
               t.max_concurrent_requests AS tenant_max_concurrent_requests,
               t.max_queued_requests AS tenant_max_queued_requests,
               t.weight AS tenant_weight,
               t.active AS tenant_active,
               t.metering_enabled
        FROM " graph-workspace-table " gw
        JOIN " workspace-table " w ON w.workspace_key = gw.workspace_key
        JOIN " tenant-table " t ON t.tenant_key = w.tenant_key
        WHERE gw.graph_id = ?"))

(defn- ->instant
  [value]
  (cond
    (nil? value) nil
    (instance? java.time.Instant value) value
    (instance? java.time.OffsetDateTime value) (.toInstant ^java.time.OffsetDateTime value)
    (instance? java.time.ZonedDateTime value) (.toInstant ^java.time.ZonedDateTime value)
    (instance? java.sql.Timestamp value) (.toInstant ^java.sql.Timestamp value)
    :else nil))

(defn ensure-control-plane-tables!
  []
  (when-not @control-plane-ready?
    (locking control-plane-ready?
      (when-not @control-plane-ready?
        (doseq [sql-str
                [(str "CREATE TABLE IF NOT EXISTS " tenant-table " ("
                      "tenant_key VARCHAR(128) PRIMARY KEY, "
                      "tenant_name TEXT NOT NULL, "
                      "max_concurrent_requests INTEGER NOT NULL DEFAULT 10, "
                      "max_queued_requests INTEGER NOT NULL DEFAULT 1000, "
                      "weight INTEGER NOT NULL DEFAULT 1, "
                      "metering_enabled BOOLEAN NOT NULL DEFAULT TRUE, "
                      "active BOOLEAN NOT NULL DEFAULT TRUE, "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                      "updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                 (str "CREATE TABLE IF NOT EXISTS " workspace-table " ("
                      "workspace_key VARCHAR(128) PRIMARY KEY, "
                      "tenant_key VARCHAR(128) NOT NULL REFERENCES " tenant-table "(tenant_key), "
                      "workspace_name TEXT NOT NULL, "
                      "max_concurrent_requests INTEGER NOT NULL DEFAULT 2, "
                      "max_queued_requests INTEGER NOT NULL DEFAULT 100, "
                      "weight INTEGER NOT NULL DEFAULT 1, "
                      "active BOOLEAN NOT NULL DEFAULT TRUE, "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                      "updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                 (str "CREATE TABLE IF NOT EXISTS " graph-workspace-table " ("
                      "graph_id INTEGER PRIMARY KEY, "
                      "workspace_key VARCHAR(128) NOT NULL REFERENCES " workspace-table "(workspace_key), "
                      "updated_by TEXT NULL, "
                      "updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                 (str "CREATE TABLE IF NOT EXISTS " graph-dependency-table " ("
                      "id BIGSERIAL PRIMARY KEY, "
                      "downstream_graph_id INTEGER NOT NULL, "
                      "upstream_graph_id INTEGER NOT NULL, "
                      "freshness_window_seconds INTEGER NULL, "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                      "UNIQUE (downstream_graph_id, upstream_graph_id))")
                 (str "CREATE INDEX IF NOT EXISTS idx_control_plane_dependency_downstream "
                      "ON " graph-dependency-table " (downstream_graph_id)")
                 (str "CREATE INDEX IF NOT EXISTS idx_control_plane_dependency_upstream "
                      "ON " graph-dependency-table " (upstream_graph_id)")
                 (str "CREATE TABLE IF NOT EXISTS " secret-store-table " ("
                      "secret_ref VARCHAR(255) PRIMARY KEY, "
                      "secret_value TEXT NOT NULL, "
                      "secret_encoding VARCHAR(32) NOT NULL DEFAULT 'plaintext', "
                      "updated_by TEXT NULL, "
                      "updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                 (str "CREATE TABLE IF NOT EXISTS " audit-event-table " ("
                      "id BIGSERIAL PRIMARY KEY, "
                      "event_type VARCHAR(128) NOT NULL, "
                      "actor TEXT NULL, "
                      "graph_id INTEGER NULL, "
                      "node_id INTEGER NULL, "
                      "run_id UUID NULL, "
                      "secret_ref VARCHAR(255) NULL, "
                      "details_json TEXT NULL, "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                 (str "CREATE TABLE IF NOT EXISTS " api-bronze-signoff-table " ("
                      "id BIGSERIAL PRIMARY KEY, "
                      "release_tag VARCHAR(255) NOT NULL, "
                      "environment VARCHAR(128) NOT NULL, "
                      "commit_sha VARCHAR(128) NOT NULL, "
                      "proof_summary_path TEXT NOT NULL, "
                      "proof_results_path TEXT NOT NULL, "
                      "proof_log_path TEXT NOT NULL, "
                      "proof_status VARCHAR(32) NOT NULL, "
                      "soak_iterations INTEGER NOT NULL DEFAULT 0, "
                      "operator_name TEXT NOT NULL, "
                      "reviewer_name TEXT NOT NULL, "
                      "operator_notes TEXT NULL, "
                      "created_by TEXT NULL, "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                 (str "CREATE INDEX IF NOT EXISTS idx_control_plane_audit_event_created "
                      "ON " audit-event-table " (created_at_utc DESC, event_type)")
                 (str "CREATE INDEX IF NOT EXISTS idx_control_plane_api_bronze_signoff_created "
                      "ON " api-bronze-signoff-table " (created_at_utc DESC, environment, release_tag)")
                 (str "ALTER TABLE " tenant-table
                      " ADD COLUMN IF NOT EXISTS max_concurrent_requests INTEGER NOT NULL DEFAULT 10")
                 (str "ALTER TABLE " tenant-table
                      " ADD COLUMN IF NOT EXISTS max_queued_requests INTEGER NOT NULL DEFAULT 1000")
                 (str "ALTER TABLE " tenant-table
                      " ADD COLUMN IF NOT EXISTS weight INTEGER NOT NULL DEFAULT 1")
                 (str "ALTER TABLE " tenant-table
                      " ADD COLUMN IF NOT EXISTS metering_enabled BOOLEAN NOT NULL DEFAULT TRUE")
                 (str "ALTER TABLE " secret-store-table
                      " ADD COLUMN IF NOT EXISTS secret_encoding VARCHAR(32) NOT NULL DEFAULT 'plaintext'")]]
          (jdbc/execute! db/ds [sql-str]))
        (jdbc/execute!
         db/ds
         [(str "INSERT INTO " tenant-table " (tenant_key, tenant_name, max_concurrent_requests, max_queued_requests, weight, metering_enabled, active, updated_at_utc)
                VALUES (?, ?, 10, 1000, 1, TRUE, TRUE, now())
                ON CONFLICT (tenant_key) DO NOTHING")
          default-tenant-key
          "Default tenant"])
        (jdbc/execute!
         db/ds
         [(str "INSERT INTO " workspace-table "
                (workspace_key, tenant_key, workspace_name, max_concurrent_requests, max_queued_requests, weight, active, updated_at_utc)
                VALUES (?, ?, ?, 2, 100, 1, TRUE, now())
                ON CONFLICT (workspace_key) DO NOTHING")
          default-workspace-key
          default-tenant-key
          "Default workspace"])
        (reset! control-plane-ready? true))))
  nil)

(defn upsert-tenant!
  [{:keys [tenant_key tenant_name max_concurrent_requests max_queued_requests weight active metering_enabled]
    :or {tenant_name "Tenant"
         max_concurrent_requests 10
         max_queued_requests 1000
         weight 1
         active true
         metering_enabled true}}]
  (ensure-control-plane-tables!)
  (let [tenant-key (or (non-blank-str tenant_key) default-tenant-key)
        tenant-name (or (non-blank-str tenant_name) tenant-key)]
    (jdbc/execute!
     db/ds
     [(str "INSERT INTO " tenant-table "
            (tenant_key, tenant_name, max_concurrent_requests, max_queued_requests, weight, metering_enabled, active, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (tenant_key) DO UPDATE
            SET tenant_name = EXCLUDED.tenant_name,
                max_concurrent_requests = EXCLUDED.max_concurrent_requests,
                max_queued_requests = EXCLUDED.max_queued_requests,
                weight = EXCLUDED.weight,
                metering_enabled = EXCLUDED.metering_enabled,
                active = EXCLUDED.active,
                updated_at_utc = now()")
      tenant-key
      tenant-name
      (max 1 (long max_concurrent_requests))
      (max 1 (long max_queued_requests))
      (max 1 (long weight))
      (boolean metering_enabled)
      (boolean active)])
    (jdbc/execute-one!
     (db-opts db/ds)
     [(str "SELECT * FROM " tenant-table " WHERE tenant_key = ?")
      tenant-key])))

(defn list-tenants
  []
  (ensure-control-plane-tables!)
  (jdbc/execute!
   (db-opts db/ds)
   [(str "SELECT * FROM " tenant-table " ORDER BY tenant_key ASC")]))

(defn upsert-workspace!
  [{:keys [workspace_key tenant_key workspace_name max_concurrent_requests max_queued_requests weight active]
    :or {tenant_key default-tenant-key
         workspace_name "Workspace"
         max_concurrent_requests 2
         max_queued_requests 100
         weight 1
         active true}}]
  (ensure-control-plane-tables!)
  (let [workspace-key (or (non-blank-str workspace_key) default-workspace-key)
        tenant-key    (or (non-blank-str tenant_key) default-tenant-key)
        workspace-name (or (non-blank-str workspace_name) workspace-key)]
    (jdbc/execute!
     db/ds
     [(str "INSERT INTO " tenant-table " (tenant_key, tenant_name, max_concurrent_requests, max_queued_requests, weight, metering_enabled, active, updated_at_utc)
            VALUES (?, ?, 10, 1000, 1, TRUE, TRUE, now())
            ON CONFLICT (tenant_key) DO NOTHING")
      tenant-key
      tenant-key])
    (jdbc/execute!
     db/ds
     [(str "INSERT INTO " workspace-table "
            (workspace_key, tenant_key, workspace_name, max_concurrent_requests, max_queued_requests, weight, active, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (workspace_key) DO UPDATE
            SET tenant_key = EXCLUDED.tenant_key,
                workspace_name = EXCLUDED.workspace_name,
                max_concurrent_requests = EXCLUDED.max_concurrent_requests,
                max_queued_requests = EXCLUDED.max_queued_requests,
                weight = EXCLUDED.weight,
                active = EXCLUDED.active,
                updated_at_utc = now()")
      workspace-key
      tenant-key
      workspace-name
      (max 1 (long max_concurrent_requests))
      (max 1 (long max_queued_requests))
      (max 1 (long weight))
      (boolean active)])
    (jdbc/execute-one!
     (db-opts db/ds)
     [(str "SELECT * FROM " workspace-table " WHERE workspace_key = ?")
      workspace-key])))

(defn list-workspaces
  []
  (ensure-control-plane-tables!)
  (jdbc/execute!
   (db-opts db/ds)
   [(str "SELECT w.*, t.tenant_name
          FROM " workspace-table " w
          JOIN " tenant-table " t ON t.tenant_key = w.tenant_key
          ORDER BY w.workspace_key ASC")]))

(defn assign-graph-workspace!
  [graph-id workspace-key updated-by]
  (ensure-control-plane-tables!)
  (let [workspace-key (or (non-blank-str workspace-key) default-workspace-key)]
    (jdbc/execute!
     db/ds
     [(str "INSERT INTO " graph-workspace-table " (graph_id, workspace_key, updated_by, updated_at_utc)
            VALUES (?, ?, ?, now())
            ON CONFLICT (graph_id) DO UPDATE
            SET workspace_key = EXCLUDED.workspace_key,
                updated_by = EXCLUDED.updated_by,
                updated_at_utc = now()")
      graph-id
      workspace-key
      (non-blank-str updated-by)])
    (jdbc/execute-one!
     (db-opts db/ds)
     [(str "SELECT gw.graph_id, gw.workspace_key, w.tenant_key, w.max_concurrent_requests, w.max_queued_requests, w.weight
                   , w.active
            FROM " graph-workspace-table " gw
            JOIN " workspace-table " w ON w.workspace_key = gw.workspace_key
            WHERE gw.graph_id = ?")
      graph-id])))

(defn- workspace-context*
  [conn workspace-key {:keys [required?]}]
  (let [workspace-key (or (non-blank-str workspace-key) default-workspace-key)
        row           (jdbc/execute-one! (db-opts conn) [workspace-context-select workspace-key])]
    (cond
      row row
      required? (throw (ex-info "Workspace not found"
                                {:workspace_key workspace-key
                                 :status 404}))
      :else nil)))

(defn- default-workspace-context*
  [conn]
  (or (workspace-context* conn default-workspace-key {:required? false})
      (throw (ex-info "Default workspace is missing"
                      {:workspace_key default-workspace-key
                       :status 500}))))

(defn workspace-context
  ([workspace-key]
   (workspace-context workspace-key {}))
  ([workspace-key {:keys [required?]}]
   (ensure-control-plane-tables!)
   (or (workspace-context* db/ds workspace-key {:required? false})
       (when required?
         (throw (ex-info "Workspace not found"
                         {:workspace_key (or (non-blank-str workspace-key) default-workspace-key)
                          :status 404})))
       (default-workspace-context* db/ds))))

(defn graph-workspace-context
  [graph-id]
  (ensure-control-plane-tables!)
  (or (jdbc/execute-one! (db-opts db/ds) [graph-workspace-context-select graph-id])
      (assoc (default-workspace-context* db/ds) :graph_id graph-id)))

(defn current-graph-version
  [graph-id]
  (some-> (jdbc/execute-one! (db-opts db/ds)
                             ["SELECT version FROM graph WHERE id = ? ORDER BY version DESC LIMIT 1" graph-id])
          :version))

(defn- current-graph-version*
  [conn graph-id]
  (some-> (jdbc/execute-one! (db-opts conn)
                             ["SELECT version FROM graph WHERE id = ? ORDER BY version DESC LIMIT 1" graph-id])
          :version))

(defn- graph-workspace-context*
  [conn graph-id]
  (or (jdbc/execute-one! (db-opts conn) [graph-workspace-context-select graph-id])
      (assoc (default-workspace-context* conn) :graph_id graph-id)))

(defn- assign-graph-workspace*!
  [conn graph-id workspace-key updated-by]
  (let [workspace-key (or (non-blank-str workspace-key) default-workspace-key)]
    (jdbc/execute!
     conn
     [(str "INSERT INTO " graph-workspace-table " (graph_id, workspace_key, updated_by, updated_at_utc)
            VALUES (?, ?, ?, now())
            ON CONFLICT (graph_id) DO UPDATE
            SET workspace_key = EXCLUDED.workspace_key,
                updated_by = EXCLUDED.updated_by,
                updated_at_utc = now()")
      graph-id
      workspace-key
      (non-blank-str updated-by)])))

(defn- persist-graph-in-tx!
  [tx g {:keys [graph-id expected-version workspace-key updated-by]}]
  (let [current-version (when (pos? graph-id) (current-graph-version* tx graph-id))]
    (when (and (some? expected-version)
               (not= expected-version current-version))
      (throw (ex-info "Graph version conflict"
                      {:graph_id graph-id
                       :expected_version expected-version
                       :current_version current-version
                       :status 409})))
    (let [saved     (db/insert-graph! tx g)
          saved-id  (get-in saved [:a :id])
          workspace (if-let [requested-workspace (non-blank-str workspace-key)]
                      (:workspace_key (workspace-context* tx requested-workspace {:required? true}))
                      (:workspace_key (graph-workspace-context* tx saved-id)))]
      (assign-graph-workspace*! tx saved-id workspace updated-by)
      saved)))

(defn persist-graph!
  [g {:keys [expected-version workspace-key updated-by]}]
  (ensure-control-plane-tables!)
  (let [graph-id         (long (or (get-in g [:a :id]) 0))
        expected-version (when (some? expected-version)
                           (long expected-version))]
    (jdbc/with-transaction [tx db/ds]
      (when (pos? graph-id)
        (jdbc/execute! tx ["SELECT pg_advisory_xact_lock(?)" graph-id]))
      (persist-graph-in-tx! tx g {:graph-id graph-id
                                  :expected-version expected-version
                                  :workspace-key workspace-key
                                  :updated-by updated-by}))))

(defn set-graph-dependencies!
  [downstream-graph-id dependencies]
  (ensure-control-plane-tables!)
  (jdbc/with-transaction [tx db/ds]
    (jdbc/execute! tx [(str "DELETE FROM " graph-dependency-table " WHERE downstream_graph_id = ?")
                       downstream-graph-id])
    (doseq [{:keys [upstream_graph_id freshness_window_seconds]} dependencies]
      (jdbc/execute!
       tx
       [(str "INSERT INTO " graph-dependency-table "
              (downstream_graph_id, upstream_graph_id, freshness_window_seconds)
              VALUES (?, ?, ?)
              ON CONFLICT (downstream_graph_id, upstream_graph_id) DO UPDATE
              SET freshness_window_seconds = EXCLUDED.freshness_window_seconds")
        downstream-graph-id
        upstream_graph_id
        freshness_window_seconds])))
  (jdbc/execute!
   (db-opts db/ds)
   [(str "SELECT * FROM " graph-dependency-table " WHERE downstream_graph_id = ? ORDER BY upstream_graph_id ASC")
    downstream-graph-id]))

(defn dependency-blockers
  [graph-id]
  (ensure-control-plane-tables!)
  (let [rows (jdbc/execute!
              (db-opts db/ds)
              [(str "SELECT d.downstream_graph_id,
                            d.upstream_graph_id,
                            d.freshness_window_seconds,
                            latest.latest_success_at_utc
                     FROM " graph-dependency-table " d
                     LEFT JOIN LATERAL (
                       SELECT COALESCE(finished_at_utc, started_at_utc) AS latest_success_at_utc
                       FROM " execution-run-table "
                       WHERE graph_id = d.upstream_graph_id
                         AND status = 'succeeded'
                       ORDER BY COALESCE(finished_at_utc, started_at_utc) DESC
                       LIMIT 1
                     ) latest ON TRUE
                     WHERE d.downstream_graph_id = ?
                     ORDER BY d.upstream_graph_id ASC")
               graph-id])]
    (->> rows
         (keep (fn [row]
                 (let [latest-success (:latest_success_at_utc row)
                       latest-success (->instant latest-success)
                       freshness-window (:freshness_window_seconds row)
                       stale? (and latest-success
                                   freshness-window
                                   (.isBefore ^java.time.Instant latest-success
                                              (.minusSeconds (now-utc) (long freshness-window))))]
                   (cond
                     (nil? latest-success)
                     {:upstream_graph_id (:upstream_graph_id row)
                      :reason "missing_successful_run"}

                     stale?
                     {:upstream_graph_id (:upstream_graph_id row)
                      :reason "stale_successful_run"
                      :latest_success_at_utc latest-success
                      :freshness_window_seconds freshness-window}

                     :else nil))))
         vec)))

(defn lineage-view
  [graph-id]
  (ensure-control-plane-tables!)
  {:graph_id graph-id
   :upstream (jdbc/execute!
              (db-opts db/ds)
              [(str "SELECT upstream_graph_id, freshness_window_seconds
                     FROM " graph-dependency-table "
                     WHERE downstream_graph_id = ?
                     ORDER BY upstream_graph_id ASC")
               graph-id])
   :downstream (jdbc/execute!
                (db-opts db/ds)
                [(str "SELECT downstream_graph_id, freshness_window_seconds
                       FROM " graph-dependency-table "
                       WHERE upstream_graph_id = ?
                       ORDER BY downstream_graph_id ASC")
                 graph-id])})

(defn put-secret!
  [{:keys [secret_ref secret_value updated_by]}]
  (ensure-control-plane-tables!)
  (let [secret-ref (non-blank-str secret_ref)]
    (when-not (and secret-ref (some? secret_value))
      (throw (ex-info "secret_ref and secret_value are required"
                      {:secret_ref secret_ref})))
    (let [{:keys [encoding stored-value]} (encrypt-managed-secret secret_value)]
    (jdbc/execute!
     db/ds
     [(str "INSERT INTO " secret-store-table " (secret_ref, secret_value, secret_encoding, updated_by, updated_at_utc)
            VALUES (?, ?, ?, ?, now())
            ON CONFLICT (secret_ref) DO UPDATE
            SET secret_value = EXCLUDED.secret_value,
                secret_encoding = EXCLUDED.secret_encoding,
                updated_by = EXCLUDED.updated_by,
                updated_at_utc = now()")
      secret-ref
      stored-value
      encoding
      (non-blank-str updated_by)])
    {:secret_ref secret-ref
     :secret_encoding encoding
     :updated_at_utc (str (now-utc))})))

(defn record-audit-event!
  [{:keys [event_type actor graph_id node_id run_id secret_ref details]}]
  (ensure-control-plane-tables!)
  (when-let [event-type (non-blank-str event_type)]
    (jdbc/execute!
     db/ds
     [(str "INSERT INTO " audit-event-table "
            (event_type, actor, graph_id, node_id, run_id, secret_ref, details_json, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, now())")
      event-type
      (non-blank-str actor)
      graph_id
      node_id
      (when run_id (java.util.UUID/fromString (str run_id)))
      (non-blank-str secret_ref)
      (when details (pr-str details))])))

(defn list-audit-events
  ([] (list-audit-events {}))
  ([{:keys [event_type limit]
     :or {limit 200}}]
   (ensure-control-plane-tables!)
   (let [limit (max 1 (min 1000 (long limit)))
         clauses (cond-> []
                   (non-blank-str event_type) (conj "event_type = ?"))
         params (cond-> []
                  (non-blank-str event_type) (conj (non-blank-str event_type)))
         sql-str (str "SELECT *
                       FROM " audit-event-table
                      (when (seq clauses)
                        (str " WHERE " (string/join " AND " clauses)))
                      " ORDER BY created_at_utc DESC
                        LIMIT ?")]
     (jdbc/execute! (db-opts db/ds) (into [sql-str] (conj params limit))))))

(defn resolve-managed-secret
  [secret-ref]
  (ensure-control-plane-tables!)
  (let [secret-ref (non-blank-str secret-ref)
        row (jdbc/execute-one!
             (db-opts db/ds)
             [(str "SELECT secret_value, secret_encoding FROM " secret-store-table " WHERE secret_ref = ?")
              secret-ref])
        secret-value (when row
                       (decrypt-managed-secret (:secret_value row) (:secret_encoding row)))]
    (when (and secret-value (parse-bool-env :bitool-audit-secret-access true))
      (try
        (record-audit-event! {:event_type "secret.read"
                              :secret_ref secret-ref
                              :actor "runtime"
                              :details {:source "resolve-managed-secret"}})
        (catch Exception e
          (log/warn e "Failed to persist managed secret audit event"
                    {:secret_ref secret-ref
                     :event_type "secret.read"}))))
    secret-value))

(defn record-api-bronze-signoff!
  [{:keys [release_tag environment commit_sha
           proof_summary_path proof_results_path proof_log_path
           proof_status soak_iterations operator_name reviewer_name
           operator_notes created_by]}]
  (ensure-control-plane-tables!)
  (let [release-tag (non-blank-str release_tag)
        environment (or (non-blank-str environment) "default")
        commit-sha (non-blank-str commit_sha)
        proof-summary-path (non-blank-str proof_summary_path)
        proof-results-path (non-blank-str proof_results_path)
        proof-log-path (non-blank-str proof_log_path)
        proof-status (-> (or (non-blank-str proof_status) "failed")
                         string/lower-case)
        soak-iterations (max 0 (long (or soak_iterations 0)))
        operator-name (non-blank-str operator_name)
        reviewer-name (non-blank-str reviewer_name)
        created-by (or (non-blank-str created_by) operator-name "system")]
    (when-not (and release-tag commit-sha proof-summary-path proof-results-path proof-log-path
                   operator-name reviewer-name)
      (throw (ex-info "Missing required API->Bronze signoff fields"
                      {:status 400
                       :release_tag release_tag
                       :environment environment})))
    (when-not (contains? #{"passed" "failed"} proof-status)
      (throw (ex-info "proof_status must be passed or failed"
                      {:status 400
                       :proof_status proof_status})))
    (doseq [[field-label path] {"proof_summary_path" proof-summary-path
                                "proof_results_path" proof-results-path
                                "proof_log_path" proof-log-path}]
      (when-not (some-> path io/file .isFile)
        (throw (ex-info (str field-label " must point to an existing file")
                        {:status 400
                         :field field-label
                         :path path}))))
    (jdbc/execute-one!
     (db-opts db/ds)
     [(str "INSERT INTO " api-bronze-signoff-table " (
             release_tag, environment, commit_sha, proof_summary_path, proof_results_path, proof_log_path,
             proof_status, soak_iterations, operator_name, reviewer_name, operator_notes, created_by, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            RETURNING *")
      release-tag
      environment
      commit-sha
      proof-summary-path
      proof-results-path
      proof-log-path
      proof-status
      soak-iterations
      operator-name
      reviewer-name
      (non-blank-str operator_notes)
      created-by])))

(defn list-api-bronze-signoffs
  ([] (list-api-bronze-signoffs {}))
  ([{:keys [environment limit]
     :or {limit 100}}]
   (ensure-control-plane-tables!)
   (let [environment (non-blank-str environment)
         limit (max 1 (min 1000 (long limit)))
         clauses (cond-> []
                   environment (conj "environment = ?"))
         params  (cond-> []
                   environment (conj environment))]
     (jdbc/execute!
      (db-opts db/ds)
      (into [(str "SELECT *
                   FROM " api-bronze-signoff-table
                  (when (seq clauses)
                    (str " WHERE " (string/join " AND " clauses)))
                  " ORDER BY created_at_utc DESC
                    LIMIT ?")]
            (conj params limit))))))

(mount/defstate ^{:on-reload :noop} control-plane-metadata
  :start
  (when (or (config/enabled-role? :api)
            (config/enabled-role? :scheduler)
            (config/enabled-role? :worker))
    (ensure-control-plane-tables!)
    {:ready? true})
  :stop nil)
