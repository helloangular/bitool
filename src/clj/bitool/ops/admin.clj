(ns bitool.ops.admin
  (:require [bitool.db :as db]
            [bitool.control-plane :as control-plane]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [cheshire.core :as json]))

;; ---------------------------------------------------------------------------
;; Internals
;; ---------------------------------------------------------------------------

(def ^:private config-table "ops_config")

(defonce ^:private admin-ready? (atom false))

(defn- db-opts
  [conn]
  (jdbc/with-options conn {:builder-fn rs/as-unqualified-lower-maps}))

(defn- now-utc []
  (java.time.Instant/now))

;; ---------------------------------------------------------------------------
;; Table bootstrap
;; ---------------------------------------------------------------------------

(defn ensure-admin-tables!
  "Create the ops_config table and indexes if they do not already exist.
   Uses a double-check locking pattern so that concurrent callers only run
   the DDL once per JVM lifetime."
  []
  (when-not @admin-ready?
    (locking admin-ready?
      (when-not @admin-ready?
        (jdbc/execute!
         db/ds
         [(str "CREATE TABLE IF NOT EXISTS " config-table " ("
               "config_id      BIGSERIAL PRIMARY KEY, "
               "config_key     TEXT NOT NULL, "
               "config_value   JSONB NOT NULL, "
               "workspace_key  TEXT, "
               "version        INT NOT NULL DEFAULT 1, "
               "updated_by     TEXT NOT NULL, "
               "updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
               "superseded     BOOLEAN NOT NULL DEFAULT false)")])
        (jdbc/execute!
         db/ds
         [(str "CREATE INDEX IF NOT EXISTS idx_ops_config_active "
               "ON " config-table "(config_key, workspace_key) "
               "WHERE NOT superseded")])
        (jdbc/execute!
         db/ds
         [(str "CREATE INDEX IF NOT EXISTS idx_ops_config_history "
               "ON " config-table "(config_key, workspace_key, version DESC)")])
        (reset! admin-ready? true))))
  nil)

;; ---------------------------------------------------------------------------
;; get-config
;; ---------------------------------------------------------------------------

(defn get-config
  "Fetch the active (non-superseded) config for a given config-key and
   optional workspace-key.  Returns the parsed config_value map, or nil."
  [{:keys [config-key workspace-key]}]
  (ensure-admin-tables!)
  (let [row (jdbc/execute-one!
             (db-opts db/ds)
             [(str "SELECT * FROM " config-table
                   " WHERE config_key = ?"
                   " AND workspace_key IS NOT DISTINCT FROM ?"
                   " AND NOT superseded")
              config-key
              workspace-key])]
    (when row
      (assoc row :config_value (json/parse-string (:config_value row) true)))))

;; ---------------------------------------------------------------------------
;; config-history
;; ---------------------------------------------------------------------------

(defn config-history
  "Return all versions of a config-key (optionally scoped to workspace-key),
   ordered by version DESC.  Each row's config_value is parsed from JSON."
  [{:keys [config-key workspace-key limit]
    :or   {limit 50}}]
  (ensure-admin-tables!)
  (let [rows (jdbc/execute!
              (db-opts db/ds)
              [(str "SELECT * FROM " config-table
                    " WHERE config_key = ?"
                    " AND workspace_key IS NOT DISTINCT FROM ?"
                    " ORDER BY version DESC"
                    " LIMIT ?")
               config-key
               workspace-key
               (max 1 (long limit))])]
    (mapv (fn [row]
            (assoc row :config_value (json/parse-string (:config_value row) true)))
          rows)))

;; ---------------------------------------------------------------------------
;; config-diff
;; ---------------------------------------------------------------------------

(defn config-diff
  "Compare two config value maps and return a vector of change descriptors.
   Each entry is {:path <key> :old <val> :new <val>} for keys that differ
   between old-value and new-value (including additions and removals)."
  [old-value new-value]
  (let [old-value (or old-value {})
        new-value (or new-value {})
        all-keys  (distinct (concat (keys old-value) (keys new-value)))]
    (->> all-keys
         (keep (fn [k]
                 (let [ov (get old-value k)
                       nv (get new-value k)]
                   (when (not= ov nv)
                     {:path k :old ov :new nv}))))
         vec)))

;; ---------------------------------------------------------------------------
;; validate-config
;; ---------------------------------------------------------------------------

(defn- validate-worker-settings
  [value]
  (let [errors (transient [])]
    (when-not (and (some? (:max_workers value))
                   (pos? (long (:max_workers value))))
      (conj! errors "max_workers must be > 0"))
    (when-not (and (some? (:lease_duration value))
                   (>= (long (:lease_duration value)) 30))
      (conj! errors "lease_duration must be >= 30"))
    (when (and (some? (:heartbeat_interval value))
               (some? (:lease_duration value))
               (>= (long (:heartbeat_interval value)) (long (:lease_duration value))))
      (conj! errors "heartbeat_interval must be < lease_duration"))
    (persistent! errors)))

(defn- validate-queue-settings
  [value]
  (let [errors (transient [])]
    (when-not (and (number? (:max_depth value))
                   (pos? (:max_depth value)))
      (conj! errors "max_depth must be > 0"))
    (when-not (and (number? (:retry_limit value))
                   (>= (:retry_limit value) 0)
                   (<= (:retry_limit value) 10))
      (conj! errors "retry_limit must be between 0 and 10"))
    (persistent! errors)))

(defn- validate-alert-thresholds
  [value]
  (let [errors (transient [])]
    (doseq [[k min-val] [[:kafka_lag_warning 0]
                         [:kafka_lag_critical 1]
                         [:freshness_warning 0]
                         [:freshness_critical 1]
                         [:bad_record_pct 0]
                         [:heartbeat_timeout 1]]]
      (when (and (some? (get value k))
                 (< (long (get value k)) min-val))
        (conj! errors (str (name k) " must be >= " min-val))))
    (when (and (some? (:bad_record_pct value))
               (> (double (:bad_record_pct value)) 100.0))
      (conj! errors "bad_record_pct must be <= 100"))
    (persistent! errors)))

(defn- validate-retention-policy
  [value]
  (let [errors (transient [])]
    (doseq [k [:manifest_retention :bad_record_retention :checkpoint_history :dlq_retention]]
      (when (and (some? (get value k))
                 (< (long (get value k)) 1))
        (conj! errors (str (name k) " must be >= 1"))))
    (when (and (contains? value :archive_destination)
               (not (string? (:archive_destination value))))
      (conj! errors "archive_destination must be a string"))
    (persistent! errors)))

(defn- validate-source-concurrency
  [value]
  (let [errors (transient [])
        sources (or (:sources value) [])]
    (when-not (sequential? sources)
      (conj! errors "sources must be a collection"))
    (doseq [source sources]
      (when-not (seq (:source_key source))
        (conj! errors "each source must have source_key"))
      (when-not (and (some? (:max_concurrent source))
                     (pos? (long (:max_concurrent source))))
        (conj! errors "max_concurrent must be > 0 for every source")))
    (persistent! errors)))

(defn normalize-config-key
  [config-key]
  (case config-key
    "retention_policies" "retention_policy"
    config-key))

(defn validate-config
  "Validate a config value map for a given config-key.
   Returns a vector of error strings; empty vector means valid."
  [config-key value]
  (case (normalize-config-key config-key)
    "worker_settings"  (validate-worker-settings value)
    "queue_settings"   (validate-queue-settings value)
    "alert_thresholds" (validate-alert-thresholds value)
    "retention_policy" (validate-retention-policy value)
    "source_concurrency" (validate-source-concurrency value)
    []))

;; ---------------------------------------------------------------------------
;; preview-config-change
;; ---------------------------------------------------------------------------

(defn preview-config-change
  "Preview a proposed config change without persisting.  Returns a map with:
     :current   - current config_value (or nil)
     :proposed  - the new value
     :diff      - vector of {:path :old :new}
     :valid     - boolean
     :errors    - vector of validation error strings
     :version   - current version (or 0 if new)"
  [{:keys [config-key workspace-key new-value]}]
  (ensure-admin-tables!)
  (let [current-row (get-config {:config-key config-key :workspace-key workspace-key})
        current-val (when current-row (:config_value current-row))
        current-ver (if current-row (long (:version current-row)) 0)
        diff        (config-diff current-val new-value)
        errors      (validate-config config-key new-value)]
    {:current  current-val
     :proposed new-value
     :diff     diff
     :valid    (empty? errors)
     :errors   errors
     :version  current-ver}))

;; ---------------------------------------------------------------------------
;; apply-config-change!
;; ---------------------------------------------------------------------------

(defn apply-config-change!
  "Apply a config change transactionally with optimistic concurrency control.
   Steps:
     1. Lock the current active row with FOR UPDATE
     2. Verify version matches expected-version (prevents lost updates)
     3. Re-validate the new value
     4. Supersede the old row
     5. Insert the new row with version+1
     6. Record an audit event with the diff

   Returns the newly inserted config row, or throws on conflict/validation."
  [{:keys [config-key workspace-key new-value expected-version operator]}]
  (ensure-admin-tables!)
  (jdbc/with-transaction [tx db/ds]
    (let [opts        (db-opts tx)
          ;; Lock the current active row
          current-row (jdbc/execute-one!
                       opts
                       [(str "SELECT * FROM " config-table
                             " WHERE config_key = ?"
                             " AND workspace_key IS NOT DISTINCT FROM ?"
                             " AND NOT superseded"
                             " FOR UPDATE")
                        config-key
                        workspace-key])
          current-ver (if current-row (long (:version current-row)) 0)
          current-val (when current-row
                        (json/parse-string (:config_value current-row) true))]

      ;; Optimistic concurrency check
      (when (and (some? expected-version)
                 (not= (long expected-version) current-ver))
        (throw (ex-info "Config version conflict"
                        {:config_key       config-key
                         :workspace_key    workspace-key
                         :expected_version expected-version
                         :actual_version   current-ver
                         :status           409})))

      ;; Re-validate
      (let [errors (validate-config config-key new-value)]
        (when (seq errors)
          (throw (ex-info "Config validation failed"
                          {:config_key config-key
                           :errors     errors
                           :status     400}))))

      ;; Supersede old row
      (when current-row
        (jdbc/execute!
         tx
         [(str "UPDATE " config-table
               " SET superseded = true"
               " WHERE config_id = ?")
          (:config_id current-row)]))

      ;; Insert new version
      (let [new-ver    (inc current-ver)
            value-json (json/generate-string new-value)
            new-row    (jdbc/execute-one!
                        opts
                        [(str "INSERT INTO " config-table
                              " (config_key, config_value, workspace_key, version, updated_by, updated_at_utc, superseded)"
                              " VALUES (?, ?::jsonb, ?, ?, ?, now(), false)"
                              " RETURNING *")
                         config-key
                         value-json
                         workspace-key
                         new-ver
                         (or operator "system")])]

        ;; Audit
        (let [diff (config-diff current-val new-value)]
          (try
            (control-plane/record-audit-event!
             {:event_type "ops.config.applied"
              :actor      (or operator "system")
              :details    {:config_key    config-key
                           :workspace_key workspace-key
                           :old_version   current-ver
                           :new_version   new-ver
                           :diff          diff}})
            (catch Exception e
              (log/warn e "Failed to record audit event for config change"
                        {:config_key config-key}))))

        (assoc new-row :config_value (json/parse-string (:config_value new-row) true))))))

;; ---------------------------------------------------------------------------
;; rollback-config!
;; ---------------------------------------------------------------------------

(defn rollback-config!
  "Rollback a config to a previous version by applying the target version's
   value as a new version.  Internally delegates to apply-config-change! so
   the same validation, versioning, and audit trail apply."
  [{:keys [config-key workspace-key target-version operator]}]
  (ensure-admin-tables!)
  (let [target-row (jdbc/execute-one!
                    (db-opts db/ds)
                    [(str "SELECT * FROM " config-table
                          " WHERE config_key = ?"
                          " AND workspace_key IS NOT DISTINCT FROM ?"
                          " AND version = ?")
                     config-key
                     workspace-key
                     (long target-version)])]
    (when-not target-row
      (throw (ex-info "Target config version not found"
                      {:config_key     config-key
                       :workspace_key  workspace-key
                       :target_version target-version
                       :status         404})))

    (let [restored-value (json/parse-string (:config_value target-row) true)
          ;; Get current version for optimistic lock
          current-row    (get-config {:config-key config-key :workspace-key workspace-key})
          current-ver    (if current-row (long (:version current-row)) 0)]

      (let [result (apply-config-change!
                    {:config-key       config-key
                     :workspace-key    workspace-key
                     :new-value        restored-value
                     :expected-version current-ver
                     :operator         (or operator "system")})]

        ;; Additional audit for the rollback action
        (try
          (control-plane/record-audit-event!
           {:event_type "ops.config.rollback"
            :actor      (or operator "system")
            :details    {:config_key     config-key
                         :workspace_key  workspace-key
                         :target_version target-version
                         :new_version    (:version result)}})
          (catch Exception e
            (log/warn e "Failed to record rollback audit event"
                      {:config_key config-key})))

        result))))
