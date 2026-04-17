(ns bitool.ops.schema-drift
  "Schema Drift Detection, Alerting, and Human Review.

   Lifecycle: DETECT → ALERT → REVIEW → APPLY → AUDIT

   Tables managed:
     - schema_drift_event     (one row per detected drift)
     - schema_notification    (in-app notification feed)
     - schema_ddl_history     (audit trail of ALTER TABLE executions)"
  (:require [bitool.config :refer [env]]
            [bitool.db :as db]
            [bitool.ingest.runtime :as ingest-runtime]
            [bitool.ops.alerts :as ops-alerts]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import [java.security MessageDigest]
           [java.util HexFormat]))

(declare create-drift-notification! fire-webhook-alert! get-drift-event generate-schema-ddl)

;; ─── Helpers ──────────────────────────────────────────────────────────

(defn- db-opts [conn]
  (jdbc/with-options conn {:builder-fn rs/as-unqualified-lower-maps}))

(defn- sha256-hex [value]
  (let [digest (.digest (doto (MessageDigest/getInstance "SHA-256")
                          (.update (.getBytes (str value) "UTF-8"))))]
    (.formatHex (HexFormat/of) digest)))

(defn- now-utc [] (java.time.Instant/now))

(defn- drift-alert-title
  [endpoint-name severity]
  (case severity
    "breaking" (format "Breaking schema drift on %s" endpoint-name)
    "warning" (format "Schema drift warning on %s" endpoint-name)
    (format "Schema drift detected on %s" endpoint-name)))

(defn- fire-drift-alert!
  [{:keys [workspace-key graph-id api-node-id endpoint-name source-system run-id
           schema-hash-before schema-hash-after drift severity event-id]}]
  (try
    (ops-alerts/fire!
     {:alert-type "schema_drift"
      :severity severity
      :workspace-key workspace-key
      :source-key (format "schema_drift:%s:%s:%s:%s:%s"
                          (or workspace-key "")
                          (or graph-id "")
                          (or api-node-id "")
                          (or endpoint-name "")
                          (or event-id ""))
      :title (drift-alert-title endpoint-name severity)
      :detail {:event_id event-id
               :graph_id graph-id
               :api_node_id api-node-id
               :endpoint_name endpoint-name
               :source_system source-system
               :run_id run-id
               :schema_hash_before schema-hash-before
               :schema_hash_after schema-hash-after
               :new_field_count (count (:new_fields drift))
               :missing_field_count (count (:missing_fields drift))
               :type_change_count (count (:type_changes drift))
               :drift_severity severity}})
    (catch Exception e
      (log/warn e "Failed to fire schema drift ops alert"
                {:workspace_key workspace-key
                 :graph_id graph-id
                 :api_node_id api-node-id
                 :endpoint_name endpoint-name
                 :event_id event-id}))))

(defn- parse-json-safe [value]
  (cond
    (nil? value)    nil
    (map? value)    value
    (vector? value) value
    (string? value) (try (json/parse-string value true) (catch Exception _ nil))
    :else           nil))

;; ─── Table DDL ────────────────────────────────────────────────────────

(def ^:private drift-event-table "schema_drift_event")
(def ^:private notification-table "schema_notification")
(def ^:private ddl-history-table "schema_ddl_history")

(def ^:private drift-event-ddl
  "CREATE TABLE IF NOT EXISTS schema_drift_event (
     event_id            BIGSERIAL PRIMARY KEY,
     workspace_key       VARCHAR(128) NOT NULL DEFAULT '',
     graph_id            INTEGER NOT NULL,
     api_node_id         INTEGER NOT NULL,
     endpoint_name       VARCHAR(512) NOT NULL,
     source_system       VARCHAR(128) NOT NULL,
     run_id              VARCHAR(64),
     snapshot_id         BIGINT,
     new_field_count     INTEGER NOT NULL DEFAULT 0,
     missing_field_count INTEGER NOT NULL DEFAULT 0,
     type_change_count   INTEGER NOT NULL DEFAULT 0,
     drift_json          TEXT NOT NULL,
     drift_severity      VARCHAR(16) NOT NULL DEFAULT 'info',
     acknowledged        BOOLEAN NOT NULL DEFAULT FALSE,
     acknowledged_by     VARCHAR(128),
     acknowledged_at     TIMESTAMP,
     schema_hash_before  VARCHAR(64),
     schema_hash_after   VARCHAR(64),
     enforcement_mode    VARCHAR(32),
     detected_at_utc     TIMESTAMP NOT NULL DEFAULT now()
   )")

(def ^:private drift-event-indexes
  ["CREATE INDEX IF NOT EXISTS idx_drift_event_endpoint
      ON schema_drift_event (workspace_key, graph_id, api_node_id, endpoint_name, detected_at_utc DESC)"
   "CREATE INDEX IF NOT EXISTS idx_drift_event_unacked
      ON schema_drift_event (workspace_key, acknowledged, detected_at_utc DESC)
      WHERE acknowledged = FALSE"])

(def ^:private notification-ddl
  "CREATE TABLE IF NOT EXISTS schema_notification (
     notification_id   BIGSERIAL PRIMARY KEY,
     workspace_key     VARCHAR(128) NOT NULL DEFAULT '',
     event_id          BIGINT NOT NULL,
     channel           VARCHAR(32) NOT NULL DEFAULT 'in_app',
     severity          VARCHAR(16) NOT NULL,
     title             VARCHAR(256) NOT NULL,
     body              TEXT,
     read              BOOLEAN NOT NULL DEFAULT FALSE,
     created_at_utc    TIMESTAMP NOT NULL DEFAULT now()
   )")

(def ^:private notification-indexes
  ["CREATE INDEX IF NOT EXISTS idx_notification_ws_unread
      ON schema_notification (workspace_key, created_at_utc DESC)
      WHERE read = FALSE"])

(def ^:private ddl-history-ddl
  "CREATE TABLE IF NOT EXISTS schema_ddl_history (
     ddl_id            BIGSERIAL PRIMARY KEY,
     workspace_key     VARCHAR(128) NOT NULL DEFAULT '',
     event_id          BIGINT,
     approval_schema_hash VARCHAR(64),
     graph_id          INTEGER NOT NULL,
     endpoint_name     VARCHAR(512) NOT NULL,
     table_name        VARCHAR(512) NOT NULL,
     action            VARCHAR(32) NOT NULL,
     column_name       VARCHAR(256) NOT NULL,
     from_type         VARCHAR(64),
     to_type           VARCHAR(64),
     sql_executed      TEXT NOT NULL,
     status            VARCHAR(16) NOT NULL,
     error_message     TEXT,
     applied_by        VARCHAR(128),
     applied_at_utc    TIMESTAMP NOT NULL DEFAULT now()
   )")

(def ^:private ddl-history-indexes
  ["CREATE INDEX IF NOT EXISTS idx_ddl_history_ws
      ON schema_ddl_history (workspace_key, graph_id, endpoint_name, applied_at_utc DESC)"])

(defonce ^:private tables-ready? (atom false))

(defn ensure-schema-drift-tables!
  "Create schema_drift_event, schema_notification, and schema_ddl_history tables if they don't exist."
  []
  (when-not @tables-ready?
    (locking tables-ready?
      (when-not @tables-ready?
        (try
          (let [ds (db-opts db/ds)]
            (jdbc/execute! ds [drift-event-ddl])
            (jdbc/execute! ds ["ALTER TABLE schema_drift_event ADD COLUMN IF NOT EXISTS workspace_key VARCHAR(128) NOT NULL DEFAULT ''"])
            (doseq [idx drift-event-indexes]
              (jdbc/execute! ds [idx]))
            (jdbc/execute! ds [notification-ddl])
            (jdbc/execute! ds ["ALTER TABLE schema_notification ADD COLUMN IF NOT EXISTS workspace_key VARCHAR(128) NOT NULL DEFAULT ''"])
            (doseq [idx notification-indexes]
              (jdbc/execute! ds [idx]))
            (jdbc/execute! ds [ddl-history-ddl])
            (jdbc/execute! ds ["ALTER TABLE schema_ddl_history ADD COLUMN IF NOT EXISTS workspace_key VARCHAR(128) NOT NULL DEFAULT ''"])
            (jdbc/execute! ds ["ALTER TABLE schema_ddl_history ADD COLUMN IF NOT EXISTS approval_schema_hash VARCHAR(64)"])
            (doseq [idx ddl-history-indexes]
              (jdbc/execute! ds [idx]))
            (reset! tables-ready? true))
          (catch Exception e
            (log/warn e "Failed to create schema drift tables")))))))

;; ─── field_decisions column on endpoint_schema_approval ──────────────

;; ─── Severity Classification ──────────────────────────────────────────

(def ^:private widening-targets
  {"BOOLEAN"   #{"BOOLEAN" "STRING"}
   "INT"       #{"INT" "BIGINT" "DOUBLE" "STRING"}
   "BIGINT"    #{"BIGINT" "DOUBLE" "STRING"}
   "DOUBLE"    #{"DOUBLE" "STRING"}
   "DATE"      #{"DATE" "TIMESTAMP" "STRING"}
   "TIMESTAMP" #{"TIMESTAMP" "STRING"}
   "STRING"    #{"STRING"}})

(defn- widening-type-change?
  [current-type inferred-type]
  (contains? (get widening-targets (or current-type "STRING") #{(or current-type "STRING")})
             (or inferred-type current-type "STRING")))

(defn- incompatible-type-changes
  [type-changes]
  (seq (remove (fn [{:keys [current_type inferred_type]}]
                 (widening-type-change? current_type inferred_type))
               type-changes)))

(defn classify-drift-severity
  "Classify drift as info, warning, or breaking.
   When missing fields carry :is_required true (from spec-derived schemas),
   missing a required field escalates to breaking."
  [{:keys [new_fields missing_fields type_changes]}]
  (let [missing-required (seq (filter :is_required missing_fields))]
    (cond
      (incompatible-type-changes type_changes) "breaking"
      missing-required "breaking"
      (or (seq missing_fields) (seq type_changes)) "warning"
      (seq new_fields) "info"
      :else nil)))

(def ^:private severity-rank {"info" 0 "warning" 1 "breaking" 2})

(defn- non-blank-str [value]
  (let [value (some-> value str string/trim)]
    (when (seq value) value)))

(defn- normalize-type-name
  [warehouse raw]
  (let [v (some-> raw str string/trim string/upper-case)]
    (cond
      (contains? #{"BOOL" "BOOLEAN"} v) "BOOLEAN"
      (contains? #{"INT" "INTEGER" "INT4" "SMALLINT" "SERIAL"} v) "INT"
      (contains? #{"BIGINT" "INT8" "BIGSERIAL"} v) "BIGINT"
      (contains? #{"DOUBLE" "DOUBLE PRECISION" "FLOAT" "FLOAT8" "REAL" "NUMERIC" "DECIMAL"} v) "DOUBLE"
      (contains? #{"DATE"} v) "DATE"
      (or (string/starts-with? v "TIMESTAMP")
          (contains? #{"DATETIME"} v)) "TIMESTAMP"
      (or (contains? #{"STRING" "TEXT" "VARCHAR" "CHARACTER VARYING" "CHAR" "JSON" "JSONB" "VARIANT"} v)
          (= warehouse "snowflake")) "STRING"
      :else "STRING")))

(defn- canonical-warehouse
  [warehouse]
  (case (some-> warehouse str string/trim string/lower-case)
    "postgresql" "postgres"
    "postgres" "postgres"
    "snowflake" "snowflake"
    "databricks" "databricks"
    "postgres"))

(defn- parse-qualified-table-name
  [table-name]
  (let [parts (->> (string/split (str table-name) #"\.")
                   (remove string/blank?)
                   vec)]
    (case (count parts)
      3 {:catalog (nth parts 0) :schema (nth parts 1) :table (nth parts 2)}
      2 {:schema (nth parts 0) :table (nth parts 1)}
      1 {:table (nth parts 0)}
      {:table (last parts)})))

(defn- fetch-table-columns
  [conn-id warehouse table-name]
  (let [db-spec (db/create-dbspec-from-id conn-id)
        {:keys [catalog schema table]} (parse-qualified-table-name table-name)
        db-name (or catalog (:dbname db-spec))
        schema-name (or schema (:schema db-spec) "public")]
    (->> (db/get-columns conn-id db-name schema-name table)
         (map (fn [col]
                (let [column-name (or (:column_name col) (:name col))
                      raw-type (or (:data_type col) (:type col))]
                  {:column_name column-name
                   :type (normalize-type-name warehouse raw-type)
                   :raw_type raw-type
                   :is_nullable (or (:is_nullable col) (:empty col))})))
         vec)))

(defn- normalize-field-decisions
  [field-decisions]
  (let [field-decisions (parse-json-safe field-decisions)]
    (cond
      (map? field-decisions)
      (reduce-kv
       (fn [acc k v]
         (assoc acc (str k)
                (cond
                  (map? v) v
                  (string? v) {:decision (string/lower-case (string/trim v))}
                  (boolean? v) {:enabled v}
                  :else {:raw v})))
       {}
       field-decisions)

      (vector? field-decisions)
      (reduce
       (fn [acc v]
         (let [column-name (some-> (or (:column_name v) (:path v) (:field v)) str)]
           (if column-name
             (assoc acc column-name v)
             acc)))
       {}
       field-decisions)

      :else
      {})))

(defn- field-enabled?
  [decision]
  (let [decision-str (some-> (:decision decision) str string/trim string/lower-case)]
    (cond
      (contains? decision :enabled) (not= false (:enabled decision))
      (contains? #{"exclude" "drop" "reject"} #{decision-str}) false
      :else true)))

(defn- apply-field-decisions
  [fields field-decisions]
  (let [decision-by-column (normalize-field-decisions field-decisions)]
    (->> fields
         (map (fn [field]
                (let [column-name (str (:column_name field))
                      decision (get decision-by-column column-name)]
                  (assoc field
                         :enabled (if decision
                                    (field-enabled? decision)
                                    (if (contains? field :enabled) (:enabled field) true))
                         :decision decision))))
         vec)))

(defn- latest-approval-for-event
  [{:keys [graph_id api_node_id endpoint_name schema_hash_after]}]
  (or (ingest-runtime/resolve-api-schema-approval
       graph_id
       api_node_id
       {:endpoint-name endpoint_name
        :schema-hash (non-blank-str schema_hash_after)
        :promoted-only true})
      (ingest-runtime/resolve-api-schema-approval
       graph_id
       api_node_id
       {:endpoint-name endpoint_name
        :promoted-only true})))

(defn- resolve-preview-context!
  [{:keys [workspace-key graph-id api-node-id endpoint-name event-id schema-hash]}]
  (let [event (when event-id
                (or (get-drift-event event-id workspace-key)
                    (throw (ex-info "Drift event not found"
                                    {:status 404
                                     :error "not_found"
                                     :event_id event-id
                                     :workspace_key workspace-key}))))
        graph-id' (or graph-id (:graph_id event))
        api-node-id' (or api-node-id (:api_node_id event))
        endpoint-name' (or endpoint-name (:endpoint_name event))
        _ (when-not (and graph-id' api-node-id' endpoint-name')
            (throw (ex-info "graph_id, api_node_id, and endpoint_name are required"
                            {:status 400
                             :error "bad_request"})))
        target-context (ingest-runtime/schema-drift-target-context graph-id' api-node-id' endpoint-name')
        approval (or (ingest-runtime/resolve-api-schema-approval
                      graph-id'
                      api-node-id'
                      {:endpoint-name endpoint-name'
                       :schema-hash (or (non-blank-str schema-hash)
                                        (some-> event :schema_hash_after non-blank-str))
                       :promoted-only true})
                     (latest-approval-for-event event))]
    (when-not approval
      (throw (ex-info "No promoted schema approval found for this endpoint"
                      {:status 409
                       :error "approval_required"
                       :graph_id graph-id'
                       :api_node_id api-node-id'
                       :endpoint_name endpoint-name'
                       :schema_hash (or (non-blank-str schema-hash)
                                        (:schema_hash_after event))})))
    (let [approved-fields (-> (:inferred_fields approval)
                              (apply-field-decisions (:field_decisions approval)))
          warehouse (canonical-warehouse (:warehouse target-context))
          current-fields (fetch-table-columns (:conn_id target-context)
                                             warehouse
                                             (:target_table target-context))]
      {:event event
       :approval approval
       :target (assoc target-context :warehouse warehouse)
       :current_fields current-fields
       :approved_fields approved-fields
       :ddl_plan (generate-schema-ddl warehouse
                                      (:target_table target-context)
                                      current-fields
                                      approved-fields)})))

;; ─── Event Persistence (with dedup) ──────────────────────────────────

(defn persist-schema-drift-event!
  "Persist a schema drift event. Deduplicates by hash pair + unacknowledged.
   Returns the event-id if created, nil if deduped."
  [{:keys [workspace-key graph-id api-node-id endpoint-name source-system
           run-id drift enforcement-mode schema-hash-before schema-hash-after]}]
  (ensure-schema-drift-tables!)
  (let [ds       (db-opts db/ds)
        ws       (or workspace-key "")
        existing (jdbc/execute-one!
                  ds
                  ["SELECT event_id FROM schema_drift_event
                    WHERE workspace_key = ? AND graph_id = ? AND api_node_id = ? AND endpoint_name = ?
                      AND schema_hash_before = ? AND schema_hash_after = ?
                      AND acknowledged = FALSE
                    LIMIT 1"
                   ws graph-id api-node-id endpoint-name
                   (or schema-hash-before "") (or schema-hash-after "")])]
    (when-not existing
      (let [severity   (or (classify-drift-severity drift) "info")
            drift-json (if (string? drift) drift (json/generate-string drift))
            result     (jdbc/execute-one!
                        ds
                        ["INSERT INTO schema_drift_event
                          (workspace_key, graph_id, api_node_id, endpoint_name, source_system,
                           run_id, new_field_count, missing_field_count, type_change_count,
                           drift_json, drift_severity, schema_hash_before, schema_hash_after,
                           enforcement_mode)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                          RETURNING event_id"
                         ws graph-id api-node-id endpoint-name (or source-system "")
                         run-id
                         (count (:new_fields drift))
                         (count (:missing_fields drift))
                         (count (:type_changes drift))
                         drift-json severity
                         (or schema-hash-before "") (or schema-hash-after "")
                         (or enforcement-mode "")])]
        (when-let [event-id (:event_id result)]
          ;; Create ops alert
          (fire-drift-alert! {:workspace-key ws
                              :graph-id graph-id
                              :api-node-id api-node-id
                              :endpoint-name endpoint-name
                              :source-system source-system
                              :run-id run-id
                              :schema-hash-before (or schema-hash-before "")
                              :schema-hash-after (or schema-hash-after "")
                              :drift drift
                              :severity severity
                              :event-id event-id})
          ;; Create in-app notification
          (create-drift-notification! ds ws event-id
                                     {:endpoint-name endpoint-name
                                      :severity severity
                                      :new-count (count (:new_fields drift))
                                      :missing-count (count (:missing_fields drift))
                                      :type-change-count (count (:type_changes drift))})
          ;; Fire optional webhook
          (fire-webhook-alert! {:workspace_key ws
                                :event_id event-id
                                :drift_severity severity
                                :endpoint_name endpoint-name
                                :graph_id graph-id
                                :new_field_count (count (:new_fields drift))
                                :missing_field_count (count (:missing_fields drift))
                                :type_change_count (count (:type_changes drift))
                                :detected_at_utc (str (now-utc))})
          event-id)))))

;; ─── Notification Creation ────────────────────────────────────────────

(defn- create-drift-notification!
  [ds ws event-id {:keys [endpoint-name severity new-count missing-count type-change-count]}]
  (let [title (case severity
                "breaking" (format "Breaking schema change on %s" endpoint-name)
                "warning"  (format "Schema warning on %s" endpoint-name)
                (format "New fields detected on %s" endpoint-name))
        body  (str (when (pos? (or new-count 0)) (format "%d new field(s). " new-count))
                   (when (pos? (or missing-count 0)) (format "%d missing field(s). " missing-count))
                   (when (pos? (or type-change-count 0)) (format "%d type change(s)." type-change-count)))]
    (try
      (jdbc/execute!
       ds
       ["INSERT INTO schema_notification (workspace_key, event_id, channel, severity, title, body)
         VALUES (?, ?, 'in_app', ?, ?, ?)"
        ws event-id severity title body])
      (catch Exception e
        (log/warn e "Failed to create drift notification")))))

;; ─── Webhook Alert ────────────────────────────────────────────────────

(defn- fire-webhook-alert!
  [event]
  (when-let [url (some-> (get env :schema-drift-webhook-url) str string/trim not-empty)]
    (let [min-severity (or (get env :schema-drift-webhook-severity) "warning")]
      (when (>= (get severity-rank (:drift_severity event) 0)
                (get severity-rank min-severity 1))
        (future
          (try
            (let [http-post (requiring-resolve 'clj-http.client/post)]
              (http-post url
                {:content-type :json
                 :body (json/generate-string
                         {:event_type      "schema_drift"
                          :severity        (:drift_severity event)
                          :endpoint_name   (:endpoint_name event)
                          :graph_id        (:graph_id event)
                          :new_fields      (:new_field_count event)
                          :missing_fields  (:missing_field_count event)
                          :type_changes    (:type_change_count event)
                          :detected_at     (:detected_at_utc event)})
                 :socket-timeout 5000
                 :connection-timeout 5000}))
            (catch Exception e
              (log/warn e "Failed to send schema drift webhook"))))))))

;; ─── Query Functions ──────────────────────────────────────────────────

(defn- safe-query
  ([sql-vec] (safe-query sql-vec []))
  ([sql-vec fallback]
   (try
     (jdbc/execute! (db-opts db/ds) sql-vec)
     (catch org.postgresql.util.PSQLException e
       (if (= "42P01" (.getSQLState e))
         fallback
         (throw e)))
     (catch Exception e
       (if (and (.getMessage e)
                (string/includes? (.getMessage e) "does not exist"))
         fallback
         (throw e))))))

(defn list-drift-events
  "List schema drift events with optional filters."
  [{:keys [workspace-key graph-id endpoint-name severity acknowledged limit offset]}]
  (ensure-schema-drift-tables!)
  (let [ws        (or workspace-key "")
        limit     (min 200 (max 1 (or limit 100)))
        offset    (max 0 (or offset 0))
        clauses   (cond-> ["workspace_key = CASE WHEN ? = '' THEN workspace_key ELSE ? END"]
                    graph-id      (conj "graph_id = ?")
                    endpoint-name (conj "endpoint_name = ?")
                    severity      (conj "drift_severity = ?")
                    (some? acknowledged) (conj (str "acknowledged = " (if acknowledged "TRUE" "FALSE"))))
        where     (str "WHERE " (string/join " AND " clauses))
        params    (cond-> [ws ws]
                    graph-id      (conj graph-id)
                    endpoint-name (conj endpoint-name)
                    severity      (conj severity))
        sql       (str "SELECT event_id, workspace_key, graph_id, api_node_id, endpoint_name,
                               source_system, run_id, new_field_count, missing_field_count,
                               type_change_count, drift_severity, acknowledged, acknowledged_by,
                               acknowledged_at, schema_hash_before, schema_hash_after,
                               enforcement_mode, detected_at_utc
                        FROM schema_drift_event " where
                       " ORDER BY detected_at_utc DESC LIMIT ? OFFSET ?")]
    (safe-query (into [sql] (concat params [limit offset])))))

(defn get-drift-event
  "Get a single drift event by id, including full drift_json."
  [event-id workspace-key]
  (ensure-schema-drift-tables!)
  (jdbc/execute-one!
   (db-opts db/ds)
   ["SELECT * FROM schema_drift_event
     WHERE event_id = ?
       AND workspace_key = CASE WHEN ? = '' THEN workspace_key ELSE ? END"
    event-id (or workspace-key "") (or workspace-key "")]))

(defn acknowledge-drift-event!
  "Mark a drift event as acknowledged."
  [event-id workspace-key actor]
  (ensure-schema-drift-tables!)
  (let [result (jdbc/execute-one!
                (db-opts db/ds)
                ["UPDATE schema_drift_event
                  SET acknowledged = TRUE, acknowledged_by = ?, acknowledged_at = now()
                  WHERE event_id = ?
                    AND workspace_key = CASE WHEN ? = '' THEN workspace_key ELSE ? END
                    AND acknowledged = FALSE
                  RETURNING event_id"
                 (or actor "operator") event-id (or workspace-key "") (or workspace-key "")])]
    (if result
      {:acknowledged true :event_id event-id}
      {:acknowledged false :event_id event-id :reason "already_acknowledged_or_not_found"})))

(defn list-notifications
  "List schema notifications."
  [{:keys [workspace-key unread-only severity limit]}]
  (ensure-schema-drift-tables!)
  (let [ws    (or workspace-key "")
        limit (min 200 (max 1 (or limit 50)))
        clauses (cond-> ["n.workspace_key = CASE WHEN ? = '' THEN n.workspace_key ELSE ? END"]
                  unread-only (conj "n.read = FALSE")
                  severity    (conj "n.severity = ?"))
        where   (str "WHERE " (string/join " AND " clauses))
        params  (cond-> [ws ws]
                  severity (conj severity))]
    (safe-query
     (into [(str "SELECT n.notification_id, n.workspace_key, n.event_id, n.channel,
                         n.severity, n.title, n.body, n.read, n.created_at_utc,
                         e.endpoint_name, e.drift_severity, e.graph_id, e.api_node_id
                  FROM schema_notification n
                  JOIN schema_drift_event e ON e.event_id = n.event_id "
                 where
                 " ORDER BY n.created_at_utc DESC LIMIT ?")]
           (concat params [limit])))))

(defn mark-notifications-read!
  "Mark notifications as read by IDs."
  [workspace-key notification-ids]
  (ensure-schema-drift-tables!)
  (when (seq notification-ids)
    (let [placeholders (string/join ", " (repeat (count notification-ids) "?"))]
      (jdbc/execute!
       (db-opts db/ds)
       (into [(str "UPDATE schema_notification SET read = TRUE
                    WHERE workspace_key = CASE WHEN ? = '' THEN workspace_key ELSE ? END
                      AND notification_id IN (" placeholders ")")
              (or workspace-key "")
              (or workspace-key "")]
             notification-ids))
      {:marked_read (count notification-ids)})))

(defn unread-notification-count
  "Count unread notifications for a workspace."
  [{:keys [workspace-key]}]
  (ensure-schema-drift-tables!)
  (let [ws (or workspace-key "")
        result (jdbc/execute-one!
                (db-opts db/ds)
                ["SELECT COUNT(*) AS cnt FROM schema_notification
                  WHERE read = FALSE
                    AND workspace_key = CASE WHEN ? = '' THEN workspace_key ELSE ? END"
                 ws ws])]
    {:unread_count (or (:cnt result) 0)}))

;; ─── Schema Timeline ──────────────────────────────────────────────────

(defn schema-timeline
  "Consolidated schema history for an endpoint."
  [{:keys [workspace-key graph-id endpoint-name limit]}]
  (ensure-schema-drift-tables!)
  (let [ws    (or workspace-key "")
        limit (min 200 (max 1 (or limit 50)))
        ;; Drift events
        events (safe-query
                (into ["SELECT 'drift_detected' AS type, detected_at_utc AS time,
                               event_id, drift_severity AS severity,
                               new_field_count, missing_field_count, type_change_count,
                               acknowledged, acknowledged_by, enforcement_mode
                        FROM schema_drift_event
                        WHERE graph_id = ? AND endpoint_name = ?
                          AND workspace_key = CASE WHEN ? = '' THEN workspace_key ELSE ? END
                        ORDER BY detected_at_utc DESC LIMIT ?"]
                      [graph-id endpoint-name ws ws limit]))
        ;; DDL history
        ddl-rows (safe-query
                  (into ["SELECT 'ddl_applied' AS type, applied_at_utc AS time,
                                 action, column_name, from_type, to_type,
                                 status, applied_by, sql_executed
                          FROM schema_ddl_history
                          WHERE graph_id = ? AND endpoint_name = ?
                            AND workspace_key = CASE WHEN ? = '' THEN workspace_key ELSE ? END
                          ORDER BY applied_at_utc DESC LIMIT ?"]
                        [graph-id endpoint-name ws ws limit]))
        ;; Merge and sort by time
        timeline (->> (concat events ddl-rows)
                      (sort-by #(str (or (:time %) "")))
                      reverse
                      (take limit)
                      vec)]
    {:graph_id graph-id
     :endpoint_name endpoint-name
     :timeline timeline}))

;; ─── DDL Generation ───────────────────────────────────────────────────

(defn- quote-ident [name]
  (str "\"" (string/replace (str name) "\"" "\"\"") "\""))

(def ^:private sql-type-map
  {"BOOLEAN"   {"postgres" "BOOLEAN"   "snowflake" "BOOLEAN"   "databricks" "BOOLEAN"}
   "INT"       {"postgres" "INTEGER"   "snowflake" "INTEGER"   "databricks" "INT"}
   "BIGINT"    {"postgres" "BIGINT"    "snowflake" "BIGINT"    "databricks" "BIGINT"}
   "DOUBLE"    {"postgres" "DOUBLE PRECISION" "snowflake" "DOUBLE" "databricks" "DOUBLE"}
   "DATE"      {"postgres" "DATE"      "snowflake" "DATE"      "databricks" "DATE"}
   "TIMESTAMP" {"postgres" "TIMESTAMP" "snowflake" "TIMESTAMP" "databricks" "TIMESTAMP"}
   "STRING"    {"postgres" "TEXT"      "snowflake" "VARCHAR"   "databricks" "STRING"}})

(defn- sql-type-for-warehouse [warehouse type-name]
  (get-in sql-type-map [(or type-name "STRING") (or warehouse "postgres")]
          "TEXT"))

(defn generate-schema-ddl
  "Generate ALTER TABLE statements for an approved schema change.
   `approved-fields` should be filtered by field_decisions already."
  [warehouse table-name current-fields approved-fields]
  (let [current-by-name  (into {} (map (juxt :column_name identity)) current-fields)
        approved-by-name (into {} (map (juxt :column_name identity)) approved-fields)
        new-columns      (->> approved-fields
                               (filter #(not (contains? current-by-name (:column_name %))))
                               (filter #(not= false (:enabled %)))
                               vec)
        type-changes     (for [[col-name approved] approved-by-name
                               :let [current (get current-by-name col-name)]
                               :when (and current
                                          (not= (:type current) (:type approved))
                                          (widening-type-change? (:type current) (:type approved)))]
                           {:column_name col-name
                            :from_type (:type current)
                            :to_type (:type approved)})]
    {:add-columns (mapv (fn [col]
                          {:sql (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s"
                                        table-name
                                        (quote-ident (:column_name col))
                                        (sql-type-for-warehouse warehouse (:type col)))
                           :column col})
                        new-columns)
     :widen-columns (mapv (fn [{:keys [column_name from_type to_type]}]
                            {:sql (format "ALTER TABLE %s ALTER COLUMN %s TYPE %s"
                                          table-name
                                          (quote-ident column_name)
                                          (sql-type-for-warehouse warehouse to_type))
                             :column_name column_name
                             :from_type from_type
                             :to_type to_type})
                          type-changes)}))

(defn preview-schema-ddl
  "Preview DDL that would be applied. Returns the plan without executing."
  [{:keys [workspace-key graph-id api-node-id endpoint-name event-id schema-hash]}]
  (let [{:keys [event approval target current_fields approved_fields ddl_plan]}
        (resolve-preview-context!
         {:workspace-key workspace-key
          :graph-id graph-id
          :api-node-id api-node-id
          :endpoint-name endpoint-name
          :event-id event-id
          :schema-hash schema-hash})
        drift (some-> event :drift_json parse-json-safe)]
    {:preview true
     :workspace_key (:workspace_key target)
     :graph_id (:graph_id target)
     :api_node_id (:api_node_id target)
     :endpoint_name (:endpoint_name target)
     :event_id (:event_id event)
     :schema_hash (:schema_hash approval)
     :review_state (:review_state approval)
     :promoted (:promoted approval)
     :table_name (:target_table target)
     :warehouse (:warehouse target)
     :drift drift
     :current_fields current_fields
     :approved_fields approved_fields
     :ddl_plan ddl_plan
     :summary {:add_column_count (count (:add-columns ddl_plan))
               :widen_column_count (count (:widen-columns ddl_plan))}}))

(defn apply-schema-ddl!
  "Execute approved DDL changes against a target table.
   Returns a summary of applied changes."
  [conn-id table-name ddl-plan {:keys [workspace-key graph-id endpoint-name event-id schema-hash applied-by]}]
  (ensure-schema-drift-tables!)
  (let [results (atom [])
        failure (atom nil)]
    (try
      (jdbc/with-transaction [tx (db/get-opts conn-id nil)]
        (doseq [{:keys [sql column]} (:add-columns ddl-plan)]
          (try
            (jdbc/execute! tx [sql])
            (swap! results conj {:action "add_column" :column (:column_name column) :status "applied" :sql sql})
            (catch Exception e
              (let [entry {:action "add_column" :column (:column_name column)
                           :status "failed" :error (.getMessage e) :sql sql}]
                (swap! results conj entry)
                (reset! failure e)
                (throw e)))))
        (doseq [{:keys [sql column_name from_type to_type]} (:widen-columns ddl-plan)]
          (try
            (jdbc/execute! tx [sql])
            (swap! results conj {:action "widen_type" :column column_name
                                 :from from_type :to to_type :status "applied" :sql sql})
            (catch Exception e
              (let [entry {:action "widen_type" :column column_name
                           :from from_type :to to_type
                           :status "failed" :error (.getMessage e) :sql sql}]
                (swap! results conj entry)
                (reset! failure e)
                (throw e))))))
      (catch Exception e
        (when-not @failure
          (reset! failure e))))
    (doseq [r @results]
      (try
        (jdbc/execute!
         (db-opts db/ds)
         ["INSERT INTO schema_ddl_history
           (workspace_key, event_id, approval_schema_hash, graph_id, endpoint_name, table_name,
            action, column_name, from_type, to_type, sql_executed, status, error_message, applied_by)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
          (or workspace-key "") event-id schema-hash graph-id endpoint-name table-name
          (:action r) (:column r) (:from r) (:to r) (:sql r) (:status r) (:error r)
          (or applied-by "system")])
        (catch Exception e
          (log/warn e "Failed to record DDL audit entry"))))
    (when-let [failure* @failure]
      (throw failure*))
    @results))

(defn apply-drift-schema-ddl!
  [{:keys [workspace-key graph-id api-node-id endpoint-name event-id schema-hash applied-by]}]
  (let [{:keys [event approval target ddl_plan]}
        (resolve-preview-context!
         {:workspace-key workspace-key
          :graph-id graph-id
          :api-node-id api-node-id
          :endpoint-name endpoint-name
          :event-id event-id
          :schema-hash schema-hash})
        results (apply-schema-ddl!
                 (:conn_id target)
                 (:target_table target)
                 ddl_plan
                 {:workspace-key (:workspace_key target)
                  :graph-id (:graph_id target)
                  :endpoint-name (:endpoint_name target)
                  :event-id (:event_id event)
                  :schema-hash (:schema_hash approval)
                  :applied-by applied-by})]
    {:applied true
     :workspace_key (:workspace_key target)
     :graph_id (:graph_id target)
     :api_node_id (:api_node_id target)
     :endpoint_name (:endpoint_name target)
     :event_id (:event_id event)
     :schema_hash (:schema_hash approval)
     :table_name (:target_table target)
     :results results
     :summary {:applied_count (count (filter #(= "applied" (:status %)) results))
               :failed_count (count (filter #(= "failed" (:status %)) results))}}))

(defn list-ddl-history
  "List DDL history for an endpoint."
  [{:keys [workspace-key graph-id endpoint-name limit]}]
  (ensure-schema-drift-tables!)
  (let [ws    (or workspace-key "")
        limit (min 200 (max 1 (or limit 50)))]
    (safe-query
     (into ["SELECT ddl_id, workspace_key, event_id, approval_schema_hash, graph_id,
                    endpoint_name, table_name, action, column_name,
                    from_type, to_type, sql_executed, status, error_message,
                    applied_by, applied_at_utc
             FROM schema_ddl_history
             WHERE graph_id = ? AND endpoint_name = ?
               AND workspace_key = CASE WHEN ? = '' THEN workspace_key ELSE ? END
             ORDER BY applied_at_utc DESC LIMIT ?"]
           [graph-id endpoint-name ws ws limit]))))
