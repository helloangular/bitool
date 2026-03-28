(ns bitool.modeling.automation
  (:require [bitool.bigquery :as bigquery]
            [bitool.config :as config]
            [bitool.compiler.core :as compiler]
            [bitool.control-plane :as control-plane]
            [bitool.databricks.jobs :as dbx-jobs]
            [bitool.db :as db]
            [bitool.gil.compiler :as gil-compiler]
            [bitool.graph2 :as g2]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [mount.core :as mount]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import [java.security MessageDigest]
           [java.util HexFormat UUID]))

;; Modeling proposal/profile tables live in the app metadata DB (`db/ds`).
;; Bronze schema snapshots live in the target audit schema and are read through
;; the target connection so proposal generation can use the same schema history
;; that the ingestion runtime persisted alongside Bronze data.

(def ^:private schema-profile-table "schema_profile_snapshot")
(def ^:private model-proposal-table "model_proposal")
(def ^:private model-validation-table "model_validation_result")
(def ^:private model-release-table "model_release")
(def ^:private model-graph-artifact-table "model_graph_artifact")
(def ^:private compiled-model-artifact-table "compiled_model_artifact")
(def ^:private compiled-model-run-table "compiled_model_run")
(defonce ^:private modeling-ready? (atom false))

(def ^:private supported-silver-types
  #{"STRING" "INT" "BIGINT" "DOUBLE" "BOOLEAN" "DATE" "TIMESTAMP"})

(def ^:private allowed-ordering-strategies
  #{"latest_event_time_wins" "latest_sequence_wins" "event_time_then_sequence" "append_only"})

(def ^:private allowed-window-units
  #{"minutes" "hours" "days"})

(declare bigquery-db-spec
         bigquery-query-result->jdbc-result)

(def ^:private allowed-late-data-modes
  #{"merge" "append"})

(def ^:private allowed-too-late-behaviors
  #{"accept" "quarantine" "drop"})

(def ^:private sql-identifier-pattern #"^[A-Za-z_][A-Za-z0-9_]*$")
(def ^:private qualified-table-name-pattern #"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$")
(def ^:private sample-query-timeout-ms 5000)

(declare proposal-columns proposal-mappings latest-validation-for-proposal parse-json-safe source-field-type
         latest-active-release poll-gold-model-run! execute-postgresql-materialization!)

(defn- parse-int-env
  [k default-value]
  (try
    (if-some [value (get config/env k)]
      (Integer/parseInt (str value))
      default-value)
    (catch Exception _
      default-value)))

(defn- parse-bool-env
  [k default-value]
  (let [raw (get config/env k)]
    (if (nil? raw)
      default-value
      (contains? #{"1" "true" "yes" "on"}
                 (-> raw str string/trim string/lower-case)))))

(defn- db-opts
  [conn]
  (jdbc/with-options conn {:builder-fn rs/as-unqualified-lower-maps}))

(defn- now-utc []
  (java.time.Instant/now))

(defn- db-timestamptz
  [value]
  (let [instant (cond
                  (nil? value) nil
                  (instance? java.time.Instant value) value
                  (instance? java.time.OffsetDateTime value) (.toInstant ^java.time.OffsetDateTime value)
                  (instance? java.time.ZonedDateTime value) (.toInstant ^java.time.ZonedDateTime value)
                  (instance? java.sql.Timestamp value) (.toInstant ^java.sql.Timestamp value)
                  (string? value) (or (try
                                        (.toInstant (java.time.OffsetDateTime/parse value))
                                        (catch Exception _ nil))
                                      (try
                                        (java.time.Instant/parse value)
                                        (catch Exception _ nil))
                                      nil)
                  :else nil)]
    (if instant
      (java.sql.Timestamp/from instant)
      value)))

(defn- non-blank-str
  [value]
  (let [value (some-> value str string/trim)]
    (when (seq value) value)))

(defn- sha256-hex
  [value]
  (let [digest (.digest (doto (MessageDigest/getInstance "SHA-256")
                          (.update (.getBytes (str value) "UTF-8"))))]
    (.formatHex (HexFormat/of) digest)))

(defn- sanitize-ident
  [value]
  (-> (or (non-blank-str value) "model")
      string/lower-case
      (string/replace #"[^a-z0-9]+" "_")
      (string/replace #"^_+" "")
      (string/replace #"_+$" "")
      (#(if (seq %) % "model"))))

(defn- quote-sql-ident
  [value]
  (str "\""
       (-> (or value "")
           str
           (string/replace "\"" "\"\""))
       "\""))

(defn- quote-sql-ident-for-warehouse
  [warehouse value]
  (let [warehouse (some-> warehouse str string/lower-case)]
    (if (contains? #{"databricks" "bigquery"} warehouse)
      (str "`"
           (-> (or value "")
               str
               (string/replace "`" "``"))
           "`")
      (quote-sql-ident value))))

(defn- quote-qualified-postgresql-ident
  [value]
  (->> (string/split (str value) #"\.")
       (remove string/blank?)
       (map quote-sql-ident)
       (string/join ".")))

(defn- split-postgresql-target-table
  [value]
  (let [parts (->> (string/split (str value) #"\.")
                   (remove string/blank?)
                   vec)]
    {:schema (or (when (> (count parts) 1)
                   (nth parts (- (count parts) 2)))
                 "public")
     :table  (or (peek parts) (str value))}))

(defn- postgresql-logical-type
  [logical-type]
  (case (-> (or logical-type "STRING") name string/upper-case)
    "STRING" "TEXT"
    "VARCHAR" "TEXT"
    "TEXT" "TEXT"
    "BOOLEAN" "BOOLEAN"
    "BOOL" "BOOLEAN"
    "DATE" "DATE"
    "TIMESTAMP" "TIMESTAMP"
    "TIMESTAMPTZ" "TIMESTAMPTZ"
    "INT" "INTEGER"
    "INTEGER" "INTEGER"
    "BIGINT" "BIGINT"
    "LONG" "BIGINT"
    "DOUBLE" "DOUBLE PRECISION"
    "FLOAT" "DOUBLE PRECISION"
    "DECIMAL" "NUMERIC"
    "NUMERIC" "NUMERIC"
    "JSON" "JSONB"
    "JSONB" "JSONB"
    "TEXT"))

(defn- deep-merge
  [& maps]
  (apply merge-with
         (fn [left right]
           (if (and (map? left) (map? right))
             (deep-merge left right)
             right))
         maps))

(defn- parse-nonnegative-long
  [value]
  (try
    (let [parsed (cond
                   (nil? value) nil
                   (number? value) (long value)
                   :else (Long/parseLong (str value)))]
      (when (and (some? parsed) (not (neg? parsed)))
        parsed))
    (catch Exception _
      nil)))

(defn- normalize-duration-window
  [window]
  (when (map? window)
    (let [value (parse-nonnegative-long (:value window))
          unit  (some-> (:unit window) non-blank-str string/lower-case)]
      (cond-> {}
        (some? value) (assoc :value value)
        unit (assoc :unit unit)))))

(defn- timestamp-role?
  [column]
  (or (= "timestamp" (:role column))
      (contains? #{"TIMESTAMP" "TIMESTAMPTZ"} (source-field-type column))
      (boolean (re-find #"(?i)(event_?time|updated_?at|created_?at|timestamp)" (str (:target_column column))))))

(defn- inferred-event-time-column
  [columns]
  (some->> columns
           (filter timestamp-role?)
           first
           :target_column
           non-blank-str))

(defn- normalize-processing-policy
  [policy]
  (when (map? policy)
    (let [business-keys (->> (or (:business_keys policy) [])
                             (keep non-blank-str)
                             vec)
          event-time-column (some-> (:event_time_column policy) non-blank-str)
          sequence-column   (some-> (:sequence_column policy) non-blank-str)
          ordering-strategy (some-> (:ordering_strategy policy) non-blank-str string/lower-case)
          late-data-mode    (some-> (:late_data_mode policy) non-blank-str string/lower-case)
          too-late-behavior (some-> (:too_late_behavior policy) non-blank-str string/lower-case)
          late-data-tolerance (normalize-duration-window (:late_data_tolerance policy))
          reprocess-window    (normalize-duration-window (:reprocess_window policy))]
      (cond-> {}
        (seq business-keys) (assoc :business_keys business-keys)
        event-time-column (assoc :event_time_column event-time-column)
        sequence-column (assoc :sequence_column sequence-column)
        ordering-strategy (assoc :ordering_strategy ordering-strategy)
        late-data-mode (assoc :late_data_mode late-data-mode)
        too-late-behavior (assoc :too_late_behavior too-late-behavior)
        (seq late-data-tolerance) (assoc :late_data_tolerance late-data-tolerance)
        (seq reprocess-window) (assoc :reprocess_window reprocess-window)))))

(defn- default-processing-policy
  [key-cols columns]
  (let [event-time-column (inferred-event-time-column columns)
        policy            (cond-> {}
                            (seq key-cols) (assoc :business_keys key-cols)
                            event-time-column (assoc :event_time_column event-time-column)
                            (and (seq key-cols) event-time-column) (assoc :ordering_strategy "latest_event_time_wins")
                            (seq key-cols) (assoc :late_data_mode "merge")
                            (seq key-cols) (assoc :too_late_behavior "accept"))]
    (when (seq policy)
      policy)))

(defn- duration-window-errors
  [field value]
  (let [raw-value (when (map? value) (:value value))
        raw-unit  (when (map? value) (:unit value))
        normalized (normalize-duration-window value)
        has-any?  (or (some? raw-value) (some? raw-unit))]
    (vec
     (concat
      (when (and (some? value) (not (map? value)))
        [{:kind "processing_policy"
          :field field
          :message (str field " must be an object with value and unit")}])
      (when (and has-any? (nil? (:value normalized)))
        [{:kind "processing_policy"
          :field field
          :message (str field " value must be a non-negative integer")}])
      (when (and has-any?
                 (not (contains? allowed-window-units (some-> raw-unit non-blank-str string/lower-case))))
        [{:kind "processing_policy"
          :field field
          :message (str field " unit must be one of minutes, hours, or days")}])))))

(defn- valid-sql-ident?
  [value]
  (boolean (and (string? value)
                (re-matches sql-identifier-pattern value))))

(defn- valid-qualified-table-name?
  [value]
  (boolean (and (string? value)
                (re-matches qualified-table-name-pattern value))))

(def ^:private source-reference-pattern #"^(?:bronze|silver)\.(?:[A-Za-z_][A-Za-z0-9_]*|\"(?:[^\"]|\"\")+\")$")
(def ^:private json-extraction-pattern #"^(?:bronze\.payload_json::jsonb|item\.value)\s*#>>\s*'\{[A-Za-z0-9_,]+\}'$")
(def ^:private source-reference-finder-pattern #"(?:bronze|silver)\.(?:[A-Za-z_][A-Za-z0-9_]*|\"(?:[^\"]|\"\")+\")")
(def ^:private json-extraction-finder-pattern #"(?:bronze\.payload_json::jsonb|item\.value)\s*#>>\s*'\{[A-Za-z0-9_,]+\}'")
(def ^:private allowed-cast-types
  #{"DATE" "VARCHAR" "STRING" "BOOLEAN" "INT" "INTEGER" "BIGINT" "DOUBLE" "DOUBLE PRECISION" "NUMERIC" "TIMESTAMP" "TIMESTAMPTZ"})
(def ^:private unsafe-expression-pattern #"(?:--|/\*|\*/|;)")
(def ^:private unsafe-expression-keyword-pattern #"(?i)\b(insert|update|delete|drop|alter|merge|copy|put|grant|revoke|truncate|create|replace|execute|call)\b")
(def ^:private general-expression-pattern #"(?is)^[A-Za-z0-9_.'\"()\[\]{}=<>!%+\-*/,:\s|&]+$")
(def ^:private databricks-expression-pattern #"(?is)^[A-Za-z0-9_.'\"`()\[\]{}=<>!%+\-*/,:\s|&]+$")
(def ^:private databricks-disallowed-keyword-pattern
  #"(?is)\b(SELECT|FROM|JOIN|QUALIFY|UNION|WITH|MERGE|INSERT|UPDATE|DELETE|COPY|OPTIMIZE|VACUUM|REORG|ALTER|DROP|CREATE|CALL)\b")
(def ^:private databricks-comment-pattern #"(?s)(--|/\*|\*/|;)")
(def ^:private databricks-function-call-pattern #"(?is)\b[A-Za-z_][A-Za-z0-9_]*\(")
(def ^:private databricks-qualified-ref-pattern
  #"(?is)(?:^|[^A-Za-z0-9_`])(?:s|t)\s*\.\s*(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)")
(def ^:private databricks-simple-literal-pattern
  #"(?is)^('([^']|'')*'|-?\d+(?:\.\d+)?|NULL|TRUE|FALSE)$")

(defn- split-top-level-args
  [value]
  (loop [idx   0
         depth 0
         start 0
         parts []]
    (if (= idx (count value))
      (when (zero? depth)
        (conj parts (subs value start idx)))
      (let [ch (.charAt value idx)]
        (cond
          (= ch \() (recur (inc idx) (inc depth) start parts)
          (= ch \)) (when (pos? depth)
                      (recur (inc idx) (dec depth) start parts))
          (and (= ch \,) (zero? depth)) (recur (inc idx) depth (inc idx) (conj parts (subs value start idx)))
          :else (recur (inc idx) depth start parts))))))

(defn- function-call-body
  [value fn-name]
  (let [value  (some-> value str string/trim)
        prefix (str fn-name "(")]
    (when (and value
               (string/starts-with? value prefix)
               (string/ends-with? value ")"))
      (subs value (count prefix) (dec (count value))))))

(defn- unwrap-parenthesized-expression
  [value]
  (loop [expr (some-> value str string/trim)]
    (if (and expr
             (string/starts-with? expr "(")
             (string/ends-with? expr ")"))
      (let [balanced? (loop [idx 0
                             depth 0]
                        (if (= idx (count expr))
                          (zero? depth)
                          (let [ch (.charAt expr idx)
                                next-depth (cond
                                             (= ch \() (inc depth)
                                             (= ch \)) (dec depth)
                                             :else depth)]
                            (cond
                              (neg? next-depth) false
                              (and (zero? next-depth) (< idx (dec (count expr)))) false
                              :else (recur (inc idx) next-depth)))))]
        (if balanced?
          (recur (string/trim (subs expr 1 (dec (count expr)))))
          expr))
      expr)))

(defn- safe-proposal-expression?
  [value]
  (let [expr (unwrap-parenthesized-expression value)]
    (boolean
     (when expr
       (or (re-matches source-reference-pattern expr)
           (re-matches json-extraction-pattern expr)
           (= "COUNT(*)" expr)
           (when-let [body (function-call-body expr "CAST")]
             (when-let [[_ inner cast-type] (re-matches #"(?is)^(.*)\s+AS\s+([A-Z ]+)\s*$" body)]
               (and (contains? allowed-cast-types (string/upper-case (string/trim cast-type)))
                    (safe-proposal-expression? (string/trim inner)))))
           (some (fn [fn-name]
                   (when-let [body (function-call-body expr fn-name)]
                     (safe-proposal-expression? body)))
                 ["TRIM" "UPPER" "LOWER" "TO_DATE" "SUM" "AVG" "MIN" "MAX"])
           (when-let [body (function-call-body expr "SUBSTRING")]
             (when-let [parts (some->> (split-top-level-args body)
                                       (map string/trim)
                                       seq)]
               (let [[source-expr & params] parts]
                 (and (safe-proposal-expression? source-expr)
                      (<= 1 (count params) 2)
                      (every? #(re-matches #"^\d+$" %) params)))))
           (and (re-matches general-expression-pattern expr)
                (not (re-find unsafe-expression-pattern expr))
                (not (re-find unsafe-expression-keyword-pattern expr))
                (or (seq (re-seq source-reference-finder-pattern expr))
                    (seq (re-seq json-extraction-finder-pattern expr))
                    (= "COUNT(*)" expr))))))))

(defn- safe-databricks-sql-fragment?
  ([value]
   (safe-databricks-sql-fragment? value {}))
  ([value {:keys [require-ref? allow-or? allow-literal-only?]
           :or {require-ref? false
                allow-or? true
                allow-literal-only? false}}]
   (let [expr         (some-> value str string/trim)
         has-ref?     (boolean (and expr (re-find databricks-qualified-ref-pattern expr)))
         literal-only? (boolean (and expr (re-matches databricks-simple-literal-pattern expr)))]
     (boolean
      (and (seq expr)
           (re-matches databricks-expression-pattern expr)
           (not (re-find unsafe-expression-pattern expr))
           (not (re-find unsafe-expression-keyword-pattern expr))
           (not (re-find databricks-comment-pattern expr))
           (not (re-find databricks-disallowed-keyword-pattern expr))
           (not (re-find databricks-function-call-pattern expr))
           (or allow-or?
               (not (re-find #"(?i)\bOR\b" expr)))
           (or (not require-ref?) has-ref?)
           (or has-ref?
               allow-literal-only?
               literal-only?))))))

(defn- materialization-mode-supported?
  [warehouse mode]
  (contains? (if (= warehouse "databricks")
               #{"merge" "table_replace" "append" "update" "delete"}
               #{"merge" "table_replace" "append"})
             mode))

(defn- materialization-extra-options
  [proposal-json]
  (let [materialization (:materialization proposal-json)
        when-not-matched-by-source (:when_not_matched_by_source materialization)
        databricks-sql (cond-> {}
                         (contains? materialization :schema_evolution)
                         (assoc :schema_evolution (boolean (:schema_evolution materialization)))
                         (contains? materialization :update_on_matched)
                         (assoc :update_on_matched (boolean (:update_on_matched materialization)))
                         (contains? materialization :insert_on_not_matched)
                         (assoc :insert_on_not_matched (boolean (:insert_on_not_matched materialization)))
                         (some-> (:matched_condition materialization) non-blank-str)
                         (assoc :matched_condition (:matched_condition materialization))
                         (some-> (:not_matched_condition materialization) non-blank-str)
                         (assoc :not_matched_condition (:not_matched_condition materialization))
                         (seq (or (:update_assignments materialization) []))
                         (assoc :update_assignments (vec (:update_assignments materialization)))
                         (map? when-not-matched-by-source)
                         (assoc :when_not_matched_by_source when-not-matched-by-source))]
    (cond-> {}
      (seq databricks-sql) (assoc :databricks_sql databricks-sql))))

(defn- materialization-from-target-config
  [target-config]
  (let [databricks-sql (get-in target-config [:options :databricks_sql])]
    (cond-> {:mode (case (:write_mode target-config)
                     "replace" "table_replace"
                     "overwrite" "table_replace"
                     "append" "append"
                     "update" "update"
                     "delete" "delete"
                     "merge")
             :keys (vec (or (:merge_keys target-config) []))}
      (map? databricks-sql)
      (merge (cond-> {}
               (contains? databricks-sql :schema_evolution)
               (assoc :schema_evolution (boolean (:schema_evolution databricks-sql)))
               (contains? databricks-sql :update_on_matched)
               (assoc :update_on_matched (boolean (:update_on_matched databricks-sql)))
               (contains? databricks-sql :insert_on_not_matched)
               (assoc :insert_on_not_matched (boolean (:insert_on_not_matched databricks-sql)))
               (some-> (:matched_condition databricks-sql) non-blank-str)
               (assoc :matched_condition (:matched_condition databricks-sql))
               (some-> (:not_matched_condition databricks-sql) non-blank-str)
               (assoc :not_matched_condition (:not_matched_condition databricks-sql))
               (seq (or (:update_assignments databricks-sql) []))
               (assoc :update_assignments (vec (:update_assignments databricks-sql)))
               (map? (:when_not_matched_by_source databricks-sql))
               (assoc :when_not_matched_by_source (:when_not_matched_by_source databricks-sql)))))))

(def ^:private allowed-review-states
  #{"reviewed" "approved" "rejected" "changes_requested"})

(def ^:private editable-in-place-statuses
  #{"draft" "invalid"})

(def ^:private clone-on-edit-statuses
  #{"validated" "reviewed" "approved" "published"})

(defn- proposal-checksum
  [proposal-json]
  (sha256-hex (json/generate-string proposal-json)))

(defn- reviewable-proposal?
  [proposal-row]
  (contains? #{"validated" "reviewed" "approved"} (:status proposal-row)))

(defn- compile-result-status
  [proposal-row]
  (let [current-status (some-> (:status proposal-row) string/lower-case)]
    (if (contains? #{"validated" "reviewed" "approved" "published"} current-status)
      current-status
      "compiled")))

(defn- publishable-proposal?
  [proposal-row]
  (contains? #{"validated" "reviewed" "approved" "published"} (:status proposal-row)))

(defn- current-valid-validation
  [proposal-id proposal-json]
  (let [latest   (latest-validation-for-proposal proposal-id)
        checksum (proposal-checksum proposal-json)]
    (when (and latest
               (= "valid" (:status latest))
               (= checksum (get (parse-json-safe (:validation_json latest)) :proposal_checksum)))
      {:validation_id (:validation_id latest)
       :proposal_id proposal-id
       :status (:status latest)
       :compiled_sql (:compiled_sql latest)
       :validation (parse-json-safe (:validation_json latest))})))

(defn- ensure-modeling-tables!
  []
  (when-not @modeling-ready?
    (locking modeling-ready?
      (when-not @modeling-ready?
        (doseq [sql-str
                [(str "CREATE TABLE IF NOT EXISTS " schema-profile-table " ("
                      "profile_id BIGSERIAL PRIMARY KEY, "
                      "tenant_key VARCHAR(128) NOT NULL, "
                      "workspace_key VARCHAR(128) NOT NULL, "
                      "graph_id INTEGER NOT NULL, "
                      "api_node_id INTEGER NOT NULL, "
                      "source_layer VARCHAR(32) NOT NULL, "
                      "source_system TEXT NOT NULL, "
                      "endpoint_name TEXT NOT NULL, "
                      "profile_source VARCHAR(64) NOT NULL, "
                      "sample_record_count INTEGER NOT NULL DEFAULT 0, "
                      "field_count INTEGER NOT NULL DEFAULT 0, "
                      "profile_json TEXT NOT NULL, "
                      "created_by TEXT NULL, "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                 (str "CREATE INDEX IF NOT EXISTS idx_schema_profile_snapshot_graph_endpoint "
                      "ON " schema-profile-table " (graph_id, api_node_id, endpoint_name, created_at_utc DESC)")
                 (str "CREATE TABLE IF NOT EXISTS " model-proposal-table " ("
                      "proposal_id BIGSERIAL PRIMARY KEY, "
                      "tenant_key VARCHAR(128) NOT NULL, "
                      "workspace_key VARCHAR(128) NOT NULL, "
                      "layer VARCHAR(32) NOT NULL, "
                      "target_model TEXT NOT NULL, "
                      "status VARCHAR(32) NOT NULL DEFAULT 'draft', "
                      "source_graph_id INTEGER NOT NULL, "
                      "source_node_id INTEGER NOT NULL, "
                      "source_endpoint_name TEXT NOT NULL, "
                      "profile_id BIGINT NULL REFERENCES " schema-profile-table "(profile_id), "
                      "proposal_json TEXT NOT NULL, "
                      "compiled_sql TEXT NULL, "
                      "confidence_score DOUBLE PRECISION NOT NULL DEFAULT 0, "
                      "created_by TEXT NULL, "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                 (str "CREATE TABLE IF NOT EXISTS " model-validation-table " ("
                      "validation_id BIGSERIAL PRIMARY KEY, "
                      "proposal_id BIGINT NOT NULL REFERENCES " model-proposal-table "(proposal_id), "
                      "status VARCHAR(32) NOT NULL, "
                      "validation_kind VARCHAR(64) NOT NULL, "
                      "validation_json TEXT NOT NULL, "
                      "compiled_sql TEXT NULL, "
                      "created_by TEXT NULL, "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                 (str "CREATE INDEX IF NOT EXISTS idx_model_validation_result_proposal "
                      "ON " model-validation-table " (proposal_id, created_at_utc DESC)")
                 (str "CREATE TABLE IF NOT EXISTS " model-release-table " ("
                      "release_id BIGSERIAL PRIMARY KEY, "
                      "proposal_id BIGINT NOT NULL REFERENCES " model-proposal-table "(proposal_id), "
                      "validation_id BIGINT NOT NULL REFERENCES " model-validation-table "(validation_id), "
                      "layer VARCHAR(32) NOT NULL, "
                      "target_model TEXT NOT NULL, "
                      "version INTEGER NOT NULL, "
                      "status VARCHAR(32) NOT NULL, "
                      "active BOOLEAN NOT NULL DEFAULT FALSE, "
                      "created_by TEXT NULL, "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                      "published_at_utc TIMESTAMPTZ NULL)")
                 (str "CREATE TABLE IF NOT EXISTS " model-graph-artifact-table " ("
                      "graph_artifact_id BIGSERIAL PRIMARY KEY, "
                      "proposal_id BIGINT NOT NULL REFERENCES " model-proposal-table "(proposal_id), "
                      "graph_kind VARCHAR(32) NOT NULL, "
                      "graph_id INTEGER NOT NULL, "
                      "graph_version INTEGER NOT NULL, "
                      "gil_json TEXT NOT NULL, "
                      "node_map_json TEXT NOT NULL, "
                      "checksum VARCHAR(128) NOT NULL, "
                      "created_by TEXT NULL, "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                 (str "ALTER TABLE " model-release-table
                      " ADD COLUMN IF NOT EXISTS graph_artifact_id BIGINT NULL REFERENCES "
                      model-graph-artifact-table "(graph_artifact_id)")
                 (str "CREATE UNIQUE INDEX IF NOT EXISTS idx_model_release_target_version "
                      "ON " model-release-table " (layer, target_model, version)")
                 (str "CREATE INDEX IF NOT EXISTS idx_model_release_target_active "
                      "ON " model-release-table " (layer, target_model, active)")
                 (str "CREATE UNIQUE INDEX IF NOT EXISTS idx_model_release_single_active "
                      "ON " model-release-table " (layer, target_model) WHERE active = TRUE")
                 (str "CREATE INDEX IF NOT EXISTS idx_model_graph_artifact_proposal "
                      "ON " model-graph-artifact-table " (proposal_id, created_at_utc DESC)")
                 (str "CREATE TABLE IF NOT EXISTS " compiled-model-artifact-table " ("
                      "artifact_id BIGSERIAL PRIMARY KEY, "
                      "release_id BIGINT NOT NULL REFERENCES " model-release-table "(release_id), "
                      "artifact_kind VARCHAR(32) NOT NULL, "
                      "sql_ir_json TEXT NOT NULL, "
                      "sql_text TEXT NOT NULL, "
                      "validation_json TEXT NULL, "
                      "checksum VARCHAR(128) NOT NULL, "
                      "created_by TEXT NULL, "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
                 (str "CREATE TABLE IF NOT EXISTS " compiled-model-run-table " ("
                      "model_run_id BIGSERIAL PRIMARY KEY, "
                      "release_id BIGINT NOT NULL REFERENCES " model-release-table "(release_id), "
                      "graph_artifact_id BIGINT NOT NULL REFERENCES " model-graph-artifact-table "(graph_artifact_id), "
                      "execution_request_id UUID NULL, "
                      "execution_backend VARCHAR(64) NOT NULL, "
                      "status VARCHAR(32) NOT NULL, "
                      "target_connection_id INTEGER NULL, "
                      "job_id TEXT NULL, "
                      "external_run_id TEXT NULL, "
                      "request_json TEXT NOT NULL, "
                      "response_json TEXT NULL, "
                      "created_by TEXT NULL, "
                      "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                      "completed_at_utc TIMESTAMPTZ NULL)")
                 (str "CREATE INDEX IF NOT EXISTS idx_compiled_model_artifact_release "
                      "ON " compiled-model-artifact-table " (release_id)")
                 (str "ALTER TABLE " compiled-model-run-table
                      " ADD COLUMN IF NOT EXISTS execution_request_id UUID NULL")
                 (str "CREATE INDEX IF NOT EXISTS idx_compiled_model_run_release "
                      "ON " compiled-model-run-table " (release_id, created_at_utc DESC)")
                 (str "CREATE INDEX IF NOT EXISTS idx_compiled_model_run_execution_request "
                      "ON " compiled-model-run-table " (execution_request_id)")
                 (str "CREATE INDEX IF NOT EXISTS idx_model_proposal_graph_endpoint "
                      "ON " model-proposal-table " (source_graph_id, source_node_id, source_endpoint_name, created_at_utc DESC)")
                 (str "CREATE INDEX IF NOT EXISTS idx_model_proposal_target_model "
                      "ON " model-proposal-table " (target_model, created_at_utc DESC)")]]
          (jdbc/execute! db/ds [sql-str]))
        (reset! modeling-ready? true))))
  nil)

(defn- connection-dbtype
  [conn-id]
  (some-> conn-id db/create-dbspec-from-id :dbtype))

(defn- audit-table
  [target conn-id table-name]
  (db/fully-qualified-table-name
   {:catalog (when (= "databricks" (connection-dbtype conn-id))
               (or (:catalog target) "sheetz_telematics"))
    :schema (or (:audit_schema target) "audit")}
   table-name))

(defn- target-connection-id
  [target]
  (or (:connection_id target) (:c target)))

(defn- target-warehouse
  [target conn-id]
  (or (some-> (:target_kind target) non-blank-str string/lower-case)
      (some-> (connection-dbtype conn-id) non-blank-str string/lower-case)
      "databricks"))

(defn- proposal-target-warehouse
  [{:keys [proposal-json target target-connection-id]}]
  (or (some-> (:target_warehouse proposal-json) non-blank-str string/lower-case)
      (target-warehouse target target-connection-id)))

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

(defn- enabled-endpoints
  [api-node]
  (->> (:endpoint_configs api-node)
       (filter #(not= false (:enabled %)))))

(defn- source-config-key
  [source-node]
  (case (:btype source-node)
    "Ap" :endpoint_configs
    "Kf" :topic_configs
    "Fs" :file_configs
    nil))

(defn- source-node-label
  [source-node]
  (case (:btype source-node)
    "Ap" "API"
    "Kf" "Kafka"
    "Fs" "File"
    "source"))

(defn- enabled-source-endpoints
  [source-node]
  (if-let [config-key (source-config-key source-node)]
    (->> (get source-node config-key)
         (filter #(not= false (:enabled %))))
    []))

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

(defn- select-source-endpoint!
  [source-node endpoint-name]
  (let [endpoints (vec (enabled-source-endpoints source-node))
        label     (source-node-label source-node)]
    (cond
      (and endpoint-name
           (some #(= endpoint-name (:endpoint_name %)) endpoints))
      (first (filter #(= endpoint-name (:endpoint_name %)) endpoints))

      endpoint-name
      (throw (ex-info (str "No enabled source config found for endpoint_name '" endpoint-name "'")
                      {:endpoint_name endpoint-name
                       :node_id (:id source-node)
                       :btype (:btype source-node)}))

      (= 1 (count endpoints))
      (first endpoints)

      (empty? endpoints)
      (throw (ex-info (str label " node has no enabled source configs")
                      {:node_id (:id source-node)
                       :btype (:btype source-node)}))

      :else
      (throw (ex-info (str "endpoint_name is required when a " label " node has multiple enabled source configs")
                      {:endpoint_names (mapv :endpoint_name endpoints)
                       :node_id (:id source-node)
                       :btype (:btype source-node)})))))

(defn- parse-json-safe
  [value]
  (when-let [value (non-blank-str value)]
    (json/parse-string value true)))

(defn- latest-endpoint-schema-snapshot
  [graph-id api-node-id source-system endpoint target]
  (when-let [conn-id (target-connection-id target)]
    (try
      (let [table-name (audit-table target conn-id "endpoint_schema_snapshot")]
        (jdbc/execute-one!
         (db/get-opts conn-id nil)
         [(str "SELECT * FROM " table-name
               " WHERE graph_id = ? AND api_node_id = ? AND source_system = ? AND endpoint_name = ?"
               " ORDER BY captured_at_utc DESC LIMIT 1")
          graph-id
          api-node-id
          source-system
          (:endpoint_name endpoint)]))
      (catch Exception e
        (throw (ex-info (str "Could not read schema snapshot from target: " (.getMessage e))
                        {:failure_class "target_connection"
                         :graph_id graph-id
                         :api_node_id api-node-id
                         :endpoint_name (:endpoint_name endpoint)
                         :cause_message (.getMessage e)}
                        e))))))

(defn- source-field-type
  [field]
  (-> (or (non-blank-str (:override_type field))
          (non-blank-str (:type field))
          "STRING")
      string/upper-case))

(defn- path-tokens
  [path]
  (or (seq (re-seq #"[A-Za-z0-9]+" (or path "")))
      []))

(defn- path-tail
  [path]
  (some-> path path-tokens last string/lower-case))

(defn- field-match-rank
  [field pk]
  (let [pk (some-> pk str string/lower-case)
        column-name (some-> (:column_name field) str string/lower-case)
        path (some-> (:path field) str string/lower-case)]
    (cond
      (= pk column-name) 0
      (= pk (path-tail path)) 1
      (= pk path) 2
      (or (string/ends-with? (or path "") (str "." pk))
          (string/ends-with? (or path "") (str "]." pk))) 3
      :else nil)))

(defn- best-key-match
  [fields pk]
  (->> fields
       (map-indexed (fn [idx field]
                      (when-let [rank (field-match-rank field pk)]
                        {:field field
                         :rank rank
                         :depth (count (path-tokens (:path field)))
                         :order idx})))
       (remove nil?)
       (sort-by (juxt :rank :depth :order))
       first
       :field))

(defn- key-columns
  [endpoint fields]
  (let [primary-keys (mapv #(-> % str string/trim) (or (:primary_key_fields endpoint) []))
        explicit (->> primary-keys
                      (keep (fn [pk]
                              (some-> (best-key-match fields pk) :column_name)))
                      distinct
                      vec)]
    (if (seq explicit)
      explicit
      (->> fields
           (map :column_name)
           (filter #(re-find #"(?:^id$|_id$)" (or % "")))
           distinct
           vec))))

(defn- timestamp-column?
  [field]
  (boolean
   (or (#{"DATE" "TIMESTAMP"} (source-field-type field))
       (re-find #"(?:^|_)(date|time|timestamp|created_at|updated_at|event_at|event_time|effective_at)$"
                (or (:column_name field) "")))))

(defn- field-role
  [field key-columns]
  (cond
    (contains? (set key-columns) (:column_name field)) "business_key"
    (timestamp-column? field) "timestamp"
    (#{"INT" "BIGINT" "DOUBLE"} (source-field-type field)) "measure_candidate"
    :else "attribute"))

(defn- infer-entity-kind
  [fields key-columns]
  (let [has-measure? (some #(= "measure_candidate" (field-role % key-columns)) fields)
        has-time? (some timestamp-column? fields)]
    (cond
      (and has-measure? has-time?) "fact"
      (seq key-columns) "dimension"
      :else "entity")))

(defn- build-schema-profile
  [{:keys [graph-id api-node-id source-system endpoint-name source snapshot-row fields endpoint]}]
  (let [fields (vec (filter #(not= false (:enabled %)) (or fields [])))
        key-cands (key-columns endpoint fields)
        timestamp-cands (->> fields (filter timestamp-column?) (mapv :column_name))
        field-types (into {}
                          (map (fn [field]
                                 [(:column_name field) {:type (source-field-type field)
                                                        :nullable (not= false (:nullable field))}]))
                          fields)]
    {:graph_id graph-id
     :api_node_id api-node-id
     :source_layer "bronze"
     :source_system source-system
     :endpoint_name endpoint-name
     :profile_source source
     :sample_record_count (int (or (:sample_record_count snapshot-row) 0))
     :field_count (count fields)
     :profile_json {:profile_source source
                    :schema_snapshot_ref (select-keys snapshot-row [:captured_at_utc :graph_version :graph_version_id])
                    :field_count (count fields)
                    :key_candidates key-cands
                    :timestamp_candidates timestamp-cands
                    :field_types field-types}}))

(defn- last-table-segment
  [table-name]
  (last (string/split (or table-name "") #"\.")))

(defn- derive-target-table
  [endpoint]
  (or (non-blank-str (:silver_table_name endpoint))
      (str "silver_" (sanitize-ident (:endpoint_name endpoint)))))

(defn- derive-gold-target-table
  [silver-proposal]
  (let [target-table (or (non-blank-str (:target_table silver-proposal))
                         (str "silver." (sanitize-ident (:target_model silver-proposal))))]
    (-> target-table
        (string/replace #"\.silver\." ".gold.")
        (string/replace #"\bsilver_" "gold_"))))

(defn- numeric-column?
  [column]
  (#{"INT" "BIGINT" "DOUBLE"} (source-field-type column)))

(defn- aggregatable-measure?
  "True when a numeric column is likely a real measure (amount, quantity, cost)
   rather than a coded dimension (year, id, code, flag, zip, status)."
  [column]
  (and (numeric-column? column)
       (not (#{"business_key" "timestamp"} (:role column)))
       (not (re-find #"(?i)(_id$|_year$|_code$|_zip$|_flag$|^year$|^id$|_count$|_version$|_status$|_type$|_num$|_number$)"
                     (or (:target_column column) "")))))

(defn- first-timestamp-column
  [columns]
  (some #(when (= "timestamp" (:role %)) %) columns))

(defn- source-alias
  [layer]
  (case layer
    "gold" "silver"
    "bronze"))

(defn- parse-json-source-path
  [path]
  (->> (string/split (or (non-blank-str path) "") #"\.")
       (map #(string/replace % #"^\$" ""))
       (remove string/blank?)
       (map (fn [segment]
              (let [array? (string/ends-with? segment "[]")
                    key    (if array?
                             (subs segment 0 (- (count segment) 2))
                             segment)]
                {:key key :array? array?})))
       vec))

(defn- common-json-array-root
  [fields]
  (let [roots (keep (fn [field]
                      (let [segments (parse-json-source-path (:path field))
                            idx      (some (fn [[i segment]]
                                             (when (:array? segment) i))
                                           (map-indexed vector segments))]
                        (when (some? idx)
                          (mapv :key (take (inc idx) segments)))))
                    fields)]
    (->> roots
         frequencies
         (sort-by (fn [[segments count]]
                    [count (count segments)])
                  #(compare %2 %1))
         ffirst)))

(defn- json-extract-text-sql
  [base-expr segments]
  (when (seq segments)
    (str "(" base-expr " #>> '{" (string/join "," segments) "}')")))

(defn- cast-json-text-sql
  [expr field-type]
  (case (source-field-type {:type field-type})
    "BOOLEAN" (str "CAST(" expr " AS BOOLEAN)")
    "DATE" (str "CAST(" expr " AS DATE)")
    "TIMESTAMP" (str "CAST(" expr " AS TIMESTAMP)")
    "INT" (str "CAST(" expr " AS INTEGER)")
    "BIGINT" (str "CAST(" expr " AS BIGINT)")
    "DOUBLE" (str "CAST(" expr " AS DOUBLE PRECISION)")
    expr))

(defn- postgres-source-expression
  [field array-root]
  (let [segments      (parse-json-source-path (:path field))
        root-len      (count array-root)
        uses-array?   (and (seq array-root)
                           (<= root-len (count segments))
                           (= array-root (mapv :key (take root-len segments))))
        relative-path (mapv :key (if uses-array?
                                   (drop root-len segments)
                                   segments))
        base-expr     (if uses-array?
                        "item.value"
                        "bronze.payload_json::jsonb")]
    (some-> (json-extract-text-sql base-expr relative-path)
            (cast-json-text-sql (source-field-type field)))))

(defn- inferred-field-expression
  [field target array-root]
  (if (= "postgresql" (target-warehouse target (target-connection-id target)))
    (or (postgres-source-expression field array-root)
        (str "bronze." (:column_name field)))
    (str "bronze." (:column_name field))))

(defn- model-source-reference
  [warehouse source-alias column-name]
  ;; Quote identifiers with the target dialect so generated Gold expressions use
  ;; the same identifier style the downstream compiler/runtime expects.
  (str source-alias "." (quote-sql-ident-for-warehouse warehouse column-name)))

(defn- proposal-confidence
  [{:keys [profile-source key-columns timestamp-columns target-table-explicit?]}]
  (min 0.9
       (+ 0.4
          (if (= "endpoint_schema_snapshot" profile-source) 0.15 0.0)
          (if (seq key-columns) 0.2 0.0)
          (if (seq timestamp-columns) 0.1 0.0)
          (if target-table-explicit? 0.05 0.0))))

(defn- build-silver-proposal
  [{:keys [tenant-key workspace-key graph-id api-node-id source-system endpoint profile created-by target]}]
  (let [warehouse         (target-warehouse target (target-connection-id target))
        source-fields     (->> (or (:inferred_fields endpoint) [])
                               (filter #(not= false (:enabled %)))
                               vec)
        array-root        (common-json-array-root source-fields)
        key-cols          (key-columns endpoint source-fields)
        timestamp-cols    (->> source-fields (filter timestamp-column?) (mapv :column_name))
        target-table      (derive-target-table endpoint)
        target-model      (sanitize-ident (last-table-segment target-table))
        entity-kind       (infer-entity-kind source-fields key-cols)
        confidence        (proposal-confidence {:profile-source (:profile_source profile)
                                                :key-columns key-cols
                                                :timestamp-columns timestamp-cols
                                                :target-table-explicit? (boolean (non-blank-str (:silver_table_name endpoint)))})
        columns           (mapv (fn [field]
                                  {:target_column (:column_name field)
                                   :type (source-field-type field)
                                   :nullable (not= false (:nullable field))
                                   :role (field-role field key-cols)
                                   :source_paths [(:path field)]
                                   :source_columns [(:column_name field)]
                                   :expression (inferred-field-expression field target array-root)
                                   :confidence (if (contains? (set key-cols) (:column_name field)) 0.95 0.85)
                                   :rule_source (if (contains? (set key-cols) (:column_name field))
                                                  "primary_key_match"
                                                  "schema_snapshot_passthrough")})
                                source-fields)
        processing-policy (default-processing-policy key-cols columns)
        materialization   {:mode (if (seq key-cols) "merge" "table_replace")
                           :keys key-cols}
        grain             (when (seq key-cols) {:keys key-cols})
        explanations      (vec (concat
                                [(str "Derived from Bronze schema profile for endpoint '" (:endpoint_name endpoint) "'.")]
                                (when (seq key-cols)
                                  [(str "Selected merge keys from endpoint primary keys or *_id heuristics: "
                                        (string/join ", " key-cols))])
                                (when (seq timestamp-cols)
                                  [(str "Detected timestamp candidates: "
                                        (string/join ", " timestamp-cols))])))]
    {:tenant_key tenant-key
     :workspace_key workspace-key
     :layer "silver"
     :target_model target-model
     :status "draft"
     :source_graph_id graph-id
     :source_node_id api-node-id
     :source_endpoint_name (:endpoint_name endpoint)
     :confidence_score confidence
     :proposal_json {:layer "silver"
                     :source_layer "bronze"
                     :target_warehouse warehouse
                     :target_model target-model
                     :target_table target-table
                     :entity_kind entity-kind
                     :source_graph_id graph-id
                     :source_node_id api-node-id
                     :source_system source-system
                     :endpoint_name (:endpoint_name endpoint)
                     :source_expansion (when (and (= "postgresql" warehouse)
                                                  (seq array-root))
                                         {:kind "jsonb_array"
                                          :alias "item"
                                          :json_column "payload_json"
                                          :path array-root})
                     :profile_summary {:sample_record_count (:sample_record_count profile)
                                       :field_count (:field_count profile)}
                     :columns columns
                     :mappings (mapv #(select-keys % [:target_column :expression :source_paths :source_columns :confidence :rule_source]) columns)
                     :processing_policy processing-policy
                     :materialization materialization
                     :grain grain
                     :explanations explanations}
     :compiled_sql nil
     :created_by created-by}))

(defn- build-gold-proposal
  [{:keys [tenant-key workspace-key silver-proposal proposal-row created-by target-warehouse]}]
  (let [warehouse        (or (some-> target-warehouse non-blank-str string/lower-case)
                             (some-> (:target_warehouse silver-proposal) non-blank-str string/lower-case)
                             "databricks")
        silver-columns   (vec (proposal-columns silver-proposal))
        source-table     (:target_table silver-proposal)
        source-model     (:target_model silver-proposal)
        source-keys      (vec (or (get-in silver-proposal [:materialization :keys]) []))
        timestamp-col    (first-timestamp-column silver-columns)
        dimension-cols   (->> silver-columns
                              (filter (fn [col]
                                        (or (contains? (set source-keys) (:target_column col))
                                            (= "business_key" (:role col)))))
                              vec)
        dimension-keys   (set (map :target_column dimension-cols))
        measure-cols     (->> silver-columns
                              (remove #(contains? dimension-keys (:target_column %)))
                              (filter aggregatable-measure?)
                              vec)
        target-table     (derive-gold-target-table silver-proposal)
        target-base      (sanitize-ident (last-table-segment target-table))
        target-model     (if (string/starts-with? target-base "gold_")
                           target-base
                           (str "gold_" target-base))
        group-columns    (cond-> (mapv :target_column dimension-cols)
                           timestamp-col (conj "event_date"))
        dimension-items  (mapv (fn [column]
                                 {:target_column (:target_column column)
                                  :type (source-field-type column)
                                  :nullable (not= false (:nullable column))
                                  :role "dimension"
                                  :source_paths (vec (or (:source_paths column) []))
                                  :source_columns [(:target_column column)]
                                  :expression (model-source-reference warehouse "silver" (:target_column column))
                                  :confidence 0.9
                                  :rule_source "silver_contract_passthrough"})
                               dimension-cols)
        time-item        (when timestamp-col
                           {:target_column "event_date"
                            :type "DATE"
                            :nullable false
                            :role "time_dimension"
                            :source_paths (vec (or (:source_paths timestamp-col) []))
                            :source_columns [(:target_column timestamp-col)]
                            :expression (str "CAST(" (model-source-reference warehouse "silver" (:target_column timestamp-col)) " AS DATE)")
                            :confidence 0.9
                            :rule_source "timestamp_to_date"})
        measure-items    (if (seq measure-cols)
                           (mapv (fn [column]
                                   {:target_column (str "sum_" (:target_column column))
                                    :type (if (= "DOUBLE" (source-field-type column)) "DOUBLE" "BIGINT")
                                    :nullable false
                                    :role "measure"
                                    :source_paths (vec (or (:source_paths column) []))
                                    :source_columns [(:target_column column)]
                                    :expression (str "SUM(" (model-source-reference warehouse "silver" (:target_column column)) ")")
                                    :confidence 0.88
                                    :rule_source "aggregate_sum"})
                                 measure-cols)
                           [{:target_column "row_count"
                             :type "BIGINT"
                             :nullable false
                             :role "measure"
                             :source_paths []
                             :source_columns []
                             :expression "COUNT(*)"
                             :confidence 0.82
                             :rule_source "aggregate_count"}])
        columns          (vec (concat dimension-items
                                      (when time-item [time-item])
                                      measure-items))
        explanations     (vec (concat
                               [(str "Derived from Silver model '" source-model "' for Gold aggregation.")]
                               (when (seq group-columns)
                                 [(str "Grouped by: " (string/join ", " group-columns))])
                               (when (seq measure-cols)
                                 [(str "Aggregated measures: "
                                       (string/join ", " (map #(str "sum_" (:target_column %)) measure-cols)))])
                               (when (and (empty? measure-cols) (empty? dimension-cols))
                                 ["No numeric measures were found; defaulted to row_count aggregate."])))]
    {:tenant_key tenant-key
     :workspace_key workspace-key
     :layer "gold"
     :target_model target-model
     :status "draft"
     :source_graph_id (:source_graph_id proposal-row)
     :source_node_id (:source_node_id proposal-row)
     :source_endpoint_name source-model
     :confidence_score 0.82
     :proposal_json {:layer "gold"
                     :source_layer "silver"
                     :source_alias "silver"
                     :target_warehouse warehouse
                     :target_model target-model
                     :target_table target-table
                     :source_model source-model
                     :source_table source-table
                     :source_graph_id (:source_graph_id proposal-row)
                     :source_node_id (:source_node_id proposal-row)
                     :source_system (or (:source_system silver-proposal) "medallion")
                     :profile_summary {:field_count (count silver-columns)}
                     :columns columns
                     :mappings (mapv #(select-keys % [:target_column :expression :source_paths :source_columns :confidence :rule_source]) columns)
                     :group_by group-columns
                     :materialization {:mode (if (seq group-columns) "merge" "table_replace")
                                       :keys group-columns}
                     :grain (when (seq group-columns) {:keys group-columns})
                     :entity_kind "mart"
                     :explanations explanations}
     :compiled_sql nil
     :created_by created-by}))

(defn- persist-schema-profile!
  [profile created-by tenant-key workspace-key]
  (jdbc/execute-one!
   (db-opts db/ds)
   [(str "INSERT INTO " schema-profile-table "
          (tenant_key, workspace_key, graph_id, api_node_id, source_layer, source_system, endpoint_name,
           profile_source, sample_record_count, field_count, profile_json, created_by, created_at_utc)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          RETURNING *")
    tenant-key
    workspace-key
    (:graph_id profile)
    (:api_node_id profile)
    (:source_layer profile)
    (:source_system profile)
    (:endpoint_name profile)
    (:profile_source profile)
    (:sample_record_count profile)
    (:field_count profile)
    (json/generate-string (:profile_json profile))
    created-by
    (db-timestamptz (now-utc))]))

(defn- persist-model-proposal!
  [proposal profile-id]
  (jdbc/execute-one!
   (db-opts db/ds)
   [(str "INSERT INTO " model-proposal-table "
          (tenant_key, workspace_key, layer, target_model, status, source_graph_id, source_node_id,
           source_endpoint_name, profile_id, proposal_json, compiled_sql, confidence_score, created_by, created_at_utc)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          RETURNING *")
    (:tenant_key proposal)
    (:workspace_key proposal)
    (:layer proposal)
    (:target_model proposal)
    (:status proposal)
    (:source_graph_id proposal)
    (:source_node_id proposal)
    (:source_endpoint_name proposal)
    profile-id
    (json/generate-string (:proposal_json proposal))
    (:compiled_sql proposal)
    (:confidence_score proposal)
    (:created_by proposal)
    (db-timestamptz (now-utc))]))

(defn- latest-model-proposal
  [graph-id api-node-id endpoint-name]
  (jdbc/execute-one!
   (db-opts db/ds)
   [(str "SELECT * FROM " model-proposal-table
         " WHERE source_graph_id = ? AND source_node_id = ? AND source_endpoint_name = ?"
         " ORDER BY created_at_utc DESC, proposal_id DESC LIMIT 1")
    graph-id
    api-node-id
    endpoint-name]))

(defn- proposal-by-id
  ([proposal-id]
   (proposal-by-id db/ds proposal-id))
  ([conn proposal-id]
   (jdbc/execute-one!
    (db-opts conn)
    [(str "SELECT * FROM " model-proposal-table " WHERE proposal_id = ?")
     proposal-id])))

(defn- schema-profile-by-id
  [profile-id]
  (when profile-id
    (jdbc/execute-one!
     (db-opts db/ds)
     [(str "SELECT * FROM " schema-profile-table " WHERE profile_id = ?")
      profile-id])))

(defn- latest-validation-for-proposal
  ([proposal-id]
   (latest-validation-for-proposal db/ds proposal-id))
  ([conn proposal-id]
   (jdbc/execute-one!
    (db-opts conn)
    [(str "SELECT * FROM " model-validation-table
          " WHERE proposal_id = ? ORDER BY created_at_utc DESC, validation_id DESC LIMIT 1")
     proposal-id])))

(defn- next-release-version
  ([layer target-model]
   (next-release-version db/ds layer target-model))
  ([conn layer target-model]
   (let [row (jdbc/execute-one!
              (db-opts conn)
              [(str "SELECT COALESCE(MAX(version), 0) AS version FROM " model-release-table
                    " WHERE layer = ? AND target_model = ?")
               layer
               target-model])]
     (inc (long (or (:version row) 0))))))

(defn- latest-graph-artifact-for-proposal
  ([proposal-id]
   (latest-graph-artifact-for-proposal db/ds proposal-id))
  ([conn proposal-id]
   (jdbc/execute-one!
    (db-opts conn)
    [(str "SELECT * FROM " model-graph-artifact-table
          " WHERE proposal_id = ? ORDER BY created_at_utc DESC, graph_artifact_id DESC LIMIT 1")
     proposal-id])))

(defn- graph-artifact-by-id
  ([graph-artifact-id]
   (graph-artifact-by-id db/ds graph-artifact-id))
  ([conn graph-artifact-id]
   (jdbc/execute-one!
    (db-opts conn)
    [(str "SELECT * FROM " model-graph-artifact-table " WHERE graph_artifact_id = ?")
     graph-artifact-id])))

(defn- release-by-id
  ([release-id]
   (release-by-id db/ds release-id))
  ([conn release-id]
   (jdbc/execute-one!
    (db-opts conn)
    [(str "SELECT * FROM " model-release-table " WHERE release_id = ?")
     release-id])))

(defn get-release
  "Public wrapper for release-by-id. Returns nil if not found."
  ([release-id]
   (get-release db/ds release-id))
  ([conn release-id]
   (some-> (release-by-id conn release-id) (update :published_at_utc str) (update :created_at_utc str))))

(defn get-graph-artifact
  "Public wrapper for graph-artifact-by-id. Returns nil if not found."
  ([graph-artifact-id]
   (get-graph-artifact db/ds graph-artifact-id))
  ([conn graph-artifact-id]
   (some-> (graph-artifact-by-id conn graph-artifact-id) (update :created_at_utc str))))

(defn resolve-active-release
  "Resolve a release binding to a concrete release row. Throws if not found."
  ([binding]
   (resolve-active-release db/ds binding))
  ([conn binding]
   (let [mode (or (:mode binding) "follow_active")]
     (case mode
       "pinned"
       (let [release-id (:pinned_release_id binding)
             row (release-by-id conn release-id)]
         (when-not row
           (throw (ex-info "Pinned release not found"
                           {:pinned_release_id release-id
                            :status 404})))
         row)
       (let [row (latest-active-release conn (:layer binding) (:target_model binding))]
         (when-not row
           (throw (ex-info "No active published release for target model"
                           {:layer (:layer binding)
                            :target_model (:target_model binding)
                            :status 404})))
         row)))))

(defn- latest-active-release
  ([layer target-model]
   (latest-active-release db/ds layer target-model))
  ([conn layer target-model]
   (jdbc/execute-one!
    (db-opts conn)
    [(str "SELECT * FROM " model-release-table
          " WHERE layer = ? AND target_model = ? AND active = TRUE"
          " ORDER BY version DESC, release_id DESC LIMIT 1")
     layer
     target-model])))

(defn- latest-compiled-model-artifact
  ([release-id]
   (latest-compiled-model-artifact db/ds release-id))
  ([conn release-id]
   (jdbc/execute-one!
     (db-opts conn)
     [(str "SELECT * FROM " compiled-model-artifact-table
          " WHERE release_id = ? ORDER BY created_at_utc DESC, artifact_id DESC LIMIT 1")
     release-id])))

(defn- inflight-model-run-for-release
  ([release-id]
   (inflight-model-run-for-release db/ds release-id))
  ([conn release-id]
   (jdbc/execute-one!
    (db-opts conn)
    [(str "SELECT * FROM " compiled-model-run-table
          " WHERE release_id = ? AND status IN ('pending', 'submitted', 'running')"
          " ORDER BY created_at_utc DESC, model_run_id DESC LIMIT 1")
     release-id])))

(defn- update-model-proposal!
  ([proposal-id attrs]
   (update-model-proposal! db/ds proposal-id attrs))
  ([conn proposal-id attrs]
   (let [updates (cond-> {}
                   (contains? attrs :status) (assoc :status (:status attrs))
                   (contains? attrs :compiled_sql) (assoc :compiled_sql (:compiled_sql attrs))
                   (contains? attrs :proposal_json) (assoc :proposal_json (json/generate-string (:proposal_json attrs)))
                   (contains? attrs :target_model) (assoc :target_model (:target_model attrs))
                   (contains? attrs :confidence_score) (assoc :confidence_score (:confidence_score attrs)))]
     (when (seq updates)
       (let [columns (keys updates)
             values  (vals updates)]
         (jdbc/execute!
          conn
          (into [(str "UPDATE " model-proposal-table
                      " SET "
                      (string/join ", " (map #(str (name %) " = ?") columns))
                      " WHERE proposal_id = ?")]
                (concat values [proposal-id]))))))))

(defn- persist-model-validation!
  ([payload]
   (persist-model-validation! db/ds payload))
  ([conn {:keys [proposal-id status validation-kind validation-json compiled-sql created-by]}]
   (jdbc/execute-one!
    (db-opts conn)
    [(str "INSERT INTO " model-validation-table "
           (proposal_id, status, validation_kind, validation_json, compiled_sql, created_by, created_at_utc)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           RETURNING *")
     proposal-id
     status
     validation-kind
     (json/generate-string validation-json)
     compiled-sql
     created-by
     (db-timestamptz (now-utc))])))

(defn- deactivate-active-releases!
  ([layer target-model]
   (deactivate-active-releases! db/ds layer target-model))
  ([conn layer target-model]
   (jdbc/execute!
    conn
    [(str "UPDATE " model-release-table
          " SET active = FALSE, status = 'superseded'"
          " WHERE layer = ? AND target_model = ? AND active = TRUE")
     layer
     target-model])))

(defn- persist-model-release!
  ([payload]
   (persist-model-release! db/ds payload))
  ([conn {:keys [proposal-id validation-id graph-artifact-id layer target-model version status active created-by published-at]}]
   (jdbc/execute-one!
    (db-opts conn)
    [(str "INSERT INTO " model-release-table "
           (proposal_id, validation_id, graph_artifact_id, layer, target_model, version, status, active, created_by, created_at_utc, published_at_utc)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           RETURNING *")
     proposal-id
     validation-id
     graph-artifact-id
     layer
     target-model
     version
     status
     active
     created-by
     (db-timestamptz (now-utc))
     (db-timestamptz published-at)])))

(defn- persist-compiled-model-artifact!
  ([payload]
   (persist-compiled-model-artifact! db/ds payload))
  ([conn {:keys [release-id artifact-kind sql-ir sql-text validation-json created-by]}]
   (jdbc/execute-one!
    (db-opts conn)
    [(str "INSERT INTO " compiled-model-artifact-table "
           (release_id, artifact_kind, sql_ir_json, sql_text, validation_json, checksum, created_by, created_at_utc)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           RETURNING *")
     release-id
     artifact-kind
     (json/generate-string sql-ir)
     sql-text
     (when validation-json (json/generate-string validation-json))
     (sha256-hex sql-text)
     created-by
     (db-timestamptz (now-utc))])))

(defn- persist-model-graph-artifact!
  ([payload]
   (persist-model-graph-artifact! db/ds payload))
  ([conn {:keys [proposal-id graph-kind graph-id graph-version gil node-map created-by]}]
   (let [gil-json (json/generate-string gil)
         node-map-json (json/generate-string node-map)]
     (jdbc/execute-one!
      (db-opts conn)
      [(str "INSERT INTO " model-graph-artifact-table "
             (proposal_id, graph_kind, graph_id, graph_version, gil_json, node_map_json, checksum, created_by, created_at_utc)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             RETURNING *")
       proposal-id
       graph-kind
       graph-id
       graph-version
       gil-json
       node-map-json
       (sha256-hex (str gil-json ":" node-map-json ":" graph-id ":" graph-version))
       created-by
       (db-timestamptz (now-utc))]))))

(defn- persist-compiled-model-run!
  ([payload]
   (persist-compiled-model-run! db/ds payload))
  ([conn {:keys [release-id graph-artifact-id execution-backend status target-connection-id job-id external-run-id request-json response-json created-by completed-at]}]
   (jdbc/execute-one!
    (db-opts conn)
    [(str "INSERT INTO " compiled-model-run-table "
           (release_id, graph_artifact_id, execution_backend, status, target_connection_id, job_id, external_run_id,
            request_json, response_json, created_by, created_at_utc, completed_at_utc)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           RETURNING *")
     release-id
     graph-artifact-id
     execution-backend
     status
     target-connection-id
     job-id
     external-run-id
     (json/generate-string request-json)
     (when response-json (json/generate-string response-json))
     created-by
     (db-timestamptz (now-utc))
     (db-timestamptz completed-at)])))

(defn complete-model-run!
  [model-run-id {:keys [status response-json completed-at external-run-id]}]
  (ensure-modeling-tables!)
  (jdbc/execute!
   db/ds
   [(str "UPDATE " compiled-model-run-table
         " SET status = ?, response_json = ?, external_run_id = COALESCE(?, external_run_id), completed_at_utc = ?"
         " WHERE model_run_id = ?")
    status
    (when response-json (json/generate-string response-json))
    external-run-id
    (db-timestamptz (or completed-at (now-utc)))
    model-run-id]))

(defn link-model-run-to-request!
  ([model-run-id request-id]
   (link-model-run-to-request! db/ds model-run-id request-id))
  ([tx model-run-id request-id]
   (ensure-modeling-tables!)
   (jdbc/execute!
    tx
    [(str "UPDATE " compiled-model-run-table
          " SET execution_request_id = ? WHERE model_run_id = ?")
     (when request-id (UUID/fromString (str request-id)))
     model-run-id])))

(defn- update-model-run-progress!
  [model-run-id {:keys [status response-json external-run-id]}]
  (ensure-modeling-tables!)
  (jdbc/execute!
   db/ds
   [(str "UPDATE " compiled-model-run-table
         " SET status = ?, response_json = ?, external_run_id = COALESCE(?, external_run_id)"
         " WHERE model_run_id = ?")
    status
    (when response-json (json/generate-string response-json))
    external-run-id
    model-run-id]))

(defn- model-run-by-id
  [model-run-id]
  (jdbc/execute-one!
   (db-opts db/ds)
   [(str "SELECT * FROM " compiled-model-run-table " WHERE model_run_id = ?")
    model-run-id]))

(defn- model-run-row->response
  [run-row]
  (some-> run-row
          (update :request_json parse-json-safe)
          (update :response_json parse-json-safe)
          (update :created_at_utc str)
          (update :completed_at_utc str)
          (update :execution_request_id #(when % (str %)))))

(defn get-model-run
  [model-run-id]
  (some-> (model-run-by-id model-run-id) model-run-row->response))

(defn get-model-run-by-execution-request-id
  [request-id]
  (ensure-modeling-tables!)
  (some-> (jdbc/execute-one!
           (db-opts db/ds)
           [(str "SELECT * FROM " compiled-model-run-table
                 " WHERE execution_request_id = ?"
                 " ORDER BY created_at_utc DESC, model_run_id DESC LIMIT 1")
            (UUID/fromString (str request-id))])
          model-run-row->response))

(defn- databricks-run->status
  [response-body]
  (let [lifecycle    (some-> (or (get-in response-body [:state :life_cycle_state])
                                 (get-in response-body ["state" "life_cycle_state"]))
                             str
                             string/upper-case)
        result-state (some-> (or (get-in response-body [:state :result_state])
                                 (get-in response-body ["state" "result_state"]))
                             str
                             string/upper-case)]
    (cond
      (contains? #{"PENDING" "QUEUED" "RUNNING" "BLOCKED" "WAITING_FOR_RETRY"} lifecycle) "running"
      (and (= "TERMINATED" lifecycle) (= "SUCCESS" result-state)) "succeeded"
      (or (= "TIMEDOUT" lifecycle)
          (= "TIMED_OUT" lifecycle)
          (= "TIMEDOUT" result-state)
          (= "TIMED_OUT" result-state)) "timed_out"
      (or (= "INTERNAL_ERROR" lifecycle)
          (= "SKIPPED" result-state)
          (= "FAILED" result-state)
          (= "ERROR" result-state)
          (= "CANCELED" result-state)
          (= "CANCELLED" result-state)) "failed"
      (= "TERMINATED" lifecycle)
      (do
        (when-not (seq result-state)
          (log/warn "Databricks run terminated without result_state; treating as failed"
                    {:response_body response-body}))
        "failed")
      :else "running")))

(defn poll-silver-model-run!
  [model-run-id]
  (ensure-modeling-tables!)
  (let [run-row (model-run-by-id model-run-id)]
    (when-not run-row
      (throw (ex-info "Silver model run not found" {:model_run_id model-run-id :status 404})))
    (let [status (:status run-row)]
      (if (not (#{"submitted" "running"} status))
        (model-run-row->response run-row)
        (let [external-run-id (:external_run_id run-row)
              conn-id         (:target_connection_id run-row)]
          (when (string/blank? (str external-run-id))
            (throw (ex-info "Silver model run has no Databricks external run id"
                            {:model_run_id model-run-id :status 409})))
          (let [response          (dbx-jobs/get-run! conn-id external-run-id)
                next-status       (databricks-run->status (:body response))
                completed?        (#{"succeeded" "failed" "timed_out"} next-status)
                _                 (if completed?
                                    (complete-model-run! model-run-id {:status next-status
                                                                       :response-json response
                                                                       :external-run-id external-run-id
                                                                       :completed-at (now-utc)})
                                    (update-model-run-progress! model-run-id {:status next-status
                                                                              :response-json response
                                                                              :external-run-id external-run-id}))
                refreshed         (model-run-by-id model-run-id)]
            (model-run-row->response refreshed)))))))

(defn- terminal-model-run-status?
  [status]
  (contains? #{"succeeded" "failed" "timed_out" "cancelled"} (some-> status str string/lower-case)))

(defn- execute-databricks-model-run-under-polling!
  [pending-run poll-fn]
  (let [response     (dbx-jobs/trigger-job! (:conn_id pending-run) (:job_id pending-run) (:params pending-run))
        model-run-id (:model_run_id pending-run)
        max-polls    (max 1 (parse-int-env :bitool-databricks-max-polls 720))]
    (update-model-run-progress! model-run-id
                                {:status "submitted"
                                 :response-json response
                                 :external-run-id (some-> (:run_id response) str)})
    (loop [poll-count 0]
      (when (>= poll-count max-polls)
        (throw (ex-info "Databricks job poll timeout exceeded"
                        {:failure_class "transient_platform_error"
                         :model_run_id model-run-id
                         :poll_count poll-count
                         :max_polls max-polls})))
      (Thread/sleep (min 30000 (* 1000 (+ 5 (* 2 poll-count)))))
      (let [poll-result (poll-fn model-run-id)
            status      (some-> (:status poll-result) str string/lower-case)]
        (cond
          (= "succeeded" status)
          (-> pending-run
              (select-keys [:model_run_id :release_id :graph_artifact_id :graph_id :graph_version])
              (assoc :status "succeeded"
                     :backend "databricks_job"
                     :job_trigger response
                     :response_json (:response_json poll-result)))

          (contains? #{"failed" "timed_out" "cancelled"} status)
          (throw (ex-info (str "Databricks job " status)
                          {:failure_class (if (= "timed_out" status)
                                            "transient_platform_error"
                                            "permanent_model_error")
                           :model_run_id model-run-id
                           :databricks_status status
                           :response poll-result}))

          :else (recur (inc poll-count)))))))

(defn- bigquery-db-spec
  [conn-id]
  (db/create-dbspec-from-id conn-id))

(defn- bigquery-query-result->jdbc-result
  [{:keys [rows update-count]}]
  (if (some? update-count)
    [{:next.jdbc/update-count update-count}]
    (vec rows)))

(defn- persist-bigquery-warehouse-validation!
  [{:keys [conn-id
           proposal-id
           graph-artifact
           compile-result
           target-table
           validation-kind
           created-by]}]
  (let [response        (bigquery/dry-run-sql! (bigquery-db-spec conn-id)
                                               (:compiled_sql compile-result))
        validation-json (cond-> {:graph_artifact_id (:graph_artifact_id graph-artifact)
                                 :graph_id (:graph_id graph-artifact)
                                 :graph_version (:graph_version graph-artifact)
                                 :backend "bigquery_sql"
                                 :request {:proposal_id proposal-id
                                           :graph_artifact_id (:graph_artifact_id graph-artifact)
                                           :target_table target-table
                                           :dry_run true}
                                 :response response
                                 :sql_ir (:sql_ir compile-result)
                                 :sql_checksum (sha256-hex (:compiled_sql compile-result))}
                          (:estimated_bytes_processed response)
                          (assoc :estimated_bytes_processed (:estimated_bytes_processed response)))
        validation-row  (persist-model-validation! {:proposal-id proposal-id
                                                    :status "valid"
                                                    :validation-kind validation-kind
                                                    :validation-json validation-json
                                                    :compiled-sql (:compiled_sql compile-result)
                                                    :created-by created-by})]
    {:validation_id (:validation_id validation-row)
     :proposal_id proposal-id
     :status "valid"
     :backend "bigquery_sql"
     :graph_artifact_id (:graph_artifact_id graph-artifact)
     :validation validation-json}))

(defn- execute-pending-model-run!
  [pending-run poll-fn]
  (case (:warehouse pending-run)
    "bigquery"
    (let [response      (bigquery/execute-sql! (bigquery-db-spec (:conn_id pending-run))
                                               (:compiled_sql pending-run))
          result-rows   (bigquery-query-result->jdbc-result response)
          response-json {:result result-rows
                         :job (:job response)
                         :raw (:raw response)}]
      (complete-model-run! (:model_run_id pending-run)
                           {:status "succeeded"
                            :response-json response-json
                            :completed-at (now-utc)})
      (-> pending-run
          (select-keys [:model_run_id :release_id :graph_artifact_id :graph_id :graph_version])
          (assoc :response result-rows
                 :status "succeeded"
                 :backend "bigquery_sql")))

    "snowflake"
    (let [response (jdbc/execute! (db/get-opts (:conn_id pending-run) nil)
                                  [(:compiled_sql pending-run)])]
      (complete-model-run! (:model_run_id pending-run)
                           {:status "succeeded"
                            :response-json {:result response}
                            :completed-at (now-utc)})
      (-> pending-run
          (select-keys [:model_run_id :release_id :graph_artifact_id :graph_id :graph_version])
          (assoc :response response
                 :status "succeeded"
                 :backend "snowflake_sql")))

    "postgresql"
    (let [response (execute-postgresql-materialization! (:conn_id pending-run)
                                                        (:sql_ir pending-run)
                                                        (:compiled_sql pending-run))]
      (complete-model-run! (:model_run_id pending-run)
                           {:status "succeeded"
                            :response-json {:result response}
                            :completed-at (now-utc)})
      (-> pending-run
          (select-keys [:model_run_id :release_id :graph_artifact_id :graph_id :graph_version])
          (assoc :response response
                 :status "succeeded"
                 :backend "postgresql_sql")))

    (execute-databricks-model-run-under-polling! pending-run poll-fn)))

(defn- reconcilable-model-runs
  [limit]
  (ensure-modeling-tables!)
  (jdbc/execute!
   (db-opts db/ds)
   [(str "SELECT model_run_id, status, target_connection_id, external_run_id
          FROM " compiled-model-run-table "
          WHERE status IN ('submitted', 'running')
          ORDER BY created_at_utc ASC, model_run_id ASC
          LIMIT ?")
    (max 1 (long limit))]))

(defn reconcile-silver-model-runs!
  ([] (reconcile-silver-model-runs! {}))
  ([{:keys [limit]
     :or {limit 25}}]
   (mapv (fn [run-row]
           (try
             (poll-silver-model-run! (:model_run_id run-row))
             (catch Exception e
               (log/error e "Failed to reconcile Silver model run"
                          {:model_run_id (:model_run_id run-row)})
               {:model_run_id (:model_run_id run-row)
                :status "reconcile_failed"
                :error (.getMessage e)})))
         (reconcilable-model-runs limit))))

(defn- lock-release-stream!
  [conn layer target-model]
  (jdbc/execute-one!
   conn
   ["SELECT pg_advisory_xact_lock(hashtext(?))"
    (str layer ":" target-model)]))

(defn- publish-proposal-tx!
  [tx proposal-id proposal-row validation-result graph-artifact-id created-by proposal-json sql-ir]
  (lock-release-stream! tx (:layer proposal-row) (:target_model proposal-row))
  (let [version      (next-release-version tx (:layer proposal-row) (:target_model proposal-row))
        published-at (now-utc)
        _            (deactivate-active-releases! tx (:layer proposal-row) (:target_model proposal-row))
        release-row  (persist-model-release! tx {:proposal-id proposal-id
                                                 :validation-id (:validation_id validation-result)
                                                 :graph-artifact-id graph-artifact-id
                                                 :layer (:layer proposal-row)
                                                 :target-model (:target_model proposal-row)
                                                 :version version
                                                 :status "published"
                                                 :active true
                                                 :created-by created-by
                                                 :published-at published-at})
        artifact-row (persist-compiled-model-artifact! tx {:release-id (:release_id release-row)
                                                           :artifact-kind "sql"
                                                           :sql-ir sql-ir
                                                           :sql-text (:compiled_sql validation-result)
                                                           :validation-json (:validation validation-result)
                                                           :created-by created-by})]
    (update-model-proposal! tx proposal-id {:status "published"
                                            :compiled_sql (:compiled_sql validation-result)})
    {:proposal_id proposal-id
     :release_id (:release_id release-row)
     :artifact_id (:artifact_id artifact-row)
     :target_model (:target_model proposal-row)
     :layer (:layer proposal-row)
     :version (:version release-row)
     :compiled_sql (:compiled_sql validation-result)
     :validation_id (:validation_id validation-result)
     :status "published"
     :proposal proposal-json}))

(defn- schema-source-for-endpoint
  [graph-id api-node-id source-system endpoint target]
  (let [endpoint-fields (vec (or (:inferred_fields endpoint) []))]
    (try
      (let [snapshot-row (latest-endpoint-schema-snapshot graph-id api-node-id source-system endpoint target)
            snapshot-fields (vec (or (parse-json-safe (:inferred_fields_json snapshot-row)) []))]
        (cond
          (seq snapshot-fields)
          {:source "endpoint_schema_snapshot"
           :snapshot-row snapshot-row
           :fields snapshot-fields}

          (seq endpoint-fields)
          {:source "endpoint_config"
           :snapshot-row nil
           :fields endpoint-fields}

          :else
          (throw (ex-info "No inferred Bronze schema is available for this endpoint"
                          {:graph_id graph-id
                           :api_node_id api-node-id
                           :endpoint_name (:endpoint_name endpoint)}))))
      (catch clojure.lang.ExceptionInfo e
        (if (and (= "target_connection" (:failure_class (ex-data e)))
                 (seq endpoint-fields))
          {:source "endpoint_config"
           :snapshot-row nil
           :fields endpoint-fields
           :fallback_reason (:failure_class (ex-data e))}
          (throw e))))))

(defn- proposal-columns
  [proposal-json]
  (vec (or (:columns proposal-json) [])))

(defn- proposal-mappings
  [proposal-json]
  (vec (or (:mappings proposal-json) [])))

(defn- source-table-for-endpoint
  [target endpoint]
  (or (non-blank-str (:bronze_table_name endpoint))
      (non-blank-str (:table_name endpoint))
      (when-let [target-table (non-blank-str (:table_name target))]
        (db/fully-qualified-table-name target target-table))
      (db/fully-qualified-table-name
       target
       (sanitize-ident
        (or (non-blank-str (:endpoint_name endpoint))
            (non-blank-str (:endpoint_url endpoint))
            (non-blank-str (:topic_name endpoint))
            (non-blank-str (:path endpoint))
            "bronze_auto")))))

(defn- sample-limit
  [value]
  (-> (or value 100)
      long
      (max 1)
      (min 500)))

(defn- split-qualified-table
  [table-name]
  (let [parts (string/split (or table-name "") #"\.")]
    {:catalog (when (= 3 (count parts)) (nth parts 0))
     :schema  (cond
                (= 3 (count parts)) (nth parts 1)
                (= 2 (count parts)) (nth parts 0)
                :else "")
     :table   (last parts)}))

(defn- resolve-proposal-context
  [proposal-id]
  (let [proposal-row (proposal-by-id proposal-id)]
    (when-not proposal-row
      (throw (ex-info "Silver proposal not found" {:proposal_id proposal-id :status 404})))
    (when-not (= "silver" (:layer proposal-row))
      (throw (ex-info "Proposal is not a Silver proposal"
                      {:proposal_id proposal-id
                       :layer (:layer proposal-row)})))
    (let [proposal-json (parse-json-safe (:proposal_json proposal-row))
          profile-row   (schema-profile-by-id (:profile_id proposal-row))
          profile-json  (parse-json-safe (:profile_json profile-row))
          graph-id      (:source_graph_id proposal-row)
          source-node-id (:source_node_id proposal-row)
          graph         (db/getGraph graph-id)
          source-node   (g2/getData graph source-node-id)
          _             (when-not (contains? #{"Ap" "Kf" "Fs"} (:btype source-node))
                          (throw (ex-info "Proposal source node is not a supported Bronze source node"
                                          {:proposal_id proposal-id
                                           :node_id source-node-id
                                           :btype (:btype source-node)})))
          endpoint      (select-source-endpoint! source-node (:source_endpoint_name proposal-row))
          target        (find-downstream-target graph source-node-id)]
      {:proposal-row proposal-row
       :proposal-json proposal-json
       :profile-row profile-row
       :profile-json profile-json
       :graph graph
       :graph-id graph-id
       :api-node-id source-node-id
       :source-node-id source-node-id
       :api-node source-node
       :source-node source-node
       :endpoint endpoint
       :target target
       :source-system (or (:source_system source-node) "samara")
       :source-table (source-table-for-endpoint target endpoint)
       :target-connection-id (target-connection-id target)})))

(defn- resolve-gold-proposal-context
  [proposal-id]
  (let [proposal-row (proposal-by-id proposal-id)]
    (when-not proposal-row
      (throw (ex-info "Gold proposal not found" {:proposal_id proposal-id :status 404})))
    (when-not (= "gold" (:layer proposal-row))
      (throw (ex-info "Proposal is not a Gold proposal"
                      {:proposal_id proposal-id
                       :layer (:layer proposal-row)})))
    (let [proposal-json (parse-json-safe (:proposal_json proposal-row))
          profile-row   (schema-profile-by-id (:profile_id proposal-row))
          profile-json  (parse-json-safe (:profile_json profile-row))
          graph-id      (:source_graph_id proposal-row)
          source-node-id (:source_node_id proposal-row)
          graph         (db/getGraph graph-id)
          source-node   (g2/getData graph source-node-id)
          target        (find-downstream-target graph source-node-id)
          source-table  (or (:source_table proposal-json)
                            (throw (ex-info "Gold proposal does not declare a Silver source table"
                                            {:proposal_id proposal-id :status 409})))]
      {:proposal-row proposal-row
       :proposal-json proposal-json
       :profile-row profile-row
       :profile-json profile-json
       :graph graph
       :graph-id graph-id
       :source-node-id source-node-id
       :source-node source-node
       :target target
       :source-system (or (:source_system proposal-json)
                          (:source_system source-node)
                          "medallion")
       :source-table source-table
       :target-connection-id (target-connection-id target)})))

(defn- available-source-columns
  [proposal-json profile-json]
  (let [profile-columns (->> (get profile-json :field_types)
                             keys
                             (mapv (fn [k]
                                     (cond
                                       (keyword? k) (name k)
                                       (nil? k) nil
                                       :else (str k))))
                             (remove nil?)
                             set)
        proposal-cols   (->> (proposal-columns proposal-json) (mapcat :source_columns) set)
        mapping-cols    (->> (proposal-mappings proposal-json) (mapcat :source_columns) set)]
    (cond
      (seq profile-columns) profile-columns
      (seq proposal-cols) proposal-cols
      :else mapping-cols)))

(defn- static-validation-errors
  [proposal-json source-table profile-json]
  (let [columns         (proposal-columns proposal-json)
        target-columns  (mapv :target_column columns)
        target-column-set (set target-columns)
        mappings        (proposal-mappings proposal-json)
        materialization (:materialization proposal-json)
        warehouse       (some-> (or (:target_warehouse proposal-json)
                                    (:target_kind proposal-json)
                                    "databricks")
                                str
                                string/lower-case)
        merge-keys      (vec (or (:keys materialization) []))
        update-assignments (vec (or (:update_assignments materialization) []))
        when-not-matched-by-source (:when_not_matched_by_source materialization)
        group-by-cols   (vec (or (:group_by proposal-json) []))
        target-table    (:target_table proposal-json)
        processing-policy (:processing_policy proposal-json)
        normalized-policy (normalize-processing-policy processing-policy)
        business-keys   (vec (or (:business_keys normalized-policy) []))
        ordering-strategy (:ordering_strategy normalized-policy)
        event-time-column (:event_time_column normalized-policy)
        sequence-column   (:sequence_column normalized-policy)
        source-alias    (or (:source_alias proposal-json)
                            (:source_layer proposal-json)
                            "bronze")
        graph-filter    (some-> (:graph_filter_sql proposal-json) str string/trim not-empty)
        available-cols  (available-source-columns proposal-json profile-json)]
    (vec
     (concat
      (when-not (seq columns)
        [{:kind "schema" :message "Proposal must declare at least one target column"}])
      (when-not (valid-qualified-table-name? source-table)
        [{:kind "schema" :field "source_table" :message "Source table must be a valid qualified table name"}])
      (when-not (valid-qualified-table-name? target-table)
        [{:kind "schema" :field "target_table" :message "Target table must be a valid qualified table name"}])
      (when-not (valid-sql-ident? source-alias)
        [{:kind "schema" :field "source_alias" :message "Source alias must be a valid SQL identifier"}])
      (when-not (every? #(valid-sql-ident? (:target_column %)) columns)
        [{:kind "schema" :message "All target columns must be valid SQL identifiers"}])
      (when-not (= (count target-columns) (count (distinct target-columns)))
        [{:kind "schema" :message "Target columns must be unique"}])
      (when-not (every? #(contains? supported-silver-types (source-field-type %)) columns)
        [{:kind "schema" :message "Proposal contains unsupported target column types"}])
      (when-not (every? #(safe-proposal-expression? (:expression %)) columns)
        [{:kind "schema" :message "All compiled expressions must use supported SQL expression syntax and source references"}])
      (when (and graph-filter
                 (or (re-find #"(?:--|/\*|\*/|;)" graph-filter)
                     (re-find #"(?i)\b(select|insert|update|delete|drop|alter|merge|union|join|from|into|copy|put)\b" graph-filter)
                     (not (re-matches #"[A-Za-z0-9_.'\"()=<>!%+\-*/,\s]+" graph-filter))))
        [{:kind "schema" :field "graph_filter_sql" :message "Graph filter contains unsupported SQL"}])
      (when-not (materialization-mode-supported? warehouse (:mode materialization))
        [{:kind "schema"
          :message (if (= warehouse "databricks")
                     "Materialization mode must be one of merge, table_replace, append, update, or delete"
                     "Materialization mode must be one of merge, table_replace, or append")}])
      (when (and (= "merge" (:mode materialization)) (empty? merge-keys))
        [{:kind "mapping" :message "Merge materialization requires at least one merge key"}])
      (when (and (= "update" (:mode materialization)) (empty? merge-keys))
        [{:kind "mapping" :message "Update materialization requires at least one merge key"}])
      (when (and (= "delete" (:mode materialization)) (empty? merge-keys))
        [{:kind "mapping" :message "Delete materialization requires at least one merge key"}])
      (when (and (seq merge-keys)
                 (not-every? target-column-set merge-keys))
        [{:kind "mapping" :message "All merge keys must be target columns"}])
      (when (and (not= warehouse "databricks")
                 (or (contains? materialization :schema_evolution)
                     (contains? materialization :update_on_matched)
                     (contains? materialization :insert_on_not_matched)
                     (seq update-assignments)
                     (map? when-not-matched-by-source)))
        [{:kind "schema"
          :field "materialization"
          :message "Advanced materialization options are currently supported only for Databricks targets"}])
      (when (and (contains? materialization :schema_evolution)
                 (not= "merge" (:mode materialization)))
        [{:kind "schema"
          :field "materialization.schema_evolution"
          :message "Schema evolution is only supported for merge materialization"}])
      (when (and (seq update-assignments)
                 (not-every? #(contains? target-column-set (:target_column %)) update-assignments))
        [{:kind "mapping"
          :field "materialization.update_assignments"
          :message "All update assignment target columns must be target columns"}])
      (when (and (seq update-assignments)
                 (not-every? #(safe-databricks-sql-fragment? (:expression %)
                                                            {:allow-literal-only? true})
                             update-assignments))
        [{:kind "mapping"
          :field "materialization.update_assignments"
          :message "All update assignment expressions must use supported Databricks SQL syntax"}])
      (when (and (some-> (:matched_condition materialization) seq)
                 (not (safe-databricks-sql-fragment? (:matched_condition materialization)
                                                    {:require-ref? true
                                                     :allow-or? false})))
        [{:kind "mapping"
          :field "materialization.matched_condition"
          :message "Matched condition must use supported Databricks SQL syntax"}])
      (when (and (some-> (:not_matched_condition materialization) seq)
                 (not (safe-databricks-sql-fragment? (:not_matched_condition materialization)
                                                    {:require-ref? true
                                                     :allow-or? false})))
        [{:kind "mapping"
          :field "materialization.not_matched_condition"
          :message "Not matched condition must use supported Databricks SQL syntax"}])
      (when (and (map? when-not-matched-by-source)
                 (not= "merge" (:mode materialization)))
        [{:kind "mapping"
          :field "materialization.when_not_matched_by_source"
          :message "WHEN NOT MATCHED BY SOURCE options are only supported for merge materialization"}])
      (when (and (map? when-not-matched-by-source)
                 (not (contains? #{"delete" "update"} (:action when-not-matched-by-source))))
        [{:kind "mapping"
          :field "materialization.when_not_matched_by_source.action"
          :message "WHEN NOT MATCHED BY SOURCE action must be delete or update"}])
      (when (and (= "update" (:action when-not-matched-by-source))
                 (empty? (or (:assignments when-not-matched-by-source) [])))
        [{:kind "mapping"
          :field "materialization.when_not_matched_by_source.assignments"
          :message "WHEN NOT MATCHED BY SOURCE update requires one or more assignments"}])
      (when (and (seq (or (:assignments when-not-matched-by-source) []))
                 (not-every? #(contains? target-column-set (:target_column %))
                             (:assignments when-not-matched-by-source)))
        [{:kind "mapping"
          :field "materialization.when_not_matched_by_source.assignments"
          :message "WHEN NOT MATCHED BY SOURCE assignments must target proposal columns"}])
      (when (and (seq (or (:assignments when-not-matched-by-source) []))
                 (not-every? #(safe-databricks-sql-fragment? (:expression %)
                                                            {:allow-literal-only? true})
                             (:assignments when-not-matched-by-source)))
        [{:kind "mapping"
          :field "materialization.when_not_matched_by_source.assignments"
          :message "WHEN NOT MATCHED BY SOURCE assignment expressions must use supported Databricks SQL syntax"}])
      (when (and (some-> (:condition when-not-matched-by-source) seq)
                 (not (safe-databricks-sql-fragment? (:condition when-not-matched-by-source)
                                                    {:require-ref? true
                                                     :allow-or? false})))
        [{:kind "mapping"
          :field "materialization.when_not_matched_by_source.condition"
          :message "WHEN NOT MATCHED BY SOURCE condition must use supported Databricks SQL syntax"}])
      (when (and (seq group-by-cols)
                 (not-every? target-column-set group-by-cols))
        [{:kind "mapping" :message "All group-by columns must be target columns"}])
      (when (and (some? processing-policy) (not (map? processing-policy)))
        [{:kind "processing_policy" :message "Processing policy must be an object"}])
      (when (and ordering-strategy
                 (not (contains? allowed-ordering-strategies ordering-strategy)))
        [{:kind "processing_policy"
          :field "ordering_strategy"
          :message "Ordering strategy must be one of latest_event_time_wins, latest_sequence_wins, event_time_then_sequence, or append_only"}])
      (when (and (seq business-keys)
                 (not-every? target-column-set business-keys))
        [{:kind "processing_policy"
          :field "business_keys"
          :message "Processing policy business keys must all be target columns"}])
      (when (and event-time-column
                 (not (contains? target-column-set event-time-column)))
        [{:kind "processing_policy"
          :field "event_time_column"
          :message "Processing policy event time column must be a target column"}])
      (when (and sequence-column
                 (not (contains? target-column-set sequence-column)))
        [{:kind "processing_policy"
          :field "sequence_column"
          :message "Processing policy sequence column must be a target column"}])
      (when (and ordering-strategy
                 (not= "append_only" ordering-strategy)
                 (empty? business-keys))
        [{:kind "processing_policy"
          :field "business_keys"
          :message "Latest-state ordering requires one or more business keys"}])
      (when (and (= "latest_event_time_wins" ordering-strategy)
                 (not event-time-column))
        [{:kind "processing_policy"
          :field "event_time_column"
          :message "latest_event_time_wins requires an event time column"}])
      (when (and (= "latest_sequence_wins" ordering-strategy)
                 (not sequence-column))
        [{:kind "processing_policy"
          :field "sequence_column"
          :message "latest_sequence_wins requires a sequence column"}])
      (when (and (= "event_time_then_sequence" ordering-strategy)
                 (not event-time-column))
        [{:kind "processing_policy"
          :field "event_time_column"
          :message "event_time_then_sequence requires an event time column"}])
      (when (and (= "event_time_then_sequence" ordering-strategy)
                 (not sequence-column))
        [{:kind "processing_policy"
          :field "sequence_column"
          :message "event_time_then_sequence requires a sequence column"}])
      (when (and (get normalized-policy :late_data_mode)
                 (not (contains? allowed-late-data-modes (:late_data_mode normalized-policy))))
        [{:kind "processing_policy"
          :field "late_data_mode"
          :message "Late data mode must be one of merge or append"}])
      (when (and (get normalized-policy :too_late_behavior)
                 (not (contains? allowed-too-late-behaviors (:too_late_behavior normalized-policy))))
        [{:kind "processing_policy"
          :field "too_late_behavior"
          :message "Too-late behavior must be one of accept, quarantine, or drop"}])
      (duration-window-errors "late_data_tolerance" (:late_data_tolerance processing-policy))
      (duration-window-errors "reprocess_window" (:reprocess_window processing-policy))
      (mapcat (fn [mapping]
                (concat
                 (when-not (valid-sql-ident? (:target_column mapping))
                   [{:kind "mapping"
                     :target_column (:target_column mapping)
                     :message "Mapping target column must be a valid SQL identifier"}])
                 (when-not (safe-proposal-expression? (:expression mapping))
                   [{:kind "mapping"
                     :target_column (:target_column mapping)
                     :message "Mapping expression must use supported SQL expression syntax and source references"}])
                 (when (seq (:source_columns mapping))
                   (for [source-column (:source_columns mapping)
                         :when (not (contains? available-cols source-column))]
                     {:kind "mapping"
                      :target_column (:target_column mapping)
                      :source_column source-column
                      :message "Mapping source column does not resolve against the proposal schema snapshot"}))))
              mappings)))))

(defn- compile-proposal*
  [{:keys [proposal-json source-table profile-json target target-connection-id]}]
  (let [errors (static-validation-errors proposal-json source-table profile-json)]
    (when (seq errors)
      (throw (ex-info "Silver proposal failed static validation"
                      {:status 400
                       :errors errors})))
    (compiler/compile-model {:target-warehouse (proposal-target-warehouse {:proposal-json proposal-json
                                                                           :target target
                                                                           :target-connection-id target-connection-id})
                             :proposal-json proposal-json
                             :source-table source-table})))

(defn- row-value
  [row column-name]
  (or (get row (keyword column-name))
      (get row (keyword (string/lower-case column-name)))
      (get row column-name)))

(defn- sample-row-summary
  [rows target-columns key-columns]
  (let [row-count        (count rows)
        null-counts      (into {}
                               (map (fn [column-name]
                                      [column-name (count (filter nil? (map #(row-value % column-name) rows)))])
                                    target-columns))
        duplicate-keys   (if (seq key-columns)
                           (let [keys-seq (map (fn [row]
                                                 (mapv #(row-value row %) key-columns))
                                               rows)]
                             (- row-count (count (set keys-seq))))
                           0)]
    {:row_count row-count
     :duplicate_key_count duplicate-keys
     :null_counts null-counts
     :row_count_plausible (pos? row-count)
     :casts_succeeded true}))

(defn- run-sample-execution!
  [{:keys [proposal-json target-connection-id]} select-sql limit]
  (when-not target-connection-id
    (throw (ex-info "No target connection is available for sample execution"
                    {:failure_class "target_connection"})))
  (let [limited-sql (str select-sql " LIMIT " (sample-limit limit))
        dbtype      (some-> (connection-dbtype target-connection-id) string/lower-case)
        query-task  (future
                      (if (= "bigquery" dbtype)
                        (:rows (bigquery/execute-sql! (bigquery-db-spec target-connection-id) limited-sql))
                        (jdbc/execute! (db/get-opts target-connection-id nil) [limited-sql])))
        rows        (deref query-task sample-query-timeout-ms ::timed-out)
        target-cols (mapv :target_column (proposal-columns proposal-json))
        key-cols    (vec (or (get-in proposal-json [:materialization :keys]) []))]
    (when (= ::timed-out rows)
      (future-cancel query-task)
      (throw (ex-info "Sample execution timed out"
                      {:failure_class "sample_execution_timeout"
                       :timeout_ms sample-query-timeout-ms})))
    {:sample_sql limited-sql
     :sample_limit (sample-limit limit)
     :summary (sample-row-summary rows target-cols key-cols)}))

(defn- silver-graph-name
  [proposal-row]
  (str "silver-" (sanitize-ident (:target_model proposal-row)) "-proposal-" (:proposal_id proposal-row)))

(defn- gold-graph-name
  [proposal-row]
  (str "gold-" (sanitize-ident (:target_model proposal-row)) "-proposal-" (:proposal_id proposal-row)))

(defn- proposal->mapping-items
  [proposal-json]
  (mapv (fn [mapping]
          {:target_column (:target_column mapping)
           :expression (:expression mapping)
           :transform (vec (or (:transform mapping) []))
           :source_paths (vec (or (:source_paths mapping) []))
           :source_columns (vec (or (:source_columns mapping) []))
           :rule_source (:rule_source mapping)})
        (proposal-mappings proposal-json)))

(defn- merge-key-filter-sql
  [proposal-json]
  (let [keys (vec (or (get-in proposal-json [:materialization :keys]) []))]
    (when (seq keys)
      (string/join " AND " (map #(str % " IS NOT NULL") keys)))))

(defn- build-silver-gil
  [{:keys [proposal-row proposal-json source-table target source-system]}]
  (let [source-ref      "bronze_source"
        projection-ref  "projection"
        mapping-ref     "mapping"
        filter-sql      (merge-key-filter-sql proposal-json)
        filter-ref      (when filter-sql "filter_keys")
        target-ref      "target"
        output-ref      "output"
        table-parts     (split-qualified-table (:target_table proposal-json))
        processing-policy (normalize-processing-policy (:processing_policy proposal-json))
        target-kind     (or (:target_kind target) "databricks")
        target-config   {:target_kind target-kind
                         :connection_id (target-connection-id target)
                         :catalog (or (:catalog target) (:catalog table-parts) "")
                         :schema (or (:schema target) (:schema table-parts) "")
                         :table_name (:table table-parts)
                         :write_mode (case (get-in proposal-json [:materialization :mode])
                                       "table_replace" "replace"
                                       "append" "append"
                                       "update" "update"
                                       "delete" "delete"
                                       "merge")
                         :table_format (if (= "bigquery" (some-> target-kind str string/lower-case))
                                         "table"
                                         "delta")
                         :merge_keys (vec (or (get-in proposal-json [:materialization :keys]) []))
                         :options (merge (or (:options target) {})
                                         (materialization-extra-options proposal-json))
                         :silver_job_id (or (:silver_job_id target) "")
                         :silver_job_params (merge {:target_model (:target_model proposal-row)
                                                    :proposal_id (:proposal_id proposal-row)}
                                                   (or (:silver_job_params target) {}))
                         :model_layer "silver"
                         :managed_release true
                         :processing_policy processing-policy
                         :business_keys (vec (or (:business_keys processing-policy) []))
                         :event_time_column (:event_time_column processing-policy)
                         :sequence_column (:sequence_column processing-policy)
                         :ordering_strategy (:ordering_strategy processing-policy)
                         :late_data_tolerance (:late_data_tolerance processing-policy)
                         :late_data_mode (:late_data_mode processing-policy)
                         :too_late_behavior (:too_late_behavior processing-policy)
                         :reprocess_window (:reprocess_window processing-policy)}
        nodes          (cond-> [{:node-ref source-ref
                                 :type "table"
                                 :alias (last-table-segment source-table)
                                 :config {:table_name source-table
                                          :source_layer "bronze"
                                          :source_system source-system
                                          :model_ref source-table}}
                                {:node-ref projection-ref
                                 :type "projection"
                                 :alias (str "projection-" (:target_model proposal-row))
                                 :config {:columns (mapv #(select-keys % [:target_column :type :nullable :source_columns :source_paths :expression])
                                                         (proposal-columns proposal-json))
                                          :proposal_id (:proposal_id proposal-row)}}
                                {:node-ref mapping-ref
                                 :type "mapping"
                                 :alias (str "mapping-" (:target_model proposal-row))
                                 :config {:mapping (proposal->mapping-items proposal-json)
                                          :proposal_id (:proposal_id proposal-row)}}]
                         filter-ref (conj {:node-ref filter-ref
                                           :type "filter"
                                           :alias (str "filter-" (:target_model proposal-row))
                                           :config {:sql filter-sql
                                                    :purpose "merge_key_not_null"}})
                         true (conj {:node-ref target-ref
                                     :type "target"
                                     :alias (:target_model proposal-row)
                                     :config target-config}
                                    {:node-ref output-ref
                                     :type "output"
                                     :alias "Output"}))
        edges          (vec (concat [[source-ref projection-ref]
                                     [projection-ref mapping-ref]]
                                    (if filter-ref
                                      [[mapping-ref filter-ref]
                                       [filter-ref target-ref]]
                                      [[mapping-ref target-ref]])
                                    [[target-ref output-ref]]))]
    {:gil-version "1.0"
     :intent :build
     :graph-name (silver-graph-name proposal-row)
     :nodes nodes
     :edges edges}))

(defn- build-gold-gil
  [{:keys [proposal-row proposal-json source-table target source-system]}]
  (let [source-ref      "silver_source"
        projection-ref  "projection"
        mapping-ref     "mapping"
        filter-sql      (merge-key-filter-sql proposal-json)
        filter-ref      (when filter-sql "filter_keys")
        target-ref      "target"
        output-ref      "output"
        table-parts     (split-qualified-table (:target_table proposal-json))
        processing-policy (normalize-processing-policy (:processing_policy proposal-json))
        target-kind     (or (:target_kind target) "databricks")
        target-config   {:target_kind target-kind
                         :connection_id (target-connection-id target)
                         :catalog (or (:catalog target) (:catalog table-parts) "")
                         :schema (or (:schema target) (:schema table-parts) "")
                         :table_name (:table table-parts)
                         :write_mode (case (get-in proposal-json [:materialization :mode])
                                       "table_replace" "replace"
                                       "append" "append"
                                       "update" "update"
                                       "delete" "delete"
                                       "merge")
                         :table_format (if (= "bigquery" (some-> target-kind str string/lower-case))
                                         "table"
                                         "delta")
                         :merge_keys (vec (or (get-in proposal-json [:materialization :keys]) []))
                         :options (merge (or (:options target) {})
                                         (materialization-extra-options proposal-json))
                         :gold_job_id (or (:gold_job_id target) "")
                         :gold_job_params (merge {:target_model (:target_model proposal-row)
                                                  :proposal_id (:proposal_id proposal-row)}
                                                 (or (:gold_job_params target) {}))
                         :model_layer "gold"
                         :managed_release true
                         :processing_policy processing-policy
                         :business_keys (vec (or (:business_keys processing-policy) []))
                         :event_time_column (:event_time_column processing-policy)
                         :sequence_column (:sequence_column processing-policy)
                         :ordering_strategy (:ordering_strategy processing-policy)
                         :late_data_tolerance (:late_data_tolerance processing-policy)
                         :late_data_mode (:late_data_mode processing-policy)
                         :too_late_behavior (:too_late_behavior processing-policy)
                         :reprocess_window (:reprocess_window processing-policy)}
        nodes           (cond-> [{:node-ref source-ref
                                  :type "table"
                                  :alias (last-table-segment source-table)
                                  :config {:table_name source-table
                                           :source_layer "silver"
                                           :source_system source-system
                                           :model_ref source-table}}
                                 {:node-ref projection-ref
                                  :type "projection"
                                  :alias (str "projection-" (:target_model proposal-row))
                                  :config {:columns (mapv #(select-keys % [:target_column :type :nullable :source_columns :source_paths :expression])
                                                          (proposal-columns proposal-json))
                                           :proposal_id (:proposal_id proposal-row)
                                           :group_by (vec (or (:group_by proposal-json) []))}}
                                 {:node-ref mapping-ref
                                  :type "mapping"
                                  :alias (str "mapping-" (:target_model proposal-row))
                                  :config {:mapping (proposal->mapping-items proposal-json)
                                           :proposal_id (:proposal_id proposal-row)
                                           :group_by (vec (or (:group_by proposal-json) []))}}]
                          filter-ref (conj {:node-ref filter-ref
                                            :type "filter"
                                            :alias (str "filter-" (:target_model proposal-row))
                                            :config {:sql filter-sql
                                                     :purpose "merge_key_not_null"}})
                          true (conj {:node-ref target-ref
                                      :type "target"
                                      :alias (:target_model proposal-row)
                                      :config target-config}
                                     {:node-ref output-ref
                                      :type "output"
                                      :alias "Output"}))
        edges           (vec (concat [[source-ref projection-ref]
                                      [projection-ref mapping-ref]]
                                     (if filter-ref
                                       [[mapping-ref filter-ref]
                                        [filter-ref target-ref]]
                                       [[mapping-ref target-ref]])
                                     [[target-ref output-ref]]))]
    {:gil-version "1.0"
     :intent :build
     :graph-name (gold-graph-name proposal-row)
     :nodes nodes
     :edges edges}))

(defn- gil-node-by-type
  [gil node-type]
  (first (filter #(= node-type (:type %)) (or (:nodes gil) []))))

(defn- target-config->qualified-table
  [target-config]
  (string/join "."
               (remove string/blank?
                       [(or (:catalog target-config) "")
                        (or (:schema target-config) "")
                        (or (:table_name target-config) "")])))

(defn- proposal-json-from-silver-gil
  [gil]
  (let [source-node     (gil-node-by-type gil "table")
        projection-node (gil-node-by-type gil "projection")
        mapping-node    (gil-node-by-type gil "mapping")
        filter-node     (gil-node-by-type gil "filter")
        target-node     (gil-node-by-type gil "target")
        target-config   (:config target-node)
        columns         (vec (or (get-in projection-node [:config :columns]) []))
        mappings        (vec (or (get-in mapping-node [:config :mapping]) []))]
    (when-not (and source-node projection-node mapping-node target-node)
      (throw (ex-info "Silver graph artifact is missing required compiler nodes"
                      {:status 409
                       :required ["table" "projection" "mapping" "target"]})))
    {:layer "silver"
     :source_layer "bronze"
     :target_model (or (:alias target-node) (:table_name target-config))
     :target_table (target-config->qualified-table target-config)
     :target_warehouse (or (:target_kind target-config) "databricks")
     :columns columns
     :mappings mappings
     :materialization (materialization-from-target-config target-config)
     :graph_filter_sql (get-in filter-node [:config :sql])}))

(defn- proposal-json-from-gold-gil
  [gil]
  (let [source-node     (gil-node-by-type gil "table")
        projection-node (gil-node-by-type gil "projection")
        mapping-node    (gil-node-by-type gil "mapping")
        filter-node     (gil-node-by-type gil "filter")
        target-node     (gil-node-by-type gil "target")
        target-config   (:config target-node)
        columns         (vec (or (get-in projection-node [:config :columns]) []))
        mappings        (vec (or (get-in mapping-node [:config :mapping]) []))
        group-by        (vec (or (get-in projection-node [:config :group_by])
                                 (get-in mapping-node [:config :group_by])
                                 []))]
    (when-not (and source-node projection-node mapping-node target-node)
      (throw (ex-info "Gold graph artifact is missing required compiler nodes"
                      {:status 409
                       :required ["table" "projection" "mapping" "target"]})))
    {:layer "gold"
     :source_layer "silver"
     :source_alias "silver"
     :source_model (last-table-segment (get-in source-node [:config :table_name]))
     :source_table (get-in source-node [:config :table_name])
     :target_model (or (:alias target-node) (:table_name target-config))
     :target_table (target-config->qualified-table target-config)
     :target_warehouse (or (:target_kind target-config) "databricks")
     :columns columns
     :mappings mappings
     :group_by group-by
     :materialization (materialization-from-target-config target-config)
     :graph_filter_sql (get-in filter-node [:config :sql])}))

(defn compile-silver-graph-artifact!
  [graph-artifact-id]
  (ensure-modeling-tables!)
  (let [graph-artifact (graph-artifact-by-id graph-artifact-id)]
    (when-not graph-artifact
      (throw (ex-info "Silver graph artifact not found"
                      {:graph_artifact_id graph-artifact-id
                       :status 404})))
    (let [gil          (parse-json-safe (:gil_json graph-artifact))
          proposal-json (proposal-json-from-silver-gil gil)
          source-node   (gil-node-by-type gil "table")
          source-table  (get-in source-node [:config :table_name])]
      (compile-proposal* {:proposal-json proposal-json
                          :source-table source-table
                          :profile-json {:field_types (into {}
                                                            (map (fn [column]
                                                                   [(:target_column column)
                                                                    {:type (:type column)}]))
                                                            (proposal-columns proposal-json))}}))))

(defn compile-gold-graph-artifact!
  [graph-artifact-id]
  (ensure-modeling-tables!)
  (let [graph-artifact (graph-artifact-by-id graph-artifact-id)]
    (when-not graph-artifact
      (throw (ex-info "Gold graph artifact not found"
                      {:graph_artifact_id graph-artifact-id
                       :status 404})))
    (let [gil           (parse-json-safe (:gil_json graph-artifact))
          proposal-json (proposal-json-from-gold-gil gil)
          source-node   (gil-node-by-type gil "table")
          source-table  (get-in source-node [:config :table_name])]
      (compile-proposal* {:proposal-json proposal-json
                          :source-table source-table
                          :profile-json {:field_types (into {}
                                                            (map (fn [column]
                                                                   [(:target_column column)
                                                                    {:type (:type column)}]))
                                                            (proposal-columns proposal-json))}}))))

(defn- synthesize-silver-graph-internal!
  [{:keys [proposal-row proposal-json graph-id target source-table source-system created-by]}]
  (let [gil      (build-silver-gil {:proposal-row proposal-row
                                    :proposal-json proposal-json
                                    :source-table source-table
                                    :target target
                                    :source-system source-system})
        result   (gil-compiler/apply-gil gil {:user created-by})
        workspace (:workspace_key (control-plane/graph-workspace-context graph-id))]
    (control-plane/assign-graph-workspace! (:graph-id result) workspace created-by)
    {:gil gil
     :graph-id (:graph-id result)
     :graph-version (:version result)
     :node-map (:node-map result)
     :panel (:panel result)}))

(defn- synthesize-gold-graph-internal!
  [{:keys [proposal-row proposal-json graph-id target source-table source-system created-by]}]
  (let [gil       (build-gold-gil {:proposal-row proposal-row
                                   :proposal-json proposal-json
                                   :source-table source-table
                                   :target target
                                   :source-system source-system})
        result    (gil-compiler/apply-gil gil {:user created-by})
        workspace (:workspace_key (control-plane/graph-workspace-context graph-id))]
    (control-plane/assign-graph-workspace! (:graph-id result) workspace created-by)
    {:gil gil
     :graph-id (:graph-id result)
     :graph-version (:version result)
     :node-map (:node-map result)
     :panel (:panel result)}))

(defn propose-silver-schema!
  [{:keys [graph-id api-node-id source-node-id endpoint-name created-by]
    :or {created-by "system"}}]
  (ensure-modeling-tables!)
  (control-plane/ensure-control-plane-tables!)
  (let [graph              (db/getGraph graph-id)
        source-node-id     (or source-node-id api-node-id)
        source-node        (g2/getData graph source-node-id)
        _                  (when-not (contains? #{"Ap" "Kf" "Fs"} (:btype source-node))
                             (throw (ex-info "Node is not a supported Bronze source node"
                                             {:node_id source-node-id
                                              :btype (:btype source-node)})))
        source-system      (or (:source_system source-node) "samara")
        target             (find-downstream-target graph source-node-id)
        workspace-context  (control-plane/graph-workspace-context graph-id)
        tenant-key         (:tenant_key workspace-context)
        workspace-key      (:workspace_key workspace-context)
        endpoint           (select-source-endpoint! source-node endpoint-name)
        {:keys [source snapshot-row fields]} (schema-source-for-endpoint graph-id source-node-id source-system endpoint target)
        profile            (build-schema-profile {:graph-id graph-id
                                                  :api-node-id source-node-id
                                                  :source-system source-system
                                                  :endpoint-name (:endpoint_name endpoint)
                                                  :source source
                                                  :snapshot-row snapshot-row
                                                  :fields fields
                                                  :endpoint endpoint})
        endpoint'          (assoc endpoint :inferred_fields fields)
        proposal           (build-silver-proposal {:tenant-key tenant-key
                                                   :workspace-key workspace-key
                                                   :graph-id graph-id
                                                   :api-node-id source-node-id
                                                   :source-system source-system
                                                   :endpoint endpoint'
                                                   :target target
                                                   :profile profile
                                                   :created-by created-by})
        latest-proposal    (latest-model-proposal graph-id source-node-id (:endpoint_name endpoint))
        latest-proposal-json (some-> latest-proposal :proposal_json parse-json-safe)
        latest-profile-json (some-> latest-proposal
                                    :profile_id
                                    schema-profile-by-id
                                    :profile_json
                                    parse-json-safe)
        proposal-json      (json/generate-string (:proposal_json proposal))]
    (if (and (= latest-proposal-json (:proposal_json proposal))
             (= latest-profile-json (:profile_json profile)))
      (let [profile-row (schema-profile-by-id (:profile_id latest-proposal))
            parsed-profile-json (parse-json-safe (:profile_json profile-row))]
        {:profile_id (:profile_id latest-proposal)
         :proposal_id (:proposal_id latest-proposal)
         :tenant_key tenant-key
         :workspace_key workspace-key
         :layer "silver"
         :status (:status latest-proposal)
         :target_model (:target_model latest-proposal)
         :confidence_score (:confidence_score latest-proposal)
         :deduped true
         :profile (merge {:graph_id graph-id
                          :api_node_id api-node-id
                          :source_layer "bronze"
                          :source_system source-system
                          :endpoint_name (:endpoint_name endpoint)
                         :profile_source (:profile_source profile-row)
                         :sample_record_count (:sample_record_count profile-row)
                         :field_count (:field_count profile-row)
                         :profile_id (:profile_id latest-proposal)
                         :tenant_key tenant-key
                         :workspace_key workspace-key}
                         parsed-profile-json)
         :proposal (parse-json-safe (:proposal_json latest-proposal))})
      (let [profile-row  (persist-schema-profile! profile created-by tenant-key workspace-key)
            proposal-row (persist-model-proposal! (assoc proposal :created_by created-by) (:profile_id profile-row))]
        {:profile_id (:profile_id profile-row)
         :proposal_id (:proposal_id proposal-row)
         :tenant_key tenant-key
         :workspace_key workspace-key
         :layer "silver"
         :status (:status proposal-row)
         :target_model (:target_model proposal-row)
         :confidence_score (:confidence_score proposal-row)
         :profile (assoc profile
                         :profile_id (:profile_id profile-row)
                         :tenant_key tenant-key
                         :workspace_key workspace-key)
         :proposal (:proposal_json proposal)}))))

(defn propose-gold-schema!
  [{:keys [silver_proposal_id created_by]
    :or {created_by "system"}}]
  (ensure-modeling-tables!)
  (control-plane/ensure-control-plane-tables!)
  (let [proposal-row      (proposal-by-id silver_proposal_id)
        _                 (when-not proposal-row
                            (throw (ex-info "Silver proposal not found"
                                            {:proposal_id silver_proposal_id :status 404})))
        _                 (when-not (= "silver" (:layer proposal-row))
                            (throw (ex-info "Gold proposals must be sourced from a Silver proposal"
                                            {:proposal_id silver_proposal_id
                                             :layer (:layer proposal-row)
                                             :status 400})))
        silver-proposal   (parse-json-safe (:proposal_json proposal-row))
        graph             (db/getGraph (:source_graph_id proposal-row))
        source-node       (g2/getData graph (:source_node_id proposal-row))
        target            (find-downstream-target graph (:source_node_id proposal-row))
        target-warehouse  (target-warehouse target (target-connection-id target))
        tenant-key        (:tenant_key proposal-row)
        workspace-key     (:workspace_key proposal-row)
        gold-profile      {:graph_id (:source_graph_id proposal-row)
                           :api_node_id (:source_node_id proposal-row)
                           :source_layer "silver"
                           :source_system (or (:source_system silver-proposal)
                                              (:source_system source-node)
                                              "medallion")
                           :endpoint_name (:target_model proposal-row)
                           :profile_source "silver_contract"
                           :sample_record_count 0
                           :field_count (count (proposal-columns silver-proposal))
                           :profile_json {:profile_source "silver_contract"
                                          :field_count (count (proposal-columns silver-proposal))
                                          :field_types (into {}
                                                             (map (fn [column]
                                                                    [(:target_column column)
                                                                     {:type (source-field-type column)
                                                                      :nullable (not= false (:nullable column))}]))
                                                             (proposal-columns silver-proposal))
                                          :source_model (:target_model proposal-row)
                                          :source_table (:target_table silver-proposal)}}
        gold-proposal     (build-gold-proposal {:tenant-key tenant-key
                                                :workspace-key workspace-key
                                                :silver-proposal silver-proposal
                                                :proposal-row proposal-row
                                                :target-warehouse target-warehouse
                                                :created-by created_by})
        latest-proposal   (latest-model-proposal (:source_graph_id proposal-row)
                                                 (:source_node_id proposal-row)
                                                 (:target_model proposal-row))
        proposal-json     (json/generate-string (:proposal_json gold-proposal))]
    (if (and latest-proposal
             (= "gold" (:layer latest-proposal))
             (= (:proposal_json latest-proposal) proposal-json))
      (let [profile-row         (schema-profile-by-id (:profile_id latest-proposal))
            parsed-profile-json (parse-json-safe (:profile_json profile-row))]
        {:profile_id (:profile_id latest-proposal)
         :proposal_id (:proposal_id latest-proposal)
         :tenant_key tenant-key
         :workspace_key workspace-key
         :layer "gold"
         :status (:status latest-proposal)
         :target_model (:target_model latest-proposal)
         :confidence_score (:confidence_score latest-proposal)
         :deduped true
         :profile (merge {:graph_id (:source_graph_id proposal-row)
                          :api_node_id (:source_node_id proposal-row)
                          :source_layer "silver"
                          :source_system (or (:source_system silver-proposal) "medallion")
                          :endpoint_name (:target_model proposal-row)
                          :profile_source (:profile_source profile-row)
                          :sample_record_count (:sample_record_count profile-row)
                          :field_count (:field_count profile-row)
                          :profile_id (:profile_id latest-proposal)
                          :tenant_key tenant-key
                          :workspace_key workspace-key}
                         parsed-profile-json)
         :proposal (parse-json-safe (:proposal_json latest-proposal))})
      (let [profile-row  (persist-schema-profile! gold-profile created_by tenant-key workspace-key)
            proposal-row (persist-model-proposal! (assoc gold-proposal :created_by created_by)
                                                 (:profile_id profile-row))]
        {:profile_id (:profile_id profile-row)
         :proposal_id (:proposal_id proposal-row)
         :tenant_key tenant-key
         :workspace_key workspace-key
         :layer "gold"
         :status (:status proposal-row)
         :target_model (:target_model proposal-row)
         :confidence_score (:confidence_score proposal-row)
         :profile (assoc gold-profile
                         :profile_id (:profile_id profile-row)
                         :tenant_key tenant-key
                         :workspace_key workspace-key)
         :proposal (:proposal_json gold-proposal)}))))

(defn- proposal-summary
  [proposal-row]
  (let [proposal-json   (parse-json-safe (:proposal_json proposal-row))
        validation-row  (latest-validation-for-proposal (:proposal_id proposal-row))
        release-row     (latest-active-release (:layer proposal-row) (:target_model proposal-row))
        graph-artifact  (or (when (= (:proposal_id release-row) (:proposal_id proposal-row))
                              (some-> (:graph_artifact_id release-row) graph-artifact-by-id))
                            (latest-graph-artifact-for-proposal (:proposal_id proposal-row)))]
    {:proposal_id (:proposal_id proposal-row)
     :layer (:layer proposal-row)
     :target_model (:target_model proposal-row)
     :status (:status proposal-row)
     :confidence_score (:confidence_score proposal-row)
     :source_graph_id (:source_graph_id proposal-row)
     :source_node_id (:source_node_id proposal-row)
     :source_endpoint_name (:source_endpoint_name proposal-row)
     :created_by (:created_by proposal-row)
     :created_at_utc (:created_at_utc proposal-row)
     :compiled_sql (:compiled_sql proposal-row)
     :proposal proposal-json
     :latest_validation (some-> validation-row
                                (select-keys [:validation_id :status :created_at_utc])
                                (assoc :validation (parse-json-safe (:validation_json validation-row))))
     :active_release (some-> release-row
                             (select-keys [:release_id :version :status :active :graph_artifact_id :published_at_utc]))
     :graph_artifact (some-> graph-artifact
                             (select-keys [:graph_artifact_id :graph_kind :graph_id :graph_version :created_at_utc]))}))

(defn- proposal-summary-lite
  [proposal-row]
  (let [release-row (latest-active-release (:layer proposal-row) (:target_model proposal-row))]
    {:proposal_id (:proposal_id proposal-row)
     :layer (:layer proposal-row)
     :target_model (:target_model proposal-row)
     :status (:status proposal-row)
     :confidence_score (:confidence_score proposal-row)
     :source_graph_id (:source_graph_id proposal-row)
     :source_node_id (:source_node_id proposal-row)
     :source_endpoint_name (:source_endpoint_name proposal-row)
     :created_by (:created_by proposal-row)
     :created_at_utc (:created_at_utc proposal-row)
     :release_id (:release_id release-row)
     :active_release (some-> release-row
                             (select-keys [:release_id :version :status :active :graph_artifact_id :published_at_utc]))}))

(defn list-silver-proposals
  [{:keys [graph-id status limit]
    :or {limit 100}}]
  (ensure-modeling-tables!)
  (let [limit (-> (or limit 100) long (max 1) (min 500))
        rows  (jdbc/execute!
               (db-opts db/ds)
               (into [(str "SELECT * FROM " model-proposal-table
                           " WHERE layer = 'silver'"
                           (when graph-id " AND source_graph_id = ?")
                           (when status " AND status = ?")
                           " ORDER BY created_at_utc DESC, proposal_id DESC LIMIT ?")]
                     (concat
                      (when graph-id [graph-id])
                      (when status [status])
                      [limit])))]
    (mapv proposal-summary-lite rows)))

(defn list-gold-proposals
  [{:keys [graph-id status limit]
    :or {limit 100}}]
  (ensure-modeling-tables!)
  (let [limit (-> (or limit 100) long (max 1) (min 500))
        rows  (jdbc/execute!
               (db-opts db/ds)
               (into [(str "SELECT * FROM " model-proposal-table
                           " WHERE layer = 'gold'"
                           (when graph-id " AND source_graph_id = ?")
                           (when status " AND status = ?")
                           " ORDER BY created_at_utc DESC, proposal_id DESC LIMIT ?")]
                     (concat
                      (when graph-id [graph-id])
                      (when status [status])
                      [limit])))]
    (mapv proposal-summary-lite rows)))

(defn get-silver-proposal
  [proposal-id]
  (ensure-modeling-tables!)
  (when-let [row (proposal-by-id proposal-id)]
    (when (= "silver" (:layer row))
      (proposal-summary row))))

(defn get-gold-proposal
  [proposal-id]
  (ensure-modeling-tables!)
  (when-let [row (proposal-by-id proposal-id)]
    (when (= "gold" (:layer row))
      (proposal-summary row))))

(defn update-silver-proposal!
  [proposal-id {:keys [proposal created_by]
                :or {created_by "system"}}]
  (ensure-modeling-tables!)
  (let [proposal-row (proposal-by-id proposal-id)]
    (when-not proposal-row
      (throw (ex-info "Silver proposal not found" {:proposal_id proposal-id :status 404})))
    (when-not (= "silver" (:layer proposal-row))
      (throw (ex-info "Proposal is not a Silver proposal" {:proposal_id proposal-id :status 400})))
    (when-not (map? proposal)
      (throw (ex-info "Updated proposal must be a map" {:proposal_id proposal-id :status 400})))
    (let [current-json (parse-json-safe (:proposal_json proposal-row))
          next-json    (deep-merge current-json proposal)
          next-model   (or (non-blank-str (:target_model next-json))
                           (:target_model proposal-row))]
      (cond
        (contains? clone-on-edit-statuses (:status proposal-row))
        (let [new-row (persist-model-proposal! {:tenant_key (:tenant_key proposal-row)
                                                :workspace_key (:workspace_key proposal-row)
                                                :layer "silver"
                                                :target_model next-model
                                                :status "draft"
                                                :source_graph_id (:source_graph_id proposal-row)
                                                :source_node_id (:source_node_id proposal-row)
                                                :source_endpoint_name (:source_endpoint_name proposal-row)
                                                :proposal_json next-json
                                                :compiled_sql nil
                                                :confidence_score (:confidence_score proposal-row)
                                                :created_by created_by}
                                               (:profile_id proposal-row))]
          (proposal-summary new-row))

        (contains? editable-in-place-statuses (:status proposal-row))
        (do
          (update-model-proposal! proposal-id {:proposal_json next-json
                                               :target_model next-model
                                               :compiled_sql nil
                                               :status "draft"})
          (proposal-summary (proposal-by-id proposal-id)))

        :else
        (throw (ex-info "Proposal cannot be edited in its current state"
                        {:proposal_id proposal-id
                         :status 409
                         :proposal_status (:status proposal-row)}))))))

(defn update-gold-proposal!
  [proposal-id {:keys [proposal created_by]
                :or {created_by "system"}}]
  (ensure-modeling-tables!)
  (let [proposal-row (proposal-by-id proposal-id)]
    (when-not proposal-row
      (throw (ex-info "Gold proposal not found" {:proposal_id proposal-id :status 404})))
    (when-not (= "gold" (:layer proposal-row))
      (throw (ex-info "Proposal is not a Gold proposal" {:proposal_id proposal-id :status 400})))
    (when-not (map? proposal)
      (throw (ex-info "Updated proposal must be a map" {:proposal_id proposal-id :status 400})))
    (let [current-json (parse-json-safe (:proposal_json proposal-row))
          next-json    (deep-merge current-json proposal)
          next-model   (or (non-blank-str (:target_model next-json))
                           (:target_model proposal-row))]
      (cond
        (contains? clone-on-edit-statuses (:status proposal-row))
        (let [new-row (persist-model-proposal! {:tenant_key (:tenant_key proposal-row)
                                                :workspace_key (:workspace_key proposal-row)
                                                :layer "gold"
                                                :target_model next-model
                                                :status "draft"
                                                :source_graph_id (:source_graph_id proposal-row)
                                                :source_node_id (:source_node_id proposal-row)
                                                :source_endpoint_name (:source_endpoint_name proposal-row)
                                                :proposal_json next-json
                                                :compiled_sql nil
                                                :confidence_score (:confidence_score proposal-row)
                                                :created_by created_by}
                                               (:profile_id proposal-row))]
          (proposal-summary new-row))

        (contains? editable-in-place-statuses (:status proposal-row))
        (do
          (update-model-proposal! proposal-id {:proposal_json next-json
                                               :target_model next-model
                                               :compiled_sql nil
                                               :status "draft"})
          (proposal-summary (proposal-by-id proposal-id)))

        :else
        (throw (ex-info "Proposal cannot be edited in its current state"
                        {:proposal_id proposal-id
                         :status 409
                         :proposal_status (:status proposal-row)}))))))

(defn synthesize-silver-graph!
  ([proposal-id] (synthesize-silver-graph! proposal-id {}))
  ([proposal-id {:keys [created_by]
                 :or {created_by "system"}}]
   (ensure-modeling-tables!)
   (let [{:keys [proposal-row proposal-json graph-id target source-system source-table]} (resolve-proposal-context proposal-id)
         latest-artifact (latest-graph-artifact-for-proposal proposal-id)
         desired-gil     (build-silver-gil {:proposal-row proposal-row
                                            :proposal-json proposal-json
                                            :source-table source-table
                                            :target target
                                            :source-system source-system})]
     (if (= desired-gil (parse-json-safe (:gil_json latest-artifact)))
       {:graph_artifact_id (:graph_artifact_id latest-artifact)
        :proposal_id proposal-id
        :graph_id (:graph_id latest-artifact)
        :graph_version (:graph_version latest-artifact)
        :deduped true
        :gil (parse-json-safe (:gil_json latest-artifact))
        :node_map (parse-json-safe (:node_map_json latest-artifact))}
       (let [result       (synthesize-silver-graph-internal! {:proposal-row proposal-row
                                                              :proposal-json proposal-json
                                                              :graph-id graph-id
                                                              :target target
                                                              :source-table source-table
                                                              :source-system source-system
                                                              :created-by created_by})
             artifact-row (persist-model-graph-artifact! {:proposal-id proposal-id
                                                          :graph-kind "silver_intermediate"
                                                          :graph-id (:graph-id result)
                                                          :graph-version (:graph-version result)
                                                          :gil (:gil result)
                                                          :node-map (:node-map result)
                                                          :created-by created_by})]
         {:graph_artifact_id (:graph_artifact_id artifact-row)
          :proposal_id proposal-id
          :graph_id (:graph-id result)
          :graph_version (:graph-version result)
          :deduped false
          :gil (:gil result)
          :node_map (:node-map result)
          :panel (:panel result)})))))

(defn synthesize-gold-graph!
  ([proposal-id] (synthesize-gold-graph! proposal-id {}))
  ([proposal-id {:keys [created_by]
                 :or {created_by "system"}}]
   (ensure-modeling-tables!)
   (let [{:keys [proposal-row proposal-json graph-id target source-system source-table]} (resolve-gold-proposal-context proposal-id)
         latest-artifact (latest-graph-artifact-for-proposal proposal-id)
         desired-gil     (build-gold-gil {:proposal-row proposal-row
                                          :proposal-json proposal-json
                                          :source-table source-table
                                          :target target
                                          :source-system source-system})]
     (if (= desired-gil (parse-json-safe (:gil_json latest-artifact)))
       {:graph_artifact_id (:graph_artifact_id latest-artifact)
        :proposal_id proposal-id
        :graph_id (:graph_id latest-artifact)
        :graph_version (:graph_version latest-artifact)
        :deduped true
        :gil (parse-json-safe (:gil_json latest-artifact))
        :node_map (parse-json-safe (:node_map_json latest-artifact))}
       (let [result       (synthesize-gold-graph-internal! {:proposal-row proposal-row
                                                            :proposal-json proposal-json
                                                            :graph-id graph-id
                                                            :target target
                                                            :source-table source-table
                                                            :source-system source-system
                                                            :created-by created_by})
             artifact-row (persist-model-graph-artifact! {:proposal-id proposal-id
                                                          :graph-kind "gold_intermediate"
                                                          :graph-id (:graph-id result)
                                                          :graph-version (:graph-version result)
                                                          :gil (:gil result)
                                                          :node-map (:node-map result)
                                                          :created-by created_by})]
         {:graph_artifact_id (:graph_artifact_id artifact-row)
          :proposal_id proposal-id
          :graph_id (:graph-id result)
          :graph_version (:graph-version result)
          :deduped false
          :gil (:gil result)
          :node_map (:node-map result)
          :panel (:panel result)})))))

(defn compile-silver-proposal!
  [proposal-id]
  (ensure-modeling-tables!)
  (let [{:keys [proposal-row] :as context} (resolve-proposal-context proposal-id)
        {:keys [sql_ir select_sql compiled_sql]} (compile-proposal* context)]
    (update-model-proposal! proposal-id {:compiled_sql compiled_sql
                                         :status (compile-result-status proposal-row)})
    {:proposal_id proposal-id
     :target_model (:target_model proposal-row)
     :layer (:layer proposal-row)
     :sql_ir sql_ir
     :select_sql select_sql
     :compiled_sql compiled_sql}))

(defn compile-gold-proposal!
  [proposal-id]
  (ensure-modeling-tables!)
  (let [{:keys [proposal-row] :as context} (resolve-gold-proposal-context proposal-id)
        {:keys [sql_ir select_sql compiled_sql]} (compile-proposal* context)]
    (update-model-proposal! proposal-id {:compiled_sql compiled_sql
                                         :status (compile-result-status proposal-row)})
    {:proposal_id proposal-id
     :target_model (:target_model proposal-row)
     :layer (:layer proposal-row)
     :sql_ir sql_ir
     :select_sql select_sql
     :compiled_sql compiled_sql}))

(defn validate-silver-proposal!
  ([proposal-id] (validate-silver-proposal! proposal-id {}))
  ([proposal-id {:keys [sample_limit created_by]
                 :or {created_by "system"}}]
   (ensure-modeling-tables!)
   (let [{:keys [proposal-row proposal-json source-table profile-json] :as context} (resolve-proposal-context proposal-id)
         errors         (static-validation-errors proposal-json source-table profile-json)
         compile-result (when-not (seq errors)
                          (compile-proposal* context))
         sample-result  (if compile-result
                          (try
                            (run-sample-execution! context (:select_sql compile-result) sample_limit)
                            (catch Exception e
                              {:sample_sql nil
                               :summary nil
                               :error {:kind "sample_execution"
                                       :message (.getMessage e)
                                       :failure_class (or (:failure_class (ex-data e)) "sample_execution")}}))
                          {:sample_sql nil
                           :summary nil})
         warnings       (vec
                         (concat
                          (when (and (get-in sample-result [:summary])
                                     (zero? (get-in sample-result [:summary :row_count])))
                            [{:kind "sample_execution"
                              :message "Sample execution returned zero rows"}])
                          (when (pos? (or (get-in sample-result [:summary :duplicate_key_count]) 0))
                            [{:kind "sample_execution"
                              :message "Sample execution detected duplicate merge keys"}])))
         all-errors     (vec (concat errors (when-let [sample-error (:error sample-result)] [sample-error])))
         status         (if (seq all-errors) "invalid" "valid")
         validation-json {:proposal_id proposal-id
                          :target_model (:target_model proposal-row)
                          :status status
                          :proposal_checksum (proposal-checksum proposal-json)
                          :schema_errors errors
                          :warnings warnings
                          :sample_execution sample-result
                          :sql_ir (:sql_ir compile-result)}
         validation-row (persist-model-validation! {:proposal-id proposal-id
                                                    :status status
                                                    :validation-kind "silver_proposal"
                                                    :validation-json validation-json
                                                    :compiled-sql (:compiled_sql compile-result)
                                                    :created-by created_by})]
     (update-model-proposal! proposal-id {:compiled_sql (when (= "valid" status)
                                                          (:compiled_sql compile-result))
                                          :status (if (= "valid" status) "validated" "draft")})
     {:validation_id (:validation_id validation-row)
      :proposal_id proposal-id
      :status status
      :compiled_sql (:compiled_sql compile-result)
     :select_sql (:select_sql compile-result)
     :sql_ir (:sql_ir compile-result)
     :validation validation-json})))

(defn validate-gold-proposal!
  ([proposal-id] (validate-gold-proposal! proposal-id {}))
  ([proposal-id {:keys [sample_limit created_by]
                 :or {created_by "system"}}]
   (ensure-modeling-tables!)
   (let [{:keys [proposal-row proposal-json source-table profile-json] :as context} (resolve-gold-proposal-context proposal-id)
         errors         (static-validation-errors proposal-json source-table profile-json)
         compile-result (when-not (seq errors)
                          (compile-proposal* context))
         sample-result  (if compile-result
                          (try
                            (run-sample-execution! context (:select_sql compile-result) sample_limit)
                            (catch Exception e
                              {:sample_sql nil
                               :summary nil
                               :error {:kind "sample_execution"
                                       :message (.getMessage e)
                                       :failure_class (or (:failure_class (ex-data e)) "sample_execution")}}))
                          {:sample_sql nil
                           :summary nil})
         warnings       (vec
                         (concat
                          (when (and (get-in sample-result [:summary])
                                     (zero? (get-in sample-result [:summary :row_count])))
                            [{:kind "sample_execution"
                              :message "Sample execution returned zero rows"}])
                          (when (pos? (or (get-in sample-result [:summary :duplicate_key_count]) 0))
                            [{:kind "sample_execution"
                              :message "Sample execution detected duplicate merge keys"}])))
         all-errors     (vec (concat errors (when-let [sample-error (:error sample-result)] [sample-error])))
         status         (if (seq all-errors) "invalid" "valid")
         validation-json {:proposal_id proposal-id
                          :target_model (:target_model proposal-row)
                          :status status
                          :proposal_checksum (proposal-checksum proposal-json)
                          :schema_errors errors
                          :warnings warnings
                          :sample_execution sample-result
                          :sql_ir (:sql_ir compile-result)}
         validation-row (persist-model-validation! {:proposal-id proposal-id
                                                    :status status
                                                    :validation-kind "gold_proposal"
                                                    :validation-json validation-json
                                                    :compiled-sql (:compiled_sql compile-result)
                                                    :created-by created_by})]
     (update-model-proposal! proposal-id {:compiled_sql (when (= "valid" status)
                                                          (:compiled_sql compile-result))
                                          :status (if (= "valid" status) "validated" "draft")})
     {:validation_id (:validation_id validation-row)
      :proposal_id proposal-id
      :status status
      :compiled_sql (:compiled_sql compile-result)
      :select_sql (:select_sql compile-result)
      :sql_ir (:sql_ir compile-result)
      :validation validation-json})))

(defn publish-silver-proposal!
  ([proposal-id] (publish-silver-proposal! proposal-id {}))
  ([proposal-id {:keys [created_by sample_limit]
                 :or {created_by "system"}}]
   (ensure-modeling-tables!)
   (let [proposal-row       (proposal-by-id proposal-id)
         _                  (when-not proposal-row
                              (throw (ex-info "Silver proposal not found" {:proposal_id proposal-id :status 404})))
         _                  (when-not (= "silver" (:layer proposal-row))
                              (throw (ex-info "Proposal is not a Silver proposal"
                                              {:proposal_id proposal-id
                                               :layer (:layer proposal-row)})))
         _                  (when-not (publishable-proposal? proposal-row)
                              (throw (ex-info "Silver proposal is not in a publishable state"
                                              {:proposal_id proposal-id
                                               :proposal_status (:status proposal-row)
                                               :status 409})))
         validation-result  (or (current-valid-validation proposal-id (parse-json-safe (:proposal_json proposal-row)))
                                (validate-silver-proposal! proposal-id {:created_by created_by
                                                                        :sample_limit sample_limit}))
         _                  (when-not (= "valid" (:status validation-result))
                              (throw (ex-info "Silver proposal is not valid and cannot be published"
                                              {:proposal_id proposal-id
                                               :status 409})))
         graph-artifact     (synthesize-silver-graph! proposal-id {:created_by created_by})
         proposal-json      (parse-json-safe (:proposal_json proposal-row))
         sql-ir             (or (get-in validation-result [:validation :sql_ir])
                                (:sql_ir (compile-silver-proposal! proposal-id)))]
     (jdbc/with-transaction [tx db/ds]
       (publish-proposal-tx! tx proposal-id proposal-row validation-result (:graph_artifact_id graph-artifact) created_by proposal-json sql-ir)))))

(defn publish-gold-proposal!
  ([proposal-id] (publish-gold-proposal! proposal-id {}))
  ([proposal-id {:keys [created_by sample_limit]
                 :or {created_by "system"}}]
   (ensure-modeling-tables!)
   (let [proposal-row       (proposal-by-id proposal-id)
         _                  (when-not proposal-row
                              (throw (ex-info "Gold proposal not found" {:proposal_id proposal-id :status 404})))
         _                  (when-not (= "gold" (:layer proposal-row))
                              (throw (ex-info "Proposal is not a Gold proposal"
                                              {:proposal_id proposal-id
                                               :layer (:layer proposal-row)})))
         _                  (when-not (publishable-proposal? proposal-row)
                              (throw (ex-info "Gold proposal is not in a publishable state"
                                              {:proposal_id proposal-id
                                               :proposal_status (:status proposal-row)
                                               :status 409})))
         validation-result  (or (current-valid-validation proposal-id (parse-json-safe (:proposal_json proposal-row)))
                                (validate-gold-proposal! proposal-id {:created_by created_by
                                                                      :sample_limit sample_limit}))
         _                  (when-not (= "valid" (:status validation-result))
                              (throw (ex-info "Gold proposal is not valid and cannot be published"
                                              {:proposal_id proposal-id
                                               :status 409})))
         graph-artifact     (synthesize-gold-graph! proposal-id {:created_by created_by})
         proposal-json      (parse-json-safe (:proposal_json proposal-row))
         sql-ir             (or (get-in validation-result [:validation :sql_ir])
                                (:sql_ir (compile-gold-proposal! proposal-id)))]
     (jdbc/with-transaction [tx db/ds]
       (publish-proposal-tx! tx proposal-id proposal-row validation-result (:graph_artifact_id graph-artifact) created_by proposal-json sql-ir)))))

(defn- target-node-id-from-graph-artifact
  [graph-artifact graph]
  (let [node-map (parse-json-safe (:node_map_json graph-artifact))]
    (or (some-> (get node-map :target) long)
        (some-> (get node-map "target") long)
        (first (for [[node-id node] (:n graph)
                     :when (= "Tg" (get-in node [:na :btype]))]
                 node-id)))))

(defn- qualified-target-table
  [target-node]
  (string/join "."
               (remove string/blank?
                       [(or (:catalog target-node) "")
                        (or (:schema target-node) "")
                        (or (:table_name target-node) "")])))

(defn- target-execution-backend
  [target-node]
  (case (target-warehouse target-node (or (:connection_id target-node) (:c target-node)))
    "snowflake" "snowflake_sql"
    "postgresql" "postgresql_sql"
    "databricks_job"))

(defn- compiled-artifact-sql-ir
  [compiled-artifact]
  (parse-json-safe (:sql_ir_json compiled-artifact)))

(defn- ensure-postgresql-target-table!
  [conn-id sql-ir]
  (let [opts         (db/get-opts conn-id nil)
        target       (get-in sql-ir [:materialization :target])
        {:keys [schema table]} (split-postgresql-target-table target)]
    (when-let [ddl-sql (compiler/compile-ddl {:target-warehouse "postgresql"
                                              :sql-ir sql-ir})]
      (jdbc/execute! opts [ddl-sql]))
    (when (and (seq table) (seq (:select sql-ir)))
      (let [existing-columns (->> (jdbc/execute! opts
                                                 ["SELECT column_name
                                                    FROM information_schema.columns
                                                   WHERE table_schema = ?
                                                     AND table_name = ?"
                                                  schema
                                                  table])
                                  (map :column_name)
                                  set)]
        (doseq [{:keys [target_column type]} (:select sql-ir)
                :let [column-name (non-blank-str target_column)]
                :when (and column-name (not (contains? existing-columns column-name)))]
          (jdbc/execute! opts
                         [(str "ALTER TABLE "
                               (quote-qualified-postgresql-ident target)
                               " ADD COLUMN "
                               (quote-sql-ident column-name)
                               " "
                               (postgresql-logical-type type))]))))))

(defn- execute-postgresql-materialization!
  [conn-id sql-ir compiled-sql]
  (ensure-postgresql-target-table! conn-id sql-ir)
  (jdbc/with-transaction [tx (db/get-opts conn-id nil)]
    (jdbc/execute! tx [compiled-sql])))

(defn review-silver-proposal!
  [proposal-id {:keys [review_state review_notes reviewed_by]
                :or {reviewed_by "system"}}]
  (ensure-modeling-tables!)
  (let [proposal-row (proposal-by-id proposal-id)]
    (when-not proposal-row
      (throw (ex-info "Silver proposal not found" {:proposal_id proposal-id :status 404})))
    (when-not (= "silver" (:layer proposal-row))
      (throw (ex-info "Proposal is not a Silver proposal" {:proposal_id proposal-id :status 400})))
    (when-not (reviewable-proposal? proposal-row)
      (throw (ex-info "Silver proposal is not ready for review"
                      {:proposal_id proposal-id
                       :proposal_status (:status proposal-row)
                       :status 409})))
    (let [proposal-json (parse-json-safe (:proposal_json proposal-row))
          review-state (or (non-blank-str review_state) "reviewed")
          _            (when-not (contains? allowed-review-states review-state)
                         (throw (ex-info "Invalid review state"
                                         {:proposal_id proposal-id
                                          :review_state review-state
                                          :status 400})))
          updated-json (assoc proposal-json
                              :review {:state review-state
                                       :notes (or review_notes "")
                                       :reviewed_by reviewed_by
                                       :reviewed_at (str (now-utc))})]
      (update-model-proposal! proposal-id {:proposal_json updated-json
                                           :status review-state})
      {:proposal_id proposal-id
       :status review-state
       :review (:review updated-json)})))

(defn review-gold-proposal!
  [proposal-id {:keys [review_state review_notes reviewed_by]
                :or {reviewed_by "system"}}]
  (ensure-modeling-tables!)
  (let [proposal-row (proposal-by-id proposal-id)]
    (when-not proposal-row
      (throw (ex-info "Gold proposal not found" {:proposal_id proposal-id :status 404})))
    (when-not (= "gold" (:layer proposal-row))
      (throw (ex-info "Proposal is not a Gold proposal" {:proposal_id proposal-id :status 400})))
    (when-not (reviewable-proposal? proposal-row)
      (throw (ex-info "Gold proposal is not ready for review"
                      {:proposal_id proposal-id
                       :proposal_status (:status proposal-row)
                       :status 409})))
    (let [proposal-json (parse-json-safe (:proposal_json proposal-row))
          review-state (or (non-blank-str review_state) "reviewed")
          _            (when-not (contains? allowed-review-states review-state)
                         (throw (ex-info "Invalid review state"
                                         {:proposal_id proposal-id
                                          :review_state review-state
                                          :status 400})))
          updated-json (assoc proposal-json
                              :review {:state review-state
                                       :notes (or review_notes "")
                                       :reviewed_by reviewed_by
                                       :reviewed_at (str (now-utc))})]
      (update-model-proposal! proposal-id {:proposal_json updated-json
                                           :status review-state})
      {:proposal_id proposal-id
       :status review-state
       :review (:review updated-json)})))

(defn validate-silver-proposal-warehouse!
  [proposal-id {:keys [created_by]
                :or {created_by "system"}}]
  (ensure-modeling-tables!)
  (let [proposal-row   (proposal-by-id proposal-id)
        _              (when-not proposal-row
                         (throw (ex-info "Silver proposal not found" {:proposal_id proposal-id :status 404})))
        _              (when-not (= "silver" (:layer proposal-row))
                         (throw (ex-info "Proposal is not a Silver proposal"
                                         {:proposal_id proposal-id :status 400})))
        graph-artifact  (synthesize-silver-graph! proposal-id {:created_by created_by})
        compile-result  (compile-silver-graph-artifact! (:graph_artifact_id graph-artifact))
        graph           (db/getGraph (:graph_id graph-artifact))
        target-node-id  (target-node-id-from-graph-artifact graph-artifact graph)
        target-node     (g2/getData graph target-node-id)
        conn-id         (or (:connection_id target-node) (:c target-node))
        warehouse       (target-warehouse target-node conn-id)
        target-table    (qualified-target-table target-node)]
    (case warehouse
      "bigquery"
      (persist-bigquery-warehouse-validation! {:conn-id conn-id
                                               :proposal-id proposal-id
                                               :graph-artifact graph-artifact
                                               :compile-result compile-result
                                               :target-table target-table
                                               :validation-kind "silver_warehouse_sql"
                                               :created-by created_by})

      "snowflake"
      (let [response        (jdbc/execute! (db/get-opts conn-id nil)
                                           [(str "EXPLAIN USING TEXT " (:select_sql compile-result))])
            validation-json {:graph_artifact_id (:graph_artifact_id graph-artifact)
                             :graph_id (:graph_id graph-artifact)
                             :graph_version (:graph_version graph-artifact)
                             :backend "snowflake_sql"
                             :request {:proposal_id proposal-id
                                       :graph_artifact_id (:graph_artifact_id graph-artifact)
                                       :target_table target-table}
                             :response response
                             :sql_ir (:sql_ir compile-result)
                             :sql_checksum (sha256-hex (:compiled_sql compile-result))}
            validation-row (persist-model-validation! {:proposal-id proposal-id
                                                       :status "valid"
                                                       :validation-kind "silver_warehouse_sql"
                                                       :validation-json validation-json
                                                       :compiled-sql (:compiled_sql compile-result)
                                                       :created-by created_by})]
        {:validation_id (:validation_id validation-row)
         :proposal_id proposal-id
         :status "valid"
         :backend "snowflake_sql"
         :graph_artifact_id (:graph_artifact_id graph-artifact)
         :validation validation-json})

      "postgresql"
      (let [sql-ir          (:sql_ir compile-result)
            _               (ensure-postgresql-target-table! conn-id sql-ir)
            response        (jdbc/execute! (db/get-opts conn-id nil)
                                           [(str "EXPLAIN " (:compiled_sql compile-result))])
            validation-json {:graph_artifact_id (:graph_artifact_id graph-artifact)
                             :graph_id (:graph_id graph-artifact)
                             :graph_version (:graph_version graph-artifact)
                             :backend "postgresql_sql"
                             :request {:proposal_id proposal-id
                                       :graph_artifact_id (:graph_artifact_id graph-artifact)
                                       :target_table target-table}
                             :response response
                             :sql_ir sql-ir
                             :sql_checksum (sha256-hex (:compiled_sql compile-result))}
            validation-row (persist-model-validation! {:proposal-id proposal-id
                                                       :status "valid"
                                                       :validation-kind "silver_warehouse_sql"
                                                       :validation-json validation-json
                                                       :compiled-sql (:compiled_sql compile-result)
                                                       :created-by created_by})]
        {:validation_id (:validation_id validation-row)
         :proposal_id proposal-id
         :status "valid"
         :backend "postgresql_sql"
         :graph_artifact_id (:graph_artifact_id graph-artifact)
         :validation validation-json})

      (let [job-id          (or (:silver_validation_job_id target-node)
                                (:silver_job_id target-node)
                                "")
            _               (when (string/blank? job-id)
                              (throw (ex-info "Generated Silver graph target has no Databricks validation job configured"
                                              {:proposal_id proposal-id
                                               :graph_id (:graph_id graph-artifact)
                                               :target_node_id target-node-id
                                               :status 400})))
            params          {:proposal_id (str proposal-id)
                             :graph_artifact_id (str (:graph_artifact_id graph-artifact))
                             :graph_id (str (:graph_id graph-artifact))
                             :graph_version (str (:graph_version graph-artifact))
                             :target_model (:target_model proposal-row)
                             :target_table target-table
                             :validation_kind "silver_warehouse_job"
                             :sql_checksum (sha256-hex (:compiled_sql compile-result))}
            response        (dbx-jobs/trigger-job! conn-id job-id params)
            validation-json {:graph_artifact_id (:graph_artifact_id graph-artifact)
                             :graph_id (:graph_id graph-artifact)
                             :graph_version (:graph_version graph-artifact)
                             :backend "databricks_job"
                             :job_id job-id
                             :request params
                             :response response
                             :sql_ir (:sql_ir compile-result)
                             :sql_checksum (sha256-hex (:compiled_sql compile-result))}
            validation-row  (persist-model-validation! {:proposal-id proposal-id
                                                        :status "submitted"
                                                        :validation-kind "silver_warehouse_job"
                                                        :validation-json validation-json
                                                        :compiled-sql (:compiled_sql compile-result)
                                                        :created-by created_by})]
        {:validation_id (:validation_id validation-row)
         :proposal_id proposal-id
         :status "submitted"
         :backend "databricks_job"
         :graph_artifact_id (:graph_artifact_id graph-artifact)
         :job_trigger response}))))

(defn validate-gold-proposal-warehouse!
  [proposal-id {:keys [created_by]
                :or {created_by "system"}}]
  (ensure-modeling-tables!)
  (let [proposal-row   (proposal-by-id proposal-id)
        _              (when-not proposal-row
                         (throw (ex-info "Gold proposal not found" {:proposal_id proposal-id :status 404})))
        _              (when-not (= "gold" (:layer proposal-row))
                         (throw (ex-info "Proposal is not a Gold proposal"
                                         {:proposal_id proposal-id :status 400})))
        graph-artifact  (synthesize-gold-graph! proposal-id {:created_by created_by})
        compile-result  (compile-gold-graph-artifact! (:graph_artifact_id graph-artifact))
        graph           (db/getGraph (:graph_id graph-artifact))
        target-node-id  (target-node-id-from-graph-artifact graph-artifact graph)
        target-node     (g2/getData graph target-node-id)
        conn-id         (or (:connection_id target-node) (:c target-node))
        warehouse       (target-warehouse target-node conn-id)
        target-table    (qualified-target-table target-node)]
    (case warehouse
      "bigquery"
      (persist-bigquery-warehouse-validation! {:conn-id conn-id
                                               :proposal-id proposal-id
                                               :graph-artifact graph-artifact
                                               :compile-result compile-result
                                               :target-table target-table
                                               :validation-kind "gold_warehouse_sql"
                                               :created-by created_by})

      "snowflake"
      (let [response        (jdbc/execute! (db/get-opts conn-id nil)
                                           [(str "EXPLAIN USING TEXT " (:select_sql compile-result))])
            validation-json {:graph_artifact_id (:graph_artifact_id graph-artifact)
                             :graph_id (:graph_id graph-artifact)
                             :graph_version (:graph_version graph-artifact)
                             :backend "snowflake_sql"
                             :request {:proposal_id proposal-id
                                       :graph_artifact_id (:graph_artifact_id graph-artifact)
                                       :target_table target-table}
                             :response response
                             :sql_ir (:sql_ir compile-result)
                             :sql_checksum (sha256-hex (:compiled_sql compile-result))}
            validation-row (persist-model-validation! {:proposal-id proposal-id
                                                       :status "valid"
                                                       :validation-kind "gold_warehouse_sql"
                                                       :validation-json validation-json
                                                       :compiled-sql (:compiled_sql compile-result)
                                                       :created-by created_by})]
        {:validation_id (:validation_id validation-row)
         :proposal_id proposal-id
         :status "valid"
         :backend "snowflake_sql"
         :graph_artifact_id (:graph_artifact_id graph-artifact)
         :validation validation-json})

      "postgresql"
      (let [sql-ir          (:sql_ir compile-result)
            _               (ensure-postgresql-target-table! conn-id sql-ir)
            response        (jdbc/execute! (db/get-opts conn-id nil)
                                           [(str "EXPLAIN " (:compiled_sql compile-result))])
            validation-json {:graph_artifact_id (:graph_artifact_id graph-artifact)
                             :graph_id (:graph_id graph-artifact)
                             :graph_version (:graph_version graph-artifact)
                             :backend "postgresql_sql"
                             :request {:proposal_id proposal-id
                                       :graph_artifact_id (:graph_artifact_id graph-artifact)
                                       :target_table target-table}
                             :response response
                             :sql_ir sql-ir
                             :sql_checksum (sha256-hex (:compiled_sql compile-result))}
            validation-row (persist-model-validation! {:proposal-id proposal-id
                                                       :status "valid"
                                                       :validation-kind "gold_warehouse_sql"
                                                       :validation-json validation-json
                                                       :compiled-sql (:compiled_sql compile-result)
                                                       :created-by created_by})]
        {:validation_id (:validation_id validation-row)
         :proposal_id proposal-id
         :status "valid"
         :backend "postgresql_sql"
         :graph_artifact_id (:graph_artifact_id graph-artifact)
         :validation validation-json})

      (let [job-id          (or (:gold_job_id target-node) "")
            _               (when (string/blank? job-id)
                              (throw (ex-info "Generated Gold graph target has no Databricks validation job configured"
                                              {:proposal_id proposal-id
                                               :graph_id (:graph_id graph-artifact)
                                               :target_node_id target-node-id
                                               :status 400})))
            params          {:proposal_id (str proposal-id)
                             :graph_artifact_id (str (:graph_artifact_id graph-artifact))
                             :graph_id (str (:graph_id graph-artifact))
                             :graph_version (str (:graph_version graph-artifact))
                             :target_model (:target_model proposal-row)
                             :target_table target-table
                             :validation_kind "gold_warehouse_job"
                             :sql_checksum (sha256-hex (:compiled_sql compile-result))}
            response        (dbx-jobs/trigger-job! conn-id job-id params)
            validation-json {:graph_artifact_id (:graph_artifact_id graph-artifact)
                             :graph_id (:graph_id graph-artifact)
                             :graph_version (:graph_version graph-artifact)
                             :backend "databricks_job"
                             :job_id job-id
                             :request params
                             :response response
                             :sql_ir (:sql_ir compile-result)
                             :sql_checksum (sha256-hex (:compiled_sql compile-result))}
            validation-row  (persist-model-validation! {:proposal-id proposal-id
                                                        :status "submitted"
                                                        :validation-kind "gold_warehouse_job"
                                                        :validation-json validation-json
                                                        :compiled-sql (:compiled_sql compile-result)
                                                        :created-by created_by})]
        {:validation_id (:validation_id validation-row)
         :proposal_id proposal-id
         :status "submitted"
         :backend "databricks_job"
         :graph_artifact_id (:graph_artifact_id graph-artifact)
         :job_trigger response}))))

(defn- execute-silver-release-tx!
  [tx release-row created-by]
  (jdbc/execute! tx ["SELECT pg_advisory_xact_lock(?)" (long (:release_id release-row))])
  (when-let [existing-run (inflight-model-run-for-release tx (:release_id release-row))]
    (throw (ex-info "Silver release already has an in-flight execution"
                    {:release_id (:release_id release-row)
                     :model_run_id (:model_run_id existing-run)
                     :status 409})))
  (let [graph-artifact    (graph-artifact-by-id tx (:graph_artifact_id release-row))
        compiled-artifact (or (latest-compiled-model-artifact tx (:release_id release-row))
                              (let [compiled (compile-silver-graph-artifact! (:graph_artifact_id release-row))]
                                (persist-compiled-model-artifact! tx {:release-id (:release_id release-row)
                                                                      :artifact-kind "sql"
                                                                      :sql-ir (:sql_ir compiled)
                                                                      :sql-text (:compiled_sql compiled)
                                                                      :validation-json nil
                                                                      :created-by created-by})))
        graph             (db/getGraph (:graph_id graph-artifact))
        target-node-id    (target-node-id-from-graph-artifact graph-artifact graph)
        target-node       (g2/getData graph target-node-id)
        _                 (when-not target-node
                            (throw (ex-info "Generated Silver graph is missing a target node"
                                            {:release_id (:release_id release-row)
                                             :graph_id (:graph_id graph-artifact)
                                             :status 409})))
        conn-id           (or (:connection_id target-node) (:c target-node))
        warehouse         (target-warehouse target-node conn-id)
        execution-backend (target-execution-backend target-node)
        job-id            (when (= "databricks" warehouse)
                            (or (:silver_job_id target-node) ""))
        _                 (when (and (= "databricks" warehouse)
                                     (string/blank? job-id))
                            (throw (ex-info "Generated Silver graph target has no Databricks silver_job_id configured"
                                            {:release_id (:release_id release-row)
                                             :graph_id (:graph_id graph-artifact)
                                             :target_node_id target-node-id
                                             :status 400})))
        request-json      {:release_id (:release_id release-row)
                           :graph_artifact_id (:graph_artifact_id graph-artifact)
                           :graph_id (:graph_id graph-artifact)
                           :graph_version (:graph_version graph-artifact)
                           :target_model (:target_model release-row)
                           :target_table (qualified-target-table target-node)
                           :compiled_artifact_id (:artifact_id compiled-artifact)
                           :sql_checksum (:checksum compiled-artifact)
                           :target_warehouse warehouse}
        params            (merge {:model_release_id (str (:release_id release-row))
                                  :graph_id (str (:graph_id graph-artifact))
                                  :graph_version (str (:graph_version graph-artifact))
                                  :target_model (:target_model release-row)
                                  :target_table (qualified-target-table target-node)
                                  :compiled_artifact_id (str (:artifact_id compiled-artifact))
                                  :sql_checksum (:checksum compiled-artifact)}
                                 (or (:silver_job_params target-node) {}))
        run-row           (persist-compiled-model-run! tx {:release-id (:release_id release-row)
                                                           :graph-artifact-id (:graph_artifact_id graph-artifact)
                                                           :execution-backend execution-backend
                                                           :status "pending"
                                                           :target-connection-id conn-id
                                                           :job-id job-id
                                                           :external-run-id nil
                                                           :request-json request-json
                                                           :response-json nil
                                                           :created-by created-by
                                                           :completed-at nil})]
    {:model_run_id (:model_run_id run-row)
     :release_id (:release_id release-row)
     :graph_artifact_id (:graph_artifact_id graph-artifact)
     :graph_id (:graph_id graph-artifact)
     :graph_version (:graph_version graph-artifact)
     :conn_id conn-id
     :warehouse warehouse
     :job_id job-id
     :sql_ir (compiled-artifact-sql-ir compiled-artifact)
     :compiled_sql (:sql_text compiled-artifact)
     :params params
     :status "pending"}))

(defn execute-silver-release!
  ([release-id] (execute-silver-release! release-id {}))
  ([release-id {:keys [created_by]
                :or {created_by "system"}}]
   (ensure-modeling-tables!)
   (let [release-row (release-by-id release-id)]
     (when-not release-row
       (throw (ex-info "Silver release not found" {:release_id release-id :status 404})))
     (when-not (= "silver" (:layer release-row))
     (throw (ex-info "Release is not a Silver release" {:release_id release-id :status 400})))
     (when-not (:graph_artifact_id release-row)
       (throw (ex-info "Release has no intermediate graph artifact" {:release_id release-id :status 409})))
     (let [pending-run (jdbc/with-transaction [tx db/ds]
                         (execute-silver-release-tx! tx release-row created_by))]
       (try
         (if (contains? #{"bigquery" "snowflake" "postgresql"} (:warehouse pending-run))
           (execute-pending-model-run! pending-run poll-silver-model-run!)
           (let [response (dbx-jobs/trigger-job! (:conn_id pending-run) (:job_id pending-run) (:params pending-run))]
             (update-model-run-progress! (:model_run_id pending-run)
                                         {:status "submitted"
                                          :response-json response
                                          :external-run-id (some-> (:run_id response) str)})
             (-> pending-run
                 (select-keys [:model_run_id :release_id :graph_artifact_id :graph_id :graph_version])
                 (assoc :job_trigger response
                        :status "submitted"
                        :backend "databricks_job"))))
         (catch Exception e
           (complete-model-run! (:model_run_id pending-run)
                                {:status "failed"
                                 :response-json {:error (.getMessage e)
                                                 :error_data (ex-data e)}
                                 :completed-at (now-utc)})
           (throw e)))))))

(defn- execute-gold-release-tx!
  [tx release-row created-by]
  (jdbc/execute! tx ["SELECT pg_advisory_xact_lock(?)" (long (:release_id release-row))])
  (when-let [existing-run (inflight-model-run-for-release tx (:release_id release-row))]
    (throw (ex-info "Gold release already has an in-flight execution"
                    {:release_id (:release_id release-row)
                     :model_run_id (:model_run_id existing-run)
                     :status 409})))
  (let [graph-artifact    (graph-artifact-by-id tx (:graph_artifact_id release-row))
        compiled-artifact (or (latest-compiled-model-artifact tx (:release_id release-row))
                              (let [compiled (compile-gold-graph-artifact! (:graph_artifact_id release-row))]
                                (persist-compiled-model-artifact! tx {:release-id (:release_id release-row)
                                                                      :artifact-kind "sql"
                                                                      :sql-ir (:sql_ir compiled)
                                                                      :sql-text (:compiled_sql compiled)
                                                                      :validation-json nil
                                                                      :created-by created-by})))
        graph             (db/getGraph (:graph_id graph-artifact))
        target-node-id    (target-node-id-from-graph-artifact graph-artifact graph)
        target-node       (g2/getData graph target-node-id)
        _                 (when-not target-node
                            (throw (ex-info "Generated Gold graph is missing a target node"
                                            {:release_id (:release_id release-row)
                                             :graph_id (:graph_id graph-artifact)
                                             :status 409})))
        conn-id           (or (:connection_id target-node) (:c target-node))
        warehouse         (target-warehouse target-node conn-id)
        execution-backend (target-execution-backend target-node)
        job-id            (when (= "databricks" warehouse)
                            (or (:gold_job_id target-node) ""))
        _                 (when (and (= "databricks" warehouse)
                                     (string/blank? job-id))
                            (throw (ex-info "Generated Gold graph target has no Databricks gold_job_id configured"
                                            {:release_id (:release_id release-row)
                                             :graph_id (:graph_id graph-artifact)
                                             :target_node_id target-node-id
                                             :status 400})))
        request-json      {:release_id (:release_id release-row)
                           :graph_artifact_id (:graph_artifact_id graph-artifact)
                           :graph_id (:graph_id graph-artifact)
                           :graph_version (:graph_version graph-artifact)
                           :target_model (:target_model release-row)
                           :target_table (qualified-target-table target-node)
                           :compiled_artifact_id (:artifact_id compiled-artifact)
                           :sql_checksum (:checksum compiled-artifact)
                           :target_warehouse warehouse}
        params            (merge {:model_release_id (str (:release_id release-row))
                                  :graph_id (str (:graph_id graph-artifact))
                                  :graph_version (str (:graph_version graph-artifact))
                                  :target_model (:target_model release-row)
                                  :target_table (qualified-target-table target-node)
                                  :compiled_artifact_id (str (:artifact_id compiled-artifact))
                                  :sql_checksum (:checksum compiled-artifact)}
                                 (or (:gold_job_params target-node) {}))
        run-row           (persist-compiled-model-run! tx {:release-id (:release_id release-row)
                                                           :graph-artifact-id (:graph_artifact_id graph-artifact)
                                                           :execution-backend execution-backend
                                                           :status "pending"
                                                           :target-connection-id conn-id
                                                           :job-id job-id
                                                           :external-run-id nil
                                                           :request-json request-json
                                                           :response-json nil
                                                           :created-by created-by
                                                           :completed-at nil})]
    {:model_run_id (:model_run_id run-row)
     :release_id (:release_id release-row)
     :graph_artifact_id (:graph_artifact_id graph-artifact)
     :graph_id (:graph_id graph-artifact)
     :graph_version (:graph_version graph-artifact)
     :conn_id conn-id
     :warehouse warehouse
     :job_id job-id
     :sql_ir (compiled-artifact-sql-ir compiled-artifact)
     :compiled_sql (:sql_text compiled-artifact)
     :params params
     :status "pending"}))

(defn execute-gold-release!
  ([release-id] (execute-gold-release! release-id {}))
  ([release-id {:keys [created_by]
                :or {created_by "system"}}]
   (ensure-modeling-tables!)
   (let [release-row (release-by-id release-id)]
     (when-not release-row
       (throw (ex-info "Gold release not found" {:release_id release-id :status 404})))
     (when-not (= "gold" (:layer release-row))
       (throw (ex-info "Release is not a Gold release" {:release_id release-id :status 400})))
     (when-not (:graph_artifact_id release-row)
       (throw (ex-info "Release has no intermediate graph artifact" {:release_id release-id :status 409})))
     (let [pending-run (jdbc/with-transaction [tx db/ds]
                         (execute-gold-release-tx! tx release-row created_by))]
       (try
         (if (contains? #{"bigquery" "snowflake" "postgresql"} (:warehouse pending-run))
           (execute-pending-model-run! pending-run poll-gold-model-run!)
           (let [response (dbx-jobs/trigger-job! (:conn_id pending-run) (:job_id pending-run) (:params pending-run))]
             (update-model-run-progress! (:model_run_id pending-run)
                                         {:status "submitted"
                                          :response-json response
                                          :external-run-id (some-> (:run_id response) str)})
             (-> pending-run
                 (select-keys [:model_run_id :release_id :graph_artifact_id :graph_id :graph_version])
                 (assoc :job_trigger response
                        :status "submitted"
                        :backend "databricks_job"))))
         (catch Exception e
           (complete-model-run! (:model_run_id pending-run)
                                {:status "failed"
                                 :response-json {:error (.getMessage e)
                                                 :error_data (ex-data e)}
                                 :completed-at (now-utc)})
           (throw e)))))))

(defn execute-queued-silver-release!
  [release-id {:keys [created_by execution_request_id]
               :or {created_by "system"}}]
  (ensure-modeling-tables!)
  (let [release-row (release-by-id release-id)]
    (when-not release-row
      (throw (ex-info "Resolved release no longer exists"
                      {:release_id release-id
                       :failure_class "config_error"})))
    (let [pending-run (jdbc/with-transaction [tx db/ds]
                        (let [run (execute-silver-release-tx! tx release-row created_by)]
                          (when execution_request_id
                            (link-model-run-to-request! tx (:model_run_id run) execution_request_id))
                          run))]
      (try
        (execute-pending-model-run! pending-run poll-silver-model-run!)
        (catch Exception e
          (complete-model-run! (:model_run_id pending-run)
                               {:status "failed"
                                :response-json {:error (.getMessage e)
                                                :error_data (ex-data e)}
                                :completed-at (now-utc)})
          (throw e))))))

(defn execute-queued-gold-release!
  [release-id {:keys [created_by execution_request_id]
               :or {created_by "system"}}]
  (ensure-modeling-tables!)
  (let [release-row (release-by-id release-id)]
    (when-not release-row
      (throw (ex-info "Resolved release no longer exists"
                      {:release_id release-id
                       :failure_class "config_error"})))
    (let [pending-run (jdbc/with-transaction [tx db/ds]
                        (let [run (execute-gold-release-tx! tx release-row created_by)]
                          (when execution_request_id
                            (link-model-run-to-request! tx (:model_run_id run) execution_request_id))
                          run))]
      (try
        (execute-pending-model-run! pending-run poll-gold-model-run!)
        (catch Exception e
          (complete-model-run! (:model_run_id pending-run)
                               {:status "failed"
                                :response-json {:error (.getMessage e)
                                                :error_data (ex-data e)}
                                :completed-at (now-utc)})
          (throw e))))))

(defn poll-gold-model-run!
  [model-run-id]
  (poll-silver-model-run! model-run-id))

(defn preview-target-data
  "Query the proposal's target table for a small sample of rows."
  [proposal-id {:keys [limit] :or {limit 10}}]
  (ensure-modeling-tables!)
  (let [proposal-row (proposal-by-id proposal-id)]
    (when-not proposal-row
      (throw (ex-info "Proposal not found" {:proposal_id proposal-id :status 404})))
    (let [proposal-json  (parse-json-safe (:proposal_json proposal-row))
          target-table   (:target_table proposal-json)
          _              (when-not (valid-qualified-table-name? target-table)
                           (throw (ex-info "No valid target table on proposal"
                                           {:proposal_id proposal-id :status 400})))
          graph-id       (:source_graph_id proposal-row)
          source-node-id (:source_node_id proposal-row)
          graph          (db/getGraph graph-id)
          target         (find-downstream-target graph source-node-id)
          conn-id        (target-connection-id target)
          _              (when-not conn-id
                           (throw (ex-info "No target connection available"
                                           {:proposal_id proposal-id :status 400})))
          safe-limit     (max 1 (min (or limit 10) 100))
          sql            (str "SELECT * FROM " target-table " LIMIT " safe-limit)
          rows           (jdbc/execute! (db/get-opts conn-id nil) [sql])]
      {:proposal_id proposal-id
       :target_table target-table
       :row_count (count rows)
       :rows (vec rows)})))

(mount/defstate ^{:on-reload :noop} silver-run-poller
  :start
  (when (and (config/enabled-role? :api)
             (parse-bool-env :bitool-enable-silver-run-poller true))
    (let [running? (atom true)
          poll-ms  (parse-int-env :bitool-silver-run-poll-ms 15000)
          limit    (parse-int-env :bitool-silver-run-poll-limit 25)
          thread   (future
                     (while @running?
                       (try
                         (reconcile-silver-model-runs! {:limit limit})
                         (catch Exception e
                           (log/error e "Silver run poller iteration failed")))
                       (Thread/sleep (long poll-ms))))]
      {:running? running?
       :thread thread}))
  :stop
  (do
    (when-let [running? (:running? silver-run-poller)]
      (reset! running? false))
    (when-let [thread (:thread silver-run-poller)]
      (future-cancel thread))))
