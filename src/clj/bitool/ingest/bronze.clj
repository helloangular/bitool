(ns bitool.ingest.bronze
  (:require [bitool.api.jsontf :as tf]
            [bitool.ingest.schema-infer :as schema-infer]
            [bitool.utils :refer [path->name]]
            [cheshire.core :as json]
            [clojure.string :as string]))

(def bronze-base-columns
  [{:column_name "ingestion_id"    :data_type "STRING"    :is_nullable "NO"}
   {:column_name "run_id"          :data_type "STRING"    :is_nullable "NO"}
   {:column_name "batch_id"        :data_type "STRING"    :is_nullable "YES"}
   {:column_name "source_system"   :data_type "STRING"    :is_nullable "NO"}
   {:column_name "endpoint_name"   :data_type "STRING"    :is_nullable "NO"}
   {:column_name "extracted_at_utc" :data_type "STRING"   :is_nullable "NO"}
   {:column_name "ingested_at_utc" :data_type "STRING"    :is_nullable "NO"}
   {:column_name "api_request_url" :data_type "STRING"    :is_nullable "YES"}
   {:column_name "api_page_number" :data_type "INT"       :is_nullable "YES"}
   {:column_name "api_cursor"      :data_type "STRING"    :is_nullable "YES"}
   {:column_name "http_status_code" :data_type "INT"      :is_nullable "YES"}
   {:column_name "record_hash"     :data_type "STRING"    :is_nullable "NO"}
   {:column_name "source_record_id" :data_type "STRING"   :is_nullable "YES"}
   {:column_name "event_time_utc"  :data_type "STRING"    :is_nullable "YES"}
   {:column_name "partition_date"  :data_type "DATE"      :is_nullable "NO"}
   {:column_name "load_date"       :data_type "DATE"      :is_nullable "NO"}
   {:column_name "payload_json"    :data_type "STRING"    :is_nullable "NO"}])

(def bad-record-columns
  [{:column_name "bad_record_id" :data_type "STRING" :is_nullable "NO"}
   {:column_name "run_id"        :data_type "STRING" :is_nullable "NO"}
   {:column_name "batch_id"      :data_type "STRING" :is_nullable "YES"}
   {:column_name "source_system" :data_type "STRING" :is_nullable "NO"}
   {:column_name "endpoint_name" :data_type "STRING" :is_nullable "NO"}
   {:column_name "error_message" :data_type "STRING" :is_nullable "NO"}
   {:column_name "row_json"      :data_type "STRING" :is_nullable "YES"}
   {:column_name "payload_json"  :data_type "STRING" :is_nullable "YES"}
   {:column_name "replay_status" :data_type "STRING" :is_nullable "YES"}
   {:column_name "replayed_run_id" :data_type "STRING" :is_nullable "YES"}
   {:column_name "replayed_at_utc" :data_type "STRING" :is_nullable "YES"}
   {:column_name "replay_error_message" :data_type "STRING" :is_nullable "YES"}
   {:column_name "created_at_utc" :data_type "STRING" :is_nullable "NO"}])

(def bronze-reserved-column-names
  (set (map :column_name bronze-base-columns)))

(defn- canonical-promoted-type [type-name]
  (let [type-name (some-> type-name str string/trim string/upper-case)]
    (case type-name
      nil "STRING"
      "" "STRING"
      "VARCHAR" "STRING"
      "TEXT" "STRING"
      "INTEGER" "INT"
      "LONG" "BIGINT"
      "FLOAT" "DOUBLE"
      "DECIMAL" "DOUBLE"
      "NUMERIC" "DOUBLE"
      "BOOL" "BOOLEAN"
      "DATETIME" "TIMESTAMP"
      type-name)))

(defn promoted-columns
  [endpoint-config]
  (mapv (fn [field]
          {:column_name (:column_name field)
           :data_type (canonical-promoted-type
                        (or (not-empty (:override_type field))
                            (:type field)
                            "STRING"))
           :is_nullable (if (:nullable field) "YES" "NO")})
        (schema-infer/effective-field-descriptors endpoint-config)))

(defn bronze-columns
  [endpoint-config]
  (vec (concat bronze-base-columns (promoted-columns endpoint-config))))

(defn- effective-type
  [descriptor]
  (canonical-promoted-type
    (or (not-empty (:override_type descriptor))
        (:type descriptor)
        "STRING")))

(defn- sha256 [s]
  (let [digest (java.security.MessageDigest/getInstance "SHA-256")]
    (.update digest (.getBytes (str s) "UTF-8"))
    (format "%064x" (java.math.BigInteger. 1 (.digest digest)))))

(defn- keyword-or-string [m k]
  (or (get m k)
      (get m (keyword k))
      (get m (name k))
      (get m (keyword (name k)))))

(defn- row-value [row field]
  (let [field-name (name field)
        direct     (or (keyword-or-string row field-name)
                       (keyword-or-string row (path->name field-name)))]
    (or direct
        (some (fn [[k v]]
                (let [k-name (if (keyword? k) (name k) (str k))]
                  (when (or (= k-name field-name)
                            (= k-name (path->name field-name))
                            (string/ends-with? k-name (str "_" field-name)))
                    v)))
              row))))

(defn- selected-mapping [descriptors records-path]
  (cond-> (zipmap (map :path descriptors) (map #(keyword (:column_name %)) descriptors))
    (seq (string/trim (str records-path))) (assoc records-path :_record)))

(defn- rows-from-body [body endpoint-config]
  (let [records-path (or (get-in endpoint-config [:json_explode_rules 0 :path]) "")
        descriptors  (schema-infer/effective-field-descriptors endpoint-config)]
    (if (seq descriptors)
      (let [mapping (selected-mapping descriptors records-path)
          opts    (if (seq (string/trim (str records-path)))
                    {:row-mode :explode-by :explode-key records-path}
                    {:row-mode :per-context})]
        (tf/rows-from-json body mapping opts))
      (mapv (fn [record] {:_record record})
            (schema-infer/logical-records-from-body body records-path)))))

(defn- source-record-id [row primary-key-fields]
  (when (seq primary-key-fields)
    (let [parts (map (fn [field]
                       (or (row-value row field)
                           ""))
                     primary-key-fields)]
      (string/join "|" parts))))

(defn- event-time [row watermark-column]
  (when (seq (str watermark-column))
    (row-value row watermark-column)))

(defn- parse-boolean [v]
  (cond
    (boolean? v) v
    (string? v) (let [normalized (string/lower-case (string/trim v))]
                  (cond
                    (#{"true" "1" "yes" "on"} normalized) true
                    (#{"false" "0" "no" "off"} normalized) false
                    :else (throw (ex-info "Value cannot be coerced to BOOLEAN"
                                          {:value v}))))
    :else (throw (ex-info "Value cannot be coerced to BOOLEAN"
                          {:value v}))))

(defn- coerce-promoted-value [descriptor value]
  (when (some? value)
    (let [type-name (effective-type descriptor)]
      (case type-name
        "STRING" (str value)
        "INT" (cond
                (integer? value) (int value)
                (number? value) (int value)
                :else (Integer/parseInt (string/trim (str value))))
        "BIGINT" (cond
                   (integer? value) (long value)
                   (number? value) (long value)
                   :else (Long/parseLong (string/trim (str value))))
        "DOUBLE" (cond
                   (number? value) (double value)
                   :else (Double/parseDouble (string/trim (str value))))
        "BOOLEAN" (parse-boolean value)
        "DATE" (cond
                 (instance? java.time.LocalDate value) value
                 :else (java.time.LocalDate/parse (string/trim (str value))))
        "TIMESTAMP" (cond
                      (instance? java.time.Instant value) value
                      :else (java.time.Instant/parse (string/trim (str value))))
        (throw (ex-info "Unsupported promoted column type"
                        {:type type-name
                         :descriptor descriptor}))))))

(defn- promoted-row
  [row endpoint-config]
  (->> (schema-infer/effective-field-descriptors endpoint-config)
       (map (fn [descriptor]
              (let [column-name (:column_name descriptor)
                    value (row-value row column-name)]
                [(keyword column-name)
                 (coerce-promoted-value descriptor value)])))
       (remove (comp nil? second))
       (into {})))

(defn- bad-record-row
  [run-id source-system endpoint-config now source-row raw-record error-message]
  {:bad_record_id (str (java.util.UUID/randomUUID))
   :run_id run-id
   :source_system source-system
   :endpoint_name (:endpoint_name endpoint-config)
   :error_message error-message
   :row_json (try
               (json/generate-string (dissoc source-row :_record))
               (catch Exception _
                 nil))
   :payload_json (try (json/generate-string raw-record)
                      (catch Exception _ (str raw-record)))
   :replay_status "pending"
   :replayed_run_id nil
   :replayed_at_utc nil
   :replay_error_message nil
   :created_at_utc (str now)})

(defn- bronze-success-row
  [source-row raw-record endpoint-config {:keys [run-id source-system now request-url page-number api-cursor http-status]}]
  (let [payload-json      (json/generate-string raw-record)
        source-id         (source-record-id source-row (:primary_key_fields endpoint-config))
        event-time-utc    (event-time source-row (:watermark_column endpoint-config))
        partition-date     (str (java.time.LocalDate/ofInstant now (java.time.ZoneOffset/UTC)))
        load-date          partition-date
        base-row          {:ingestion_id     run-id
                           :run_id           run-id
                           :source_system    source-system
                           :endpoint_name    (:endpoint_name endpoint-config)
                           :extracted_at_utc (str now)
                           :ingested_at_utc  (str now)
                           :api_request_url  request-url
                           :api_page_number  page-number
                           :api_cursor       api-cursor
                           :http_status_code http-status
                           :record_hash      (sha256 payload-json)
                           :source_record_id source-id
                           :event_time_utc   event-time-utc
                           :partition_date   partition-date
                           :load_date        load-date
                           :payload_json     payload-json}
        promoted          (promoted-row source-row endpoint-config)]
    (merge base-row promoted)))

(defn build-page-rows
  [{:keys [body page state response]} endpoint-config {:keys [run-id source-system now request-url]}]
  (let [rows               (rows-from-body body endpoint-config)
        page-number        (or page 1)
        api-cursor         (or (:cursor state) (:next-url state) (:last-cursor state))
        http-status        (:status response)]
    (reduce (fn [{:keys [rows bad-records]} row]
              (let [raw-record (or (:_record row) row)]
                (try
                  (let [success-row (bronze-success-row row raw-record endpoint-config
                                                       {:run-id run-id
                                                        :source-system source-system
                                                        :now now
                                                        :request-url request-url
                                                        :page-number page-number
                                                        :api-cursor api-cursor
                                                        :http-status http-status})]
                    {:rows (conj rows success-row)
                     :bad-records bad-records})
                  (catch Exception e
                    {:rows rows
                     :bad-records (conj bad-records (bad-record-row run-id source-system endpoint-config now row raw-record (.getMessage e)))}))))
            {:rows [] :bad-records []}
            rows)))

(defn build-record-rows
  [records endpoint-config {:keys [run-id source-system now request-url page-number cursor http-status]}]
  (reduce (fn [{:keys [rows bad-records]} [idx raw-record]]
            (let [source-row (if (map? raw-record) raw-record {:_record raw-record})]
              (if-let [parse-error (:_bitool_parse_error source-row)]
                {:rows rows
                 :bad-records (conj bad-records
                                    (bad-record-row run-id
                                                    source-system
                                                    endpoint-config
                                                    now
                                                    (dissoc source-row :_bitool_parse_error)
                                                    (or (:_record source-row) raw-record)
                                                    parse-error))}
                (try
                  (let [success-row (bronze-success-row source-row raw-record endpoint-config
                                                       {:run-id run-id
                                                        :source-system source-system
                                                        :now now
                                                        :request-url request-url
                                                        :page-number (or page-number (inc idx))
                                                        :api-cursor cursor
                                                        :http-status http-status})]
                    {:rows (conj rows success-row)
                     :bad-records bad-records})
                  (catch Exception e
                    {:rows rows
                     :bad-records (conj bad-records
                                        (bad-record-row run-id
                                                        source-system
                                                        endpoint-config
                                                        now
                                                        source-row
                                                        raw-record
                                                        (.getMessage e)))})))))
          {:rows [] :bad-records []}
          (map-indexed vector (or records []))))

(defn replay-bad-records->rows
  [bad-record-rows endpoint-config {:keys [run-id source-system now request-url]}]
  (reduce
   (fn [{:keys [rows bad-records succeeded-source-bad-record-ids failed-source-bad-record-ids]} [idx bad-record-row*]]
     (let [raw-record (try
                        (json/parse-string (or (:payload_json bad-record-row*) "null") true)
                        (catch Exception _
                          (:payload_json bad-record-row*)))
           source-row (or (try
                            (when-let [raw-row-json (:row_json bad-record-row*)]
                              (json/parse-string raw-row-json true))
                            (catch Exception _ nil))
                          raw-record)]
       (try
         (let [success-row (bronze-success-row source-row raw-record endpoint-config
                                              {:run-id run-id
                                               :source-system source-system
                                               :now now
                                               :request-url request-url
                                               :page-number (inc idx)
                                               :api-cursor nil
                                               :http-status 200})]
           {:rows (conj rows success-row)
            :bad-records bad-records
            :succeeded-source-bad-record-ids (cond-> succeeded-source-bad-record-ids
                                                (seq (:bad_record_id bad-record-row*))
                                                (conj (:bad_record_id bad-record-row*)))
            :failed-source-bad-record-ids failed-source-bad-record-ids})
         (catch Exception e
           {:rows rows
            :bad-records (conj bad-records
                               (-> (bad-record-row run-id source-system endpoint-config now source-row raw-record (.getMessage e))
                                   (assoc :replay_status "failed")))
            :succeeded-source-bad-record-ids succeeded-source-bad-record-ids
            :failed-source-bad-record-ids (cond-> failed-source-bad-record-ids
                                             (seq (:bad_record_id bad-record-row*))
                                             (conj (:bad_record_id bad-record-row*)))}))))
   {:rows []
    :bad-records []
    :succeeded-source-bad-record-ids []
    :failed-source-bad-record-ids []}
   (map-indexed vector bad-record-rows)))
