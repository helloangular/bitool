(ns bitool.ingest.checkpoint
  (:require [clojure.string :as string]))

(def ^:private oracle-utc-formatter
  (-> (java.time.format.DateTimeFormatterBuilder.)
      (.appendPattern "yyyy-MM-dd HH:mm:ss")
      (.optionalStart)
      (.appendFraction java.time.temporal.ChronoField/NANO_OF_SECOND 0 9 true)
      (.optionalEnd)
      (.appendLiteral " UTC")
      (.toFormatter)))

(defn watermark-query-param
  [endpoint-config]
  (or (:watermark_param endpoint-config)
      (:time_param endpoint-config)
      (:watermark_column endpoint-config)))

(def ingestion-checkpoint-columns
  [{:column_name "source_system"             :data_type "STRING" :is_nullable "NO"}
   {:column_name "endpoint_name"             :data_type "STRING" :is_nullable "NO"}
   {:column_name "last_successful_watermark" :data_type "STRING" :is_nullable "YES"}
   {:column_name "last_attempted_watermark"  :data_type "STRING" :is_nullable "YES"}
   {:column_name "last_successful_cursor"    :data_type "STRING" :is_nullable "YES"}
   {:column_name "last_attempted_cursor"     :data_type "STRING" :is_nullable "YES"}
   {:column_name "last_successful_run_id"    :data_type "STRING" :is_nullable "YES"}
   {:column_name "last_successful_batch_id"  :data_type "STRING" :is_nullable "YES"}
   {:column_name "last_successful_batch_seq" :data_type "INT"    :is_nullable "YES"}
   {:column_name "last_status"               :data_type "STRING" :is_nullable "YES"}
   {:column_name "rows_ingested"             :data_type "INT"    :is_nullable "YES"}
   {:column_name "updated_at_utc"            :data_type "STRING" :is_nullable "NO"}])

(defn parse-instant [s]
  (when (and (some? s) (seq (string/trim (str s))))
    (let [value (string/trim (str s))]
      (or (try
            (java.time.Instant/parse value)
            (catch Exception _
              nil))
          (try
            (.toInstant (java.time.OffsetDateTime/parse value))
            (catch Exception _
              nil))
          (try
            (-> (java.time.LocalDateTime/parse value oracle-utc-formatter)
                (.atZone java.time.ZoneOffset/UTC)
                (.toInstant))
            (catch Exception _
              nil))
          (throw (ex-info (str "Unsupported watermark timestamp format: " value)
                          {:value value})))))) 

(defn instant->str [inst]
  (when inst (str inst)))

(defn window-start
  [checkpoint-row endpoint-config now]
  (let [wm-col       (:watermark_column endpoint-config)
        overlap-mins (long (or (:watermark_overlap_minutes endpoint-config) 0))
        checkpoint   (some-> checkpoint-row :last_successful_watermark parse-instant)]
    (cond
      (string/blank? wm-col) nil
      checkpoint             (instant->str (.minusSeconds checkpoint (* overlap-mins 60)))
      :else                  nil)))

(defn watermark-query-params
  [checkpoint-row endpoint-config now]
  (let [query-param (watermark-query-param endpoint-config)
        start       (window-start checkpoint-row endpoint-config now)]
    (if (and (seq (str query-param)) start)
      {(keyword query-param) start}
      {})))

(defn success-row
  [{:keys [source_system endpoint_name run_id batch_id batch_seq rows_ingested max_watermark next_cursor now status]}]
  {:source_system             source_system
   :endpoint_name             endpoint_name
   :last_successful_watermark max_watermark
   :last_attempted_watermark  max_watermark
   :last_successful_cursor    next_cursor
   :last_attempted_cursor     next_cursor
   :last_successful_run_id    run_id
   :last_successful_batch_id  batch_id
   :last_successful_batch_seq batch_seq
   :last_status               (or status "success")
   :rows_ingested             rows_ingested
   :updated_at_utc            (str now)})

(defn failure-row
  [{:keys [source_system endpoint_name attempted_watermark attempted_cursor now]}]
  {:source_system             source_system
   :endpoint_name             endpoint_name
   :last_attempted_watermark  attempted_watermark
   :last_attempted_cursor     attempted_cursor
   :last_status               "failed"
   :updated_at_utc            (str now)})
