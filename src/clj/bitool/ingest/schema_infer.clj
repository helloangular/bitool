(ns bitool.ingest.schema-infer
  (:require [bitool.api.jsontf :as tf]
            [bitool.utils :refer [path->name]]
            [clojure.string :as string]))

(defn logical-records-from-body
  [body records-path]
  (if (seq (string/trim (str records-path)))
    (->> (tf/rows-from-json body {records-path :_record}
                            {:row-mode :explode-by
                             :explode-key records-path})
         (map :_record)
         (filter some?)
         vec)
    [body]))

(defn- join-path [path segment]
  (cond
    (= path "$") (str "$." segment)
    (string/ends-with? path "[]") (str path "." segment)
    :else (str path "." segment)))

(defn- scalar?
  [v]
  (or (nil? v)
      (string? v)
      (number? v)
      (boolean? v)
      (keyword? v)))

(defn- value-type [v]
  (cond
    (nil? v) "NULL"
    (boolean? v) "BOOLEAN"
    (integer? v) (if (<= Integer/MIN_VALUE v Integer/MAX_VALUE) "INT" "BIGINT")
    (float? v) "DOUBLE"
    (double? v) "DOUBLE"
    (decimal? v) "DOUBLE"
    (string? v)
    (cond
      (try (java.time.Instant/parse v) true (catch Exception _ false)) "TIMESTAMP"
      (try (java.time.LocalDate/parse v) true (catch Exception _ false)) "DATE"
      :else "STRING")
    :else "STRING"))

(defn- flatten-observations
  ([value] (flatten-observations value "$" 0 4))
  ([value path depth max-depth]
   (cond
     (> depth max-depth) []
     (map? value)
     (mapcat (fn [[k v]]
               (flatten-observations v (join-path path (name k)) (inc depth) max-depth))
             value)

     (sequential? value)
     (let [path' (str path "[]")]
       (mapcat (fn [v]
                 (if (scalar? v)
                   [{:path path' :value v :depth depth :array_mode "array_scalar"}]
                   (flatten-observations v path' (inc depth) max-depth)))
               value))

     :else
     [{:path path :value value :depth depth :array_mode "scalar"}])))

(defn- final-type
  [observed-types {:keys [type_inference_enabled array_mode]}]
  (let [nonnull-types (remove #{"NULL"} observed-types)]
    (cond
      (not type_inference_enabled) "STRING"
      (not= array_mode "scalar") "STRING"
      (empty? nonnull-types) "STRING"
      (= 1 (count nonnull-types)) (first nonnull-types)
      (every? #{"INT" "BIGINT"} nonnull-types) "BIGINT"
      (every? #{"INT" "BIGINT" "DOUBLE"} nonnull-types) "DOUBLE"
      :else "STRING")))

(defn- confidence
  [coverage observed-types]
  (let [nonnull-types (remove #{"NULL"} observed-types)
        consistency (if (empty? nonnull-types)
                      0.5
                      (/ 1.0 (double (count (distinct nonnull-types)))))]
    (* coverage consistency)))

(defn- prepend-records-path [records-path relative-path]
  (if (seq (string/trim (str records-path)))
    (if (= "$" relative-path)
      records-path
      (str records-path (subs relative-path 1)))
    relative-path))

(defn infer-fields-from-records
  [records {:keys [records_path sample_records max_inferred_columns type_inference_enabled]
            :or {sample_records 100 max_inferred_columns 100 type_inference_enabled true}}]
  (let [sampled       (vec (take sample_records (remove nil? records)))
        record-count  (count sampled)
        observations  (map-indexed
                        (fn [idx record]
                          [idx (flatten-observations record "$" 0 4)])
                        sampled)
        aggregate     (reduce
                        (fn [acc [record-idx leaves]]
                          (reduce
                            (fn [m {:keys [path value depth array_mode]}]
                              (-> m
                                  (update-in [path :observed_types] (fnil conj []) (value-type value))
                                  (update-in [path :record_ids] (fnil conj #{}) record-idx)
                                  (assoc-in [path :depth] depth)
                                  (assoc-in [path :array_mode] array_mode)))
                            acc
                            leaves))
                        {}
                        observations)]
    (->> aggregate
         (map (fn [[path {:keys [observed_types record_ids depth array_mode]}]]
                (let [coverage (/ (double (count record_ids)) (double (max 1 record-count)))
                      full-path (prepend-records-path records_path path)
                      final     (final-type (distinct observed_types)
                                            {:type_inference_enabled type_inference_enabled
                                             :array_mode array_mode})]
                  {:path full-path
                   :column_name (path->name full-path)
                   :source_kind "inferred"
                   :enabled true
                   :type final
                   :observed_types (vec (distinct observed_types))
                   :nullable (or (< (count record_ids) record-count)
                                 (some #{"NULL"} observed_types))
                   :confidence (confidence coverage (distinct observed_types))
                   :sample_coverage coverage
                   :depth depth
                   :array_mode array_mode
                   :override_type ""
                   :notes ""})))
         (sort-by (juxt (comp - :sample_coverage) :depth :path))
         (take max_inferred_columns)
         vec)))

(defn infer-fields-from-pages
  [pages endpoint]
  (let [records-path  (or (get-in endpoint [:json_explode_rules 0 :path]) "")
        sample-records (or (:sample_records endpoint) 100)
        records       (->> pages
                           (mapcat #(logical-records-from-body (:body %) records-path))
                           (take sample-records)
                           vec)]
    (infer-fields-from-records
      records
      {:records_path records-path
       :sample_records sample-records
       :max_inferred_columns (or (:max_inferred_columns endpoint) 100)
       :type_inference_enabled (not= false (:type_inference_enabled endpoint))})))

(defn manual-selected-fields
  [selected-nodes]
  (mapv (fn [path]
          {:path path
           :column_name (path->name path)
           :source_kind "manual"
           :enabled true
           :type "STRING"
           :observed_types ["STRING"]
           :nullable true
           :confidence 1.0
           :sample_coverage 1.0
           :depth 0
           :array_mode "scalar"
           :override_type ""
           :notes ""})
        (or selected-nodes [])))

(defn effective-field-descriptors
  [endpoint]
  (let [manual   (manual-selected-fields (:selected_nodes endpoint))
        inferred (->> (:inferred_fields endpoint)
                      (filter #(not= false (:enabled %)))
                      vec)
        schema-mode (or (:schema_mode endpoint) "manual")]
    (case schema-mode
      "infer" inferred
      "hybrid" (->> (concat manual inferred)
                    (reduce (fn [acc field]
                              (assoc acc (:path field) field))
                            {})
                    vals
                    vec)
      manual)))

(defn infer-endpoint-fields
  [endpoint body]
  (let [records-path (or (get-in endpoint [:json_explode_rules 0 :path]) "")
        records      (logical-records-from-body body records-path)]
    (infer-fields-from-records
      records
      {:records_path records-path
       :sample_records (or (:sample_records endpoint) 100)
       :max_inferred_columns (or (:max_inferred_columns endpoint) 100)
       :type_inference_enabled (not= false (:type_inference_enabled endpoint))})))
