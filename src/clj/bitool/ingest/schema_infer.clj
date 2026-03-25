(ns bitool.ingest.schema-infer
  (:require [bitool.api.jsontf :as tf]
            [bitool.utils :refer [path->name]]
            [clojure.string :as string]))

(declare join-path)

(defn logical-records-from-body
  [body records-path]
  (if (seq (string/trim (str records-path)))
    (let [;; Normalize path: strip "$.", "[]" suffixes, then split by "."
          clean-path (-> (str records-path)
                         (string/replace #"^\$\.?" "")
                         (string/replace #"\[\]" ""))
          path-parts (remove string/blank? (string/split clean-path #"\."))
          data (reduce (fn [m k]
                         (cond
                           (and (map? m) (get m k)) (get m k)
                           (and (map? m) (get m (keyword k))) (get m (keyword k))
                           ;; Handle arrays: if current value is sequential, map over items
                           (sequential? m) (mapcat (fn [item]
                                                     (let [v (or (get item k) (get item (keyword k)))]
                                                       (if (sequential? v) v (when v [v]))))
                                                   m)
                           :else nil))
                       body path-parts)]
      (cond
        (sequential? data) (vec (filter some? data))
        (some? data) [data]
        :else [body]))
    [body]))

(defn- candidate-record-array-paths
  ([value] (candidate-record-array-paths value "$" 0 4))
  ([value path depth max-depth]
   (cond
     (> depth max-depth) []

     (map? value)
     (mapcat (fn [[k v]]
               (candidate-record-array-paths v (join-path path (name k)) (inc depth) max-depth))
             value)

     (sequential? value)
     (let [items     (vec value)
           path'     (str path "[]")
           map-items (filter map? items)
           candidate (when (seq map-items)
                       [{:path path'
                         :depth depth
                         :map_item_count (count map-items)
                         :item_count (count items)}])
           nested    (->> map-items
                          (take 5)
                          (mapcat #(candidate-record-array-paths % path' (inc depth) max-depth)))]
       (vec (concat candidate nested)))

     :else [])))

(defn detect-dominant-records-path
  [body]
  (some->> (candidate-record-array-paths body)
           (sort-by (fn [{:keys [depth map_item_count item_count path]}]
                      [depth (- map_item_count) (- item_count) (count path) path]))
           first
           :path))

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
  (let [manual     (manual-selected-fields (:selected_nodes endpoint))
        inferred   (->> (:inferred_fields endpoint)
                        (filter #(not= false (:enabled %)))
                        vec)
        spec-fields (vec (or (:spec_fields endpoint) []))
        schema-mode (or (:schema_mode endpoint) "manual")]
    (case schema-mode
      "infer" inferred
      "spec"  (if (seq spec-fields) spec-fields inferred)
      "hybrid" (->> (concat spec-fields manual inferred)
                    (reduce (fn [acc field]
                              (if (contains? acc (:path field))
                                acc
                                (assoc acc (:path field) field)))
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

;; ─── OpenAPI Spec → Field Descriptors Bridge ──────────────────────────

(defn- openapi-type->bitool-type
  "Map an OpenAPI/JSON Schema type+format to the closest BiTool column type."
  [type-str format-str]
  (let [t (some-> type-str string/lower-case)
        f (some-> format-str string/lower-case)]
    (case t
      "integer" (if (= f "int64") "BIGINT" "INT")
      "number"  "DOUBLE"
      "boolean" "BOOLEAN"
      "string"  (case f
                  "date"      "DATE"
                  "date-time" "TIMESTAMP"
                  "STRING")
      "STRING")))

(defn- flatten-openapi-schema
  "Recursively flatten an OpenAPI object schema into a sequence of field descriptors.
   Walks nested properties like the runtime inferrer does (path = $.field.subfield).
   `resolve-fn` resolves $ref within the spec.
   `parent-required?` tracks whether every ancestor in the path is itself required —
   a child is only truly required if every parent on the path to the root is also required."
  ([schema required-set records-path resolve-fn]
   (flatten-openapi-schema schema required-set records-path resolve-fn "$" 0 4 true))
  ([schema required-set records-path resolve-fn prefix depth max-depth]
   (flatten-openapi-schema schema required-set records-path resolve-fn prefix depth max-depth true))
  ([schema required-set records-path resolve-fn prefix depth max-depth parent-required?]
   (when (and schema (<= depth max-depth))
     (let [props (or (:properties schema) {})]
       (mapcat
        (fn [[k v]]
          (let [v          (if resolve-fn (resolve-fn v) v)
                field-name (name k)
                path       (if (or (= prefix "$") (= prefix "$[]"))
                             (str prefix "." field-name)
                             (str prefix "." field-name))
                full-path  (if (seq (string/trim (str records-path)))
                             (str records-path (subs path 1))
                             path)
                type-str   (:type v)
                format-str (:format v)
                locally-required? (or (contains? required-set field-name)
                                      (contains? required-set (keyword field-name)))
                ;; A field is only truly required if it AND every ancestor are required
                effectively-required? (and parent-required? locally-required?)
                nullable   (or (true? (:nullable v))
                               (not effectively-required?))
                required?  effectively-required?]
            (cond
              ;; Nested object — recurse, propagating whether this parent is required
              (= type-str "object")
              (let [nested-required (set (or (:required v) []))]
                (flatten-openapi-schema v nested-required records-path resolve-fn
                                        path (inc depth) max-depth effectively-required?))

              ;; Array of objects — recurse into items
              ;; Arrays are inherently nullable containers, so children are not required
              (and (= type-str "array") (= "object" (:type (:items v))))
              (let [items       (if resolve-fn (resolve-fn (:items v)) (:items v))
                    arr-path    (str path "[]")
                    items-req   (set (or (:required items) []))]
                (flatten-openapi-schema items items-req records-path resolve-fn
                                        arr-path (inc depth) max-depth false))

              ;; Scalar field
              :else
              [{:path        full-path
                :column_name (path->name full-path)
                :source_kind "spec"
                :enabled     true
                :type        (openapi-type->bitool-type type-str format-str)
                :observed_types [(openapi-type->bitool-type type-str format-str)]
                :nullable    nullable
                :is_required required?
                :confidence  1.0
                :sample_coverage 1.0
                :depth       depth
                :array_mode  "scalar"
                :override_type ""
                :notes       (or (:description v) "")}])))
        props)))))

(defn openapi-schema->field-descriptors
  "Convert a resolved OpenAPI response schema into BiTool inferred_fields format.

   Arguments:
     schema       - A resolved OpenAPI schema map (no unresolved $refs).
                    Must have {:type \"object\" :properties {...} :required [...]}
     records-path - The json_explode_rules path (e.g. \"$.data[]\"), or \"\" for root.
     resolve-fn   - Optional fn to resolve remaining $refs. Pass nil if fully resolved.

   Returns a vector of field descriptor maps compatible with inferred_fields."
  [schema records-path resolve-fn]
  (let [root-is-array? (and (= "array" (:type schema)) (:items schema))
        effective-schema (if root-is-array?
                           (if resolve-fn (resolve-fn (:items schema)) (:items schema))
                           schema)
        ;; When the root is an array, start paths at $[] to match runtime inferrer
        ;; (which produces $[].field for top-level array responses).
        start-prefix (if root-is-array? "$[]" "$")
        required-set (set (or (:required effective-schema) []))]
    (vec (flatten-openapi-schema effective-schema required-set (or records-path "")
                                 resolve-fn start-prefix 0 4))))
