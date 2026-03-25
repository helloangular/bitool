(ns bitool.schema-infer-test
  (:require [bitool.ingest.schema-infer :as infer]
            [bitool.ops.schema-drift :as schema-drift]
            [clojure.test :refer :all]))

(deftest infer-fields-from-records-builds-descriptors
  (let [records [{:id "t1"
                  :vehicle {:id "v1"}
                  :speed 42
                  :created_at "2026-03-14T10:00:00Z"
                  :tags ["cold" "regional"]}
                 {:id "t2"
                  :vehicle {:id "v2"}
                  :speed 41
                  :created_at "2026-03-14T10:05:00Z"
                  :tags ["regional"]}]
        out (infer/infer-fields-from-records records {:records_path "$.data[]"
                                                      :sample_records 10
                                                      :max_inferred_columns 10
                                                      :type_inference_enabled true})]
    (is (some #(= "$.data[].id" (:path %)) out))
    (is (= "INT" (:type (first (filter #(= "$.data[].speed" (:path %)) out)))))
    (is (= "TIMESTAMP" (:type (first (filter #(= "$.data[].created_at" (:path %)) out)))))
    (is (= "STRING" (:type (first (filter #(= "$.data[].tags[]" (:path %)) out)))))
    (is (= "data_items_vehicle_id" (:column_name (first (filter #(= "$.data[].vehicle.id" (:path %)) out)))))))

(deftest effective-field-descriptors-merges-manual-and-inferred
  (let [endpoint {:schema_mode "hybrid"
                  :selected_nodes ["$.data[].id"]
                  :inferred_fields [{:path "$.data[].vehicle.id"
                                     :column_name "data_items_vehicle_id"
                                     :enabled true
                                     :type "STRING"}]}
        out (infer/effective-field-descriptors endpoint)]
    (is (= #{"$.data[].id" "$.data[].vehicle.id"} (set (map :path out))))))

;; ─── OpenAPI Spec → Field Descriptors ─────────────────────────────────

(def ^:private sample-openapi-schema
  {:type "object"
   :required ["id" "name" "created_at"]
   :properties {:id          {:type "integer" :format "int64" :description "Unique identifier"}
                :name        {:type "string"}
                :email       {:type "string" :nullable true}
                :age         {:type "integer" :format "int32"}
                :score       {:type "number" :format "double"}
                :is_active   {:type "boolean"}
                :created_at  {:type "string" :format "date-time"}
                :birth_date  {:type "string" :format "date"}
                :address     {:type "object"
                              :required ["city"]
                              :properties {:city  {:type "string"}
                                           :state {:type "string" :nullable true}}}
                :tags        {:type "array"
                              :items {:type "string"}}}})

(deftest openapi-schema-converts-basic-types
  (let [fields (infer/openapi-schema->field-descriptors sample-openapi-schema "" nil)
        by-col (into {} (map (juxt :column_name identity)) fields)]
    ;; integer int64 → BIGINT
    (is (= "BIGINT" (:type (get by-col "id"))))
    ;; integer int32 → INT
    (is (= "INT" (:type (get by-col "age"))))
    ;; number double → DOUBLE
    (is (= "DOUBLE" (:type (get by-col "score"))))
    ;; boolean → BOOLEAN
    (is (= "BOOLEAN" (:type (get by-col "is_active"))))
    ;; string → STRING
    (is (= "STRING" (:type (get by-col "name"))))
    ;; string date-time → TIMESTAMP
    (is (= "TIMESTAMP" (:type (get by-col "created_at"))))
    ;; string date → DATE
    (is (= "DATE" (:type (get by-col "birth_date"))))))

(deftest openapi-schema-sets-nullability-from-required
  (let [fields (infer/openapi-schema->field-descriptors sample-openapi-schema "" nil)
        by-col (into {} (map (juxt :column_name identity)) fields)]
    ;; id is required → not nullable, is_required = true
    (is (false? (:nullable (get by-col "id"))))
    (is (true? (:is_required (get by-col "id"))))
    ;; name is required
    (is (true? (:is_required (get by-col "name"))))
    ;; email is not required and explicitly nullable
    (is (true? (:nullable (get by-col "email"))))
    (is (false? (:is_required (get by-col "email"))))
    ;; age is not required
    (is (false? (:is_required (get by-col "age"))))))

(deftest openapi-schema-flattens-nested-objects
  (let [fields (infer/openapi-schema->field-descriptors sample-openapi-schema "" nil)
        by-col (into {} (map (juxt :column_name identity)) fields)]
    ;; Nested object fields should appear with flattened path
    (is (some? (get by-col "address_city")))
    (is (= "STRING" (:type (get by-col "address_city"))))
    (is (some? (get by-col "address_state")))))

(deftest openapi-schema-source-kind-is-spec
  (let [fields (infer/openapi-schema->field-descriptors sample-openapi-schema "" nil)]
    (is (every? #(= "spec" (:source_kind %)) fields))
    (is (every? #(= 1.0 (:confidence %)) fields))))

(deftest openapi-schema-prepends-records-path
  (let [fields (infer/openapi-schema->field-descriptors
                 {:type "object"
                  :properties {:name {:type "string"}}}
                 "$.data[]"
                 nil)
        f (first fields)]
    (is (= "$.data[].name" (:path f)))
    (is (= "data_items_name" (:column_name f)))))

(deftest openapi-schema-handles-array-response-with-bracket-paths
  (let [fields (infer/openapi-schema->field-descriptors
                 {:type "array"
                  :items {:type "object"
                          :required ["id"]
                          :properties {:id {:type "integer"}
                                       :value {:type "string"}}}}
                 ""
                 nil)
        by-path (into {} (map (juxt :path identity)) fields)]
    (is (= 2 (count fields)))
    ;; Root array must produce $[].field paths to match runtime inferrer
    (is (some? (get by-path "$[].id")))
    (is (some? (get by-path "$[].value")))
    ;; column_name uses path->name which turns $[] into _items_
    (is (= "items_id" (:column_name (get by-path "$[].id"))))
    (is (= "items_value" (:column_name (get by-path "$[].value"))))))

(deftest effective-field-descriptors-spec-mode
  (let [spec-fields [{:path "$.id" :column_name "id" :source_kind "spec"
                       :enabled true :type "INT" :is_required true :nullable false}]
        endpoint {:schema_mode "spec" :spec_fields spec-fields}
        out (infer/effective-field-descriptors endpoint)]
    (is (= 1 (count out)))
    (is (= "spec" (:source_kind (first out))))))

(deftest effective-field-descriptors-hybrid-spec-takes-precedence
  (let [spec-fields [{:path "$.id" :column_name "id" :source_kind "spec"
                       :enabled true :type "BIGINT" :is_required true}]
        inferred    [{:path "$.id" :column_name "id" :source_kind "inferred"
                       :enabled true :type "INT"}
                     {:path "$.extra" :column_name "extra" :source_kind "inferred"
                       :enabled true :type "STRING"}]
        endpoint {:schema_mode "hybrid" :spec_fields spec-fields :inferred_fields inferred}
        out (infer/effective-field-descriptors endpoint)
        by-col (into {} (map (juxt :column_name identity)) out)]
    ;; spec field should win for $.id
    (is (= "spec" (:source_kind (get by-col "id"))))
    (is (= "BIGINT" (:type (get by-col "id"))))
    ;; inferred extra should still appear
    (is (some? (get by-col "extra")))))

;; ─── Drift severity with spec nullability ──────────────────────────

(deftest nested-requiredness-propagates-through-optional-parent
  (let [schema {:type "object"
                :required ["id"]
                :properties {:id      {:type "integer"}
                             :address {:type "object"
                                       :required ["city"]
                                       :properties {:city  {:type "string"}
                                                    :state {:type "string"}}}}}
        fields (infer/openapi-schema->field-descriptors schema "" nil)
        by-col (into {} (map (juxt :column_name identity)) fields)]
    ;; id is required at root → truly required
    (is (true? (:is_required (get by-col "id"))))
    ;; address is NOT in root required, so address.city is NOT truly required
    ;; even though city is required within address
    (is (false? (:is_required (get by-col "address_city"))))
    (is (true? (:nullable (get by-col "address_city"))))))

(deftest nested-requiredness-propagates-through-required-parent
  (let [schema {:type "object"
                :required ["id" "address"]
                :properties {:id      {:type "integer"}
                             :address {:type "object"
                                       :required ["city"]
                                       :properties {:city  {:type "string"}
                                                    :state {:type "string"}}}}}
        fields (infer/openapi-schema->field-descriptors schema "" nil)
        by-col (into {} (map (juxt :column_name identity)) fields)]
    ;; address IS required at root, and city IS required within address
    ;; → city is truly required
    (is (true? (:is_required (get by-col "address_city"))))
    (is (false? (:nullable (get by-col "address_city"))))
    ;; state is NOT required within address → not truly required
    (is (false? (:is_required (get by-col "address_state"))))))

(deftest drift-severity-breaking-when-required-field-missing
  (is (= "breaking"
         (schema-drift/classify-drift-severity
          {:new_fields []
           :missing_fields [{:path "$.id" :type "INT" :is_required true}]
           :type_changes []}))))

(deftest drift-severity-warning-when-optional-field-missing
  (is (= "warning"
         (schema-drift/classify-drift-severity
          {:new_fields []
           :missing_fields [{:path "$.email" :type "STRING" :is_required false}]
           :type_changes []}))))
