(ns bitool.schema-infer-test
  (:require [bitool.ingest.schema-infer :as infer]
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
