(ns bitool.grain-planner-test
  (:require [bitool.ingest.grain-planner :as grain-planner]
            [clojure.test :refer :all]))

(deftest recommend-endpoint-config-picks-parent-grain-and-child-entity
  (let [endpoint {:endpoint_name "ap/invoices"
                  :endpoint_url "ap/invoices"}
        inferred-fields [{:path "$.data[].INVOICE_ID"
                          :column_name "data_items_INVOICE_ID"
                          :type "STRING"
                          :nullable false
                          :sample_coverage 1.0}
                         {:path "$.data[].INVOICE_NUM"
                          :column_name "data_items_INVOICE_NUM"
                          :type "STRING"
                          :nullable false
                          :sample_coverage 1.0}
                         {:path "$.data[].LAST_UPDATE_DATE"
                          :column_name "data_items_LAST_UPDATE_DATE"
                          :type "TIMESTAMP"
                          :nullable false
                          :sample_coverage 1.0}
                         {:path "$.data[].LINES[].LINE_NUMBER"
                          :column_name "data_items_LINES_items_LINE_NUMBER"
                          :type "INT"
                          :nullable false
                          :sample_coverage 1.0}
                         {:path "$.data[].LINES[].LINE_AMOUNT"
                          :column_name "data_items_LINES_items_LINE_AMOUNT"
                          :type "DOUBLE"
                          :nullable true
                          :sample_coverage 1.0}]
        recommendation (grain-planner/recommend-endpoint-config endpoint
                                                                inferred-fields
                                                                {:detected-records-path "data"})]
    (is (= "data" (get-in recommendation [:grain :path])))
    (is (= ["INVOICE_ID"] (:primary_key_fields recommendation)))
    (is (= "data_items_LAST_UPDATE_DATE" (:watermark_column recommendation)))
    (is (= "data.LINES" (get-in recommendation [:children 0 :path])))
    (is (= ["INVOICE_ID"] (get-in recommendation [:children 0 :parentKeys])))
    (is (= ["LINE_NUMBER"] (get-in recommendation [:children 0 :idCandidates])))))

(deftest recommend-endpoint-config-prefers-configured-records-path
  (let [endpoint {:endpoint_name "tickets"
                  :endpoint_url "/tickets"}
        inferred-fields [{:path "$.data[].ticket_id"
                          :column_name "data_items_ticket_id"
                          :type "STRING"
                          :nullable false
                          :sample_coverage 1.0}
                         {:path "$.data[].updated_at"
                          :column_name "data_items_updated_at"
                          :type "TIMESTAMP"
                          :nullable false
                          :sample_coverage 1.0}
                         {:path "$.data[].comments[].comment_id"
                          :column_name "data_items_comments_items_comment_id"
                          :type "STRING"
                          :nullable false
                          :sample_coverage 1.0}]
        recommendation (grain-planner/recommend-endpoint-config endpoint
                                                                inferred-fields
                                                                {:configured-records-path "$.data[]"})]
    (is (= "data" (get-in recommendation [:grain :path])))
    (is (= ["ticket_id"] (:primary_key_fields recommendation)))
    (is (= "data_items_updated_at" (:watermark_column recommendation)))))

;; ---------------------------------------------------------------------------
;; Pipeline planner inference tests
;; ---------------------------------------------------------------------------

(require '[bitool.pipeline.planner :as planner])

(deftest plan-pipeline-infers-silver-from-bronze-when-not-specified
  (let [intent {:source {:system "samsara"}
                :target {:platform "databricks" :catalog "main"}
                :bronze [{:object "fleet/vehicles"}
                         {:object "fleet/safety-events"}]}
        spec   (planner/plan-pipeline intent)]
    (is (seq (:silver-proposals spec))
        "Silver should be inferred from Bronze endpoints")
    (is (seq (:gold-models spec))
        "Gold should be inferred when Silver exists")
    (is (some #(= "dimension" (:entity-kind %)) (:silver-proposals spec))
        "fleet/vehicles should become a dimension")
    (is (some #(= "fact" (:entity-kind %)) (:silver-proposals spec))
        "fleet/safety-events should become a fact (contains 'event')")
    (is (every? #(string? (:source-endpoint %)) (:silver-proposals spec))
        "Every Silver entity should have a non-nil source-endpoint")))

(deftest plan-pipeline-preserves-explicit-gold-grain
  (let [intent {:source {:system "samsara"}
                :target {:platform "databricks" :catalog "main"}
                :bronze [{:object "fleet/vehicles"}]
                :gold   [{:model "fleet_utilization" :grain "week"}]}
        spec   (planner/plan-pipeline intent)]
    (is (= "week" (:grain (first (:gold-models spec))))
        "Explicit Gold grain should be preserved")))

(deftest plan-pipeline-handles-bronze-only-intent-without-npe
  (let [intent {:source {:system "samsara"}
                :target {:platform "databricks"}
                :bronze [{:object "fleet/drivers"}]}
        spec   (planner/plan-pipeline intent)]
    (is (= 2 (count (:bronze-nodes spec))) "api node + target node")
    (is (seq (:silver-proposals spec)))
    (is (seq (:gold-models spec)))))
