(ns bitool.pipeline-compiler-test
  (:require [bitool.pipeline.compiler :as compiler]
            [clojure.test :refer :all]))

(deftest apply-silver-proposals-creates-real-proposals
  (let [propose-calls (atom [])
        update-calls  (atom [])]
    (with-redefs [bitool.modeling.automation/propose-silver-schema!
                  (fn [params]
                    (swap! propose-calls conj params)
                    {:proposal_id 701
                     :target_model "silver.dim_vehicle"
                     :status "draft"
                     :layer "silver"})
                  bitool.modeling.automation/update-silver-proposal!
                  (fn [proposal-id params]
                    (swap! update-calls conj [proposal-id params])
                    {:proposal_id proposal-id
                     :target_model (get-in params [:proposal :target_model])
                     :status "draft"
                     :layer "silver"})]
      (is (= [{:proposal_id 701
               :target_model "silver.dim_vehicle"
               :status "draft"
               :layer "silver"
               :source_endpoint "fleet/vehicles"}]
             (compiler/apply-silver-proposals!
              [{:graph-id 91
                :api-node-id 12
                :endpoint-name "fleet/vehicles"
                :target-model "silver.dim_vehicle"
                :entity-kind "dimension"
                :business-keys ["id"]
                :columns [{:name "vehicle_id" :source_path "$.id"}]
                :processing-policy {:dedupe "latest"}}]
              {:created-by "demo-user"})))
      (is (= [{:graph-id 91
               :api-node-id 12
               :endpoint-name "fleet/vehicles"
               :created-by "demo-user"}]
             @propose-calls))
      (is (= [[701
               {:proposal {:target_model "silver.dim_vehicle"
                           :entity_kind "dimension"
                           :materialization {:mode "merge" :keys ["id"]}
                           :processing_policy {:dedupe "latest"}
                           :columns [{:name "vehicle_id" :source_path "$.id"}]}
                :created_by "demo-user"}]]
             @update-calls)))))

(deftest apply-gold-proposals-creates-real-proposals
  (let [propose-calls (atom [])
        update-calls  (atom [])]
    (with-redefs [bitool.modeling.automation/propose-gold-schema!
                  (fn [params]
                    (swap! propose-calls conj params)
                    {:proposal_id 801
                     :target_model "gold.seed"
                     :status "draft"
                     :layer "gold"})
                  bitool.modeling.automation/update-gold-proposal!
                  (fn [proposal-id params]
                    (swap! update-calls conj [proposal-id params])
                    {:proposal_id proposal-id
                     :target_model (get-in params [:proposal :target_model])
                     :status "draft"
                     :layer "gold"})]
      (is (= [{:proposal_id 801
               :target_model "gold.fleet_utilization_daily"
               :status "draft"
               :layer "gold"
               :depends_on ["silver.fct_trip"]
               :source_proposal_id 701}]
             (compiler/apply-gold-proposals!
              [{:target-model "gold.fleet_utilization_daily"
                :grain "day"
                :depends-on ["silver.fct_trip"]
                :measures [{:name "trip_count"}]
                :dimensions [{:name "trip_date"}]
                :sql-template "select * from silver.fct_trip"}]
              {:created-by "demo-user"
               :silver-proposal-ids [{:proposal_id 701
                                      :target_model "silver.fct_trip"}]})))
      (is (= [{:silver_proposal_id 701
               :created_by "demo-user"}]
             @propose-calls))
      (is (= [[801
               {:proposal {:target_model "gold.fleet_utilization_daily"
                           :semantic_grain "day"
                           :depends_on ["silver.fct_trip"]
                           :measures [{:name "trip_count"}]
                           :dimensions [{:name "trip_date"}]
                           :sql_template "select * from silver.fct_trip"}
                :created_by "demo-user"}]]
             @update-calls)))))

(deftest apply-pipeline-creates-bronze-silver-and-gold-results
  (let [spec {:pipeline-id "sheetz-samsara"
              :pipeline-name "Sheetz Samsara"
              :silver-proposals [{:source-endpoint "fleet/vehicles"
                                  :target-model "silver.dim_vehicle"
                                  :entity-kind "dimension"
                                  :business-keys ["id"]
                                  :columns [{:name "vehicle_id" :source_path "$.id"}]}]
              :gold-models [{:target-model "gold.fleet_utilization_daily"
                             :grain "day"
                             :depends-on ["silver.dim_vehicle"]
                             :measures [{:name "active_vehicle_count"}]
                             :dimensions [{:name "activity_date"}]}]}
        silver-calls (atom [])
        gold-calls   (atom [])]
    (with-redefs [bitool.pipeline.compiler/apply-bronze!
                  (fn [_ _]
                    {:graph_id 44
                     :graph_version 3
                     :api_node_id 12
                     :target_node_id 15})
                  bitool.pipeline.compiler/apply-silver-proposals!
                  (fn [plans opts]
                    (swap! silver-calls conj [plans opts])
                    [{:proposal_id 701
                      :target_model "silver.dim_vehicle"
                      :status "draft"
                      :layer "silver"
                      :source_endpoint "fleet/vehicles"}])
                  bitool.pipeline.compiler/apply-gold-proposals!
                  (fn [plans opts]
                    (swap! gold-calls conj [plans opts])
                    [{:proposal_id 801
                      :target_model "gold.fleet_utilization_daily"
                      :status "draft"
                      :layer "gold"
                      :depends_on ["silver.dim_vehicle"]
                      :source_proposal_id 701}])]
      (let [result (compiler/apply-pipeline! spec {:created-by "demo-user"
                                                   :connection-id 9})]
        (is (= {:graph_id 44
                :graph_version 3
                :api_node_id 12
                :target_node_id 15}
               (:bronze result)))
        (is (= 701 (get-in result [:silver 0 :proposal_id])))
        (is (= 801 (get-in result [:gold 0 :proposal_id])))
        (is (= "draft" (get-in result [:silver 0 :status])))
        (is (= "draft" (get-in result [:gold 0 :status])))
        (is (= [[[{:graph-id 44
                   :api-node-id 12
                   :endpoint-name "fleet/vehicles"
                   :target-model "silver.dim_vehicle"
                   :entity-kind "dimension"
                   :business-keys ["id"]
                   :columns [{:name "vehicle_id" :source_path "$.id"}]
                   :processing-policy nil}]
                 {:created-by "demo-user"
                  :connection-id 9}]]
               @silver-calls))
        (is (= [[[{:target-model "gold.fleet_utilization_daily"
                   :grain "day"
                   :depends-on ["silver.dim_vehicle"]
                   :measures [{:name "active_vehicle_count"}]
                   :dimensions [{:name "activity_date"}]}]
                 {:created-by "demo-user"
                  :connection-id 9
                  :silver-proposal-ids [{:proposal_id 701
                                         :target_model "silver.dim_vehicle"
                                         :status "draft"
                                         :layer "silver"
                                         :source_endpoint "fleet/vehicles"}]}]]
               @gold-calls))))))
