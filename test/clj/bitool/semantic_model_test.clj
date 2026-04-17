(ns bitool.semantic-model-test
  (:require [clojure.test :refer :all]
            [bitool.semantic.model :as sem]
            [bitool.modeling.automation :as modeling]
            [bitool.db :as db]
            [cheshire.core :as json]
            [next.jdbc :as jdbc]))

;; ---------------------------------------------------------------------------
;; Assembly unit tests (no DB needed — pure functions via #')
;; ---------------------------------------------------------------------------

(deftest build-entity-from-silver-extracts-columns-and-kind
  (let [proposal {:target_model "silver_trips"
                  :proposal_id 10
                  :source_graph_id 1
                  :source_node_id 2
                  :confidence_score 0.92
                  :proposal_json (json/generate-string
                                  {:entity_kind "fact"
                                   :target_table "silver.fact_trips"
                                   :grain "day"
                                   :columns [{:target_column "trip_id"
                                              :type "STRING"
                                              :role "business_key"
                                              :nullable false}
                                             {:target_column "distance"
                                              :type "DOUBLE"
                                              :role "measure_candidate"
                                              :nullable true}
                                             {:target_column "updated_at"
                                              :type "TIMESTAMP"
                                              :role "timestamp"
                                              :nullable true}]})}
        entity (#'sem/build-entity-from-silver proposal)]
    (is (= "fact" (:kind entity)))
    (is (= "silver.fact_trips" (:table entity)))
    (is (= "day" (:grain entity)))
    (is (= 3 (count (:columns entity))))
    (is (= 0.92 (:confidence entity)))
    (is (= "business_key" (-> entity :columns first :role)))
    (is (= 10 (get-in entity [:source :proposal_id])))))

(deftest merge-gold-into-entity-adds-measures
  (let [base-entity {:kind "fact"
                     :table "silver.fact_trips"
                     :grain "day"
                     :columns [{:name "trip_id" :type "STRING" :role "business_key" :nullable false}]
                     :confidence 0.9
                     :source {:layer "silver" :proposal_id 10}}
        gold-proposal {:proposal_id 20
                       :target_model "gold_trips"
                       :proposal_json (json/generate-string
                                       {:target_table "gold.agg_trips_daily"
                                        :columns [{:target_column "total_distance"
                                                   :type "DOUBLE"
                                                   :role "measure"
                                                   :nullable true}
                                                  {:target_column "trip_date"
                                                   :type "DATE"
                                                   :role "time_dimension"
                                                   :nullable false}]
                                        :group_by ["vehicle_id" "trip_date"]})}
        merged (#'sem/merge-gold-into-entity base-entity gold-proposal)]
    (is (= 3 (count (:columns merged))))
    (is (= "measure" (-> merged :columns second :role)))
    (is (= "time_dimension" (-> merged :columns (nth 2) :role)))
    (is (= {:value "day" :group_by ["vehicle_id" "trip_date"]} (:grain merged)))
    (is (= 20 (get-in merged [:gold_source :proposal_id])))))

(deftest enrich-entity-with-context-adds-descriptions
  (let [entity {:kind "fact"
                :table "silver.fact_trips"
                :columns [{:name "trip_id" :type "STRING" :role "business_key" :nullable false}
                          {:name "distance" :type "DOUBLE" :role "measure_candidate" :nullable true}]}
        context [{:table_name "fact_trips" :column_name nil
                  :description "Trip records from telematics"}
                 {:table_name "fact_trips" :column_name "trip_id"
                  :description "Unique trip identifier"
                  :sample_values_json "[\"t-001\",\"t-002\"]"}
                 {:table_name "fact_trips" :column_name "distance"
                  :description "Distance traveled in miles"
                  :sample_values_json nil}]
        enriched (#'sem/enrich-entity-with-context entity context)]
    (is (= "Trip records from telematics" (:description enriched)))
    (is (= "Unique trip identifier" (-> enriched :columns first :description)))
    (is (= "Distance traveled in miles" (-> enriched :columns second :description)))
    (is (= ["t-001" "t-002"] (-> enriched :columns first :sample_values)))))

(deftest build-relationships-from-joins-handles-empty-gracefully
  (let [entities {"trips" {:kind "fact" :table "silver.fact_trips"}
                  "drivers" {:kind "dimension" :table "silver.dim_drivers"}}]
    ;; Snowflake/Databricks/BigQuery case: empty joins
    (is (= [] (#'sem/build-relationships-from-joins [] entities)))
    (is (= [] (#'sem/build-relationships-from-joins nil entities)))
    ;; PostgreSQL/MySQL case: FK metadata available
    (let [joins [{:from_table "fact_trips" :from_column "driver_id"
                  :to_table "dim_drivers" :to_column "id"}]
          rels (#'sem/build-relationships-from-joins joins entities)]
      (is (= 1 (count rels)))
      (is (= "trips" (:from (first rels))))
      (is (= "drivers" (:to (first rels))))
      (is (= "many_to_one" (:type (first rels)))))))

(deftest build-relationships-skips-unknown-tables
  (let [entities {"trips" {:kind "fact" :table "silver.fact_trips"}}
        joins [{:from_table "fact_trips" :from_column "unknown_id"
                :to_table "unknown_table" :to_column "id"}]
        rels (#'sem/build-relationships-from-joins joins entities)]
    (is (= [] rels))))

(deftest avg-confidence-handles-edge-cases
  (is (= 0.8 (#'sem/avg-confidence [])))
  (is (= 0.8 (#'sem/avg-confidence [{:other "field"}])))
  (is (< (abs (- 0.9 (#'sem/avg-confidence [{:confidence_score 0.85}
                                              {:confidence_score 0.95}])))
          0.001)))

(deftest build-lineage-summarizes-tables
  (let [silver [{:target_model "trips"
                 :proposal_json (json/generate-string {:target_table "silver.fact_trips"})}]
        gold [{:target_model "agg_trips"
               :proposal_json (json/generate-string {:target_table "gold.agg_trips_daily"})}]
        lineage (#'sem/build-lineage silver gold)]
    (is (= ["silver.fact_trips"] (:silver lineage)))
    (is (= ["gold.agg_trips_daily"] (:gold lineage)))))

;; ---------------------------------------------------------------------------
;; propose-semantic-model! integration test (mocked DB)
;; ---------------------------------------------------------------------------

(deftest propose-semantic-model-assembles-from-proposals
  (let [silver-proposals [{:proposal_id 10
                           :target_model "silver_trips"
                           :source_graph_id 1
                           :source_node_id 2
                           :confidence_score 0.90
                           :proposal_json (json/generate-string
                                           {:entity_kind "fact"
                                            :target_table "silver.fact_trips"
                                            :grain "day"
                                            :columns [{:target_column "trip_id"
                                                       :type "STRING"
                                                       :role "business_key"
                                                       :nullable false}
                                                      {:target_column "distance"
                                                       :type "DOUBLE"
                                                       :role "measure_candidate"
                                                       :nullable true}]})}]
        gold-proposals [{:proposal_id 20
                         :target_model "gold_trips_daily"
                         :confidence_score 0.85
                         :proposal_json (json/generate-string
                                         {:target_table "gold.agg_trips_daily"
                                          :columns [{:target_column "total_distance"
                                                     :type "DOUBLE"
                                                     :role "measure"}]})}]
        persisted-row (atom nil)]
    (with-redefs [modeling/list-silver-proposals (fn [{:keys [graph-id]}]
                                                  (when (= 1 graph-id) silver-proposals))
                  modeling/list-gold-proposals (fn [{:keys [graph-id]}]
                                                (when (= 1 graph-id) gold-proposals))
                  db/get-schema-context (fn [_ & _] [])
                  db/discover-joins (fn [_ & _] [])
                  sem/ensure-semantic-tables! (fn [])
                  jdbc/execute-one! (fn [_ sqlvec]
                                     (let [sql (first sqlvec)]
                                       (if (.contains sql "INSERT INTO semantic_model ")
                                         ;; Model INSERT
                                         (let [model-json (nth sqlvec 4)]
                                           (reset! persisted-row
                                                   {:model_id 1
                                                    :conn_id 5
                                                    :schema_name "public"
                                                    :name "public_model"
                                                    :version 1
                                                    :status "draft"
                                                    :model_json model-json})
                                           @persisted-row)
                                         ;; Version INSERT — just return nil
                                         nil)))]
      (let [result (sem/propose-semantic-model!
                    {:conn-id 5
                     :schema "public"
                     :graph-ids [1]
                     :created-by "test-user"})
            model (:model result)]
        (is (= "draft" (:status result)))
        (is (= 1 (:model_id result)))
        ;; Model should have entities
        (is (contains? (:entities model) :silver_trips))
        ;; Relationships empty (discover-joins returned [])
        (is (= [] (:relationships model)))
        ;; Confidence averaged
        (is (number? (:confidence model)))
        ;; Lineage populated
        (is (seq (:lineage model)))))))

(deftest propose-semantic-model-throws-when-no-proposals
  (with-redefs [modeling/list-silver-proposals (fn [_] [])
                modeling/list-gold-proposals (fn [_] [])
                sem/ensure-semantic-tables! (fn [])]
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"No Silver or Gold proposals"
         (sem/propose-semantic-model!
          {:conn-id 1 :schema "public" :graph-ids [99]})))))

(deftest propose-semantic-model-tolerates-discover-joins-failure
  (let [silver-proposals [{:proposal_id 10
                           :target_model "silver_trips"
                           :source_graph_id 1
                           :source_node_id 2
                           :confidence_score 0.9
                           :proposal_json (json/generate-string
                                           {:entity_kind "fact"
                                            :target_table "silver.fact_trips"
                                            :grain "day"
                                            :columns [{:target_column "trip_id"
                                                       :type "STRING"
                                                       :role "business_key"}]})}]]
    (with-redefs [modeling/list-silver-proposals (fn [_] silver-proposals)
                  modeling/list-gold-proposals (fn [_] [])
                  db/get-schema-context (fn [_ & _] [])
                  db/discover-joins (fn [_ & _] (throw (Exception. "FK introspection not supported")))
                  sem/ensure-semantic-tables! (fn [])
                  jdbc/execute-one! (fn [_ sqlvec]
                                     (let [sql (first sqlvec)]
                                       (if (.contains sql "INSERT INTO semantic_model ")
                                         {:model_id 2
                                          :conn_id 1
                                          :name "public_model"
                                          :version 1
                                          :status "draft"
                                          :model_json (nth sqlvec 4)}
                                         nil)))]
      (let [result (sem/propose-semantic-model!
                    {:conn-id 1 :schema "public" :graph-ids [1]})]
        ;; Should succeed despite discover-joins throwing
        (is (= 2 (:model_id result)))
        (is (= [] (get-in result [:model :relationships])))))))

(deftest propose-semantic-model-rejects-three-part-schema-mismatch
  (let [;; Proposal targets db.analytics.fact_trips — schema is "analytics"
        silver-proposals [{:proposal_id 10
                           :target_model "silver_trips"
                           :source_graph_id 1
                           :source_node_id 2
                           :confidence_score 0.9
                           :proposal_json (json/generate-string
                                           {:entity_kind "fact"
                                            :target_table "mydb.analytics.fact_trips"
                                            :grain "day"
                                            :columns [{:target_column "trip_id"
                                                       :type "STRING"
                                                       :role "business_key"}]})}]]
    (with-redefs [modeling/list-silver-proposals (fn [_] silver-proposals)
                  modeling/list-gold-proposals (fn [_] [])
                  sem/ensure-semantic-tables! (fn [])]
      ;; Requesting schema "public" but proposals are in "analytics"
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"No proposals match the requested schema"
           (sem/propose-semantic-model!
            {:conn-id 1 :schema "public" :graph-ids [1]}))))))
