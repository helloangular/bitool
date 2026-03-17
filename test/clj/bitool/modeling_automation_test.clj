(ns bitool.modeling-automation-test
  (:require [bitool.control-plane :as control-plane]
            [bitool.compiler.core :as compiler]
            [bitool.db :as db]
            [bitool.graph2 :as g2]
            [bitool.modeling.automation :as modeling]
            [cheshire.core :as json]
            [next.jdbc :as jdbc]
            [clojure.test :refer :all]))

(deftest build-silver-proposal-prefers-merge-when-business-key-exists
  (let [endpoint {:endpoint_name "Trips"
                  :silver_table_name "sheetz_telematics.silver.fact_trip"
                  :primary_key_fields ["id"]
                  :inferred_fields [{:path "$.data[].id"
                                     :column_name "trip_id"
                                     :type "STRING"
                                     :nullable false
                                     :enabled true}
                                    {:path "$.data[].vehicle.id"
                                     :column_name "vehicle_id"
                                     :type "STRING"
                                     :nullable true
                                     :enabled true}
                                    {:path "$.data[].updated_at"
                                     :column_name "updated_at"
                                     :type "TIMESTAMP"
                                     :nullable true
                                     :enabled true}
                                    {:path "$.data[].distance"
                                     :column_name "distance"
                                     :type "DOUBLE"
                                     :nullable true
                                     :enabled true}]}
        profile {:profile_source "endpoint_schema_snapshot"
                 :sample_record_count 25
                 :field_count 4
                 :profile_json {:field_types {"trip_id" {:type "STRING"}
                                              "vehicle_id" {:type "STRING"}
                                              "updated_at" {:type "TIMESTAMP"}
                                              "distance" {:type "DOUBLE"}}}}
        proposal (#'bitool.modeling.automation/build-silver-proposal
                  {:tenant-key "tenant-a"
                   :workspace-key "ops"
                   :graph-id 99
                   :api-node-id 2
                   :source-system "samara"
                   :endpoint endpoint
                   :profile profile
                   :created-by "alice"})]
    (is (= "fact_trip" (:target_model proposal)))
    (is (= "merge" (get-in proposal [:proposal_json :materialization :mode])))
    (is (= ["trip_id"] (get-in proposal [:proposal_json :materialization :keys])))
    (is (= "fact" (get-in proposal [:proposal_json :entity_kind])))
    (is (= "timestamp" (get-in proposal [:proposal_json :columns 2 :role])))))

(deftest build-schema-profile-uses-endpoint-primary-keys
  (let [endpoint {:primary_key_fields ["id"]}
        profile (#'bitool.modeling.automation/build-schema-profile
                 {:graph-id 99
                  :api-node-id 2
                  :source-system "samara"
                  :endpoint-name "trips"
                  :source "endpoint_schema_snapshot"
                  :snapshot-row {:sample_record_count 10}
                  :endpoint endpoint
                  :fields [{:path "$.data[].id"
                            :column_name "trip_id"
                            :type "STRING"
                            :nullable false
                            :enabled true}
                           {:path "$.data[].vehicle.id"
                            :column_name "vehicle_id"
                            :type "STRING"
                            :nullable true
                            :enabled true}]})]
    (is (= ["trip_id"] (get-in profile [:profile_json :key_candidates])))))

(deftest key-columns-prefers-best-single-match-for-generic-id
  (let [endpoint {:primary_key_fields ["id"]}
        key-cols (#'bitool.modeling.automation/key-columns
                  endpoint
                  [{:path "$.data[].trip.id" :column_name "trip_id"}
                   {:path "$.data[].route.id" :column_name "route_id"}
                   {:path "$.data[].vehicle.id" :column_name "vehicle_id"}])]
    (is (= ["trip_id"] key-cols))))

(deftest schema-source-falls-back-to-endpoint-config-when-no-snapshot-exists
  (with-redefs-fn {#'bitool.modeling.automation/latest-endpoint-schema-snapshot (fn [& _] nil)}
    (fn []
      (let [endpoint {:endpoint_name "drivers"
                      :inferred_fields [{:path "$.data[].id"
                                         :column_name "driver_id"
                                         :type "STRING"
                                         :enabled true}]}
            source (#'bitool.modeling.automation/schema-source-for-endpoint
                    99 2 "samara" endpoint {:connection_id 9})]
        (is (= "endpoint_config" (:source source)))
        (is (= "driver_id" (get-in source [:fields 0 :column_name])))))))

(deftest schema-source-falls-back-to-endpoint-config-on-target-connection-failure
  (with-redefs-fn {#'bitool.modeling.automation/latest-endpoint-schema-snapshot (fn [& _]
                                                                                  (throw (ex-info "Could not read schema snapshot from target: boom"
                                                                                                  {:failure_class "target_connection"})))}
    (fn []
      (let [endpoint {:endpoint_name "drivers"
                      :inferred_fields [{:path "$.data[].id"
                                         :column_name "driver_id"
                                         :type "STRING"
                                         :enabled true}]}
            source (#'bitool.modeling.automation/schema-source-for-endpoint
                    99 2 "samara" endpoint {:connection_id 9})]
        (is (= "endpoint_config" (:source source)))
        (is (= "target_connection" (:fallback_reason source)))))))

(deftest propose-silver-schema-persists-profile-and-proposal
  (let [captured-profile (atom nil)
        captured-proposal (atom nil)
        graph {:a {:id 99 :v 7}} 
        api-node {:btype "Ap"
                  :source_system "samara"
                  :endpoint_configs [{:endpoint_name "trips"
                                      :enabled true
                                      :primary_key_fields ["id"]
                                      :silver_table_name "silver_trip"}]}]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'control-plane/ensure-control-plane-tables! (fn [] true)
                     #'db/getGraph (fn [_] graph)
                     #'g2/getData (fn [_ _] api-node)
                     #'control-plane/graph-workspace-context (fn [_]
                                                               {:tenant_key "tenant-a"
                                                                :workspace_key "ops"})
                     #'bitool.modeling.automation/find-downstream-target (fn [_ _] {:connection_id 9})
                     #'bitool.modeling.automation/schema-source-for-endpoint (fn [_ _ _ endpoint _]
                                                                               {:source "endpoint_schema_snapshot"
                                                                                :snapshot-row {:sample_record_count 3
                                                                                               :captured_at_utc "2026-03-14T01:00:00Z"}
                                                                                :fields [{:path "$.data[].id"
                                                                                          :column_name "trip_id"
                                                                                          :type "STRING"
                                                                                          :nullable false
                                                                                          :enabled true}
                                                                                         {:path "$.data[].distance"
                                                                                          :column_name "distance"
                                                                                          :type "DOUBLE"
                                                                                          :nullable true
                                                                                          :enabled true}]
                                                                                :endpoint endpoint})
                     #'bitool.modeling.automation/latest-model-proposal (fn [& _] nil)
                     #'bitool.modeling.automation/persist-schema-profile! (fn [profile created-by tenant-key workspace-key]
                                                                            (reset! captured-profile [profile created-by tenant-key workspace-key])
                                                                            {:profile_id 11})
                     #'bitool.modeling.automation/persist-model-proposal! (fn [proposal profile-id]
                                                                            (reset! captured-proposal [proposal profile-id])
                                                                            {:proposal_id 22
                                                                             :status "draft"
                                                                             :target_model (:target_model proposal)
                                                                             :confidence_score (:confidence_score proposal)})}
      (fn []
        (let [result (modeling/propose-silver-schema! {:graph-id 99
                                                       :api-node-id 2
                                                       :created-by "alice"})]
          (is (= 11 (:profile_id result)))
          (is (= 22 (:proposal_id result)))
          (is (= "tenant-a" (:tenant_key result)))
          (is (= "ops" (:workspace_key result)))
          (is (= "silver_trip" (get-in result [:proposal :target_model])))
          (is (= "merge" (get-in result [:proposal :materialization :mode])))
          (is (= "alice" (second @captured-profile)))
          (is (= "tenant-a" (nth @captured-profile 2)))
          (is (= 11 (second @captured-proposal))))))))

(deftest propose-silver-schema-rejects-non-api-node
  (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                   #'control-plane/ensure-control-plane-tables! (fn [] true)
                   #'db/getGraph (fn [_] {:a {:id 99 :v 1}})
                   #'g2/getData (fn [_ _] {:btype "Fi"})}
    (fn []
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Node is not an API node"
                            (modeling/propose-silver-schema! {:graph-id 99
                                                              :api-node-id 7}))))))

(deftest propose-silver-schema-dedupes-identical-latest-proposal
  (let [graph {:a {:id 99 :v 7}}
        api-node {:btype "Ap"
                  :source_system "samara"
                  :endpoint_configs [{:endpoint_name "trips"
                                      :enabled true
                                      :primary_key_fields ["id"]
                                      :silver_table_name "silver_trip"}]}
        persist-profile-called? (atom false)
        persist-proposal-called? (atom false)
        latest-proposal-row (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'control-plane/ensure-control-plane-tables! (fn [] true)
                     #'db/getGraph (fn [_] graph)
                     #'g2/getData (fn [_ _] api-node)
                     #'control-plane/graph-workspace-context (fn [_]
                                                               {:tenant_key "tenant-a"
                                                                :workspace_key "ops"})
                     #'bitool.modeling.automation/find-downstream-target (fn [_ _] {:connection_id 9})
                     #'bitool.modeling.automation/schema-source-for-endpoint (fn [_ _ _ endpoint _]
                                                                               {:source "endpoint_schema_snapshot"
                                                                                :snapshot-row {:sample_record_count 3
                                                                                               :captured_at_utc "2026-03-14T01:00:00Z"}
                                                                                :fields [{:path "$.data[].id"
                                                                                          :column_name "trip_id"
                                                                                          :type "STRING"
                                                                                          :nullable false
                                                                                          :enabled true}]
                                                                                :endpoint endpoint})
                     #'bitool.modeling.automation/schema-profile-by-id (fn [_]
                                                                         {:profile_id 11
                                                                          :profile_source "endpoint_schema_snapshot"
                                                                          :sample_record_count 3
                                                                          :field_count 1
                                                                          :profile_json "{\"key_candidates\":[\"trip_id\"],\"timestamp_candidates\":[],\"field_types\":{\"trip_id\":{\"type\":\"STRING\",\"nullable\":false}}}"})
                     #'bitool.modeling.automation/persist-schema-profile! (fn [& _]
                                                                            (reset! persist-profile-called? true)
                                                                            {:profile_id 11})
                     #'bitool.modeling.automation/persist-model-proposal! (fn [& _]
                                                                            (reset! persist-proposal-called? true)
                                                                            {:proposal_id 22})}
      (fn []
        (let [proposal-template (#'bitool.modeling.automation/build-silver-proposal
                                 {:tenant-key "tenant-a"
                                  :workspace-key "ops"
                                  :graph-id 99
                                  :api-node-id 2
                                  :source-system "samara"
                                  :endpoint {:endpoint_name "trips"
                                             :silver_table_name "silver_trip"
                                             :primary_key_fields ["id"]
                                             :inferred_fields [{:path "$.data[].id"
                                                                :column_name "trip_id"
                                                                :type "STRING"
                                                                :nullable false
                                                                :enabled true}]}
                                  :profile {:profile_source "endpoint_schema_snapshot"
                                            :sample_record_count 3
                                            :field_count 1
                                            :profile_json {:field_types {"trip_id" {:type "STRING"}}}}
                                  :created-by "alice"})]
          (reset! latest-proposal-row {:proposal_id 22
                                       :profile_id 11
                                       :status "draft"
                                       :target_model (:target_model proposal-template)
                                       :confidence_score (:confidence_score proposal-template)
                                       :proposal_json (json/generate-string (:proposal_json proposal-template))})
          (with-redefs-fn {#'bitool.modeling.automation/latest-model-proposal (fn [& _] @latest-proposal-row)}
            (fn []
              (let [result (modeling/propose-silver-schema! {:graph-id 99
                                                             :api-node-id 2
                                                             :created-by "alice"})]
                (is (:deduped result))
                (is (= 22 (:proposal_id result)))
                (is (false? @persist-profile-called?))
                (is (false? @persist-proposal-called?))))))))))

(deftest compile-silver-proposal-builds-sql-without-persisting-proposal-state
  (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                   #'bitool.modeling.automation/resolve-proposal-context (fn [_]
                                                                          {:proposal-row {:proposal_id 22
                                                                                          :target_model "silver_trip"
                                                                                          :layer "silver"}
                                                                           :proposal-json {:target_table "sheetz_telematics.silver.trip"
                                                                                           :columns [{:target_column "trip_id"
                                                                                                      :expression "bronze.trip_id"
                                                                                                      :type "STRING"
                                                                                                      :source_columns ["trip_id"]}
                                                                                                     {:target_column "distance"
                                                                                                      :expression "bronze.distance"
                                                                                                      :type "DOUBLE"
                                                                                                      :source_columns ["distance"]}]
                                                                                           :mappings [{:target_column "trip_id"
                                                                                                       :expression "bronze.trip_id"
                                                                                                       :source_columns ["trip_id"]}
                                                                                                      {:target_column "distance"
                                                                                                       :expression "bronze.distance"
                                                                                                       :source_columns ["distance"]}]
                                                                                           :materialization {:mode "merge"
                                                                                                             :keys ["trip_id"]}}
                                                                           :profile-json {:field_types {"trip_id" {:type "STRING"}
                                                                                                        "distance" {:type "DOUBLE"}}}
                                                                           :source-table "sheetz_telematics.bronze.trip_raw"})}
    (fn []
      (let [result (modeling/compile-silver-proposal! 22)]
        (is (= 22 (:proposal_id result)))
        (is (re-find #"MERGE INTO sheetz_telematics.silver.trip" (:compiled_sql result)))
        (is (re-find #"FROM sheetz_telematics.bronze.trip_raw bronze" (:select_sql result)))))))

(deftest compile-silver-proposal-rejects-unsafe-expression
  (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                   #'bitool.modeling.automation/resolve-proposal-context (fn [_]
                                                                          {:proposal-row {:proposal_id 22
                                                                                          :target_model "silver_trip"
                                                                                          :layer "silver"}
                                                                           :proposal-json {:target_table "sheetz_telematics.silver.trip"
                                                                                           :columns [{:target_column "trip_id"
                                                                                                      :expression "bronze.trip_id; DROP TABLE silver.trip"
                                                                                                      :type "STRING"
                                                                                                      :source_columns ["trip_id"]}]
                                                                                           :mappings [{:target_column "trip_id"
                                                                                                       :expression "bronze.trip_id; DROP TABLE silver.trip"
                                                                                                       :source_columns ["trip_id"]}]
                                                                                           :materialization {:mode "append"
                                                                                                             :keys []}}
                                                                           :profile-json {:field_types {"trip_id" {:type "STRING"}}}
                                                                           :source-table "sheetz_telematics.bronze.trip_raw"})}
    (fn []
      (try
        (modeling/compile-silver-proposal! 22)
        (is false "Expected compile to reject unsafe expression")
        (catch clojure.lang.ExceptionInfo e
          (is (= 400 (:status (ex-data e))))
          (is (= "Silver proposal failed static validation" (ex-message e))))))))

(deftest compiler-emits-snowflake-sql-for-snowflake-target
  (let [result (compiler/compile-model {:target-warehouse :snowflake
                                        :proposal-json {:target_table "sheetz_telematics.silver.trip"
                                                        :columns [{:target_column "trip_id"
                                                                   :expression "bronze.trip_id"
                                                                   :type "STRING"
                                                                   :source_columns ["trip_id"]}
                                                                  {:target_column "distance"
                                                                   :expression "bronze.distance"
                                                                   :type "DOUBLE"
                                                                   :source_columns ["distance"]}]
                                                        :materialization {:mode "merge"
                                                                          :keys ["trip_id"]}}
                                        :source-table "sheetz_telematics.bronze.trip_raw"})]
    (is (= "snowflake" (:target_warehouse result)))
    (is (re-find #"MERGE INTO \"sheetz_telematics\"\.\"silver\"\.\"trip\"" (:compiled_sql result)))
    (is (re-find #"FROM \"sheetz_telematics\"\.\"bronze\"\.\"trip_raw\" bronze" (:select_sql result)))))

(deftest validate-silver-proposal-persists-valid-validation
  (let [captured-update (atom nil)
        captured-validation (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/resolve-proposal-context (fn [_]
                                                                            {:proposal-row {:proposal_id 22
                                                                                            :target_model "silver_trip"}
                                                                             :proposal-json {:target_table "sheetz_telematics.silver.trip"
                                                                                             :columns [{:target_column "trip_id"
                                                                                                        :expression "bronze.trip_id"
                                                                                                        :type "STRING"
                                                                                                        :source_columns ["trip_id"]}
                                                                                                       {:target_column "distance"
                                                                                                        :expression "bronze.distance"
                                                                                                        :type "DOUBLE"
                                                                                                        :source_columns ["distance"]}]
                                                                                             :mappings [{:target_column "trip_id"
                                                                                                         :expression "bronze.trip_id"
                                                                                                         :source_columns ["trip_id"]}
                                                                                                        {:target_column "distance"
                                                                                                         :expression "bronze.distance"
                                                                                                         :source_columns ["distance"]}]
                                                                                             :materialization {:mode "merge"
                                                                                                               :keys ["trip_id"]}}
                                                                             :profile-json {:field_types {"trip_id" {:type "STRING"}
                                                                                                          "distance" {:type "DOUBLE"}}}
                                                                             :source-table "sheetz_telematics.bronze.trip_raw"
                                                                             :target-connection-id 9})
                     #'bitool.modeling.automation/run-sample-execution! (fn [_ _ _]
                                                                          {:sample_sql "SELECT ... LIMIT 10"
                                                                           :sample_limit 10
                                                                           :summary {:row_count 5
                                                                                     :duplicate_key_count 0
                                                                                     :null_counts {"trip_id" 0 "distance" 1}
                                                                                     :row_count_plausible true
                                                                                     :casts_succeeded true}})
                     #'bitool.modeling.automation/persist-model-validation! (fn [payload]
                                                                              (reset! captured-validation payload)
                                                                              {:validation_id 101})
                     #'bitool.modeling.automation/update-model-proposal! (fn [proposal-id attrs]
                                                                           (reset! captured-update [proposal-id attrs])
                                                                           nil)}
      (fn []
        (let [result (modeling/validate-silver-proposal! 22 {:sample_limit 10 :created_by "alice"})]
          (is (= "valid" (:status result)))
          (is (= 101 (:validation_id result)))
          (is (= "valid" (:status @captured-validation)))
          (is (= [22 {:compiled_sql (:compiled_sql result) :status "validated"}] @captured-update))
          (is (re-find #"MERGE INTO sheetz_telematics.silver.trip" (:compiled_sql result)))
          (is (= 5 (get-in result [:validation :sample_execution :summary :row_count]))))))))

(deftest validate-silver-proposal-clears-compiled-sql-when-static-validation-fails
  (let [captured-update (atom nil)
        captured-validation (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/resolve-proposal-context (fn [_]
                                                                            {:proposal-row {:proposal_id 22
                                                                                            :target_model "silver_trip"}
                                                                             :proposal-json {:target_table "sheetz_telematics.silver.trip"
                                                                                             :columns [{:target_column "trip_id"
                                                                                                        :expression "bronze.trip_id; DROP TABLE x"
                                                                                                        :type "STRING"
                                                                                                        :source_columns ["trip_id"]}]
                                                                                             :mappings [{:target_column "trip_id"
                                                                                                         :expression "bronze.trip_id; DROP TABLE x"
                                                                                                         :source_columns ["trip_id"]}]
                                                                                             :materialization {:mode "append"
                                                                                                               :keys []}}
                                                                             :profile-json {:field_types {"trip_id" {:type "STRING"}}}
                                                                             :source-table "sheetz_telematics.bronze.trip_raw"
                                                                             :target-connection-id 9})
                     #'bitool.modeling.automation/persist-model-validation! (fn [payload]
                                                                              (reset! captured-validation payload)
                                                                              {:validation_id 102})
                     #'bitool.modeling.automation/update-model-proposal! (fn [proposal-id attrs]
                                                                           (reset! captured-update [proposal-id attrs])
                                                                           nil)}
      (fn []
        (let [result (modeling/validate-silver-proposal! 22 {:created_by "alice"})]
          (is (= "invalid" (:status result)))
          (is (nil? (:compiled_sql result)))
          (is (nil? (:compiled-sql @captured-validation)))
          (is (= [22 {:compiled_sql nil :status "draft"}] @captured-update)))))))

(deftest publish-silver-proposal-tx-creates-release-and-artifact
  (let [deactivated (atom nil)
        updated (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/lock-release-stream! (fn [_ layer target-model]
                                                                         (reset! deactivated [:locked layer target-model])
                                                                         nil)
                     #'bitool.modeling.automation/next-release-version (fn [_ _ _] 3)
                     #'bitool.modeling.automation/deactivate-active-releases! (fn [_ layer target-model]
                                                                                (reset! deactivated [layer target-model])
                                                                                nil)
                     #'bitool.modeling.automation/persist-model-release! (fn [_ payload]
                                                                           (is (= 3 (:version payload)))
                                                                           {:release_id 700
                                                                            :version 3})
                     #'bitool.modeling.automation/persist-compiled-model-artifact! (fn [_ payload]
                                                                                     (is (= 700 (:release-id payload)))
                                                                                     {:artifact_id 800})
                     #'bitool.modeling.automation/update-model-proposal! (fn [_ proposal-id attrs]
                                                                           (reset! updated [proposal-id attrs])
                                                                           nil)}
      (fn []
        (let [result (#'bitool.modeling.automation/publish-silver-proposal-tx!
                      {}
                      22
                      {:proposal_id 22
                       :layer "silver"
                       :target_model "silver_trip"}
                      {:validation_id 101
                       :compiled_sql "MERGE INTO ..."
                       :validation {:sql_ir {:materialization {:mode "merge"}}}}
                      901
                      "alice"
                      {:target_model "silver_trip"
                       :target_table "sheetz_telematics.silver.trip"}
                      {:materialization {:mode "merge"}})]
          (is (= "published" (:status result)))
          (is (= 700 (:release_id result)))
          (is (= 800 (:artifact_id result)))
          (is (= ["silver" "silver_trip"] @deactivated))
          (is (= [22 {:status "published" :compiled_sql "MERGE INTO ..."}] @updated)))))))

(deftest validate-silver-proposal-warehouse-uses-snowflake-sql-path
  (let [captured-validation (atom nil)
        captured-sql (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/proposal-by-id (fn [_] {:proposal_id 22
                                                                          :layer "silver"
                                                                          :target_model "silver_trip"})
                     #'bitool.modeling.automation/synthesize-silver-graph! (fn [_ _]
                                                                             {:graph_artifact_id 77
                                                                              :graph_id 88
                                                                              :graph_version 3})
                     #'bitool.modeling.automation/compile-silver-graph-artifact! (fn [_]
                                                                                   {:sql_ir {:materialization {:mode "merge"}}
                                                                                    :select_sql "SELECT bronze.trip_id AS \"trip_id\" FROM \"db\".\"bronze\".\"trip_raw\" bronze"
                                                                                    :compiled_sql "CREATE OR REPLACE TABLE \"db\".\"silver\".\"trip\" AS SELECT ..."})
                     #'db/getGraph (fn [_] {:n {9 {:na {:btype "Tg"}}}})
                     #'bitool.modeling.automation/target-node-id-from-graph-artifact (fn [_ _] 9)
                     #'g2/getData (fn [_ _] {:target_kind "snowflake"
                                             :connection_id 42
                                             :catalog "db"
                                             :schema "silver"
                                             :table_name "trip"})
                     #'db/get-opts (fn [_ _] :snowflake-opts)
                     #'jdbc/execute! (fn [opts [sql]]
                                       (reset! captured-sql [opts sql])
                                       [{:plan "ok"}])
                     #'bitool.modeling.automation/persist-model-validation! (fn [payload]
                                                                              (reset! captured-validation payload)
                                                                              {:validation_id 501})}
      (fn []
        (let [result (modeling/validate-silver-proposal-warehouse! 22 {:created_by "alice"})]
          (is (= "valid" (:status result)))
          (is (= "silver_warehouse_sql" (:validation-kind @captured-validation)))
          (is (= :snowflake-opts (first @captured-sql)))
          (is (re-find #"^EXPLAIN USING TEXT " (second @captured-sql))))))))

(deftest execute-silver-release-runs-snowflake-synchronously
  (let [completed (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/release-by-id (fn [_] {:release_id 91
                                                                         :layer "silver"
                                                                         :graph_artifact_id 77})
                     #'bitool.modeling.automation/execute-silver-release-tx! (fn [_ _ _]
                                                                               {:model_run_id 601
                                                                                :release_id 91
                                                                                :graph_artifact_id 77
                                                                                :graph_id 88
                                                                                :graph_version 5
                                                                                :conn_id 42
                                                                                :warehouse "snowflake"
                                                                                :compiled_sql "CREATE OR REPLACE TABLE \"db\".\"silver\".\"trip\" AS SELECT 1"
                                                                                :status "pending"})
                     #'db/get-opts (fn [_ _] :snowflake-opts)
                     #'jdbc/execute! (fn [opts [sql]]
                                       (is (= :snowflake-opts opts))
                                       (is (re-find #"CREATE OR REPLACE TABLE" sql))
                                       [{:status "Statement executed successfully."}])
                     #'bitool.modeling.automation/complete-model-run! (fn [model-run-id payload]
                                                                        (reset! completed [model-run-id payload])
                                                                        nil)}
      (fn []
        (let [result (modeling/execute-silver-release! 91 {:created_by "alice"})]
          (is (= "succeeded" (:status result)))
          (is (= "snowflake_sql" (:backend result)))
          (is (= 601 (first @completed)))
          (is (= "succeeded" (get-in @completed [1 :status]))))))))

(deftest update-silver-proposal-clones-published-proposal-into-new-draft
  (let [captured-persist (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/proposal-by-id (fn [_]
                                                                   {:proposal_id 22
                                                                    :profile_id 11
                                                                    :tenant_key "tenant-a"
                                                                    :workspace_key "ops"
                                                                    :layer "silver"
                                                                    :status "published"
                                                                    :target_model "silver_trip"
                                                                    :source_graph_id 99
                                                                    :source_node_id 2
                                                                    :source_endpoint_name "trips"
                                                                    :confidence_score 0.8
                                                                    :proposal_json "{\"target_model\":\"silver_trip\",\"target_table\":\"sheetz_telematics.silver.trip\"}"})
                     #'bitool.modeling.automation/persist-model-proposal! (fn [proposal profile-id]
                                                                            (reset! captured-persist [proposal profile-id])
                                                                            {:proposal_id 23
                                                                             :layer "silver"
                                                                             :status "draft"
                                                                             :target_model (:target_model proposal)
                                                                             :source_graph_id (:source_graph_id proposal)
                                                                             :source_node_id (:source_node_id proposal)
                                                                             :source_endpoint_name (:source_endpoint_name proposal)
                                                                             :created_by (:created_by proposal)
                                                                             :created_at_utc "2026-03-14T00:00:00Z"
                                                                             :proposal_json (json/generate-string (:proposal_json proposal))
                                                                             :confidence_score (:confidence_score proposal)})
                     #'bitool.modeling.automation/latest-validation-for-proposal (fn [_] nil)
                     #'bitool.modeling.automation/latest-active-release (fn [& _] nil)
                     #'bitool.modeling.automation/latest-graph-artifact-for-proposal (fn [& _] nil)}
      (fn []
        (let [result (modeling/update-silver-proposal! 22 {:proposal {:target_model "silver_trip_v2"}
                                                           :created_by "alice"})]
          (is (= 23 (:proposal_id result)))
          (is (= 11 (second @captured-persist)))
          (is (= "silver_trip_v2" (get-in @captured-persist [0 :target_model])))
          (is (= "draft" (:status result))))))))

(deftest update-silver-proposal-deep-merges-materialization
  (let [captured-update (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/proposal-by-id (fn [proposal-id]
                                                                   {:proposal_id proposal-id
                                                                    :layer "silver"
                                                                    :status "draft"
                                                                    :target_model "silver_trip"
                                                                    :created_by "alice"
                                                                    :created_at_utc "2026-03-14T00:00:00Z"
                                                                    :source_graph_id 99
                                                                    :source_node_id 2
                                                                    :source_endpoint_name "trips"
                                                                    :confidence_score 0.8
                                                                    :proposal_json "{\"target_model\":\"silver_trip\",\"materialization\":{\"mode\":\"merge\",\"keys\":[\"trip_id\"]}}"})
                     #'bitool.modeling.automation/update-model-proposal! (fn [proposal-id attrs]
                                                                           (reset! captured-update [proposal-id attrs])
                                                                           nil)
                     #'bitool.modeling.automation/latest-validation-for-proposal (fn [_] nil)
                     #'bitool.modeling.automation/latest-active-release (fn [& _] nil)
                     #'bitool.modeling.automation/latest-graph-artifact-for-proposal (fn [& _] nil)}
      (fn []
        (modeling/update-silver-proposal! 22 {:proposal {:materialization {:mode "append"}}
                                              :created_by "alice"})
        (is (= "append" (get-in @captured-update [1 :proposal_json :materialization :mode])))
        (is (= ["trip_id"] (get-in @captured-update [1 :proposal_json :materialization :keys])))))))

(deftest synthesize-silver-graph-persists-graph-artifact
  (let [captured-artifact (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/resolve-proposal-context (fn [_]
                                                                            {:proposal-row {:proposal_id 22
                                                                                            :target_model "silver_trip"
                                                                                            :source_node_id 2
                                                                                            :source_endpoint_name "trips"}
                                                                             :proposal-json {:target_model "silver_trip"
                                                                                             :target_table "sheetz_telematics.silver.trip"
                                                                                             :columns [{:target_column "trip_id"
                                                                                                        :expression "bronze.trip_id"
                                                                                                        :type "STRING"
                                                                                                        :nullable false
                                                                                                        :source_columns ["trip_id"]
                                                                                                        :source_paths ["$.data[].id"]}]
                                                                                             :mappings [{:target_column "trip_id"
                                                                                                         :expression "bronze.trip_id"
                                                                                                         :source_columns ["trip_id"]
                                                                                                         :source_paths ["$.data[].id"]}]
                                                                                             :materialization {:mode "merge"
                                                                                                               :keys ["trip_id"]}}
                                                                             :graph-id 99
                                                                             :target {:connection_id 9
                                                                                      :catalog "sheetz_telematics"
                                                                                      :schema "silver"
                                                                                      :silver_job_id "111"}
                                                                             :source-system "samara"
                                                                             :source-table "sheetz_telematics.bronze.trip_raw"})
                     #'bitool.modeling.automation/latest-graph-artifact-for-proposal (fn [& _] nil)
                     #'bitool.modeling.automation/synthesize-silver-graph-internal! (fn [_]
                                                                                      {:gil {:graph-name "silver-silver_trip-proposal-22"}
                                                                                       :graph-id 801
                                                                                       :graph-version 3
                                                                                       :node-map {"target" 5}
                                                                                       :panel []})
                     #'bitool.modeling.automation/persist-model-graph-artifact! (fn [payload]
                                                                                  (reset! captured-artifact payload)
                                                                                  {:graph_artifact_id 91})}
      (fn []
        (let [result (modeling/synthesize-silver-graph! 22 {:created_by "alice"})]
          (is (= 91 (:graph_artifact_id result)))
          (is (= 801 (:graph_id result)))
          (is (= "silver_intermediate" (:graph-kind @captured-artifact)))
          (is (= 22 (:proposal-id @captured-artifact))))))))

(deftest compile-silver-graph-artifact-builds-sql-from-gil
  (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                   #'bitool.modeling.automation/graph-artifact-by-id (fn [_]
                                                                       {:graph_artifact_id 91
                                                                        :gil_json (json/generate-string
                                                                                   {:nodes [{:type "table"
                                                                                             :config {:table_name "sheetz_telematics.bronze.trip"}}
                                                                                            {:type "projection"
                                                                                             :config {:columns [{:target_column "trip_id"
                                                                                                                 :type "STRING"
                                                                                                                 :nullable false
                                                                                                                 :source_columns ["trip_id"]
                                                                                                                 :source_paths ["$.trip_id"]
                                                                                                                 :expression "bronze.trip_id"}]}}
                                                                                            {:type "mapping"
                                                                                             :config {:mapping [{:target_column "trip_id"
                                                                                                                 :expression "bronze.trip_id"
                                                                                                                 :source_columns ["trip_id"]
                                                                                                                 :source_paths ["$.trip_id"]}]}}
                                                                                            {:type "filter"
                                                                                             :config {:sql "trip_id IS NOT NULL"}}
                                                                                            {:type "target"
                                                                                             :alias "silver_trip"
                                                                                             :config {:catalog "sheetz_telematics"
                                                                                                      :schema "silver"
                                                                                                      :table_name "trip"
                                                                                                      :write_mode "merge"
                                                                                                      :merge_keys ["trip_id"]}}]})})}
    (fn []
      (let [result (modeling/compile-silver-graph-artifact! 91)]
        (is (re-find #"MERGE INTO sheetz_telematics.silver.trip" (:compiled_sql result)))
        (is (re-find #"WHERE trip_id IS NOT NULL" (:select_sql result)))
        (is (= "merge" (get-in result [:sql_ir :materialization :mode])))))))

(deftest review-silver-proposal-updates-review-metadata
  (let [updated (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/proposal-by-id (fn [_]
                                                                   {:proposal_id 22
                                                                    :layer "silver"
                                                                    :proposal_json "{\"target_model\":\"silver_trip\"}"})
                     #'bitool.modeling.automation/update-model-proposal! (fn [proposal-id attrs]
                                                                           (reset! updated [proposal-id attrs])
                                                                           nil)}
      (fn []
        (let [result (modeling/review-silver-proposal! 22 {:review_state "approved"
                                                           :review_notes "looks good"
                                                           :reviewed_by "alice"})]
          (is (= "approved" (:status result)))
          (is (= 22 (first @updated)))
          (is (= "approved" (get-in @updated [1 :status])))
          (is (= "alice" (get-in @updated [1 :proposal_json :review :reviewed_by]))))))))

(deftest validate-silver-proposal-warehouse-submits-job
  (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                   #'bitool.modeling.automation/proposal-by-id (fn [_]
                                                                 {:proposal_id 22
                                                                  :layer "silver"
                                                                  :target_model "silver_trip"})
                   #'bitool.modeling.automation/synthesize-silver-graph! (fn [_ _]
                                                                           {:graph_artifact_id 91
                                                                            :graph_id 801
                                                                            :graph_version 3})
                   #'bitool.modeling.automation/compile-silver-graph-artifact! (fn [_]
                                                                                  {:compiled_sql "MERGE INTO silver.trip ..."
                                                                                   :sql_ir {:materialization {:mode "merge"}}})
                   #'db/getGraph (fn [_]
                                   {:n {5 {:na {:btype "Tg"
                                                :connection_id 9
                                                :catalog "sheetz_telematics"
                                                :schema "silver"
                                                :table_name "trip"
                                                :silver_validation_job_id "222"}}}})
                   #'bitool.modeling.automation/graph-artifact-by-id (fn [_]
                                                                        {:graph_artifact_id 91
                                                                         :graph_id 801
                                                                         :graph_version 3
                                                                         :node_map_json "{\"target\":5}"})
                   #'bitool.databricks.jobs/trigger-job! (fn [_ job-id params]
                                                           (is (= "222" job-id))
                                                           (is (= "22" (:proposal_id params)))
                                                           {:job_id job-id :run_id 777})
                   #'bitool.modeling.automation/persist-model-validation! (fn [payload]
                                                                            (is (= "submitted" (:status payload)))
                                                                            {:validation_id 501})}
    (fn []
      (let [result (modeling/validate-silver-proposal-warehouse! 22 {:created_by "alice"})]
        (is (= "submitted" (:status result)))
        (is (= 501 (:validation_id result)))
        (is (= 91 (:graph_artifact_id result)))))))

(deftest execute-silver-release-triggers-databricks-job-and-persists-run
  (let [captured-update (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/execute-silver-release-tx! (fn [_ release-row created-by]
                                                                                (is (= 700 (:release_id release-row)))
                                                                                (is (= "alice" created-by))
                                                                                {:model_run_id 501
                                                                                 :release_id 700
                                                                                 :graph_artifact_id 91
                                                                                 :graph_id 801
                                                                                 :graph_version 3
                                                                                 :conn_id 9
                                                                                 :job_id "111"
                                                                                 :params {:model_release_id "700"
                                                                                          :graph_id "801"
                                                                                          :graph_version "3"
                                                                                          :target_model "silver_trip"
                                                                                          :target_table "sheetz_telematics.silver.trip"
                                                                                          :compiled_artifact_id "601"
                                                                                          :sql_checksum "abc123"}
                                                                                 :status "pending"})
                     #'bitool.modeling.automation/release-by-id (fn [_]
                                                                  {:release_id 700
                                                                   :layer "silver"
                                                                   :graph_artifact_id 91
                                                                   :target_model "silver_trip"})
                     #'bitool.databricks.jobs/trigger-job! (fn [_ job-id params]
                                                             (is (= "111" job-id))
                                                             (is (= "700" (:model_release_id params)))
                                                             (is (= "601" (:compiled_artifact_id params)))
                                                             (is (= "abc123" (:sql_checksum params)))
                                                             (is (nil? (:compiled_sql params)))
                                                             {:job_id job-id :run_id 123})
                     #'bitool.modeling.automation/update-model-run-progress! (fn [model-run-id payload]
                                                                                (reset! captured-update [model-run-id payload])
                                                                                nil)}
      (fn []
        (let [result (modeling/execute-silver-release! 700 {:created_by "alice"})]
          (is (= 501 (:model_run_id result)))
          (is (= "submitted" (:status result)))
          (is (= 501 (first @captured-update)))
          (is (= "submitted" (get-in @captured-update [1 :status])))
          (is (= "123" (get-in @captured-update [1 :external-run-id])))
          (is (= 123 (get-in @captured-update [1 :response-json :run_id]))))))))

(deftest execute-silver-release-rejects-duplicate-inflight-run
  (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                   #'bitool.modeling.automation/release-by-id (fn [_]
                                                                {:release_id 700
                                                                 :layer "silver"
                                                                 :graph_artifact_id 91
                                                                 :target_model "silver_trip"})
                   #'bitool.modeling.automation/execute-silver-release-tx! (fn [_ release-row _]
                                                                              (throw (ex-info "Silver release already has an in-flight execution"
                                                                                              {:release_id (:release_id release-row)
                                                                                               :model_run_id 501
                                                                                               :status 409})))}
    (fn []
      (try
        (modeling/execute-silver-release! 700 {:created_by "alice"})
        (is false "Expected duplicate in-flight execution to be rejected")
        (catch clojure.lang.ExceptionInfo e
          (is (= 409 (:status (ex-data e))))
          (is (= 501 (:model_run_id (ex-data e)))))))))

(deftest execute-silver-release-marks-run-failed-when-trigger-fails
  (let [completion (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/release-by-id (fn [_]
                                                                  {:release_id 700
                                                                   :layer "silver"
                                                                   :graph_artifact_id 91
                                                                   :target_model "silver_trip"})
                     #'bitool.modeling.automation/execute-silver-release-tx! (fn [_ _ _]
                                                                                {:model_run_id 501
                                                                                 :release_id 700
                                                                                 :graph_artifact_id 91
                                                                                 :graph_id 801
                                                                                 :graph_version 3
                                                                                 :conn_id 9
                                                                                 :job_id "111"
                                                                                 :params {:model_release_id "700"}
                                                                                 :status "pending"})
                     #'bitool.databricks.jobs/trigger-job! (fn [& _]
                                                             (throw (ex-info "trigger failed" {:http_status 500})))
                     #'bitool.modeling.automation/complete-model-run! (fn [model-run-id payload]
                                                                        (reset! completion [model-run-id payload])
                                                                        nil)}
      (fn []
        (try
          (modeling/execute-silver-release! 700 {:created_by "alice"})
          (is false "Expected Databricks trigger failure")
          (catch clojure.lang.ExceptionInfo e
            (is (= "trigger failed" (.getMessage e)))
            (is (= 501 (first @completion)))
            (is (= "failed" (get-in @completion [1 :status])))
            (is (some? (get-in @completion [1 :completed-at])))))))))

(deftest poll-silver-model-run-updates-terminal-status
  (let [updates (atom [])]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/model-run-by-id (let [state (atom {:model_run_id 501
                                                                                     :status "submitted"
                                                                                     :target_connection_id 9
                                                                                     :external_run_id "123"
                                                                                     :request_json "{}"
                                                                                     :response_json nil})]
                                                                     (fn [_]
                                                                       (if (seq @updates)
                                                                         (assoc @state
                                                                                :status "succeeded"
                                                                                :response_json "{\"body\":{\"state\":{\"life_cycle_state\":\"TERMINATED\",\"result_state\":\"SUCCESS\"}}}")
                                                                         @state)))
                     #'bitool.databricks.jobs/get-run! (fn [_ run-id]
                                                         (is (= "123" run-id))
                                                         {:run_id run-id
                                                          :http_status 200
                                                          :body {:state {:life_cycle_state "TERMINATED"
                                                                         :result_state "SUCCESS"}}})
                     #'bitool.modeling.automation/complete-model-run! (fn [model-run-id payload]
                                                                        (swap! updates conj [model-run-id payload])
                                                                        nil)}
      (fn []
        (let [result (modeling/poll-silver-model-run! 501)]
          (is (= 1 (count @updates)))
          (is (= 501 (ffirst @updates)))
          (is (= "succeeded" (get-in @updates [0 1 :status])))
          (is (= "123" (get-in @updates [0 1 :external-run-id])))
          (is (= "SUCCESS" (get-in @updates [0 1 :response-json :body :state :result_state])))
          (is (some? (get-in @updates [0 1 :completed-at])))
          (is (= "succeeded" (:status result))))))))

(deftest propose-gold-schema-persists-profile-and-proposal
  (let [captured-profile (atom nil)
        captured-proposal (atom nil)
        silver-proposal-row {:proposal_id 22
                             :layer "silver"
                             :tenant_key "tenant-a"
                             :workspace_key "ops"
                             :source_graph_id 99
                             :source_node_id 2
                             :target_model "silver_trip"
                             :proposal_json "{\"target_model\":\"silver_trip\",\"target_table\":\"sheetz_telematics.silver.trip\",\"source_system\":\"samara\",\"columns\":[{\"target_column\":\"trip_id\",\"type\":\"STRING\",\"role\":\"business_key\"},{\"target_column\":\"event_time\",\"type\":\"TIMESTAMP\",\"role\":\"timestamp\"},{\"target_column\":\"distance\",\"type\":\"DOUBLE\",\"role\":\"measure\"}],\"materialization\":{\"mode\":\"merge\",\"keys\":[\"trip_id\"]}}"}]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'control-plane/ensure-control-plane-tables! (fn [] true)
                     #'bitool.modeling.automation/proposal-by-id (fn [_] silver-proposal-row)
                     #'bitool.modeling.automation/latest-model-proposal (fn [& _] nil)
                     #'bitool.modeling.automation/persist-schema-profile! (fn [profile created-by tenant-key workspace-key]
                                                                            (reset! captured-profile [profile created-by tenant-key workspace-key])
                                                                            {:profile_id 41})
                     #'bitool.modeling.automation/persist-model-proposal! (fn [proposal profile-id]
                                                                            (reset! captured-proposal [proposal profile-id])
                                                                            {:proposal_id 42
                                                                             :status "draft"
                                                                             :target_model (:target_model proposal)
                                                                             :confidence_score (:confidence_score proposal)})}
      (fn []
        (let [result (modeling/propose-gold-schema! {:silver_proposal_id 22
                                                     :created_by "alice"})]
          (is (= 41 (:profile_id result)))
          (is (= 42 (:proposal_id result)))
          (is (= "gold" (:layer result)))
          (is (= "gold_trip" (get-in result [:proposal :target_model])))
          (is (= ["trip_id" "event_date"] (get-in result [:proposal :group_by])))
          (is (= "SUM(silver.distance)" (get-in result [:proposal :columns 2 :expression])))
          (is (= 41 (second @captured-proposal))))))))

(deftest compiler-emits-grouped-gold-sql-for-snowflake-target
  (let [result (compiler/compile-model {:target-warehouse :snowflake
                                        :proposal-json {:target_table "sheetz_telematics.gold.trip_daily"
                                                        :target_warehouse "snowflake"
                                                        :source_alias "silver"
                                                        :group_by ["trip_id" "event_date"]
                                                        :materialization {:mode "merge"
                                                                          :keys ["trip_id" "event_date"]}
                                                        :columns [{:target_column "trip_id"
                                                                   :expression "silver.trip_id"
                                                                   :type "STRING"
                                                                   :nullable false}
                                                                  {:target_column "event_date"
                                                                   :expression "CAST(silver.event_time AS DATE)"
                                                                   :type "DATE"
                                                                   :nullable false}
                                                                  {:target_column "sum_distance"
                                                                   :expression "SUM(silver.distance)"
                                                                   :type "DOUBLE"
                                                                   :nullable false}]}
                                        :source-table "sheetz_telematics.silver.trip"})]
    (is (re-find #"FROM \"sheetz_telematics\"\.\"silver\"\.\"trip\" silver" (:select_sql result)))
    (is (re-find #"GROUP BY silver\.trip_id, CAST\(silver\.event_time AS DATE\)" (:select_sql result)))
    (is (re-find #"MERGE INTO \"sheetz_telematics\"\.\"gold\"\.\"trip_daily\"" (:compiled_sql result)))))

(deftest execute-gold-release-runs-snowflake-synchronously
  (let [completed (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/release-by-id (fn [_]
                                                                  {:release_id 191
                                                                   :layer "gold"
                                                                   :graph_artifact_id 901})
                     #'bitool.modeling.automation/execute-gold-release-tx! (fn [_ _ _]
                                                                             {:model_run_id 601
                                                                              :release_id 191
                                                                              :graph_artifact_id 901
                                                                              :graph_id 77
                                                                              :graph_version 2
                                                                              :conn_id 31
                                                                              :warehouse "snowflake"
                                                                              :compiled_sql "CREATE OR REPLACE TABLE \"db\".\"gold\".\"trip_daily\" AS SELECT 1"
                                                                              :params {}
                                                                              :status "pending"})
                     #'db/get-opts (fn [_ _] :snowflake-opts)
                     #'jdbc/execute! (fn [opts [sql]]
                                       (is (= :snowflake-opts opts))
                                       (is (re-find #"gold" sql))
                                       [{:status "ok"}])
                     #'bitool.modeling.automation/complete-model-run! (fn [model-run-id payload]
                                                                        (reset! completed [model-run-id payload]))}
      (fn []
        (let [result (modeling/execute-gold-release! 191 {:created_by "alice"})]
          (is (= 601 (:model_run_id result)))
          (is (= "snowflake_sql" (:backend result)))
          (is (= "succeeded" (:status result)))
          (is (= 601 (first @completed))))))))
