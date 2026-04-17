(ns bitool.modeling-automation-test
  (:require [bitool.control-plane :as control-plane]
            [bitool.compiler.core :as compiler]
            [bitool.compiler.dialect.databricks :as databricks]
            [bitool.db :as db]
            [bitool.graph2 :as g2]
            [bitool.modeling.automation :as modeling]
            [cheshire.core :as json]
            [clojure.string :as str]
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
    (is (= ["trip_id"] (get-in proposal [:proposal_json :processing_policy :business_keys])))
    (is (= "updated_at" (get-in proposal [:proposal_json :processing_policy :event_time_column])))
    (is (= "latest_event_time_wins" (get-in proposal [:proposal_json :processing_policy :ordering_strategy])))
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

(deftest source-table-for-endpoint-falls-back-to-auto-qualified-target-table
  (is (= "bitool.public.fleet_vehicles"
         (#'bitool.modeling.automation/source-table-for-endpoint
          {:catalog "bitool"
           :schema "public"
           :table_name ""}
          {:endpoint_name "fleet/vehicles"
           :endpoint_url "fleet/vehicles"
           :bronze_table_name ""
           :table_name ""}))))

(deftest available-source-columns-normalizes-profile-field-type-keys-to-strings
  (is (= #{"data_items_id" "pagination_endCursor"}
         (#'bitool.modeling.automation/available-source-columns
          {:columns [{:source_columns ["data_items_id"]}]}
          {:field_types {:data_items_id {:type "STRING"}
                         :pagination_endCursor {:type "STRING"}}}))))

(deftest modeling-persistence-binds-timestamptz-values-as-instants
  (let [captured-params (atom nil)
        published-at (java.time.Instant/parse "2026-03-20T23:43:27Z")]
    (with-redefs-fn {#'bitool.modeling.automation/db-opts (fn [_] ::db-opts)
                     #'jdbc/execute-one! (fn [_ sql-params]
                                           (reset! captured-params sql-params)
                                           {:proposal_id 1
                                            :created_at_utc (nth sql-params (dec (count sql-params)))})}
      (fn []
        (#'bitool.modeling.automation/persist-schema-profile!
         {:graph_id 99
          :api_node_id 2
          :source_layer "bronze"
          :source_system "samara"
          :endpoint_name "trips"
          :profile_source "endpoint_config"
          :sample_record_count 1
          :field_count 1
          :profile_json {:field_types {"trip_id" {:type "STRING"}}}}
         "alice"
         "tenant-a"
         "ops")
        (is (instance? java.sql.Timestamp (last @captured-params)))
        (#'bitool.modeling.automation/persist-model-release!
         {:proposal-id 22
          :validation-id 33
          :graph-artifact-id 44
          :layer "silver"
          :target-model "silver_trip"
          :version 1
          :status "published"
          :active true
          :created-by "alice"
         :published-at (str published-at)})
        (is (instance? java.sql.Timestamp (nth @captured-params 10)))))))

(deftest ensure-postgresql-target-table-adds-missing-columns-for-evolved-models
  (let [calls (atom [])]
    (with-redefs-fn {#'bitool.compiler.core/compile-ddl (fn [_]
                                                          "CREATE TABLE IF NOT EXISTS \"gold_fleet_vehicles\" (\"data_items_id\" TEXT)")
                     #'bitool.db/get-opts (fn [_ _] ::db-opts)
                     #'jdbc/execute! (fn [_ sql-params]
                                       (swap! calls conj sql-params)
                                       (if (str/includes? (first sql-params) "information_schema.columns")
                                         [{:column_name "data_items_id"}]
                                         []))}
      (fn []
        (#'bitool.modeling.automation/ensure-postgresql-target-table!
         9
         {:materialization {:target "gold_fleet_vehicles"}
          :select [{:target_column "data_items_id" :type "STRING"}
                   {:target_column "row_count" :type "BIGINT"}]})
        (is (= ["CREATE TABLE IF NOT EXISTS \"gold_fleet_vehicles\" (\"data_items_id\" TEXT)"]
               (first @calls)))
        (is (= ["SELECT column_name
                                                    FROM information_schema.columns
                                                   WHERE table_schema = ?
                                                     AND table_name = ?"
                "public"
                "gold_fleet_vehicles"]
               (second @calls)))
        (is (= [(str "ALTER TABLE \"gold_fleet_vehicles\" ADD COLUMN \"row_count\" BIGINT")]
               (nth @calls 2)))))))

(deftest safe-proposal-expression-allows-supported-transform-nesting
  (is (#'bitool.modeling.automation/safe-proposal-expression? "UPPER(TRIM(bronze.data_items_name))"))
  (is (#'bitool.modeling.automation/safe-proposal-expression? "SUBSTRING(LOWER(bronze.data_items_name), 1, 3)"))
  (is (#'bitool.modeling.automation/safe-proposal-expression? "CAST(TRIM(bronze.data_items_name) AS VARCHAR)"))
  (is (#'bitool.modeling.automation/safe-proposal-expression? "CAST(TRIM(bronze.data_items_createdAtTime) AS DATE)"))
  (is (#'bitool.modeling.automation/safe-proposal-expression? "silver.\"data_items_createdAtTime\""))
  (is (#'bitool.modeling.automation/safe-proposal-expression? "SUM(silver.\"distanceMiles\")"))
  (is (#'bitool.modeling.automation/safe-proposal-expression? "CAST(silver.\"data_items_createdAtTime\" AS DATE)"))
  (is (#'bitool.modeling.automation/safe-proposal-expression? "CASE WHEN silver.\"status\" = 'active' THEN UPPER(silver.\"driverName\") ELSE COALESCE(silver.\"driverAlias\", silver.\"driverName\") END"))
  (is (#'bitool.modeling.automation/safe-proposal-expression? "silver.\"firstName\" || ' ' || silver.\"lastName\""))
  (is (#'bitool.modeling.automation/safe-proposal-expression? "CAST((bronze.payload_json::jsonb #>> '{pagination,hasNextPage}') AS BOOLEAN)"))
  (is (#'bitool.modeling.automation/safe-proposal-expression? "CAST((item.value #>> '{createdAtTime}') AS TIMESTAMP)"))
  (is
   (not (#'bitool.modeling.automation/safe-proposal-expression?
         "DROP TABLE public.gold_fleet_vehicles"))))

(deftest build-silver-proposal-for-postgresql-api-uses-payload-json-expressions
  (let [endpoint {:endpoint_name "fleet/vehicles"
                  :primary_key_fields ["id"]
                  :inferred_fields [{:path "$.pagination.endCursor"
                                     :column_name "pagination_endCursor"
                                     :type "STRING"
                                     :nullable true
                                     :enabled true}
                                    {:path "$.pagination.hasNextPage"
                                     :column_name "pagination_hasNextPage"
                                     :type "BOOLEAN"
                                     :nullable true
                                     :enabled true}
                                    {:path "$.data[].id"
                                     :column_name "data_items_id"
                                     :type "STRING"
                                     :nullable false
                                     :enabled true}
                                    {:path "$.data[].createdAtTime"
                                     :column_name "data_items_createdAtTime"
                                     :type "TIMESTAMP"
                                     :nullable true
                                     :enabled true}]}
        profile {:profile_source "endpoint_schema_snapshot"
                 :sample_record_count 10
                 :field_count 4
                 :profile_json {:field_types {"pagination_endCursor" {:type "STRING"}
                                              "pagination_hasNextPage" {:type "BOOLEAN"}
                                              "data_items_id" {:type "STRING"}
                                              "data_items_createdAtTime" {:type "TIMESTAMP"}}}}
        proposal (#'bitool.modeling.automation/build-silver-proposal
                  {:tenant-key "tenant-a"
                   :workspace-key "ops"
                   :graph-id 99
                   :api-node-id 2
                   :source-system "samara"
                   :endpoint endpoint
                   :target {:target_kind "postgresql" :connection_id 9}
                   :profile profile
                   :created-by "alice"})]
    (is (= {:kind "jsonb_array"
            :alias "item"
            :json_column "payload_json"
            :path ["data"]}
           (get-in proposal [:proposal_json :source_expansion])))
    (is (= "(bronze.payload_json::jsonb #>> '{pagination,endCursor}')"
           (get-in proposal [:proposal_json :columns 0 :expression])))
    (is (= "CAST((bronze.payload_json::jsonb #>> '{pagination,hasNextPage}') AS BOOLEAN)"
           (get-in proposal [:proposal_json :columns 1 :expression])))
    (is (= "(item.value #>> '{id}')"
           (get-in proposal [:proposal_json :columns 2 :expression])))
    (is (= "CAST((item.value #>> '{createdAtTime}') AS TIMESTAMP)"
           (get-in proposal [:proposal_json :columns 3 :expression])))))

(deftest build-gold-proposal-for-postgresql-quotes-mixed-case-silver-columns
  (let [silver-proposal {:target_warehouse "postgresql"
                         :target_model "silver_fleet_vehicles"
                         :target_table "public.silver_fleet_vehicles"
                         :materialization {:keys ["data_items_id"]}
                         :columns [{:target_column "data_items_id"
                                    :type "STRING"
                                    :nullable false
                                    :role "business_key"
                                    :source_paths ["$.data[].id"]
                                    :source_columns ["data_items_id"]
                                    :expression "(item.value #>> '{id}')"}
                                   {:target_column "data_items_createdAtTime"
                                    :type "TIMESTAMP"
                                    :nullable true
                                    :role "timestamp"
                                    :source_paths ["$.data[].createdAtTime"]
                                    :source_columns ["data_items_createdAtTime"]
                                    :expression "CAST((item.value #>> '{createdAtTime}') AS TIMESTAMP)"}
                                   {:target_column "distanceMiles"
                                    :type "DOUBLE"
                                    :nullable true
                                    :role "measure_candidate"
                                    :source_paths ["$.data[].distanceMiles"]
                                    :source_columns ["distanceMiles"]
                                    :expression "CAST((item.value #>> '{distanceMiles}') AS DOUBLE PRECISION)"}]}
        proposal (#'bitool.modeling.automation/build-gold-proposal
                  {:tenant-key "tenant-a"
                   :workspace-key "ops"
                   :silver-proposal silver-proposal
                   :proposal-row {:source_graph_id 99
                                  :source_node_id 2}
                   :target-warehouse "postgresql"
                   :created-by "alice"})]
    (is (= "postgresql" (get-in proposal [:proposal_json :target_warehouse])))
    (is (= "silver.\"data_items_id\"" (get-in proposal [:proposal_json :columns 0 :expression])))
    (is (= "CAST(silver.\"data_items_createdAtTime\" AS DATE)" (get-in proposal [:proposal_json :columns 1 :expression])))
    (is (= "SUM(silver.\"distanceMiles\")" (get-in proposal [:proposal_json :columns 2 :expression])))))

(deftest build-gold-proposal-for-databricks-quotes-silver-columns-with-backticks
  (let [silver-proposal {:target_warehouse "databricks"
                         :target_model "silver_fleet_vehicles"
                         :target_table "main.silver.fleet_vehicles"
                         :materialization {:keys ["data_items_id"]}
                         :columns [{:target_column "data_items_id"
                                    :type "STRING"
                                    :nullable false
                                    :role "business_key"
                                    :source_columns ["data_items_id"]
                                    :expression "bronze.data_items_id"}
                                   {:target_column "data_items_createdAtTime"
                                    :type "TIMESTAMP"
                                    :nullable true
                                    :role "timestamp"
                                    :source_columns ["data_items_createdAtTime"]
                                    :expression "bronze.data_items_createdAtTime"}
                                   {:target_column "distanceMiles"
                                    :type "DOUBLE"
                                    :nullable true
                                    :role "measure_candidate"
                                    :source_columns ["distanceMiles"]
                                    :expression "bronze.distanceMiles"}]}
        proposal (#'bitool.modeling.automation/build-gold-proposal
                  {:tenant-key "tenant-a"
                   :workspace-key "ops"
                   :silver-proposal silver-proposal
                   :proposal-row {:source_graph_id 99
                                  :source_node_id 2}
                   :target-warehouse "databricks"
                   :created-by "alice"})]
    (is (= "databricks" (get-in proposal [:proposal_json :target_warehouse])))
    (is (= "silver.`data_items_id`" (get-in proposal [:proposal_json :columns 0 :expression])))
    (is (= "CAST(silver.`data_items_createdAtTime` AS DATE)" (get-in proposal [:proposal_json :columns 1 :expression])))
    (is (= "SUM(silver.`distanceMiles`)" (get-in proposal [:proposal_json :columns 2 :expression])))))

(deftest static-validation-allows-supported-transform-functions
  (is (= []
         (#'bitool.modeling.automation/static-validation-errors
          {:layer "silver"
           :source_layer "bronze"
           :source_alias "bronze"
           :target_table "bitool.public.silver_fleet_drivers"
           :columns [{:target_column "driver_name"
                      :type "STRING"
                      :nullable true
                      :source_columns ["data_items_name"]
                      :expression "UPPER(TRIM(bronze.data_items_name))"}]
           :mappings [{:target_column "driver_name"
                       :source_columns ["data_items_name"]
                       :expression "UPPER(TRIM(bronze.data_items_name))"}]
           :materialization {:mode "table_replace"
                             :keys []}}
          "bitool.public.fleet_drivers"
          {:field_types {"data_items_name" {:type "STRING"}}}))))

(deftest static-validation-enforces-processing-policy-contract
  (let [errors (#'bitool.modeling.automation/static-validation-errors
                {:layer "silver"
                 :source_layer "bronze"
                 :source_alias "bronze"
                 :target_table "bitool.public.silver_trip"
                 :columns [{:target_column "trip_id"
                            :type "STRING"
                            :nullable false
                            :source_columns ["trip_id"]
                            :expression "bronze.trip_id"}
                           {:target_column "event_time"
                            :type "TIMESTAMP"
                            :nullable true
                            :source_columns ["event_time"]
                            :expression "bronze.event_time"}]
                 :mappings [{:target_column "trip_id"
                             :source_columns ["trip_id"]
                             :expression "bronze.trip_id"}
                            {:target_column "event_time"
                             :source_columns ["event_time"]
                             :expression "bronze.event_time"}]
                 :processing_policy {:business_keys ["trip_id"]
                                     :ordering_strategy "event_time_then_sequence"
                                     :event_time_column "event_time"
                                     :reprocess_window {:value "abc"
                                                        :unit "months"}}
                 :materialization {:mode "merge"
                                   :keys ["trip_id"]}}
                "bitool.public.trip_raw"
                {:field_types {"trip_id" {:type "STRING"}
                               "event_time" {:type "TIMESTAMP"}}})]
    (is (some #(= "sequence_column" (:field %)) errors))
    (is (some #(= "reprocess_window" (:field %)) errors))))

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

(deftest propose-silver-schema-rejects-unsupported-source-node
  (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                   #'control-plane/ensure-control-plane-tables! (fn [] true)
                   #'db/getGraph (fn [_] {:a {:id 99 :v 1}})
                   #'g2/getData (fn [_ _] {:btype "Fi"})}
    (fn []
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Node is not a supported Bronze source node"
                            (modeling/propose-silver-schema! {:graph-id 99
                                                              :api-node-id 7}))))))

(deftest propose-silver-schema-supports-kafka-source-node
  (let [captured-proposal (atom nil)
        graph {:a {:id 99 :v 7}}
        kafka-node {:btype "Kf"
                    :source_system "kafka"
                    :topic_configs [{:endpoint_name "orders.events"
                                     :enabled true
                                     :primary_key_fields ["id"]
                                     :silver_table_name "silver_orders"
                                     :inferred_fields [{:path "$._record.id"
                                                        :column_name "order_id"
                                                        :type "STRING"
                                                        :nullable false
                                                        :enabled true}]}]}]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'control-plane/ensure-control-plane-tables! (fn [] true)
                     #'db/getGraph (fn [_] graph)
                     #'g2/getData (fn [_ _] kafka-node)
                     #'control-plane/graph-workspace-context (fn [_]
                                                               {:tenant_key "tenant-a"
                                                                :workspace_key "ops"})
                     #'bitool.modeling.automation/find-downstream-target (fn [_ _] {:connection_id 9})
                     #'bitool.modeling.automation/schema-source-for-endpoint (fn [_ _ _ endpoint _]
                                                                               {:source "endpoint_config"
                                                                                :snapshot-row nil
                                                                                :fields (:inferred_fields endpoint)})
                     #'bitool.modeling.automation/latest-model-proposal (fn [& _] nil)
                     #'bitool.modeling.automation/persist-schema-profile! (fn [_ _ _ _]
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
                                                       :endpoint-name "orders.events"
                                                       :created-by "alice"})]
          (is (= 22 (:proposal_id result)))
          (is (= "silver_orders" (get-in result [:proposal :target_table])))
          (is (= "orders.events" (get-in result [:proposal :endpoint_name])))
          (is (= "orders.events" (get-in @captured-proposal [0 :source_endpoint_name]))))))))

(deftest propose-silver-schema-supports-file-source-node
  (let [captured-proposal (atom nil)
        graph {:a {:id 99 :v 7}}
        file-node {:btype "Fs"
                   :source_system "file"
                   :file_configs [{:endpoint_name "orders.jsonl"
                                   :enabled true
                                   :primary_key_fields ["id"]
                                   :bronze_table_name "bronze.orders_raw"
                                   :inferred_fields [{:path "$._record.id"
                                                      :column_name "order_id"
                                                      :type "STRING"
                                                      :nullable false
                                                      :enabled true}]}]}]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'control-plane/ensure-control-plane-tables! (fn [] true)
                     #'db/getGraph (fn [_] graph)
                     #'g2/getData (fn [_ _] file-node)
                     #'control-plane/graph-workspace-context (fn [_]
                                                               {:tenant_key "tenant-a"
                                                                :workspace_key "ops"})
                     #'bitool.modeling.automation/find-downstream-target (fn [_ _] {:connection_id 9})
                     #'bitool.modeling.automation/schema-source-for-endpoint (fn [_ _ _ endpoint _]
                                                                               {:source "endpoint_config"
                                                                                :snapshot-row nil
                                                                                :fields (:inferred_fields endpoint)})
                     #'bitool.modeling.automation/latest-model-proposal (fn [& _] nil)
                     #'bitool.modeling.automation/persist-schema-profile! (fn [_ _ _ _]
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
                                                       :endpoint-name "orders.jsonl"
                                                       :created-by "alice"})]
          (is (= 22 (:proposal_id result)))
          (is (= "orders.jsonl" (get-in result [:proposal :endpoint_name])))
          (is (= "orders.jsonl" (get-in @captured-proposal [0 :source_endpoint_name]))))))))

(deftest propose-silver-schema-rebuilds-when-latest-stored-shape-does-not-match-current-profile
  (let [graph {:a {:id 99 :v 7}}
        api-node {:btype "Ap"
                  :source_system "samara"
                  :endpoint_configs [{:endpoint_name "trips"
                                      :enabled true
                                      :primary_key_fields ["id"]
                                      :silver_table_name "silver_trip"}]}
        profile-template (#'bitool.modeling.automation/build-schema-profile
                          {:graph-id 99
                           :api-node-id 2
                           :source-system "samara"
                           :endpoint-name "trips"
                           :source "endpoint_schema_snapshot"
                           :snapshot-row {:sample_record_count 3
                                          :captured_at_utc "2026-03-14T01:00:00Z"}
                           :endpoint {:primary_key_fields ["id"]}
                           :fields [{:path "$.data[].id"
                                     :column_name "trip_id"
                                     :type "STRING"
                                     :nullable false
                                     :enabled true}]})
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
                                                                          :profile_json (json/generate-string (:profile_json profile-template))})
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
                (is (nil? (:deduped result)))
                (is (= 22 (:proposal_id result)))
                (is @persist-profile-called?)
                (is @persist-proposal-called?)))))))))

(deftest propose-silver-schema-does-not-dedupe-when-profile-json-changes
  (let [graph {:a {:id 99 :v 7}}
        api-node {:btype "Ap"
                  :source_system "samara"
                  :endpoint_configs [{:endpoint_name "trips"
                                      :enabled true
                                      :primary_key_fields ["id"]
                                      :silver_table_name "silver_trip"}]}
        persist-profile-called? (atom false)
        persist-proposal-called? (atom false)]
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
                                                                                         {:path "$.data[].vehicle.id"
                                                                                          :column_name "vehicle_id"
                                                                                          :type "STRING"
                                                                                          :nullable true
                                                                                          :enabled true}]
                                                                                :endpoint endpoint})
                     #'bitool.modeling.automation/schema-profile-by-id (fn [_]
                                                                         {:profile_id 11
                                                                          :profile_source "endpoint_schema_snapshot"
                                                                          :sample_record_count 3
                                                                          :field_count 1
                                                                          :profile_json "{\"field_types\":{\"trip_id\":{\"type\":\"STRING\",\"nullable\":false}}}"})
                     #'bitool.modeling.automation/persist-schema-profile! (fn [_ _ _ _]
                                                                            (reset! persist-profile-called? true)
                                                                            {:profile_id 12})
                     #'bitool.modeling.automation/persist-model-proposal! (fn [proposal profile-id]
                                                                            (reset! persist-proposal-called? [proposal profile-id])
                                                                            {:proposal_id 23
                                                                             :status "draft"
                                                                             :target_model (:target_model proposal)
                                                                             :confidence_score (:confidence_score proposal)})}
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
                                  :created-by "alice"})
              latest-proposal-row {:proposal_id 22
                                   :profile_id 11
                                   :status "draft"
                                   :target_model (:target_model proposal-template)
                                   :confidence_score (:confidence_score proposal-template)
                                   :proposal_json (json/generate-string (:proposal_json proposal-template))}]
          (with-redefs-fn {#'bitool.modeling.automation/latest-model-proposal (fn [& _] latest-proposal-row)}
            (fn []
              (let [result (modeling/propose-silver-schema! {:graph-id 99
                                                             :api-node-id 2
                                                             :created-by "alice"})]
                (is (nil? (:deduped result)))
                (is (= 23 (:proposal_id result)))
                (is @persist-profile-called?)
                (is (= 12 (second @persist-proposal-called?)))))))))))

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
        (is (re-find #"MERGE INTO `sheetz_telematics`\.`silver`\.`trip`" (:compiled_sql result)))
        (is (re-find #"FROM `sheetz_telematics`\.`bronze`\.`trip_raw` bronze" (:select_sql result)))))))

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

(deftest compile-silver-proposal-persists-compiled-status
  (let [updated (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/resolve-proposal-context (fn [_]
                                                                            {:proposal-row {:proposal_id 22
                                                                                            :target_model "silver_trip"
                                                                                            :layer "silver"}
                                                                             :proposal-json {:target_table "sheetz_telematics.silver.trip"
                                                                                             :columns [{:target_column "trip_id"
                                                                                                        :expression "bronze.trip_id"
                                                                                                        :type "STRING"
                                                                                                        :source_columns ["trip_id"]}]
                                                                                             :mappings [{:target_column "trip_id"
                                                                                                         :expression "bronze.trip_id"
                                                                                                         :source_columns ["trip_id"]}]
                                                                                             :materialization {:mode "append"
                                                                                                               :keys []}}
                                                                             :profile-json {:field_types {"trip_id" {:type "STRING"}}}
                                                                             :source-table "sheetz_telematics.bronze.trip_raw"})
                     #'bitool.modeling.automation/update-model-proposal! (fn [proposal-id attrs]
                                                                           (reset! updated [proposal-id attrs])
                                                                           nil)}
      (fn []
        (let [result (modeling/compile-silver-proposal! 22)]
          (is (= 22 (:proposal_id result)))
          (is (= 22 (first @updated)))
          (is (= "compiled" (get-in @updated [1 :status])))
          (is (string? (get-in @updated [1 :compiled_sql])))
          (is (not (str/blank? (get-in @updated [1 :compiled_sql])))))))))

(deftest compile-silver-proposal-does-not-downgrade-validated-status
  (let [updated (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/resolve-proposal-context (fn [_]
                                                                            {:proposal-row {:proposal_id 22
                                                                                            :target_model "silver_trip"
                                                                                            :layer "silver"
                                                                                            :status "validated"}
                                                                             :proposal-json {:target_table "sheetz_telematics.silver.trip"
                                                                                             :columns [{:target_column "trip_id"
                                                                                                        :expression "bronze.trip_id"
                                                                                                        :type "STRING"
                                                                                                        :source_columns ["trip_id"]}]
                                                                                             :mappings [{:target_column "trip_id"
                                                                                                         :expression "bronze.trip_id"
                                                                                                         :source_columns ["trip_id"]}]
                                                                                             :materialization {:mode "append"
                                                                                                               :keys []}}
                                                                             :profile-json {:field_types {"trip_id" {:type "STRING"}}}
                                                                             :source-table "sheetz_telematics.bronze.trip_raw"})
                     #'bitool.modeling.automation/update-model-proposal! (fn [proposal-id attrs]
                                                                           (reset! updated [proposal-id attrs])
                                                                           nil)}
      (fn []
        (modeling/compile-silver-proposal! 22)
        (is (= 22 (first @updated)))
        (is (= "validated" (get-in @updated [1 :status])))))))

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

(deftest compiler-emits-databricks-sql-for-databricks-target
  (let [result (compiler/compile-model {:target-warehouse :databricks
                                        :proposal-json {:target_table "main.silver.trip"
                                                        :columns [{:target_column "trip_id"
                                                                   :expression "bronze.`trip_id`"
                                                                   :type "STRING"
                                                                   :source_columns ["trip_id"]}
                                                                  {:target_column "distanceMiles"
                                                                   :expression "bronze.`distanceMiles`"
                                                                   :type "DOUBLE"
                                                                   :source_columns ["distanceMiles"]}]
                                                        :materialization {:mode "merge"
                                                                          :keys ["trip_id"]}}
                                        :source-table "main.bronze.trip_raw"})]
    (is (= "databricks" (:target_warehouse result)))
    (is (re-find #"FROM `main`\.`bronze`\.`trip_raw` bronze" (:select_sql result)))
    (is (re-find #"MERGE INTO `main`\.`silver`\.`trip`" (:compiled_sql result)))
    (is (re-find #"bronze\.`distanceMiles` AS `distanceMiles`" (:select_sql result)))
    (is (not (re-find #"DELETE FROM" (:compiled_sql result))))
    (is (not (re-find #"\"" (:compiled_sql result))))))

(deftest compiler-emits-databricks-update-sql-for-databricks-target
  (let [result (compiler/compile-model {:target-warehouse :databricks
                                        :proposal-json {:target_table "main.silver.trip"
                                                        :columns [{:target_column "trip_id"
                                                                   :expression "bronze.`trip_id`"
                                                                   :type "STRING"
                                                                   :source_columns ["trip_id"]}
                                                                  {:target_column "distanceMiles"
                                                                   :expression "bronze.`distanceMiles`"
                                                                   :type "DOUBLE"
                                                                   :source_columns ["distanceMiles"]}]
                                                        :materialization {:mode "update"
                                                                          :keys ["trip_id"]}}
                                        :source-table "main.bronze.trip_raw"})]
    (is (re-find #"^MERGE INTO `main`\.`silver`\.`trip` t USING" (:compiled_sql result)))
    (is (re-find #"WHEN MATCHED THEN UPDATE SET t\.`trip_id` = s\.`trip_id`, t\.`distanceMiles` = s\.`distanceMiles`" (:compiled_sql result)))
    (is (not (re-find #"WHEN NOT MATCHED THEN INSERT" (:compiled_sql result))))))

(deftest compiler-emits-databricks-update-sql-with-custom-assignments
  (let [result (compiler/compile-model {:target-warehouse :databricks
                                        :proposal-json {:target_table "main.silver.trip"
                                                        :columns [{:target_column "trip_id"
                                                                   :expression "bronze.`trip_id`"
                                                                   :type "STRING"
                                                                   :source_columns ["trip_id"]}
                                                                  {:target_column "distanceMiles"
                                                                   :expression "bronze.`distanceMiles`"
                                                                   :type "DOUBLE"
                                                                   :source_columns ["distanceMiles"]}]
                                                        :materialization {:mode "update"
                                                                          :keys ["trip_id"]
                                                                          :update_assignments [{:target_column "distanceMiles"
                                                                                                :expression "s.`distanceMiles` + 1"}]}}
                                        :source-table "main.bronze.trip_raw"})]
    (is (re-find #"WHEN MATCHED THEN UPDATE SET t\.`distanceMiles` = s\.`distanceMiles` \+ 1" (:compiled_sql result)))
    (is (not (re-find #"t\.`trip_id` = s\.`trip_id`," (:compiled_sql result))))))

(deftest compiler-emits-databricks-delete-sql-for-databricks-target
  (let [result (compiler/compile-model {:target-warehouse :databricks
                                        :proposal-json {:target_table "main.silver.trip"
                                                        :columns [{:target_column "trip_id"
                                                                   :expression "bronze.`trip_id`"
                                                                   :type "STRING"
                                                                   :source_columns ["trip_id"]}]
                                                        :materialization {:mode "delete"
                                                                          :keys ["trip_id"]}}
                                        :source-table "main.bronze.trip_raw"})]
    (is (re-find #"^DELETE FROM `main`\.`silver`\.`trip` t WHERE EXISTS" (:compiled_sql result)))
    (is (re-find #"t\.`trip_id` = s\.`trip_id`" (:compiled_sql result)))))

(deftest compiler-emits-databricks-merge-with-schema-evolution-and-not-matched-by-source-delete
  (let [result (compiler/compile-model {:target-warehouse :databricks
                                        :proposal-json {:target_table "main.silver.trip"
                                                        :columns [{:target_column "trip_id"
                                                                   :expression "bronze.`trip_id`"
                                                                   :type "STRING"
                                                                   :source_columns ["trip_id"]}
                                                                  {:target_column "distanceMiles"
                                                                   :expression "bronze.`distanceMiles`"
                                                                   :type "DOUBLE"
                                                                   :source_columns ["distanceMiles"]}]
                                                        :materialization {:mode "merge"
                                                                          :keys ["trip_id"]
                                                                          :schema_evolution true
                                                                          :matched_condition "s.`distanceMiles` >= 0"
                                                                          :insert_on_not_matched false
                                                                          :when_not_matched_by_source {:action "delete"
                                                                                                       :condition "t.`trip_id` IS NOT NULL"}}}
                                        :source-table "main.bronze.trip_raw"})]
    (is (re-find #"^MERGE WITH SCHEMA EVOLUTION INTO `main`\.`silver`\.`trip` t" (:compiled_sql result)))
    (is (re-find #"WHEN MATCHED AND s\.`distanceMiles` >= 0 THEN UPDATE SET" (:compiled_sql result)))
    (is (not (re-find #"WHEN NOT MATCHED THEN INSERT" (:compiled_sql result))))
    (is (re-find #"WHEN NOT MATCHED BY SOURCE AND t\.`trip_id` IS NOT NULL THEN DELETE" (:compiled_sql result)))))

(deftest compiler-emits-databricks-append-sql
  (let [result (compiler/compile-model {:target-warehouse :databricks
                                        :proposal-json {:target_table "main.silver.trip"
                                                        :columns [{:target_column "trip_id"
                                                                   :expression "bronze.`trip_id`"
                                                                   :type "STRING"
                                                                   :source_columns ["trip_id"]}]
                                                        :materialization {:mode "append"}}
                                        :source-table "main.bronze.trip_raw"})]
    (is (re-find #"^INSERT INTO `main`\.`silver`\.`trip` \(`trip_id`\) SELECT" (:compiled_sql result)))))

(deftest compiler-emits-databricks-table-replace-sql
  (let [result (compiler/compile-model {:target-warehouse :databricks
                                        :proposal-json {:target_table "main.silver.trip"
                                                        :columns [{:target_column "trip_id"
                                                                   :expression "bronze.`trip_id`"
                                                                   :type "STRING"
                                                                   :source_columns ["trip_id"]}]
                                                        :materialization {:mode "table_replace"}}
                                        :source-table "main.bronze.trip_raw"})]
    (is (re-find #"^CREATE OR REPLACE TABLE `main`\.`silver`\.`trip` AS SELECT" (:compiled_sql result)))))

(deftest databricks-compiler-rejects-empty-merge-keys
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"requires at least one merge key"
                        (databricks/compile-materialization-sql
                         {:select [{:target_column "trip_id"
                                    :expression "s.`trip_id`"}]
                          :materialization {:mode "merge"
                                            :target "main.silver.trip"
                                            :keys []}}
                         "SELECT 1 AS `trip_id`"))))

(deftest databricks-compiler-rejects-unsafe-condition-fragments
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"Unsafe Databricks matched condition"
                        (databricks/compile-materialization-sql
                         {:select [{:target_column "trip_id"
                                    :expression "s.`trip_id`"}]
                          :materialization {:mode "merge"
                                            :target "main.silver.trip"
                                            :keys ["trip_id"]
                                            :matched_condition "s.`trip_id` = 1 OR 1 = 1"}}
                         "SELECT 1 AS `trip_id`"))))

(deftest databricks-compiler-rejects-unsafe-assignment-expressions
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"Unsafe Databricks matched assignment expression"
                        (databricks/compile-materialization-sql
                         {:select [{:target_column "trip_id"
                                    :expression "s.`trip_id`"}]
                          :materialization {:mode "update"
                                            :target "main.silver.trip"
                                            :keys ["trip_id"]
                                            :update_assignments [{:target_column "trip_id"
                                                                  :expression "dangerous_udf(s.`trip_id`)"}]}}
                         "SELECT 1 AS `trip_id`"))))

(deftest compiler-emits-bigquery-sql-for-bigquery-target
  (let [result (compiler/compile-model {:target-warehouse :bigquery
                                        :proposal-json {:target_table "demo_project.silver.trip"
                                                        :columns [{:target_column "trip_id"
                                                                   :expression "bronze.`trip_id`"
                                                                   :type "STRING"
                                                                   :source_columns ["trip_id"]}
                                                                  {:target_column "distanceMiles"
                                                                   :expression "bronze.`distanceMiles`"
                                                                   :type "DOUBLE"
                                                                   :source_columns ["distanceMiles"]}]
                                                        :materialization {:mode "merge"
                                                                          :keys ["trip_id"]}}
                                        :source-table "demo_project.bronze.trip_raw"})]
    (is (= "bigquery" (:target_warehouse result)))
    (is (re-find #"FROM `demo_project`\.`bronze`\.`trip_raw` bronze" (:select_sql result)))
    (is (re-find #"MERGE INTO `demo_project`\.`silver`\.`trip`" (:compiled_sql result)))
    (is (re-find #"bronze\.`distanceMiles` AS `distanceMiles`" (:select_sql result)))))

(deftest compiler-emits-bigquery-physical-layout-for-table-replace
  (let [result (compiler/compile-model {:target-warehouse :bigquery
                                        :proposal-json {:target_table "demo_project.gold.trip_daily"
                                                        :columns [{:target_column "event_date"
                                                                   :expression "DATE(bronze.`event_time`)"
                                                                   :type "DATE"
                                                                   :source_columns ["event_time"]}
                                                                  {:target_column "region"
                                                                   :expression "bronze.`region`"
                                                                   :type "STRING"
                                                                   :source_columns ["region"]}
                                                                  {:target_column "trip_count"
                                                                   :expression "COUNT(*)"
                                                                   :type "INTEGER"}]
                                                        :group_by ["event_date" "region"]
                                                        :materialization {:mode "table_replace"
                                                                          :partition_by "event_date"
                                                                          :cluster_by ["region"]}}
                                        :source-table "demo_project.silver.trip"})]
    (is (re-find #"CREATE OR REPLACE TABLE `demo_project`\.`gold`\.`trip_daily` PARTITION BY `event_date` CLUSTER BY `region` AS" (:compiled_sql result)))
    (is (= "event_date" (get-in result [:sql_ir :materialization :partition_by])))
    (is (= ["region"] (get-in result [:sql_ir :materialization :cluster_by])))))

(deftest compiler-emits-snowflake-clustering-for-table-replace
  (let [result (compiler/compile-model {:target-warehouse :snowflake
                                        :proposal-json {:target_table "sheetz_telematics.gold.trip_daily"
                                                        :columns [{:target_column "event_date"
                                                                   :expression "CAST(silver.event_time AS DATE)"
                                                                   :type "DATE"}
                                                                  {:target_column "region"
                                                                   :expression "silver.region"
                                                                   :type "STRING"}]
                                                        :materialization {:mode "table_replace"
                                                                          :cluster_by ["region"]}}
                                        :source-table "sheetz_telematics.silver.trip"})]
    (is (re-find #"CREATE OR REPLACE TABLE \"sheetz_telematics\"\.\"gold\"\.\"trip_daily\" CLUSTER BY \(\"region\"\) AS" (:compiled_sql result)))
    (is (= ["region"] (get-in result [:sql_ir :materialization :cluster_by])))))

(deftest compiler-emits-postgresql-sql-for-postgresql-target
  (let [result (compiler/compile-model {:target-warehouse :postgresql
                                        :proposal-json {:target_table "bitool.public.silver_trip"
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
                                        :source-table "bitool.public.trip_raw"})]
    (is (= "postgresql" (:target_warehouse result)))
    (is (re-find #"FROM \"bitool\"\.\"public\"\.\"trip_raw\" bronze" (:select_sql result)))
    (is (re-find #"WITH source_rows AS" (:compiled_sql result)))
    (is (re-find #"DELETE FROM \"bitool\"\.\"public\"\.\"silver_trip\" t USING source_rows s" (:compiled_sql result)))
    (is (re-find #"CREATE TABLE IF NOT EXISTS \"bitool\"\.\"public\"\.\"silver_trip\"" (:ddl_sql result)))
    (is (not (re-find #"`" (:compiled_sql result))))))

(deftest compiler-emits-ranked-postgresql-sql-for-processing-policy
  (let [result (compiler/compile-model {:target-warehouse :postgresql
                                        :proposal-json {:target_table "bitool.public.silver_trip"
                                                        :columns [{:target_column "trip_id"
                                                                   :expression "bronze.trip_id"
                                                                   :type "STRING"
                                                                   :source_columns ["trip_id"]}
                                                                  {:target_column "event_time"
                                                                   :expression "bronze.event_time"
                                                                   :type "TIMESTAMP"
                                                                   :source_columns ["event_time"]}
                                                                  {:target_column "distance"
                                                                   :expression "bronze.distance"
                                                                   :type "DOUBLE"
                                                                   :source_columns ["distance"]}]
                                                        :processing_policy {:business_keys ["trip_id"]
                                                                            :event_time_column "event_time"
                                                                            :ordering_strategy "latest_event_time_wins"
                                                                            :reprocess_window {:value 24
                                                                                               :unit "hours"}}
                                                        :materialization {:mode "merge"
                                                                          :keys ["trip_id"]}}
                                        :source-table "bitool.public.trip_raw"})]
    (is (= ["trip_id"] (get-in result [:sql_ir :processing_policy :business_keys])))
    (is (re-find #"ROW_NUMBER\(\) OVER \(PARTITION BY s\.\"trip_id\" ORDER BY s\.\"event_time\" DESC NULLS LAST, md5\(row_to_json\(s\)::text\) DESC\)" (:select_sql result)))
    (is (re-find #"WHERE s\.\"event_time\" >= NOW\(\) - INTERVAL '24 hours'" (:select_sql result)))
    (is (re-find #"WITH source_rows AS \(WITH source_rows AS" (:compiled_sql result)))))

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
          (is (re-find #"MERGE INTO `sheetz_telematics`\.`silver`\.`trip`" (:compiled_sql result)))
          (is (string? (get-in @captured-validation [:validation-json :proposal_checksum])))
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

(deftest publish-proposal-tx-creates-release-and-artifact
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
        (let [result (#'bitool.modeling.automation/publish-proposal-tx!
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

(deftest validate-silver-proposal-warehouse-uses-bigquery-sql-path
  (let [captured-validation (atom nil)
        captured-sql (atom nil)]
    (with-redefs [bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                  bitool.modeling.automation/proposal-by-id (fn [_] {:proposal_id 22
                                                                     :layer "silver"
                                                                     :target_model "silver_trip"})
                  bitool.modeling.automation/synthesize-silver-graph! (fn [_ _]
                                                                        {:graph_artifact_id 77
                                                                         :graph_id 88
                                                                         :graph_version 3})
                  bitool.modeling.automation/compile-silver-graph-artifact! (fn [_]
                                                                              {:sql_ir {:materialization {:mode "merge"}}
                                                                               :select_sql "SELECT bronze.`trip_id` AS `trip_id` FROM `demo_project`.`bronze`.`trip_raw` bronze"
                                                                               :compiled_sql "MERGE INTO `demo_project`.`silver`.`trip` t USING (SELECT bronze.`trip_id` AS `trip_id` FROM `demo_project`.`bronze`.`trip_raw` bronze) s ON t.`trip_id` = s.`trip_id` WHEN MATCHED THEN UPDATE SET t.`trip_id` = s.`trip_id` WHEN NOT MATCHED THEN INSERT (`trip_id`) VALUES (s.`trip_id`)"})
                  db/getGraph (fn [_] {:n {9 {:na {:btype "Tg"}}}})
                  bitool.modeling.automation/target-node-id-from-graph-artifact (fn [_ _] 9)
                  g2/getData (fn [_ _] {:target_kind "bigquery"
                                        :connection_id 42
                                        :catalog "demo_project"
                                        :schema "silver"
                                        :table_name "trip"})
                  db/create-dbspec-from-id (fn [_]
                                             {:dbtype "bigquery"
                                              :project-id "demo-project"
                                              :dataset "analytics"
                                              :location "US"
                                              :token "{\"type\":\"service_account\"}"})
                  bitool.bigquery/dry-run-sql! (fn [_ sql]
                                                (reset! captured-sql sql)
                                                {:jobReference {:jobId "job-1"}
                                                 :estimated_bytes_processed 2048
                                                 :totalBytesProcessed "2048"})
                  bitool.modeling.automation/persist-model-validation! (fn [payload]
                                                                         (reset! captured-validation payload)
                                                                         {:validation_id 701})]
      (let [result (modeling/validate-silver-proposal-warehouse! 22 {:created_by "alice"})]
        (is (= "valid" (:status result)))
        (is (= "bigquery_sql" (:backend result)))
        (is (= "silver_warehouse_sql" (:validation-kind @captured-validation)))
        (is (= 2048 (get-in result [:validation :estimated_bytes_processed])))
        (is (re-find #"^MERGE INTO `demo_project`\.`silver`\.`trip`" @captured-sql))))))

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

(deftest update-silver-proposal-deep-merges-processing-policy
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
                                                                    :proposal_json "{\"target_model\":\"silver_trip\",\"materialization\":{\"mode\":\"merge\",\"keys\":[\"trip_id\"]},\"processing_policy\":{\"business_keys\":[\"trip_id\"],\"event_time_column\":\"event_time\",\"ordering_strategy\":\"latest_event_time_wins\"}}"})
                     #'bitool.modeling.automation/update-model-proposal! (fn [proposal-id attrs]
                                                                           (reset! captured-update [proposal-id attrs])
                                                                           nil)
                     #'bitool.modeling.automation/latest-validation-for-proposal (fn [_] nil)
                     #'bitool.modeling.automation/latest-active-release (fn [& _] nil)
                     #'bitool.modeling.automation/latest-graph-artifact-for-proposal (fn [& _] nil)}
      (fn []
        (modeling/update-silver-proposal! 22 {:proposal {:processing_policy {:reprocess_window {:value 24
                                                                                                :unit "hours"}}}
                                              :created_by "alice"})
        (is (= ["trip_id"] (get-in @captured-update [1 :proposal_json :processing_policy :business_keys])))
        (is (= "event_time" (get-in @captured-update [1 :proposal_json :processing_policy :event_time_column])))
        (is (= {:value 24 :unit "hours"}
               (get-in @captured-update [1 :proposal_json :processing_policy :reprocess_window])))))))

(deftest update-silver-proposal-clones-validated-proposal-into-new-draft
  (let [captured-persist (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/proposal-by-id (fn [_]
                                                                   {:proposal_id 22
                                                                    :profile_id 11
                                                                    :tenant_key "tenant-a"
                                                                    :workspace_key "ops"
                                                                    :layer "silver"
                                                                    :status "validated"
                                                                    :target_model "silver_trip"
                                                                    :source_graph_id 99
                                                                    :source_node_id 2
                                                                    :source_endpoint_name "trips"
                                                                    :confidence_score 0.8
                                                                    :proposal_json "{\"target_model\":\"silver_trip\"}"})
                     #'bitool.modeling.automation/persist-model-proposal! (fn [proposal _]
                                                                            (reset! captured-persist proposal)
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
          (is (= "silver_trip_v2" (:target_model @captured-persist)))
          (is (= "draft" (:status result))))))))

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

(deftest build-silver-gil-preserves-snowflake-target-settings
  (let [gil (#'bitool.modeling.automation/build-silver-gil
             {:proposal-row {:proposal_id 22
                             :target_model "silver_trip"}
              :proposal-json {:target_table "analytics.silver.trip"
                              :columns [{:target_column "trip_id"
                                         :expression "bronze.trip_id"
                                         :type "STRING"
                                         :nullable false
                                         :source_columns ["trip_id"]}]
                              :mappings [{:target_column "trip_id"
                                          :expression "bronze.trip_id"
                                          :source_columns ["trip_id"]}]
                              :materialization {:mode "merge"
                                                :keys ["trip_id"]
                                                :cluster_by ["region"]}}
              :source-table "analytics.bronze.trip_raw"
              :source-system "samsara"
              :target {:target_kind "snowflake"
                       :connection_id 31
                       :catalog "analytics"
                       :sf_load_method "stage_copy"
                       :sf_stage_name "@silver_stage"
                       :sf_warehouse "COMPUTE_WH"
                       :sf_file_format "CSV"
                       :sf_on_error "CONTINUE"
                       :sf_purge true}})
        target-node (first (filter #(= "target" (:type %)) (:nodes gil)))]
    (is (= ["region"] (get-in target-node [:config :cluster_by])))
    (is (= "stage_copy" (get-in target-node [:config :sf_load_method])))
    (is (= "@silver_stage" (get-in target-node [:config :sf_stage_name])))
    (is (= "COMPUTE_WH" (get-in target-node [:config :sf_warehouse])))))

(deftest build-gold-gil-preserves-bigquery-layout-settings
  (let [gil (#'bitool.modeling.automation/build-gold-gil
             {:proposal-row {:proposal_id 29
                             :target_model "gold_trip_daily"}
              :proposal-json {:target_table "demo_project.gold.trip_daily"
                              :columns [{:target_column "event_date"
                                         :expression "DATE(silver.event_time)"
                                         :type "DATE"
                                         :nullable false
                                         :source_columns ["event_time"]}
                                        {:target_column "region"
                                         :expression "silver.region"
                                         :type "STRING"
                                         :nullable false
                                         :source_columns ["region"]}]
                              :mappings [{:target_column "event_date"
                                          :expression "DATE(silver.event_time)"
                                          :source_columns ["event_time"]}
                                         {:target_column "region"
                                          :expression "silver.region"
                                          :source_columns ["region"]}]
                              :group_by ["event_date" "region"]
                              :materialization {:mode "table_replace"
                                                :partition_by "event_date"
                                                :cluster_by ["region"]}}
              :source-table "demo_project.silver.trip"
              :source-system "samsara"
              :target {:target_kind "bigquery"
                       :connection_id 88
                       :catalog "demo_project"}})
        target-node (first (filter #(= "target" (:type %)) (:nodes gil)))]
    (is (= ["event_date"] (get-in target-node [:config :partition_columns])))
    (is (= ["region"] (get-in target-node [:config :cluster_by])))
    (is (= "table" (get-in target-node [:config :table_format])))))

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
        (is (re-find #"MERGE INTO `sheetz_telematics`\.`silver`\.`trip`" (:compiled_sql result)))
        (is (re-find #"WHERE trip_id IS NOT NULL" (:select_sql result)))
        (is (= "merge" (get-in result [:sql_ir :materialization :mode])))))))

(deftest review-silver-proposal-updates-review-metadata
  (let [updated (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/proposal-by-id (fn [_]
                                                                   {:proposal_id 22
                                                                    :layer "silver"
                                                                    :status "validated"
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

(deftest review-silver-proposal-rejects-draft-proposal
  (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                   #'bitool.modeling.automation/proposal-by-id (fn [_]
                                                                 {:proposal_id 22
                                                                  :layer "silver"
                                                                  :status "draft"
                                                                  :proposal_json "{\"target_model\":\"silver_trip\"}"})}
    (fn []
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"not ready for review"
                            (modeling/review-silver-proposal! 22 {:review_state "approved"}))))))

(deftest review-gold-proposal-persists-approved-state
  (let [updated (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/proposal-by-id (fn [_]
                                                                   {:proposal_id 29
                                                                    :layer "gold"
                                                                    :status "validated"
                                                                    :proposal_json "{\"target_model\":\"gold_trip\"}"})
                     #'bitool.modeling.automation/update-model-proposal! (fn [proposal-id attrs]
                                                                           (reset! updated [proposal-id attrs])
                                                                           nil)}
      (fn []
        (let [result (modeling/review-gold-proposal! 29 {:review_state "approved"
                                                         :review_notes "ship it"
                                                         :reviewed_by "alice"})]
          (is (= "approved" (:status result)))
          (is (= 29 (first @updated)))
          (is (= "approved" (get-in @updated [1 :status])))
          (is (= "alice" (get-in @updated [1 :proposal_json :review :reviewed_by]))))))))

(deftest review-gold-proposal-allows-changes-requested
  (let [updated (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/proposal-by-id (fn [_]
                                                                   {:proposal_id 29
                                                                    :layer "gold"
                                                                    :status "validated"
                                                                    :proposal_json "{\"target_model\":\"gold_trip\"}"})
                     #'bitool.modeling.automation/update-model-proposal! (fn [proposal-id attrs]
                                                                           (reset! updated [proposal-id attrs])
                                                                           nil)}
      (fn []
        (let [result (modeling/review-gold-proposal! 29 {:review_state "changes_requested"
                                                         :review_notes "fix grain"
                                                         :reviewed_by "alice"})]
          (is (= "changes_requested" (:status result)))
          (is (= "changes_requested" (get-in @updated [1 :status])))
          (is (= "fix grain" (get-in @updated [1 :proposal_json :review :notes]))))))))

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

(deftest validate-silver-proposal-warehouse-uses-postgresql-sql-path
  (let [captured-ddl (atom nil)
        captured-explain (atom nil)]
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
                                                                                    {:compiled_sql "WITH source_rows AS (SELECT bronze.trip_id AS \"trip_id\" FROM \"bitool\".\"public\".\"trip_raw\" bronze) INSERT INTO \"bitool\".\"public\".\"silver_trip\" (\"trip_id\") SELECT s.\"trip_id\" FROM source_rows s"
                                                                                     :sql_ir {:sources [{:alias "bronze"
                                                                                                         :relation "bitool.public.trip_raw"}]
                                                                                              :select [{:target_column "trip_id"
                                                                                                        :expression "bronze.trip_id"
                                                                                                        :type "STRING"}]
                                                                                              :materialization {:mode "append"
                                                                                                                :target "bitool.public.silver_trip"
                                                                                                                :keys []}}})
                     #'db/getGraph (fn [_]
                                     {:n {5 {:na {:btype "Tg"
                                                  :connection_id 9
                                                  :catalog "bitool"
                                                  :schema "public"
                                                  :table_name "silver_trip"
                                                  :target_kind "postgresql"}}}})
                     #'bitool.modeling.automation/graph-artifact-by-id (fn [_]
                                                                          {:graph_artifact_id 91
                                                                           :graph_id 801
                                                                           :graph_version 3
                                                                           :node_map_json "{\"target\":5}"})
                     #'db/get-opts (fn [& _] ::pg-conn)
                     #'next.jdbc/execute! (fn [conn [sql]]
                                            (cond
                                              (re-find #"CREATE TABLE IF NOT EXISTS" sql)
                                              (do (reset! captured-ddl [conn sql]) [])

                                              (re-find #"information_schema\.columns" sql)
                                              [{:column_name "trip_id"}]

                                              (re-find #"^EXPLAIN " sql)
                                              (do (reset! captured-explain [conn sql]) [{:plan "ok"}])

                                              :else (throw (ex-info "unexpected sql" {:sql sql}))))
                     #'bitool.modeling.automation/persist-model-validation! (fn [payload]
                                                                              (is (= "valid" (:status payload)))
                                                                              {:validation_id 501})}
      (fn []
        (let [result (modeling/validate-silver-proposal-warehouse! 22 {:created_by "alice"})]
          (is (= "valid" (:status result)))
          (is (= 501 (:validation_id result)))
          (is (= ::pg-conn (first @captured-ddl)))
          (is (re-find #"CREATE TABLE IF NOT EXISTS" (second @captured-ddl)))
          (is (= ::pg-conn (first @captured-explain)))
          (is (re-find #"^EXPLAIN WITH source_rows AS" (second @captured-explain))))))))

(deftest compile-silver-proposal-retargets-stale-databricks-bindings-to-current-postgresql-target
  (let [captured-compile (atom nil)
        captured-update  (atom nil)
        graph            {:n {2 {:na {:btype "Ap"
                                      :source_system "samsara"
                                      :endpoint_configs [{:endpoint_name "fleet/vehicles"
                                                          :enabled true
                                                          :bronze_table_name "main.bronze.samsara_fleet_vehicles_raw"}]}
                                  :e {3 {}}}
                              3 {:na {:btype "Tg"
                                      :target_kind "postgresql"
                                      :connection_id 473
                                      :catalog "bitool"
                                      :schema "public"
                                      :table_name "sheetz_samsara_demo_seed"}}}}]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/proposal-by-id (fn [_]
                                                                   {:proposal_id 276
                                                                    :layer "silver"
                                                                    :target_model "silver_vehicle_master"
                                                                    :profile_id 41
                                                                    :source_graph_id 2451
                                                                    :source_node_id 2
                                                                    :source_endpoint_name "fleet/vehicles"
                                                                    :proposal_json "{\"layer\":\"silver\",\"target_model\":\"silver_vehicle_master\",\"target_warehouse\":\"databricks\",\"target_table\":\"main.silver.silver_vehicle_master\",\"columns\":[{\"target_column\":\"vehicle_id\",\"type\":\"STRING\",\"nullable\":false,\"expression\":\"bronze.vehicle_id\",\"source_columns\":[\"vehicle_id\"]}],\"mappings\":[{\"target_column\":\"vehicle_id\",\"expression\":\"bronze.vehicle_id\",\"source_columns\":[\"vehicle_id\"]}],\"materialization\":{\"mode\":\"merge\",\"keys\":[\"vehicle_id\"]}}"})
                     #'bitool.modeling.automation/schema-profile-by-id (fn [_]
                                                                        {:profile_id 41
                                                                         :profile_json "{\"field_types\":{\"vehicle_id\":{\"type\":\"STRING\"}}}"})
                     #'db/getGraph (fn [_] graph)
                     #'g2/getData (fn [g nid] (get-in g [:n nid :na]))
                     #'compiler/compile-model (fn [payload]
                                                (reset! captured-compile payload)
                                                {:sql_ir {:materialization {:target (get-in payload [:proposal-json :target_table])}}
                                                 :select_sql "SELECT 1"
                                                 :compiled_sql "compiled"})
                     #'bitool.modeling.automation/update-model-proposal! (fn [_ attrs]
                                                                           (reset! captured-update attrs)
                                                                           nil)}
      (fn []
        (let [result (modeling/compile-silver-proposal! 276)]
          (is (= "postgresql" (:target-warehouse @captured-compile)))
          (is (= "public.samsara_fleet_vehicles_raw" (:source-table @captured-compile)))
          (is (= "public.silver_vehicle_master" (get-in @captured-compile [:proposal-json :target_table])))
          (is (= "postgresql" (get-in @captured-compile [:proposal-json :target_warehouse])))
          (is (= "public.silver_vehicle_master" (get-in @captured-update [:proposal_json :target_table])))
          (is (= "postgresql" (get-in @captured-update [:proposal_json :target_warehouse])))
          (is (= "compiled" (:compiled_sql result))))))))

(deftest compile-gold-proposal-retargets-stale-databricks-bindings-to-current-postgresql-target
  (let [captured-compile (atom nil)
        captured-update  (atom nil)
        graph            {:n {2 {:na {:btype "Ap"
                                      :source_system "samsara"}
                                  :e {3 {}}}
                              3 {:na {:btype "Tg"
                                      :target_kind "postgresql"
                                      :connection_id 473
                                      :catalog "bitool"
                                      :schema "public"
                                      :table_name "sheetz_samsara_demo_seed"}}}}]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/proposal-by-id (fn [_]
                                                                   {:proposal_id 278
                                                                    :layer "gold"
                                                                    :target_model "gold_fleet_utilization_daily"
                                                                    :profile_id 51
                                                                    :source_graph_id 2451
                                                                    :source_node_id 2
                                                                    :proposal_json "{\"layer\":\"gold\",\"target_model\":\"gold_fleet_utilization_daily\",\"target_warehouse\":\"databricks\",\"target_table\":\"main.gold.gold_fleet_utilization_daily\",\"source_model\":\"silver_vehicle_master\",\"source_table\":\"main.silver.silver_vehicle_master\",\"source_alias\":\"silver\",\"columns\":[{\"target_column\":\"vehicle_id\",\"type\":\"STRING\",\"nullable\":false,\"expression\":\"silver.vehicle_id\",\"source_columns\":[\"vehicle_id\"]}],\"mappings\":[{\"target_column\":\"vehicle_id\",\"expression\":\"silver.vehicle_id\",\"source_columns\":[\"vehicle_id\"]}],\"group_by\":[\"vehicle_id\"],\"materialization\":{\"mode\":\"merge\",\"keys\":[\"vehicle_id\"]}}"})
                     #'bitool.modeling.automation/schema-profile-by-id (fn [_]
                                                                        {:profile_id 51
                                                                         :profile_json "{\"field_types\":{\"vehicle_id\":{\"type\":\"STRING\"}}}"})
                     #'db/getGraph (fn [_] graph)
                     #'g2/getData (fn [g nid] (get-in g [:n nid :na]))
                     #'compiler/compile-model (fn [payload]
                                                (reset! captured-compile payload)
                                                {:sql_ir {:materialization {:target (get-in payload [:proposal-json :target_table])}}
                                                 :select_sql "SELECT 1"
                                                 :compiled_sql "compiled"})
                     #'bitool.modeling.automation/update-model-proposal! (fn [_ attrs]
                                                                           (reset! captured-update attrs)
                                                                           nil)}
      (fn []
        (let [result (modeling/compile-gold-proposal! 278)]
          (is (= "postgresql" (:target-warehouse @captured-compile)))
          (is (= "public.silver_vehicle_master" (:source-table @captured-compile)))
          (is (= "public.gold_fleet_utilization_daily" (get-in @captured-compile [:proposal-json :target_table])))
          (is (= "public.silver_vehicle_master" (get-in @captured-compile [:proposal-json :source_table])))
          (is (= "public.gold_fleet_utilization_daily" (get-in @captured-update [:proposal_json :target_table])))
          (is (= "public.silver_vehicle_master" (get-in @captured-update [:proposal_json :source_table])))
          (is (= "compiled" (:compiled_sql result))))))))

(deftest refresh-proposals-for-graph-target-updates-stale-bindings
  (let [updates (atom [])]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/proposal-rows-for-graph (fn [_]
                                                                            [{:proposal_id 276
                                                                              :layer "silver"
                                                                              :status "validated"
                                                                              :target_model "silver_vehicle_master"
                                                                              :proposal_json "{\"target_table\":\"main.silver.silver_vehicle_master\",\"target_warehouse\":\"databricks\"}"}
                                                                             {:proposal_id 278
                                                                              :layer "gold"
                                                                              :status "draft"
                                                                              :target_model "gold_fleet_utilization_daily"
                                                                              :proposal_json "{\"target_table\":\"main.gold.gold_fleet_utilization_daily\",\"source_table\":\"main.silver.silver_vehicle_master\",\"target_warehouse\":\"databricks\"}"}])
                     #'bitool.modeling.automation/resolve-proposal-context (fn [_]
                                                                             {:proposal-json {:target_table "public.silver_vehicle_master"
                                                                                              :target_warehouse "postgresql"}})
                     #'bitool.modeling.automation/resolve-gold-proposal-context (fn [_]
                                                                                  {:proposal-json {:target_table "public.gold_fleet_utilization_daily"
                                                                                                   :source_table "public.silver_vehicle_master"
                                                                                                   :target_warehouse "postgresql"}})
                     #'bitool.modeling.automation/update-model-proposal! (fn [proposal-id attrs]
                                                                           (swap! updates conj [proposal-id attrs])
                                                                           nil)}
      (fn []
        (let [result (modeling/refresh-proposals-for-graph-target! 2451 {:updated_by "alice"})]
          (is (= 2 (count (:refreshed result))))
          (is (= [276 {:proposal_json {:target_table "public.silver_vehicle_master"
                                       :target_warehouse "postgresql"}
                       :compiled_sql nil
                       :status "draft"}]
                 (first @updates)))
          (is (= [278 {:proposal_json {:target_table "public.gold_fleet_utilization_daily"
                                       :source_table "public.silver_vehicle_master"
                                       :target_warehouse "postgresql"}
                       :compiled_sql nil
                       :status "draft"}]
                 (second @updates))))))))

(deftest refresh-proposals-for-graph-target-preserves-published-status
  (let [updates (atom [])]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/proposal-rows-for-graph (fn [_]
                                                                            [{:proposal_id 341
                                                                              :layer "silver"
                                                                              :status "published"
                                                                              :target_model "silver_vehicle_master"
                                                                              :proposal_json "{\"target_table\":\"main.silver.silver_vehicle_master\",\"target_warehouse\":\"databricks\"}"}])
                     #'bitool.modeling.automation/resolve-proposal-context (fn [_]
                                                                             {:proposal-json {:target_table "public.silver_vehicle_master"
                                                                                              :target_warehouse "postgresql"}})
                     #'bitool.modeling.automation/update-model-proposal! (fn [proposal-id attrs]
                                                                           (swap! updates conj [proposal-id attrs])
                                                                           nil)}
      (fn []
        (let [result (modeling/refresh-proposals-for-graph-target! 2525 {:updated_by "alice"})]
          (is (= 1 (count (:refreshed result))))
          (is (= [[341 {:proposal_json {:target_table "public.silver_vehicle_master"
                                        :target_warehouse "postgresql"}
                        :compiled_sql nil}]]
                 @updates)))))))

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

(deftest execute-silver-release-runs-postgresql-sql-and-persists-run
  (let [captured-complete (atom nil)
        captured-exec (atom nil)]
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
                                                                                 :warehouse "postgresql"
                                                                                 :sql_ir {:materialization {:target "bitool.public.silver_trip"}}
                                                                                 :compiled_sql "INSERT INTO \"bitool\".\"public\".\"silver_trip\" (\"trip_id\") SELECT bronze.trip_id AS \"trip_id\" FROM \"bitool\".\"public\".\"trip_raw\" bronze"
                                                                                 :params {}
                                                                                 :status "pending"})
                     #'bitool.modeling.automation/release-by-id (fn [_]
                                                                  {:release_id 700
                                                                   :layer "silver"
                                                                   :graph_artifact_id 91
                                                                   :target_model "silver_trip"})
                     #'bitool.modeling.automation/execute-postgresql-materialization! (fn [conn-id sql-ir compiled-sql]
                                                                                        (reset! captured-exec [conn-id sql-ir compiled-sql])
                                                                                        [{:update-count 8}])
                     #'bitool.modeling.automation/complete-model-run! (fn [model-run-id payload]
                                                                        (reset! captured-complete [model-run-id payload])
                                                                        nil)}
      (fn []
        (let [result (modeling/execute-silver-release! 700 {:created_by "alice"})]
          (is (= 501 (:model_run_id result)))
          (is (= "succeeded" (:status result)))
          (is (= "postgresql_sql" (:backend result)))
          (is (= 9 (first @captured-exec)))
          (is (= {:materialization {:target "bitool.public.silver_trip"}} (second @captured-exec)))
          (is (re-find #"INSERT INTO \"bitool\"\.\"public\"\.\"silver_trip\"" (nth @captured-exec 2)))
          (is (= 501 (first @captured-complete)))
          (is (= "succeeded" (get-in @captured-complete [1 :status]))))))))

(deftest execute-silver-release-runs-bigquery-synchronously
  (let [captured-complete (atom nil)
        captured-exec (atom nil)]
    (with-redefs [bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                  bitool.modeling.automation/execute-silver-release-tx! (fn [_ release-row created-by]
                                                                           (is (= 700 (:release_id release-row)))
                                                                           (is (= "alice" created-by))
                                                                           {:model_run_id 501
                                                                            :release_id 700
                                                                            :graph_artifact_id 91
                                                                            :graph_id 801
                                                                            :graph_version 3
                                                                            :conn_id 9
                                                                            :warehouse "bigquery"
                                                                            :compiled_sql "MERGE INTO `demo_project`.`silver`.`trip` t USING (SELECT 1 AS `trip_id`) s ON t.`trip_id` = s.`trip_id` WHEN MATCHED THEN UPDATE SET t.`trip_id` = s.`trip_id` WHEN NOT MATCHED THEN INSERT (`trip_id`) VALUES (s.`trip_id`)"
                                                                            :status "pending"})
                  bitool.modeling.automation/release-by-id (fn [_]
                                                             {:release_id 700
                                                              :layer "silver"
                                                              :graph_artifact_id 91
                                                              :target_model "silver_trip"})
                  db/create-dbspec-from-id (fn [_]
                                             {:dbtype "bigquery"
                                              :project-id "demo-project"
                                              :dataset "analytics"
                                              :location "US"
                                              :token "{\"type\":\"service_account\"}"})
                  bitool.bigquery/execute-sql! (fn [_ sql]
                                                (reset! captured-exec sql)
                                                {:rows []
                                                 :update-count 8
                                                 :job {:job_id "job-88"}
                                                 :raw {:jobComplete true}})
                  bitool.modeling.automation/complete-model-run! (fn [model-run-id payload]
                                                                   (reset! captured-complete [model-run-id payload])
                                                                   nil)]
      (let [result (modeling/execute-silver-release! 700 {:created_by "alice"})]
        (is (= 501 (:model_run_id result)))
        (is (= "succeeded" (:status result)))
        (is (= "bigquery_sql" (:backend result)))
        (is (re-find #"^MERGE INTO `demo_project`\.`silver`\.`trip`" @captured-exec))
        (is (= 501 (first @captured-complete)))
        (is (= [{:next.jdbc/update-count 8}]
               (get-in @captured-complete [1 :response-json :result])))
        (is (= "succeeded" (get-in @captured-complete [1 :status])))))))

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

(deftest execute-queued-silver-release-times-out-stuck-databricks-polling
  (let [completion (atom nil)
        poll-count (atom 0)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/parse-int-env (fn [k default]
                                                                  (if (= k :bitool-databricks-max-polls) 2 default))
                     #'bitool.modeling.automation/release-by-id (fn [_]
                                                                  {:release_id 700
                                                                   :layer "silver"
                                                                   :graph_artifact_id 91
                                                                   :target_model "silver_trip"})
                     #'bitool.modeling.automation/execute-silver-release-tx! (fn [_ _ created-by]
                                                                                (is (= "alice" created-by))
                                                                                {:model_run_id 501
                                                                                 :release_id 700
                                                                                 :graph_artifact_id 91
                                                                                 :graph_id 801
                                                                                 :graph_version 3
                                                                                 :conn_id 9
                                                                                 :job_id "111"
                                                                                 :params {:model_release_id "700"}
                                                                                 :status "pending"})
                     #'bitool.modeling.automation/link-model-run-to-request! (fn [& _] nil)
                     #'bitool.databricks.jobs/trigger-job! (fn [_ job-id params]
                                                             (is (= "111" job-id))
                                                             (is (= "700" (:model_release_id params)))
                                                             {:job_id job-id :run_id 123})
                     #'bitool.modeling.automation/poll-silver-model-run! (fn [model-run-id]
                                                                            (is (= 501 model-run-id))
                                                                            (swap! poll-count inc)
                                                                            {:model_run_id model-run-id
                                                                             :status "running"})
                     #'bitool.modeling.automation/complete-model-run! (fn [model-run-id payload]
                                                                        (reset! completion [model-run-id payload])
                                                                        nil)}
      (fn []
        (try
          (modeling/execute-queued-silver-release! 700 {:created_by "alice"
                                                        :execution_request_id "49cbdf13-2cc5-41af-8072-2d3fa40b1cfc"})
          (is false "Expected Databricks polling timeout")
          (catch clojure.lang.ExceptionInfo e
            (is (= "transient_platform_error" (:failure_class (ex-data e))))
            (is (= 2 (:poll_count (ex-data e))))
            (is (= 501 (first @completion)))
            (is (= "failed" (get-in @completion [1 :status])))))))))

(deftest execute-queued-silver-release-links-execution-request-before-running
  (let [linked (atom nil)
        executed (atom nil)]
    (with-redefs-fn {#'bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                     #'bitool.modeling.automation/release-by-id (fn [_]
                                                                  {:release_id 700
                                                                   :layer "silver"
                                                                   :graph_artifact_id 91
                                                                   :target_model "silver_trip"})
                     #'bitool.modeling.automation/execute-silver-release-tx! (fn [_ release-row created-by]
                                                                                (is (= 700 (:release_id release-row)))
                                                                                (is (= "alice" created-by))
                                                                                {:model_run_id 501
                                                                                 :release_id 700
                                                                                 :graph_artifact_id 91
                                                                                 :graph_id 801
                                                                                 :graph_version 3
                                                                                 :warehouse "postgresql"
                                                                                 :compiled_sql "select 1"
                                                                                 :params {}
                                                                                 :status "pending"})
                     #'bitool.modeling.automation/link-model-run-to-request! (fn [tx model-run-id request-id]
                                                                                (reset! linked [tx model-run-id request-id])
                                                                                nil)
                     #'bitool.modeling.automation/execute-pending-model-run! (fn [run _poll-fn]
                                                                               (reset! executed run)
                                                                               (assoc run :status "succeeded"))}
      (fn []
        (let [result (modeling/execute-queued-silver-release! 700 {:created_by "alice"
                                                                   :execution_request_id "49cbdf13-2cc5-41af-8072-2d3fa40b1cfc"})]
          (is (= 501 (:model_run_id result)))
          (is (= "49cbdf13-2cc5-41af-8072-2d3fa40b1cfc" (nth @linked 2)))
          (is (= 501 (second @linked)))
          (is (= 501 (:model_run_id @executed))))))))

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
                     #'db/getGraph (fn [_] {:a {:id 99 :v 1}})
                     #'g2/getData (fn [_ _] {:btype "Ap" :source_system "samara"})
                     #'bitool.modeling.automation/find-downstream-target (fn [_ _] {:target_kind "postgresql"
                                                                                    :connection_id 9})
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
          (is (= "SUM(silver.\"distance\")" (get-in result [:proposal :columns 2 :expression])))
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

(deftest execute-gold-release-runs-bigquery-synchronously
  (let [completed (atom nil)
        captured-exec (atom nil)]
    (with-redefs [bitool.modeling.automation/ensure-modeling-tables! (fn [] true)
                  bitool.modeling.automation/release-by-id (fn [_]
                                                             {:release_id 191
                                                              :layer "gold"
                                                              :graph_artifact_id 901})
                  bitool.modeling.automation/execute-gold-release-tx! (fn [_ _ _]
                                                                        {:model_run_id 602
                                                                         :release_id 191
                                                                         :graph_artifact_id 901
                                                                         :graph_id 77
                                                                         :graph_version 2
                                                                         :conn_id 31
                                                                         :warehouse "bigquery"
                                                                         :compiled_sql "MERGE INTO `demo_project`.`gold`.`trip_daily` t USING (SELECT 1 AS `trip_id`) s ON t.`trip_id` = s.`trip_id` WHEN MATCHED THEN UPDATE SET t.`trip_id` = s.`trip_id` WHEN NOT MATCHED THEN INSERT (`trip_id`) VALUES (s.`trip_id`)"
                                                                         :params {}
                                                                         :status "pending"})
                  db/create-dbspec-from-id (fn [_]
                                             {:dbtype "bigquery"
                                              :project-id "demo-project"
                                              :dataset "analytics"
                                              :location "US"
                                              :token "{\"type\":\"service_account\"}"})
                  bitool.bigquery/execute-sql! (fn [_ sql]
                                                (reset! captured-exec sql)
                                                {:rows []
                                                 :update-count 12
                                                 :job {:job_id "job-99"}
                                                 :raw {:jobComplete true}})
                  bitool.modeling.automation/complete-model-run! (fn [model-run-id payload]
                                                                   (reset! completed [model-run-id payload]))]
      (let [result (modeling/execute-gold-release! 191 {:created_by "alice"})]
        (is (= 602 (:model_run_id result)))
        (is (= "bigquery_sql" (:backend result)))
        (is (= "succeeded" (:status result)))
        (is (re-find #"^MERGE INTO `demo_project`\.`gold`\.`trip_daily`" @captured-exec))
        (is (= 602 (first @completed)))
        (is (= [{:next.jdbc/update-count 12}]
               (get-in @completed [1 :response-json :result])))))))
