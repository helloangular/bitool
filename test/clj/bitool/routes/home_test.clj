(ns bitool.routes.home-test
  (:require [bitool.db :as db]
            [bitool.config :as config]
            [bitool.control-plane :as control-plane]
            [bitool.endpoint :as endpoint]
            [bitool.graph2 :as g2]
            [bitool.ingest.databricks-control-plane :as dbx-control]
	            [bitool.ingest.execution :as ingest-execution]
	            [bitool.ingest.runtime :as ingest-runtime]
	            [bitool.modeling.automation :as modeling-automation]
	            [bitool.operations :as operations]
	            [bitool.routes.home :as home]
            [bitool.ai.assistant :as ai-assistant]
            [cheshire.core :as json]
            [clojure.walk :as walk]
            [clojure.test :refer :all]
            [reitit.ring :as ring]
            [ring.mock.request :as mock]))

(defn- test-handler []
  (ring/ring-handler
    (ring/router
      [(home/home-routes)])))

(defn- response-body->map [response]
  (let [body (:body response)
        body-str (cond
                   (or (map? body) (vector? body)) body
                   (string? body) body
                   (instance? java.io.InputStream body) (slurp body)
                   :else (str body))]
    (walk/keywordize-keys
     (if (string? body-str)
       (json/parse-string body-str true)
       body-str))))

(deftest run-api-ingestion-route-returns-success-response
  (let [captured-call (atom nil)]
    (with-redefs [ingest-execution/enqueue-api-request! (fn [gid node-id opts]
                                                          (reset! captured-call [gid node-id opts])
                                                          {:created? true
                                                           :request_id "req-1"
                                                           :run_id "run-1"
                                                           :graph_id gid
                                                           :node_id node-id
                                                           :status "queued"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/runApiIngestion")
                             :params {:gid "99"
                                      :id "2"
                                      :endpoint_name "trips"}
                             :session {}))
            body     (response-body->map response)]
        (is (= 202 (:status response)))
        (is (= "/executionRuns/run-1" (get-in response [:headers "Location"])))
        (is (= [99 2 {:endpoint-name "trips" :trigger-type "manual"}] @captured-call))
        (is (= "queued" (:status body)))))))

(deftest list-models-route-returns-latest-models
  (with-redefs [db/list-models (fn []
                                 [{:id 2434 :version 6 :name "API Draft 2434"}
                                  {:id 2433 :version 3 :name "API Draft 2433"}])]
    (let [response ((test-handler) (mock/request :get "/listModels"))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= 2 (count body)))
      (is (= 2434 (:id (first body))))
      (is (= "API Draft 2434" (:name (first body)))))))

(deftest graph-route-loads-model-and-sets-session
  (let [graph {:a {:id 2434 :v 6 :name "API Draft 2434"}
               :n {1 {:na {:name "O" :btype "O" :tcols {}} :e {}}
                   2 {:na {:name "api" :btype "Ap" :x 10 :y 20} :e {1 {}}}}}]
    (with-redefs [db/getGraph (fn [_] graph)]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/graph")
                             :params {:gid "2434"}
                             :session {:user "demo"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (sequential? body))
        (is (= 2434 (get-in response [:session :gid])))
        (is (= 6 (get-in response [:session :ver])))
        (is (= "demo" (get-in response [:session :user])))))))

(deftest get-fx-from-btype-dispatches-kafka-and-file-nodes
  (is (= g2/get-kafka-source-item (home/get-fx-from-btype "Kf")))
  (is (= g2/get-file-source-item (home/get-fx-from-btype "Fs"))))

(deftest run-api-ingestion-route-returns-bad-request-for-bad-endpoint-name
  (with-redefs [ingest-execution/enqueue-api-request! (fn [_ _ _]
                                                        (throw (ex-info "No enabled endpoint config found for endpoint_name 'missing'"
                                                                        {:endpoint_name "missing"})))]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/runApiIngestion")
                           :params {:gid "99"
                                    :id "2"
                                    :endpoint_name "missing"}
                           :session {}))
          body     (response-body->map response)]
      (is (= 400 (:status response)))
      (is (= "No enabled endpoint config found for endpoint_name 'missing'" (:error body)))
      (is (= "missing" (get-in body [:data :endpoint_name]))))))

(deftest run-kafka-ingestion-route-returns-success-response
  (let [captured-call (atom nil)]
    (with-redefs [ingest-execution/enqueue-kafka-request! (fn [gid node-id opts]
                                                            (reset! captured-call [gid node-id opts])
                                                            {:created? true
                                                             :request_id "req-k1"
                                                             :run_id "run-k1"
                                                             :graph_id gid
                                                             :node_id node-id
                                                             :status "queued"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/runKafkaIngestion")
                             :params {:gid "99"
                                      :id "2"
                                      :endpoint_name "orders.events"}
                             :session {}))
            body     (response-body->map response)]
        (is (= 202 (:status response)))
        (is (= [99 2 {:endpoint-name "orders.events" :trigger-type "manual"}] @captured-call))
        (is (= "queued" (:status body)))))))

(deftest run-file-ingestion-route-returns-success-response
  (let [captured-call (atom nil)]
    (with-redefs [ingest-execution/enqueue-file-request! (fn [gid node-id opts]
                                                           (reset! captured-call [gid node-id opts])
                                                           {:created? true
                                                            :request_id "req-f1"
                                                            :run_id "run-f1"
                                                            :graph_id gid
                                                            :node_id node-id
                                                            :status "queued"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/runFileIngestion")
                             :params {:gid "99"
                                      :id "2"
                                      :endpoint_name "orders.jsonl"}
                             :session {}))
            body     (response-body->map response)]
        (is (= 202 (:status response)))
        (is (= [99 2 {:endpoint-name "orders.jsonl" :trigger-type "manual"}] @captured-call))
        (is (= "queued" (:status body)))))))

(deftest test-db-connection-route-persists-connection-name-for-validation
  (let [captured-insert (atom nil)
        captured-test (atom nil)]
    (with-redefs [db/insert-data (fn [_table conn-params]
                                   (reset! captured-insert conn-params)
                                   {:connection/id 472})
                  db/test-connection (fn [conn-id]
                                       (reset! captured-test conn-id)
                                       true)]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/testDbConnection")
                             :params {:connection_name "Local PG"
                                      :dbtype "postgresql"
                                      :host "localhost"
                                      :port "5432"
                                      :dbname "bitool"
                                      :schema "public"
                                      :username "postgres"
                                      :password "postgres"}))
            body     (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= "Local PG" (:connection_name @captured-insert)))
        (is (= 5432 (:port @captured-insert)))
        (is (= 472 @captured-test))
        (is (= "ok" (:status body)))))))

(deftest test-db-connection-route-persists-bigquery-native-fields
  (let [captured-insert (atom nil)]
    (with-redefs [db/insert-data (fn [_table conn-params]
                                   (reset! captured-insert conn-params)
                                   {:connection/id 611})
                  db/test-connection (fn [_] true)]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/testDbConnection")
                             :params {:connection_name "BQ"
                                      :dbtype "bigquery"
                                      :host "demo-project"
                                      :dbname "analytics"
                                      :schema "US"
                                      :token "{\"type\":\"service_account\"}"}))
            body     (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= "bigquery" (:dbtype @captured-insert)))
        (is (= "demo-project" (:host @captured-insert)))
        (is (= "analytics" (:dbname @captured-insert)))
        (is (= "US" (:schema @captured-insert)))
        (is (= "{\"type\":\"service_account\"}" (:token @captured-insert)))
        (is (= "ok" (:status body)))))))

(deftest bronze-source-batches-route-returns-batches
  (let [captured-call (atom nil)]
    (with-redefs [ingest-runtime/list-bronze-source-batches (fn [graph-id node-id opts]
                                                              (reset! captured-call [graph-id node-id opts])
                                                              {:batch_count 1
                                                               :batches [{:batch_id "b1"}]})]
      (let [response ((test-handler)
                      (assoc (mock/request :get "/bronzeSourceBatches")
                             :params {:gid "99"
                                      :source_node_id "2"
                                      :source_kind "kafka"
                                      :endpoint_name "orders.events"}))
            body     (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [99 2 {:source-kind :kafka
                      :endpoint-name "orders.events"
                      :run-id nil
                      :status nil
                      :active-only nil
                      :replayable-only nil
                      :archived-only nil
                      :limit nil}]
               @captured-call))
        (is (= 1 (:batch_count body)))))))

(deftest bronze-source-observability-summary-route-returns-summary
  (let [captured-call (atom nil)]
    (with-redefs [ingest-runtime/bronze-source-observability-summary (fn [graph-id node-id opts]
                                                                       (reset! captured-call [graph-id node-id opts])
                                                                       {:alerts []
                                                                        :endpoint_name "orders.events"})]
      (let [response ((test-handler)
                      (assoc (mock/request :get "/bronzeSourceObservabilitySummary")
                             :params {:gid "99"
                                      :source_node_id "2"
                                      :source_kind "kafka"
                                      :endpoint_name "orders.events"}))
            body     (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [99 2 {:source-kind :kafka
                      :endpoint-name "orders.events"}]
               @captured-call))
        (is (= "orders.events" (:endpoint_name body)))))))

(deftest preview-copybook-schema-route-returns-field-specs
  (let [captured-call (atom nil)]
    (with-redefs [ingest-runtime/preview-copybook-schema (fn [opts]
                                                           (reset! captured-call opts)
                                                           {:field_count 1
                                                            :field_specs [{:name "customer_id"}]})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/previewCopybookSchema")
                             :params {:copybook "05 CUSTOMER-ID PIC 9(8)."
                                      :encoding "EBCDIC"}))
            body     (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= {:copybook "05 CUSTOMER-ID PIC 9(8)."
                :encoding "EBCDIC"}
               @captured-call))
        (is (= 1 (:field_count body)))))))

(deftest preview-api-schema-inference-route-returns-inferred-fields
  (let [captured-call (atom nil)]
    (with-redefs [ingest-runtime/preview-endpoint-schema! (fn [api-node endpoint]
                                                            (reset! captured-call [api-node endpoint])
                                                            {:endpoint_name "drivers"
                                                             :http_status 200
                                                             :sampled_records 2
                                                             :inferred_fields [{:path "$.data[].id"
                                                                                :column_name "data_items_id"}]})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/previewApiSchemaInference")
                             :params {:base_url "https://api.example.com"
                                      :auth_ref {:type "bearer" :secret_ref "SAMSARA_TOKEN"}
                                      :endpoint_config {:endpoint_name "drivers"
                                                        :endpoint_url "/fleet/drivers"
                                                        :schema_mode "infer"
                                                        :pagination_strategy "none"
                                                        :primary_key_fields ["id"]}}
                             :session {}))
            body     (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= "https://api.example.com" (get-in @captured-call [0 :base_url])))
        (is (= "/fleet/drivers" (get-in @captured-call [1 :endpoint_url])))
        (is (= 2 (:sampled_records body)))
        (is (= "$.data[].id" (get-in body [:inferred_fields 0 :path])))))))

(deftest preview-api-schema-inference-route-returns-bad-request
  (with-redefs [ingest-runtime/preview-endpoint-schema! (fn [_ _]
                                                          (throw (ex-info "Schema preview request failed with HTTP 500"
                                                                          {:status 500})))]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/previewApiSchemaInference")
                           :params {:base_url "https://api.example.com"
                                    :auth_ref {}
                                    :endpoint_config {:endpoint_name "drivers"
                                                      :endpoint_url "/fleet/drivers"
                                                      :schema_mode "infer"
                                                      :pagination_strategy "none"
                                                      :primary_key_fields ["id"]}}
                           :session {}))
          body     (response-body->map response)]
      (is (= 400 (:status response)))
      (is (= "Schema preview request failed with HTTP 500" (:error body)))
      (is (= 500 (get-in body [:data :status]))))))

(deftest graph-route-loads-graph-and-updates-session
  (with-redefs [db/getGraph (fn [gid]
                              {:a {:id gid :v 7}
                               :n {1 {:na {:name "Output" :btype "O"} :e {}}}})
                home/mapCoordinates (fn [_]
                                      [{:id 1 :btype "O" :alias "Output"}])]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/graph")
                           :params {:gid "42"}
                           :session {:user "tester"}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= [{:id 1 :btype "O" :alias "Output"}] body))
      (is (= 42 (get-in response [:session :gid])))
      (is (= 7 (get-in response [:session :ver])))
      (is (= "tester" (get-in response [:session :user]))))))

(deftest propose-silver-schema-route-returns-proposal
  (let [captured-args (atom nil)]
    (with-redefs [modeling-automation/propose-silver-schema! (fn [opts]
                                                               (reset! captured-args opts)
                                                               {:proposal_id 12
                                                                :profile_id 7
                                                                :target_model "silver_trip"
                                                                :proposal {:materialization {:mode "merge"}}})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/proposeSilverSchema")
                             :params {:gid "99"
                                      :id "2"
                                      :endpoint_name "trips"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= {:graph-id 99
                :api-node-id 2
                :endpoint-name "trips"
                :created-by "alice"}
               @captured-args))
        (is (= 12 (:proposal_id body)))
        (is (= "merge" (get-in body [:proposal :materialization :mode])))))))

(deftest propose-silver-schema-route-returns-bad-request
  (with-redefs [modeling-automation/propose-silver-schema! (fn [_]
                                                             (throw (ex-info "No inferred Bronze schema is available for this endpoint"
                                                                             {:endpoint_name "trips"})))]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/proposeSilverSchema")
                           :params {:gid "99" :id "2" :endpoint_name "trips"}))
          body (response-body->map response)]
      (is (= 400 (:status response)))
      (is (= "No inferred Bronze schema is available for this endpoint" (:error body)))
      (is (= "trips" (get-in body [:data :endpoint_name]))))))

(deftest propose-silver-schema-route-returns-debug-on-unexpected-error
  (with-redefs [modeling-automation/propose-silver-schema! (fn [_]
                                                             (throw (RuntimeException. "boom")))]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/proposeSilverSchema")
                           :params {:gid "99" :id "2" :endpoint_name "trips"}))
          body (response-body->map response)]
      (is (= 500 (:status response)))
      (is (= "boom" (:error body))))))

(deftest list-silver-proposals-route-returns-proposals
  (let [captured-opts (atom nil)]
    (with-redefs [modeling-automation/list-silver-proposals (fn [opts]
                                                              (reset! captured-opts opts)
                                                              [{:proposal_id 22 :target_model "silver_trip"}])]
        (let [response ((test-handler)
                      (assoc (mock/request :get "/silverProposals")
                             :params {:gid "99" :status "draft" :limit "25"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= {:graph-id 99 :status "draft" :limit 25} @captured-opts))
        (is (= 22 (:proposal_id (first body))))))))

(deftest get-silver-proposal-route-returns-proposal
  (with-redefs [modeling-automation/get-silver-proposal (fn [proposal-id]
                                                          {:proposal_id proposal-id :target_model "silver_trip"})]
    (let [response ((test-handler)
                    (assoc (mock/request :get "/silverProposals/22")
                           :path-params {:proposal_id "22"}))
          body (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= 22 (:proposal_id body))))))

(deftest update-silver-proposal-route-returns-updated-proposal
  (let [captured-call (atom nil)]
    (with-redefs [modeling-automation/update-silver-proposal! (fn [proposal-id opts]
                                                                (reset! captured-call [proposal-id opts])
                                                                {:proposal_id proposal-id :status "draft" :target_model "silver_trip_v2"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/updateSilverProposal")
                             :params {:proposal_id "22"
                                      :proposal {:target_model "silver_trip_v2"}}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [22 {:proposal {:target_model "silver_trip_v2"} :created_by "alice"}] @captured-call))
        (is (= "silver_trip_v2" (:target_model body)))))))

(deftest compile-silver-proposal-route-returns-compiled-sql
  (let [captured-id (atom nil)]
    (with-redefs [modeling-automation/compile-silver-proposal! (fn [proposal-id]
                                                                 (reset! captured-id proposal-id)
                                                                 {:proposal_id proposal-id
                                                                  :compiled_sql "MERGE INTO silver.trip ..."
                                                                  :sql_ir {:materialization {:mode "merge"}}})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/compileSilverProposal")
                             :params {:proposal_id "22"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= 22 @captured-id))
        (is (= "MERGE INTO silver.trip ..." (:compiled_sql body)))
        (is (= "merge" (get-in body [:sql_ir :materialization :mode])))))))

(deftest synthesize-silver-graph-route-returns-graph-artifact
  (let [captured-call (atom nil)]
    (with-redefs [modeling-automation/synthesize-silver-graph! (fn [proposal-id opts]
                                                                 (reset! captured-call [proposal-id opts])
                                                                 {:proposal_id proposal-id
                                                                  :graph_artifact_id 91
                                                                  :graph_id 801
                                                                  :graph_version 3})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/synthesizeSilverGraph")
                             :params {:proposal_id "22"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [22 {:created_by "alice"}] @captured-call))
        (is (= 91 (:graph_artifact_id body)))
        (is (= 801 (:graph_id body)))))))

(deftest validate-silver-proposal-route-returns-validation
  (let [captured-call (atom nil)]
    (with-redefs [modeling-automation/validate-silver-proposal! (fn [proposal-id opts]
                                                                  (reset! captured-call [proposal-id opts])
                                                                  {:proposal_id proposal-id
                                                                   :validation_id 101
                                                                   :status "valid"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/validateSilverProposal")
                             :params {:proposal_id "22" :sample_limit "25"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [22 {:sample_limit 25 :created_by "alice"}] @captured-call))
        (is (= "valid" (:status body)))
        (is (= 101 (:validation_id body)))))))

(deftest validate-silver-proposal-warehouse-route-returns-submitted-validation
  (let [captured-call (atom nil)]
    (with-redefs [modeling-automation/validate-silver-proposal-warehouse! (fn [proposal-id opts]
                                                                            (reset! captured-call [proposal-id opts])
                                                                            {:proposal_id proposal-id
                                                                             :validation_id 201
                                                                             :status "submitted"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/validateSilverProposalWarehouse")
                             :params {:proposal_id "22"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [22 {:created_by "alice"}] @captured-call))
        (is (= "submitted" (:status body)))
        (is (= 201 (:validation_id body)))))))

(deftest review-silver-proposal-route-returns-review-state
  (let [captured-call (atom nil)]
    (with-redefs [modeling-automation/review-silver-proposal! (fn [proposal-id opts]
                                                                (reset! captured-call [proposal-id opts])
                                                                {:proposal_id proposal-id
                                                                 :status "approved"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/reviewSilverProposal")
                             :params {:proposal_id "22"
                                      :review_state "approved"
                                      :review_notes "ok"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [22 {:review_state "approved" :review_notes "ok" :reviewed_by "alice"}] @captured-call))
        (is (= "approved" (:status body)))))))

(deftest review-gold-proposal-route-returns-review-state
  (let [captured-call (atom nil)]
    (with-redefs [modeling-automation/review-gold-proposal! (fn [proposal-id opts]
                                                              (reset! captured-call [proposal-id opts])
                                                              {:proposal_id proposal-id
                                                               :status "approved"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/reviewGoldProposal")
                             :params {:proposal_id "29"
                                      :review_state "approved"
                                      :review_notes "ok"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [29 {:review_state "approved" :review_notes "ok" :reviewed_by "alice"}] @captured-call))
        (is (= "approved" (:status body)))))))

(deftest publish-silver-proposal-route-returns-release
  (let [captured-call (atom nil)]
    (with-redefs [modeling-automation/publish-silver-proposal! (fn [proposal-id opts]
                                                                 (reset! captured-call [proposal-id opts])
                                                                 {:proposal_id proposal-id
                                                                  :release_id 700
                                                                  :artifact_id 800
                                                                  :status "published"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/publishSilverProposal")
                             :params {:proposal_id "22"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [22 {:sample_limit nil :created_by "alice"}] @captured-call))
        (is (= "published" (:status body)))
        (is (= 700 (:release_id body)))
        (is (= 800 (:artifact_id body)))))))

(deftest execute-silver-release-route-enqueues-request
  (let [captured-call (atom nil)]
    (with-redefs [modeling-automation/execute-silver-release! (fn [release-id opts]
                                                                (reset! captured-call [release-id opts])
                                                                {:model_run_id 501
                                                                 :release_id release-id
                                                                 :status "submitted"
                                                                 :backend "databricks_job"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/executeSilverRelease")
                             :params {:release_id "700"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [700 {:created_by "alice"}] @captured-call))
        (is (= 501 (:model_run_id body)))
        (is (= "submitted" (:status body)))))))

(deftest execute-silver-release-route-preserves-429-from-queue
  (with-redefs [modeling-automation/execute-silver-release! (fn [& _]
                                                              (throw (ex-info "Workspace queue is full"
                                                                              {:status 429 :workspace_key "ops"})))]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/executeSilverRelease")
                           :params {:release_id "700"}
                           :session {:user "alice"}))
          body (response-body->map response)]
      (is (= 429 (:status response)))
      (is (= "Workspace queue is full" (:error body))))))

(deftest poll-silver-model-run-route-returns-updated-run
  (let [captured-call (atom nil)]
    (with-redefs [modeling-automation/poll-silver-model-run! (fn [model-run-id]
                                                               (reset! captured-call model-run-id)
                                                               {:model_run_id model-run-id
                                                                :status "succeeded"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/pollSilverModelRun")
                             :params {:model_run_id "501"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= 501 @captured-call))
        (is (= 501 (:model_run_id body)))
        (is (= "succeeded" (:status body)))))))

(deftest propose-gold-schema-route-returns-proposal
  (let [captured-args (atom nil)]
    (with-redefs [modeling-automation/propose-gold-schema! (fn [opts]
                                                             (reset! captured-args opts)
                                                             {:proposal_id 32
                                                              :profile_id 17
                                                              :target_model "gold_trip"
                                                              :layer "gold"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/proposeGoldSchema")
                             :params {:silver_proposal_id "22"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= {:silver_proposal_id 22 :created_by "alice"} @captured-args))
        (is (= 32 (:proposal_id body)))
        (is (= "gold" (:layer body)))))))

(deftest compile-gold-proposal-route-returns-compiled-sql
  (let [captured-id (atom nil)]
    (with-redefs [modeling-automation/compile-gold-proposal! (fn [proposal-id]
                                                               (reset! captured-id proposal-id)
                                                               {:proposal_id proposal-id
                                                                :compiled_sql "MERGE INTO gold.trip_daily ..."
                                                                :sql_ir {:materialization {:mode "merge"}}})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/compileGoldProposal")
                             :params {:proposal_id "32"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= 32 @captured-id))
        (is (= "MERGE INTO gold.trip_daily ..." (:compiled_sql body)))))))

(deftest execute-gold-release-route-enqueues-request
  (let [captured-call (atom nil)]
    (with-redefs [modeling-automation/execute-gold-release! (fn [release-id opts]
                                                              (reset! captured-call [release-id opts])
                                                              {:model_run_id 701
                                                               :release_id release-id
                                                               :status "submitted"
                                                               :backend "databricks_job"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/executeGoldRelease")
                             :params {:release_id "900"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [900 {:created_by "alice"}] @captured-call))
        (is (= 701 (:model_run_id body)))
        (is (= "submitted" (:status body)))))))

(deftest execute-gold-release-route-preserves-429-from-queue
  (with-redefs [modeling-automation/execute-gold-release! (fn [& _]
                                                            (throw (ex-info "Workspace queue is full"
                                                                            {:status 429 :workspace_key "ops"})))]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/executeGoldRelease")
                           :params {:release_id "900"}
                           :session {:user "alice"}))
          body (response-body->map response)]
      (is (= 429 (:status response)))
      (is (= "Workspace queue is full" (:error body))))))

(deftest rollback-api-batch-route-returns-rollback-result
  (let [captured-call (atom nil)]
    (with-redefs [ingest-runtime/rollback-api-batch! (fn [graph-id api-node-id batch-id opts]
                                                       (reset! captured-call [graph-id api-node-id batch-id opts])
                                                       {:batch_id batch-id
                                                        :status "rolled_back"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/rollbackApiBatch")
                             :params {:graph_id "99"
                                      :api_node_id "2"
                                      :batch_id "b1"
                                      :endpoint_name "trips"
                                      :rollback_reason "bad_batch"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [99 2 "b1" {:endpoint-name "trips"
                           :rollback-reason "bad_batch"
                           :rolled-back-by "alice"}]
               @captured-call))
        (is (= "rolled_back" (:status body)))))))

(deftest list-api-batches-route-returns-batch-summaries
  (let [captured-call (atom nil)]
    (with-redefs [ingest-runtime/list-api-batches (fn [graph-id api-node-id opts]
                                                    (reset! captured-call [graph-id api-node-id opts])
                                                    {:batch_count 1
                                                     :batches [{:batch_id "b1"
                                                                :artifact_state "archived"}]})]
      (let [response ((test-handler)
                      (assoc (mock/request :get "/apiBatches")
                             :params {:graph_id "99"
                                      :api_node_id "2"
                                      :endpoint_name "trips"
                                      :archived_only "true"
                                      :limit "20"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [99 2 {:endpoint-name "trips"
                      :run-id nil
                      :status nil
                      :active-only nil
                      :replayable-only nil
                      :archived-only true
                      :limit 20}]
               @captured-call))
        (is (= 1 (:batch_count body)))
        (is (= "archived" (get-in body [:batches 0 :artifact_state])))))))

(deftest archive-api-batch-route-returns-archive-result
  (let [captured-call (atom nil)]
    (with-redefs [ingest-runtime/archive-api-batch! (fn [graph-id api-node-id batch-id opts]
                                                      (reset! captured-call [graph-id api-node-id batch-id opts])
                                                      {:batch_id batch-id
                                                       :artifact_state "archived"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/archiveApiBatch")
                             :params {:graph_id "99"
                                      :api_node_id "2"
                                      :batch_id "b1"
                                      :endpoint_name "trips"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [99 2 "b1" {:endpoint-name "trips"
                           :archived-by "alice"}]
               @captured-call))
        (is (= "archived" (:artifact_state body)))))))

(deftest apply-api-retention-route-returns-summary
  (let [captured-call (atom nil)]
    (with-redefs [ingest-runtime/apply-api-retention! (fn [graph-id api-node-id opts]
                                                        (reset! captured-call [graph-id api-node-id opts])
                                                        {:archived_count 2
                                                         :deleted_count 1})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/applyApiRetention")
                             :params {:graph_id "99"
                                      :api_node_id "2"
                                      :endpoint_name "trips"
                                      :archive_days "30"
                                      :retention_days "365"
                                      :dry_run "true"
                                      :limit "15"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [99 2 {:endpoint-name "trips"
                      :archive-days 30
                      :retention-days 365
                      :bad-record-payload-archive-days nil
                      :bad-record-retention-days nil
                      :dry-run true
                      :limit 15
                      :archived-by "alice"}]
               @captured-call))
        (is (= 2 (:archived_count body)))
        (is (= 1 (:deleted_count body)))))))

(deftest list-api-schema-approvals-route-forwards-filters
  (let [captured-call (atom nil)]
    (with-redefs [ingest-runtime/list-api-schema-approvals (fn [graph-id api-node-id opts]
                                                              (reset! captured-call [graph-id api-node-id opts])
                                                              {:approval_count 1
                                                               :approvals [{:schema_hash "abc"}]})]
      (let [response ((test-handler)
                      (assoc (mock/request :get "/apiSchemaApprovals")
                             :params {:graph_id "99"
                                      :api_node_id "2"
                                      :endpoint_name "trips"
                                      :include_snapshots "true"
                                      :promoted_only "true"
                                      :limit "25"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [99 2 {:endpoint-name "trips"
                      :include-snapshots true
                      :promoted-only true
                      :limit 25}]
               @captured-call))
        (is (= 1 (:approval_count body)))))))

(deftest review-api-schema-route-forwards-review-fields
  (let [captured-call (atom nil)]
    (with-redefs [ingest-runtime/review-api-schema! (fn [graph-id api-node-id opts]
                                                      (reset! captured-call [graph-id api-node-id opts])
                                                      {:schema_hash "hash-1"
                                                       :review_state "approved"
                                                       :promoted true})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/reviewApiSchema")
                             :params {:graph_id "99"
                                      :api_node_id "2"
                                      :endpoint_name "trips"
                                      :schema_hash "hash-1"
                                      :review_state "approved"
                                      :review_notes "looks good"
                                      :promote "true"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [99 2 {:endpoint-name "trips"
                      :schema-hash "hash-1"
                      :review-state "approved"
                      :review-notes "looks good"
                      :promote? true
                      :reviewed-by "alice"}]
               @captured-call))
        (is (= "approved" (:review_state body)))
        (is (= true (:promoted body)))))))

(deftest reset-api-checkpoint-route-forwards-reset-values
  (let [captured-call (atom nil)]
    (with-redefs [ingest-runtime/reset-api-checkpoint! (fn [graph-id api-node-id opts]
                                                          (reset! captured-call [graph-id api-node-id opts])
                                                          {:endpoint_name "trips"
                                                           :checkpoint {:last_status "reset"}})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/resetApiCheckpoint")
                             :params {:graph_id "99"
                                      :api_node_id "2"
                                      :endpoint_name "trips"
                                      :reset_to_cursor "abc"
                                      :reason "operator-reset"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [99 2 {:endpoint-name "trips"
                      :reset-to-cursor "abc"
                      :reset-to-watermark nil
                      :reason "operator-reset"
                      :requested-by "alice"}]
               @captured-call))
        (is (= "reset" (get-in body [:checkpoint :last_status])))))))

(deftest api-observability-summary-route-forwards-endpoint
  (let [captured-call (atom nil)]
    (with-redefs [ingest-runtime/api-observability-summary (fn [graph-id api-node-id opts]
                                                             (reset! captured-call [graph-id api-node-id opts])
                                                             {:endpoint_name "trips"
                                                              :alert_count 1})]
      (let [response ((test-handler)
                      (assoc (mock/request :get "/apiObservabilitySummary")
                             :params {:graph_id "99"
                                      :api_node_id "2"
                                      :endpoint_name "trips"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [99 2 {:endpoint-name "trips"}] @captured-call))
        (is (= "trips" (:endpoint_name body)))))))

(deftest api-bronze-proof-signoff-route-persists-signoff
  (let [captured-call (atom nil)]
    (with-redefs [control-plane/record-api-bronze-signoff! (fn [payload]
                                                              (reset! captured-call payload)
                                                              {:id 10
                                                               :proof_status "passed"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/apiBronzeProofSignoff")
                             :params {:release_tag "r2026.03.16"
                                      :environment "stage"
                                      :commit_sha "abcdef"
                                      :proof_summary_path "tmp/sum.json"
                                      :proof_results_path "tmp/res.ndjson"
                                      :proof_log_path "tmp/log.txt"
                                      :proof_status "passed"
                                      :reviewer_name "bob"
                                      :soak_iterations "12"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= {:release_tag "r2026.03.16"
                :environment "stage"
                :commit_sha "abcdef"
                :proof_summary_path "tmp/sum.json"
                :proof_results_path "tmp/res.ndjson"
                :proof_log_path "tmp/log.txt"
                :proof_status "passed"
                :soak_iterations 12
                :operator_name "alice"
                :reviewer_name "bob"
                :operator_notes nil
                :created_by "alice"}
               @captured-call))
        (is (= "passed" (:proof_status body)))))))

(deftest list-api-bad-records-route-returns-bad-record-summaries
  (let [captured-call (atom nil)]
    (with-redefs [ingest-runtime/list-api-bad-records (fn [graph-id api-node-id opts]
                                                         (reset! captured-call [graph-id api-node-id opts])
                                                         {:bad_record_count 1
                                                          :bad_records [{:bad_record_id "br-1"
                                                                         :replay_status "pending"}]})]
      (let [response ((test-handler)
                      (assoc (mock/request :get "/apiBadRecords")
                             :params {:graph_id "99"
                                      :api_node_id "2"
                                      :endpoint_name "trips"
                                      :batch_id "b1"
                                      :include_payloads "true"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [99 2 {:endpoint-name "trips"
                      :run-id nil
                      :batch-id "b1"
                      :include-succeeded nil
                      :include-payloads true
                      :limit nil}]
               @captured-call))
        (is (= 1 (:bad_record_count body)))))))

(deftest replay-api-bad-records-route-returns-replay-summary
  (let [captured-call (atom nil)]
    (with-redefs [ingest-runtime/replay-api-bad-records! (fn [graph-id api-node-id opts]
                                                            (reset! captured-call [graph-id api-node-id opts])
                                                            {:run_id "run-bad-replay"
                                                             :rows_replayed 8
                                                             :bad_records_remaining 1})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/replayApiBadRecords")
                             :params {:graph_id "99"
                                      :api_node_id "2"
                                      :endpoint_name "trips"
                                      :batch_id "b1"
                                      :limit "50"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [99 2 {:endpoint-name "trips"
                      :batch-id "b1"
                      :source-run-id nil
                      :limit 50
                      :include-succeeded? nil
                      :replayed-by "alice"}]
               @captured-call))
        (is (= "run-bad-replay" (:run_id body)))
        (is (= 8 (:rows_replayed body)))))))

(deftest save-managed-secret-route-persists-secret
  (let [captured (atom nil)]
    (with-redefs [control-plane/put-secret! (fn [secret]
                                              (reset! captured secret)
                                              {:secret_ref (:secret_ref secret)
                                               :secret_encoding "aes_gcm_v1"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/controlPlane/secrets")
                             :params {:secret_ref "SAMSARA_TOKEN"
                                      :secret_value "abc123"}
                             :session {:user "alice"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= {:secret_ref "SAMSARA_TOKEN"
                :secret_value "abc123"
                :updated_by "alice"}
               @captured))
        (is (= "aes_gcm_v1" (:secret_encoding body)))))))

(deftest list-audit-events-route-returns-audit-rows
  (let [captured (atom nil)]
    (with-redefs [control-plane/list-audit-events (fn [opts]
                                                    (reset! captured opts)
                                                    [{:event_type "api.rollback_batch"}])]
      (let [response ((test-handler)
                      (assoc (mock/request :get "/controlPlane/auditEvents")
                             :params {:event_type "api.rollback_batch"
                                      :limit "10"}))
            body (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= {:event_type "api.rollback_batch" :limit 10} @captured))
        (is (= "api.rollback_batch" (-> body first :event_type)))))))

(deftest run-api-ingestion-route-returns-forbidden-when-rbac-enabled-and-role-missing
  (with-redefs [config/env {:bitool-rbac-enabled "true"}]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/runApiIngestion")
                           :params {:gid "99" :id "2"}
                           :session {:roles ["viewer"]}))
          body (response-body->map response)]
      (is (= 403 (:status response)))
      (is (= "Forbidden" (:error body))))))

(deftest request-roles-does-not-trust-query-param-role
  (with-redefs [config/env {:bitool-rbac-enabled "true"}]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/runSchedulerIngestion")
                           :params {:gid "99" :id "4" :role "admin"}
                           :session {}))
          body (response-body->map response)]
      (is (= 403 (:status response)))
      (is (= "Forbidden" (:error body))))))

(deftest propose-silver-schema-route-requires-authorization-when-rbac-enabled
  (with-redefs [config/env {:bitool-rbac-enabled "true"}]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/proposeSilverSchema")
                           :params {:gid "99" :id "2" :endpoint_name "trips"}
                           :session {:roles ["viewer"]}))
          body (response-body->map response)]
      (is (= 403 (:status response)))
      (is (= "Forbidden" (:error body))))))

(deftest preview-api-schema-inference-rejects-localhost-base-url
  (with-redefs [config/env {:bitool-rbac-enabled "true"}]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/previewApiSchemaInference")
                           :params {:base_url "http://127.0.0.1:8080"
                                    :endpoint_config {:endpoint_name "trips"}}
                           :session {:roles ["api.ops"]}))
          body (response-body->map response)]
      (is (= 400 (:status response)))
      (is (= "Preview base_url must not target local or private addresses" (:error body))))))

(deftest preview-api-schema-inference-allows-local-mock-server
  (with-redefs [config/env {:bitool-rbac-enabled "true"}
                ingest-runtime/preview-endpoint-schema! (fn [api-node endpoint]
                                                          {:http_status 200
                                                           :sampled_records 1
                                                           :base_url (:base_url api-node)
                                                           :endpoint_name (:endpoint_name endpoint)
                                                           :inferred_fields [{:path "$.data[].id"
                                                                              :column_name "id"}]})]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/previewApiSchemaInference")
                           :params {:base_url "http://localhost:3001"
                                    :auth_ref {:type "bearer" :token "mock-samsara-token"}
                                    :endpoint_config {:endpoint_name "fleet/vehicles"
                                                      :endpoint_url "/fleet/vehicles"}}
                           :session {:roles ["api.ops"]}))
          body (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "http://localhost:3001" (:base_url body)))
      (is (= "fleet/vehicles" (:endpoint_name body)))
      (is (= "$.data[].id" (get-in body [:inferred_fields 0 :path]))))))

(deftest html-handler-rejects-unsafe-template-name
  (let [response (home/html-handler {:path-params {:file "../../etc/passwd"}})
        body (response-body->map response)]
    (is (= 400 (:status response)))
    (is (= "Invalid template name" (:error body)))))

(deftest fn-handler-rejects-unsupported-save-function
  (let [response (home/fn-handler {:path-params {:fn "persist-graph!"}})
        body (response-body->map response)]
    (is (= 404 (:status response)))
    (is (= "Unsupported save function" (:error body)))))

(deftest run-scheduler-ingestion-route-returns-success-response
  (let [captured-call (atom nil)]
    (with-redefs [ingest-execution/enqueue-scheduler-request! (fn [gid scheduler-id opts]
                                                                (reset! captured-call [gid scheduler-id opts])
                                                                {:created? true
                                                                 :request_id "req-2"
                                                                 :run_id "run-2"
                                                                 :graph_id gid
                                                                 :node_id scheduler-id
                                                                 :status "queued"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/runSchedulerIngestion")
                             :params {:gid "99"
                                      :id "4"}
                             :session {}))
            body     (response-body->map response)]
        (is (= 202 (:status response)))
        (is (= "/executionRuns/run-2" (get-in response [:headers "Location"])))
        (is (= [99 4 {:trigger-type "manual"}] @captured-call))
        (is (= "queued" (:status body)))))))

(deftest run-scheduler-ingestion-route-returns-bad-request
  (with-redefs [ingest-execution/enqueue-scheduler-request! (fn [_ _ _]
                                                              (throw (ex-info "Scheduler node has no reachable API nodes"
                                                                              {:scheduler_node_id 4})))]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/runSchedulerIngestion")
                           :params {:gid "99"
                                    :id "4"}
                           :session {}))
          body     (response-body->map response)]
      (is (= 400 (:status response)))
      (is (= "Scheduler node has no reachable API nodes" (:error body)))
      (is (= 4 (get-in body [:data :scheduler_node_id]))))))

(deftest list-execution-runs-route-returns-queue-backed-results
  (let [captured-opts (atom nil)]
    (with-redefs [ingest-execution/list-execution-runs (fn [opts]
                                                         (reset! captured-opts opts)
                                                         [{:run_id "run-1" :status "queued"}])]
      (let [response ((test-handler)
                      (assoc (mock/request :get "/executionRuns")
                             :params {:gid "99" :status "queued" :limit "10"}))
            body     (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= {:graph-id 99
                :status "queued"
                :workspace-key nil
                :tenant-key nil
                :endpoint-name nil
                :request-kind nil
                :workload-class nil
                :queue-partition nil
                :limit 10}
               @captured-opts))
        (is (= "run-1" (-> body first :run_id)))))))

(deftest list-execution-runs-route-supports-tenant-workspace-filters
  (let [captured-opts (atom nil)]
    (with-redefs [ingest-execution/list-execution-runs (fn [opts]
                                                         (reset! captured-opts opts)
                                                         [{:run_id "run-2" :status "succeeded"}])]
      (let [response ((test-handler)
                      (assoc (mock/request :get "/executionRuns")
                             :params {:workspace_key "ops"
                                      :tenant_key "tenant-a"
                                      :endpoint_name "trips"
                                      :request_kind "api"}))
            body     (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= {:workspace-key "ops"
                :tenant-key "tenant-a"
                :endpoint-name "trips"
                :request-kind "api"
                :workload-class nil
                :queue-partition nil
                :graph-id nil
                :status nil
                :limit nil}
               @captured-opts))
        (is (= "run-2" (-> body first :run_id)))))))

(deftest get-execution-run-route-returns-run-details
  (with-redefs [ingest-execution/get-execution-run (fn [run-id]
                                                     {:run_id run-id
                                                      :status "success"
                                                      :node_runs [{:node_id 2 :status "success"}]})]
    (let [response ((test-handler) (mock/request :get "/executionRuns/run-1"))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "success" (:status body)))
      (is (= 2 (get-in body [:node_runs 0 :node_id]))))))

(deftest replay-execution-run-route-returns-accepted-response
  (let [captured-run-id (atom nil)]
    (with-redefs [operations/replay-execution-run! (fn [run-id]
                                                     (reset! captured-run-id run-id)
                                                     {:created? true
                                                      :request_id "req-replay"
                                                      :run_id "run-replay"
                                                      :status "queued"})]
      (let [response ((test-handler) (mock/request :post "/executionRuns/run-1/replay"))
            body     (response-body->map response)]
        (is (= 202 (:status response)))
        (is (= "run-1" @captured-run-id))
        (is (= "/executionRuns/run-replay" (get-in response [:headers "Location"])))
        (is (= "queued" (:status body)))))))

(deftest execution-demand-route-returns-demand-snapshot
  (with-redefs [ingest-execution/execution-demand-snapshot (fn []
                                                             [{:tenant_key "tenant-a"
                                                               :workspace_key "ops"
                                                               :queue_partition "p00"
                                                               :workload_class "api"
                                                               :queued_count 5
                                                               :active_count 2}])]
    (let [response ((test-handler) (mock/request :get "/executionDemand"))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "tenant-a" (:tenant_key (first body))))
      (is (= 5 (:queued_count (first body)))))))

(deftest freshness-dashboard-route-returns-dashboard-rows
  (let [captured-opts (atom nil)]
    (with-redefs [operations/freshness-dashboard (fn [opts]
                                                   (reset! captured-opts opts)
                                                   [{:graph_id 99
                                                     :endpoint_name "trips"
                                                     :overdue? true}])]
      (let [response ((test-handler)
                    (assoc (mock/request :get "/freshnessDashboard")
                           :params {:workspace_key "ops"}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= {:graph-id nil :workspace-key "ops" :tenant-key nil :limit nil} @captured-opts))
      (is body)))))

(deftest usage-dashboard-route-forwards-filters
  (let [captured-opts (atom nil)]
    (with-redefs [operations/usage-dashboard (fn [opts]
                                               (reset! captured-opts opts)
                                               [{:tenant_key "tenant-a"
                                                 :request_count 4}])]
      (let [response ((test-handler)
                      (assoc (mock/request :get "/usageDashboard")
                             :params {:tenant_key "tenant-a"
                                      :request_kind "api"
                                      :workload_class "replay"
                                      :usage_date "2026-03-14"
                                      :limit "25"}))
            body     (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= {:tenant-key "tenant-a"
                :workspace-key nil
                :request-kind "api"
                :workload-class "replay"
                :usage-date "2026-03-14"
                :limit 25}
               @captured-opts))
        (is (= 4 (:request_count (first body))))))))

(deftest usage-dashboard-route-returns-bad-request-for-invalid-date
  (let [response ((test-handler)
                  (assoc (mock/request :get "/usageDashboard")
                         :params {:usage_date "2026-14-99"}))
        body     (response-body->map response)]
    (is (= 400 (:status response)))
    (is (= "usage_date must be YYYY-MM-DD" (:error body)))))

(deftest graph-lineage-route-returns-upstream-and-downstream
  (with-redefs [operations/graph-lineage (fn [graph-id]
                                           {:graph_id graph-id
                                            :upstream [{:upstream_graph_id 10}]
                                            :downstream [{:downstream_graph_id 11}]})]
    (let [response ((test-handler)
                    (assoc (mock/request :get "/graphLineage")
                           :params {:gid "99"}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= 10 (get-in body [:upstream 0 :upstream_graph_id])))
      (is (= 11 (get-in body [:downstream 0 :downstream_graph_id]))))))

(deftest save-workspace-route-persists-workspace-config
  (let [captured (atom nil)]
    (with-redefs [control-plane/upsert-workspace! (fn [workspace]
                                                    (reset! captured workspace)
                                                    workspace)]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/controlPlane/workspaces")
                           :params {:workspace_key "ops"
                                    :tenant_key "tenant-a"
                                    :workspace_name "Ops"
                                    :max_concurrent_requests "4"
                                    :max_queued_requests "200"
                                    :weight "2"}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "ops" (:workspace_key body)))
      (is (= "tenant-a" (:tenant_key body)))
      (is (= 4 (:max_concurrent_requests @captured)))
      (is (= 200 (:max_queued_requests @captured)))
      (is (= 2 (:weight @captured)))))))

(deftest save-workspace-route-requires-authorization-when-rbac-enabled
  (with-redefs [config/env {:bitool-rbac-enabled "true"}]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/controlPlane/workspaces")
                           :params {:workspace_key "ops" :tenant_key "tenant-a"}
                           :session {:roles ["viewer"]}))
          body (response-body->map response)]
      (is (= 403 (:status response)))
      (is (= "Forbidden" (:error body))))))

(deftest save-tenant-route-persists-tenant-config
  (let [captured (atom nil)]
    (with-redefs [control-plane/upsert-tenant! (fn [tenant]
                                                 (reset! captured tenant)
                                                 tenant)]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/controlPlane/tenants")
                             :params {:tenant_key "tenant-a"
                                      :tenant_name "Tenant A"
                                      :max_concurrent_requests "8"
                                      :max_queued_requests "900"
                                      :weight "3"
                                      :metering_enabled "false"
                                      :active "false"}))
            body     (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= "tenant-a" (:tenant_key body)))
        (is (= 8 (:max_concurrent_requests @captured)))
        (is (= 900 (:max_queued_requests @captured)))
        (is (= 3 (:weight @captured)))
        (is (= false (:metering_enabled @captured)))
        (is (= false (:active @captured)))))))

(deftest assign-graph-workspace-route-persists-binding
  (with-redefs [control-plane/assign-graph-workspace! (fn [graph-id workspace-key updated-by]
                                                        {:graph_id graph-id
                                                         :workspace_key workspace-key
                                                         :updated_by updated-by})]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/controlPlane/graphAssignment")
                           :params {:gid "99"
                                    :workspace_key "ops"
                                    :updated_by "alice"}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= 99 (:graph_id body)))
      (is (= "ops" (:workspace_key body))))))

(deftest save-graph-dependencies-route-persists-upstream-graph-list
  (let [captured (atom nil)]
    (with-redefs [control-plane/set-graph-dependencies! (fn [downstream-graph-id deps]
                                                          (reset! captured [downstream-graph-id deps])
                                                          deps)]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/controlPlane/graphDependencies")
                             :params {:gid "99"
                                      :upstream_graph_ids ["10" "11"]
                                      :freshness_window_seconds "600"}))
            body     (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= [99 [{:upstream_graph_id 10 :freshness_window_seconds 600}
                    {:upstream_graph_id 11 :freshness_window_seconds 600}]]
               @captured))
        (is body)))))

(deftest save-target-route-returns-normalized-target-config
  (let [graph {:a {:id 99 :v 0 :name "test"}
               :n {1 {:na {:name "O" :btype "O" :tcols {}} :e {}}
                   2 {:na {:name "target" :btype "Tg" :tcols {}} :e {1 {}}}}}]
    (with-redefs [db/getGraph (fn [_] graph)
                  control-plane/persist-graph! (fn [g _] g)]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/saveTarget")
                             :params {:id "2"
                                      :connection_id "42"
                                      :target_kind "databricks"
                                      :catalog "sheetz_telematics"
                                      :schema "bronze"
                                      :table_name "samara_trips_raw"
                                      :partition_columns "partition_date, load_date"
                                      :options "{\"raw_payload\":true}"
                                      :silver_job_id "111"}
                             :session {:gid 99}))
            body     (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= 42 (get body :connection_id)))
        (is (= ["partition_date" "load_date"] (get body :partition_columns)))
        (is (= true (get-in body [:options :raw_payload])))
        (is (= "111" (get body :silver_job_id)))))))

(deftest save-kafka-source-route-returns-normalized-topic-config
  (let [graph {:a {:id 99 :v 0 :name "test"}
               :n {1 {:na {:name "O" :btype "O" :tcols {}} :e {}}
                   2 {:na {:name "kafka" :btype "Kf" :tcols {}} :e {1 {}}}}}]
    (with-redefs [db/getGraph (fn [_] graph)
                  control-plane/persist-graph! (fn [g _] g)]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/saveKafkaSource")
                             :params {:id "2"
                                      :connection_id "42"
                                      :topic_configs [{:topic_name "orders.events"
                                                       :value_deserializer "json"
                                                       :primary_key_fields "id"}]}
                             :session {:gid 99}))
            body     (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= 42 (get body :connection_id)))
        (is (= "orders.events" (get-in body [:topic_configs 0 :topic_name])))
        (is (= ["id"] (get-in body [:topic_configs 0 :primary_key_fields])))))))

(deftest save-file-source-route-returns-normalized-file-config
  (let [graph {:a {:id 99 :v 0 :name "test"}
               :n {1 {:na {:name "O" :btype "O" :tcols {}} :e {}}
                   2 {:na {:name "file" :btype "Fs" :tcols {}} :e {1 {}}}}}]
    (with-redefs [db/getGraph (fn [_] graph)
                  control-plane/persist-graph! (fn [g _] g)]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/saveFileSource")
                             :params {:id "2"
                                      :connection_id "42"
                                      :base_path "/tmp"
                                      :file_configs [{:path "orders.jsonl"
                                                      :format "jsonl"
                                                      :primary_key_fields "id"}]}
                             :session {:gid 99}))
            body     (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= 42 (get body :connection_id)))
        (is (= "orders.jsonl" (get-in body [:file_configs 0 :path])))
        (is (= ["id"] (get-in body [:file_configs 0 :primary_key_fields])))))))

(deftest save-scheduler-route-persists-enabled-flag
  (let [graph {:a {:id 99 :v 0 :name "test"}
               :n {1 {:na {:name "O" :btype "O" :tcols {}} :e {}}
                   2 {:na {:name "schedule" :btype "Sc" :tcols {}} :e {1 {}}}}}]
    (with-redefs [db/getGraph (fn [_] graph)
                  control-plane/persist-graph! (fn [g _] g)]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/saveSc")
                             :params {:id "2"
                                      :enabled false
                                      :cron_expression "0 * * * *"
                                      :timezone "UTC"
                                      :params []}
                             :session {:gid 99}))
            body     (response-body->map response)]
        (is (= 200 (:status response)))
        (is (= false (get body :enabled)))
        (is (= "0 * * * *" (get body :cron_expression)))))))

(deftest save-api-route-returns-conflict-for-stale-graph-version
  (let [graph {:a {:id 99 :v 4 :name "test"}
               :n {1 {:na {:name "O" :btype "O" :tcols {}} :e {}}
                   2 {:na {:name "api" :btype "Ap" :tcols {}} :e {1 {}}}}}]
    (with-redefs [db/getGraph (fn [_] graph)
                  control-plane/persist-graph! (fn [_ _]
                                                 (throw (ex-info "Graph version conflict"
                                                                 {:status 409
                                                                  :expected_version 4
                                                                  :current_version 5})))]
        (let [response ((test-handler)
                      (assoc (mock/request :post "/saveApi")
                             :params {:id "2"
                                      :expected_version "4"
                                      :base_url "https://api.example.com"
                                      :endpoint_configs []}
                             :session {:gid 99}))
            body     (response-body->map response)]
        (is (= 409 (:status response)))
        (is (= "Graph version conflict" (:error body)))
        (is (= 5 (get-in body [:data :current_version])))))))

(deftest save-api-route-returns-json-bad-request-for-invalid-id
  (let [response ((test-handler)
                  (assoc (mock/request :post "/saveApi")
                         :params {:id "abc"}
                         :session {:gid 99}))
        body     (response-body->map response)]
    (is (= 400 (:status response)))
    (is (string? (:body response)))
    (is (= "application/json; charset=utf-8"
           (get-in response [:headers "Content-Type"])))
    (is (= "Invalid numeric parameter" (:error body)))))

(deftest save-api-route-returns-success-for-rich-endpoint-config
  (let [graph {:a {:id 99 :v 0 :name "test"}
               :n {1 {:na {:name "O" :btype "O" :tcols {}} :e {}}
                   2 {:na {:name "api" :btype "Ap" :tcols {}} :e {1 {}}}}}
        persisted (atom nil)]
    (with-redefs [db/getGraph (fn [_] graph)
                  control-plane/persist-graph! (fn [g _]
                                                 (reset! persisted g)
                                                 g)]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/saveApi")
                             :params {:id "2"
                                      :api_name "samara"
                                      :source_system "samara"
                                      :base_url "http://localhost:3001"
                                      :auth_ref {:type "bearer"
                                                 :secret_ref ""
                                                 :token "mock-samsara-token"}
                                      :endpoint_configs [{:endpoint_name "fleet/vehicles"
                                                          :endpoint_url "fleet/vehicles"
                                                          :http_method "GET"
                                                          :schema_mode "manual"
                                                          :load_type "full"
                                                          :pagination_strategy "cursor"
                                                          :pagination_location "query"
                                                          :cursor_field "pagination.endCursor"
                                                          :cursor_param "after"
                                                          :page_size 10
                                                          :sample_records 100
                                                          :max_inferred_columns 100
                                                          :type_inference_enabled true
                                                          :schema_evolution_mode "advisory"
                                                          :primary_key_fields ["id"]
                                                          :request_headers {}
                                                          :query_params {}
                                                          :body_params {}
                                                          :json_explode_rules []
                                                          :inferred_fields []
                                                          :retry_policy {:max_retries 3
                                                                         :base_backoff_ms 1000}
                                                          :enabled true}]}
                             :session {:gid 99}))
            body     (response-body->map response)]
        (is (= 200 (:status response)))
        (is (sequential? body))
        (is (some #(= "api" (:alias %)) body))
        (is (some #(= "Output" (:alias %)) body))
        (is (= #{1 2}
               (set (keys (:n @persisted)))))
        (is (nil? (some (fn [[_ node]]
                          (when (#{"Mp" "Tg"} (get-in node [:na :btype]))
                            true))
                        (:n @persisted))))))))

(deftest save-endpoint-route-returns-conflict-for-stale-graph-version
  (let [graph {:a {:id 99 :v 4 :name "test"}
               :n {1 {:na {:name "O" :btype "O" :tcols {}} :e {}}
                   2 {:na {:name "endpoint" :btype "Ev" :tcols {}} :e {1 {}}}}}]
    (with-redefs [db/getGraph (fn [_] graph)
                  g2/save-endpoint (fn [g _ _] g)
                  control-plane/persist-graph! (fn [_ _]
                                                 (throw (ex-info "Graph version conflict"
                                                                 {:status 409
                                                                  :expected_version 4
                                                                  :current_version 5})))
                  g2/getData (fn [_ _] {:endpoint_name "trips"})
                  endpoint/register-endpoint! (fn [& _] true)
                  g2/get-endpoint-item (fn [_ _] {:endpoint_name "trips"})]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/saveEndpoint")
                             :params {:id "2"
                                      :expected_version "4"
                                      :endpoint_name "trips"}
                             :session {:gid 99}))
            body     (response-body->map response)]
        (is (= 409 (:status response)))
        (is (= "Graph version conflict" (:error body)))
        (is (= 5 (get-in body [:data :current_version])))))))

(deftest save-filter-route-returns-conflict-for-stale-graph-version
  (let [graph {:a {:id 99 :v 4 :name "test"}
               :n {1 {:na {:name "O" :btype "O" :tcols {}} :e {}}
                   2 {:na {:name "filter" :btype "Fi" :tcols {}} :e {1 {}}}}}]
    (with-redefs [db/getGraph (fn [_] graph)
                  control-plane/persist-graph! (fn [_ _]
                                                 (throw (ex-info "Graph version conflict"
                                                                 {:status 409
                                                                  :expected_version 4
                                                                  :current_version 5})))]
      (let [response ((test-handler)
                      (assoc (mock/request :post "/saveFilter")
                             :params {:id "2"
                                      :expected_version "4"
                                      :where "x = 1"}
                             :session {:gid 99}))
            body     (response-body->map response)]
        (is (= 409 (:status response)))
        (is (= "Graph version conflict" (:error body)))
        (is (= 5 (get-in body [:data :current_version])))))))

;; ---------------------------------------------------------------------------
;; AI Assist routes (P1)
;; ---------------------------------------------------------------------------

(deftest ai-explain-preview-schema-returns-envelope
  (with-redefs [ai-assistant/explain-preview-schema!
                (fn [_]
                  {:summary "Record grain is data[]. PK is id."
                   :confidence 0.85
                   :recommendations ["Use updatedAt as watermark"]
                   :edits {}
                   :open_questions []
                   :warnings []
                   :debug {:task "explain_preview" :source "deterministic_plus_ai"}})]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiExplainPreviewSchema")
                           :params {:endpoint_config {:endpoint_name "fleet/vehicles"}
                                    :inferred_fields [{:column_name "id" :type "STRING"}]}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "Record grain is data[]. PK is id." (:summary body)))
      (is (= 0.85 (:confidence body)))
      (is (= "deterministic_plus_ai" (get-in body [:debug :source]))))))

(deftest ai-explain-preview-schema-requires-params
  (with-redefs [ai-assistant/explain-preview-schema! (fn [_] (throw (Exception. "should not be called")))]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiExplainPreviewSchema")
                           :params {:endpoint_config {:endpoint_name "fleet/vehicles"}}))]
      (is (= 400 (:status response))))))

(deftest ai-suggest-bronze-keys-returns-envelope
  (with-redefs [ai-assistant/suggest-bronze-keys!
                (fn [_]
                  {:summary "Recommended PK: id, watermark: updatedAtTime"
                   :confidence 0.9
                   :recommendations [{:text "id is a stable identifier"}]
                   :edits {}
                   :open_questions []
                   :warnings []
                   :debug {:task "suggest_bronze_keys" :source "deterministic_plus_ai"}})]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiSuggestBronzeKeys")
                           :params {:endpoint_config {:endpoint_name "fleet/vehicles"}
                                    :inferred_fields [{:column_name "id" :type "STRING"}]}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (string? (:summary body)))
      (is (= "suggest_bronze_keys" (get-in body [:debug :task]))))))

(deftest ai-explain-model-proposal-returns-envelope
  (with-redefs [ai-assistant/explain-model-proposal!
                (fn [_]
                  {:summary "This proposal creates a deduped Silver table."
                   :confidence 0.8
                   :recommendations []
                   :edits {}
                   :open_questions []
                   :warnings []
                   :debug {:task "explain_proposal" :source "deterministic_plus_ai"}})]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiExplainModelProposal")
                           :params {:proposal_id "1"
                                    :proposal_json {:columns [{:target_column "id" :type "STRING"}]}}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "explain_proposal" (get-in body [:debug :task]))))))

(deftest ai-explain-model-proposal-requires-proposal-id
  (with-redefs [ai-assistant/explain-model-proposal! (fn [_] (throw (Exception. "should not be called")))]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiExplainModelProposal")
                           :params {:proposal_json {:columns []}}))]
      (is (= 400 (:status response))))))

(deftest ai-explain-proposal-validation-returns-envelope
  (with-redefs [ai-assistant/explain-proposal-validation!
                (fn [_]
                  {:summary "2 checks failed: missing PK and type mismatch."
                   :confidence 0.75
                   :recommendations ["Add primary key column"]
                   :edits {}
                   :open_questions []
                   :warnings ["Deterministic fallback used"]
                   :debug {:task "explain_validation" :source "deterministic_only"}})]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiExplainProposalValidation")
                           :params {:proposal_id "1"
                                    :validation_result {:status "invalid"
                                                        :checks [{:kind "pk_check" :result "fail"}]}}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "explain_validation" (get-in body [:debug :task]))))))

(deftest ai-explain-proposal-validation-requires-validation-result
  (with-redefs [ai-assistant/explain-proposal-validation! (fn [_] (throw (Exception. "should not be called")))]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiExplainProposalValidation")
                           :params {:proposal_id "1"}))]
      (is (= 400 (:status response))))))

;; ---------------------------------------------------------------------------
;; P2 AI Routes
;; ---------------------------------------------------------------------------

(deftest ai-suggest-silver-transforms-returns-envelope
  (with-redefs [ai-assistant/suggest-silver-transforms!
                (fn [_]
                  {:summary "2 transform suggestions"
                   :confidence 0.8
                   :edits {:type_casts [{:description "Cast to TIMESTAMP" :target_column "updated_at"}]}
                   :recommendations []
                   :open_questions []
                   :warnings []
                   :debug {:task "suggest_silver_transforms" :source "deterministic_plus_ai"}})]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiSuggestSilverTransforms")
                           :params {:proposal_id "1"
                                    :proposal {:columns [{:target_column "id"}]}}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "suggest_silver_transforms" (get-in body [:debug :task]))))))

(deftest ai-suggest-silver-transforms-requires-proposal
  (with-redefs [ai-assistant/suggest-silver-transforms! (fn [_] (throw (Exception. "should not be called")))]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiSuggestSilverTransforms")
                           :params {:proposal_id "1"}))]
      (is (= 400 (:status response))))))

(deftest ai-generate-silver-from-brd-returns-envelope
  (with-redefs [ai-assistant/generate-silver-proposal-from-brd!
                (fn [_]
                  {:summary "Mapped 3 of 4 requirements"
                   :confidence 0.75
                   :edits {:target_columns [{:target_column "id" :source_column "id"}]}
                   :recommendations []
                   :open_questions []
                   :warnings []
                   :debug {:task "silver_from_brd" :source "deterministic_plus_ai"}})]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiGenerateSilverFromBRD")
                           :params {:brd_text "Need vehicle tracking"
                                    :source_columns [{:column_name "id"}]}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "silver_from_brd" (get-in body [:debug :task]))))))

(deftest ai-generate-silver-from-brd-requires-brd-text
  (with-redefs [ai-assistant/generate-silver-proposal-from-brd! (fn [_] (throw (Exception. "should not be called")))]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiGenerateSilverFromBRD")
                           :params {:source_columns [{:column_name "id"}]}))]
      (is (= 400 (:status response))))))

(deftest ai-generate-gold-from-brd-returns-envelope
  (with-redefs [ai-assistant/generate-gold-proposal-from-brd!
                (fn [_]
                  {:summary "Gold mart design from BRD"
                   :confidence 0.7
                   :edits {}
                   :recommendations []
                   :open_questions []
                   :warnings []
                   :debug {:task "gold_from_brd" :source "deterministic_plus_ai"}})]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiGenerateGoldFromBRD")
                           :params {:brd_text "Daily vehicle count" :source_columns [{:column_name "id"}]}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "gold_from_brd" (get-in body [:debug :task]))))))

(deftest ai-suggest-gold-mart-returns-envelope
  (with-redefs [ai-assistant/suggest-gold-mart-design!
                (fn [_]
                  {:summary "Consider adding partition keys"
                   :confidence 0.85
                   :edits {:add_columns [{:description "Add day partition" :target_column "day"}]}
                   :recommendations []
                   :open_questions []
                   :warnings []
                   :debug {:task "suggest_gold_mart" :source "deterministic_plus_ai"}})]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiSuggestGoldMartDesign")
                           :params {:proposal_id "5"
                                    :proposal {:columns [{:target_column "count"}]}}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "suggest_gold_mart" (get-in body [:debug :task]))))))

(deftest ai-explain-schema-drift-returns-envelope
  (with-redefs [ai-assistant/explain-schema-drift!
                (fn [_]
                  {:summary "New field gps_accuracy added"
                   :confidence 0.9
                   :severity "info"
                   :recommendations []
                   :open_questions []
                   :warnings []
                   :debug {:task "explain_drift" :source "deterministic_plus_ai"}})]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiExplainSchemaDrift")
                           :params {:event_id "42"}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "explain_drift" (get-in body [:debug :task]))))))

(deftest ai-suggest-drift-remediation-returns-envelope
  (with-redefs [ai-assistant/suggest-drift-remediation!
                (fn [_]
                  {:summary "Add gps_accuracy to Bronze table"
                   :confidence 0.85
                   :edits {:bronze_changes [{:description "Add column" :action "add" :target "gps_accuracy"}]}
                   :recommendations []
                   :open_questions []
                   :warnings []
                   :debug {:task "suggest_drift_remediation" :source "deterministic_plus_ai"}})]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiSuggestDriftRemediation")
                           :params {:event_id "42"}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "suggest_drift_remediation" (get-in body [:debug :task]))))))

;; ---------------------------------------------------------------------------
;; P3-A: Explain Endpoint Business Shape
;; ---------------------------------------------------------------------------

(deftest ai-explain-endpoint-business-shape-returns-envelope
  (with-redefs [ai-assistant/explain-endpoint-business-shape!
                (fn [_]
                  {:summary "Event entity — GPS pings per vehicle"
                   :confidence 0.88
                   :entity_type "event"
                   :recommendations []
                   :open_questions []
                   :warnings []
                   :debug {:task "explain_business_shape" :source "deterministic_plus_ai"}})]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiExplainEndpointBusinessShape")
                           :params {:endpoint_config {:endpoint_name "vehicles"}
                                    :inferred_fields [{:column_name "id" :type "STRING"}]}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "explain_business_shape" (get-in body [:debug :task]))))))

(deftest ai-explain-endpoint-business-shape-requires-params
  (let [response ((test-handler)
                  (assoc (mock/request :post "/aiExplainEndpointBusinessShape")
                         :params {}))
        body     (response-body->map response)]
    (is (= 400 (:status response)))
    (is (re-find #"endpoint_config" (:error body)))))

;; ---------------------------------------------------------------------------
;; P3-B: Explain Target Strategy
;; ---------------------------------------------------------------------------

(deftest ai-explain-target-strategy-returns-envelope
  (with-redefs [ai-assistant/explain-target-strategy!
                (fn [_]
                  {:summary "Merge mode upserts on primary key"
                   :confidence 0.9
                   :write_mode_notes "MERGE ensures idempotent upserts"
                   :recommendations []
                   :open_questions []
                   :warnings []
                   :debug {:task "explain_target_strategy" :source "deterministic_plus_ai"}})]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiExplainTargetStrategy")
                           :params {:target_config {:write_mode "merge" :target_kind "databricks"}}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "explain_target_strategy" (get-in body [:debug :task]))))))

(deftest ai-explain-target-strategy-requires-target-config
  (let [response ((test-handler)
                  (assoc (mock/request :post "/aiExplainTargetStrategy")
                         :params {}))
        body     (response-body->map response)]
    (is (= 400 (:status response)))
    (is (re-find #"target_config" (:error body)))))

;; ---------------------------------------------------------------------------
;; P3-C: Generate Metric Glossary
;; ---------------------------------------------------------------------------

(deftest ai-generate-metric-glossary-returns-envelope
  (with-redefs [ai-assistant/generate-metric-glossary!
                (fn [_]
                  {:summary "Generated 2 metric definitions"
                   :confidence 0.85
                   :glossary [{:metric "total_trips" :definition "Count of trips"}]
                   :recommendations []
                   :open_questions []
                   :warnings []
                   :debug {:task "generate_metric_glossary" :source "deterministic_plus_ai"}})]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiGenerateMetricGlossary")
                           :params {:proposal_id "100"
                                    :proposal {:columns [{:column_name "total_trips"}]}}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "generate_metric_glossary" (get-in body [:debug :task]))))))

(deftest ai-generate-metric-glossary-requires-proposal
  (let [response ((test-handler)
                  (assoc (mock/request :post "/aiGenerateMetricGlossary")
                         :params {:proposal_id "100"}))
        body     (response-body->map response)]
    (is (= 400 (:status response)))
    (is (re-find #"proposal" (:error body)))))

;; ---------------------------------------------------------------------------
;; P3-D: Explain Run/KPI Anomaly
;; ---------------------------------------------------------------------------

(deftest ai-explain-run-or-kpi-anomaly-returns-envelope
  (with-redefs [ai-assistant/explain-run-or-kpi-anomaly!
                (fn [_]
                  {:summary "Row count doubled after schema change"
                   :confidence 0.82
                   :likely_causes ["Schema drift"]
                   :recommendations ["Verify rows are not duplicates"]
                   :open_questions []
                   :warnings []
                   :debug {:task "explain_anomaly" :source "deterministic_plus_ai"}})]
    (let [response ((test-handler)
                    (assoc (mock/request :post "/aiExplainRunOrKpiAnomaly")
                           :params {:proposal_id "100"}))
          body     (response-body->map response)]
      (is (= 200 (:status response)))
      (is (= "explain_anomaly" (get-in body [:debug :task]))))))

;; AI routes respect RBAC

(deftest ai-routes-return-403-when-rbac-enabled-and-role-missing
  (with-redefs [config/env {:bitool-rbac-enabled "true"}]
    (doseq [[route params] [["/aiExplainPreviewSchema"       {:endpoint_config {} :inferred_fields []}]
                             ["/aiSuggestBronzeKeys"          {:endpoint_config {} :inferred_fields []}]
                             ["/aiExplainModelProposal"       {:proposal_id "1" :proposal_json {}}]
                             ["/aiExplainProposalValidation"  {:proposal_id "1" :validation_result {}}]
                             ["/aiSuggestSilverTransforms"    {:proposal_id "1" :proposal {}}]
                             ["/aiGenerateSilverFromBRD"      {:brd_text "test" :source_columns []}]
                             ["/aiGenerateGoldFromBRD"        {:brd_text "test" :source_columns []}]
                             ["/aiSuggestGoldMartDesign"      {:proposal_id "1" :proposal {}}]
                             ["/aiExplainSchemaDrift"         {:event_id "1"}]
                             ["/aiSuggestDriftRemediation"    {:event_id "1"}]
                             ["/aiExplainEndpointBusinessShape" {:endpoint_config {} :inferred_fields []}]
                             ["/aiExplainTargetStrategy"      {:target_config {}}]
                             ["/aiGenerateMetricGlossary"     {:proposal_id "1" :proposal {}}]
                             ["/aiExplainRunOrKpiAnomaly"     {:proposal_id "1"}]]]
      (let [response ((test-handler)
                      (assoc (mock/request :post route)
                             :params params
                             :session {:roles ["viewer"]}))
            body (response-body->map response)]
        (is (= 403 (:status response)) (str route " should return 403"))
        (is (= "Forbidden" (:error body)) (str route " error message"))))))
