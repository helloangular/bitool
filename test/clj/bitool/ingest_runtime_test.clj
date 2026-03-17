(ns bitool.ingest-runtime-test
  (:require [bitool.connector.api :as api]
    [bitool.config :as config]
    [bitool.control-plane :as control-plane]
    [bitool.db :as db]
    [bitool.graph2 :as g2]
    [bitool.ingest.bronze :as bronze]
    [bitool.ingest.checkpoint :as checkpoint]
    [bitool.ingest.runtime :as runtime]
    [bitool.operations :as operations]
    [cheshire.core :as json]
    [clojure.core.async :as async]
    [clojure.test :refer :all]
    [next.jdbc :as jdbc]
    [next.jdbc.sql :as sql]))

(defn- base-graph
  [node-id btype]
  {:a {:name "test" :v 0 :id 99}
   :n {1       {:na {:name "O" :btype "O" :tcols {}} :e {}}
       node-id {:na {:name (str btype "-node") :btype btype :tcols {}} :e {1 {}}}}})

(def ^:private real-ensure-checkpoint-columns!
  @#'bitool.ingest.runtime/ensure-checkpoint-columns!)

(def ^:private real-ensure-batch-manifest-columns!
  @#'bitool.ingest.runtime/ensure-batch-manifest-columns!)

(use-fixtures
  :each
  (fn [f]
    (reset! @#'bitool.ingest.runtime/adaptive-backpressure-state {})
    (reset! @#'bitool.ingest.runtime/source-circuit-breaker-state {})
    (with-redefs-fn {#'bitool.ingest.runtime/connection-dbtype (fn [_] "databricks")
                     #'bitool.ingest.runtime/ensure-checkpoint-columns! (fn [& _] nil)
                     #'bitool.ingest.runtime/ensure-batch-manifest-columns! (fn [& _] nil)
                     #'bitool.ingest.runtime/ensure-bad-record-columns! (fn [& _] nil)
                     #'bitool.ingest.runtime/incomplete-manifest-rows (fn [& _] [])}
      f)))

(deftest save-api-persists-rich-endpoint-config
  (let [g  (base-graph 2 "Ap")
        g' (g2/save-api g 2 {:api_name "samara"
                             :source_system "samara"
                             :base_url "https://api.example.com"
                             :endpoint_configs [{:endpoint_name "trips"
                                                 :endpoint_url "/fleet/trips"
                                                 :load_type "incremental"
                                                 :schema_mode "infer"
                                                 :schema_enforcement_mode "strict"
                                                 :circuit_breaker_enabled true
                                                 :circuit_breaker_failure_threshold 4
                                                 :circuit_breaker_window_seconds 180
                                                 :circuit_breaker_reset_timeout_seconds 120
                                                 :bad_record_alert_ratio 0.15
                                                 :type_inference_enabled true
                                                 :pagination_strategy "cursor"
                                                 :cursor_field "endCursor"
                                                 :watermark_column "eventTime"
                                                 :primary_key_fields ["id"]
                                                 :selected_nodes ["$.data[].id" "$.data[].eventTime"]
                                                 :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}]})
        item (g2/get-api-item 2 g')]
    (is (= "samara" (:api_name (g2/getData g' 2))))
    (is (= 1 (count (:endpoint_configs (g2/getData g' 2)))))
    (is (= "incremental" (get-in (g2/getData g' 2) [:endpoint_configs 0 :load_type])))
    (is (= "infer" (get-in (g2/getData g' 2) [:endpoint_configs 0 :schema_mode])))
    (is (= "strict" (get-in (g2/getData g' 2) [:endpoint_configs 0 :schema_enforcement_mode])))
    (is (= 4 (get-in (g2/getData g' 2) [:endpoint_configs 0 :circuit_breaker_failure_threshold])))
    (is (= 0.15 (get-in (g2/getData g' 2) [:endpoint_configs 0 :bad_record_alert_ratio])))
    (is (= "https://api.example.com" (get item "base_url")))))

(deftest save-target-normalizes-job-and-list-config
  (let [g  (base-graph 2 "Tg")
        g' (g2/save-target g 2 {:connection_id "42"
                                :target_kind "databricks"
                                :catalog "sheetz_telematics"
                                :schema "bronze"
                                :table_name "samara_trips_raw"
                                :partition_columns "partition_date, load_date"
                                :merge_keys "id"
                                :cluster_by "vehicle_id, driver_id"
                                :silver_job_id "111"
                                :gold_job_id "222"
                                :options "{\"raw_payload\":true}"
                                :silver_job_params "{\"mode\":\"silver\"}"
                                :gold_job_params "{\"mode\":\"gold\"}"
                                :trigger_gold_on_success true})
        item (g2/get-target-item 2 g')]
    (is (= 42 (:connection_id (g2/getData g' 2))))
    (is (= ["partition_date" "load_date"] (:partition_columns (g2/getData g' 2))))
    (is (= "111" (get item "silver_job_id")))
    (is (= true (get-in item ["options" :raw_payload])))))

(deftest save-target-snowflake-fields-round-trip
  (let [g  (base-graph 2 "Tg")
        g' (g2/save-target g 2 {:target_kind "snowflake"
                                :connection_id "99"
                                :catalog "analytics"
                                :schema "bronze"
                                :table_name "events_raw"
                                :sf_load_method "put_copy"
                                :sf_stage_name "@my_stage"
                                :sf_warehouse "COMPUTE_WH"
                                :sf_file_format "CSV"
                                :sf_on_error "CONTINUE"
                                :sf_purge true})
        tmap (g2/getData g' 2)
        item (g2/get-target-item 2 g')]
    ;; verify persistence
    (is (= "snowflake" (:target_kind tmap)))
    (is (= "put_copy" (:sf_load_method tmap)))
    (is (= "@my_stage" (:sf_stage_name tmap)))
    (is (= "COMPUTE_WH" (:sf_warehouse tmap)))
    (is (= "CSV" (:sf_file_format tmap)))
    (is (= "CONTINUE" (:sf_on_error tmap)))
    (is (true? (:sf_purge tmap)))
    ;; verify get-target-item returns them
    (is (= "put_copy" (get item "sf_load_method")))
    (is (= "@my_stage" (get item "sf_stage_name")))
    (is (= "COMPUTE_WH" (get item "sf_warehouse")))
    (is (= "CSV" (get item "sf_file_format")))
    (is (= "CONTINUE" (get item "sf_on_error")))
    (is (true? (get item "sf_purge")))))

(deftest save-kafka-source-normalizes-topic-config
  (let [g  (base-graph 2 "Kf")
        g' (g2/save-kafka-source g 2 {:connection_id "42"
                                      :source_system "kafka"
                                      :topic_configs [{:topic_name "orders.events"
                                                       :value_deserializer "json"
                                                       :primary_key_fields "id"
                                                       :bronze_table_name "bronze.orders_raw"}]})
        item (g2/get-kafka-source-item 2 g')]
    (is (= 42 (:connection_id (g2/getData g' 2))))
    (is (= "orders.events" (get-in (g2/getData g' 2) [:topic_configs 0 :topic_name])))
    (is (= ["id"] (get-in (g2/getData g' 2) [:topic_configs 0 :primary_key_fields])))
    (is (= "orders.events" (get-in item ["topic_configs" 0 :endpoint_name])))))

(deftest save-file-source-normalizes-file-config
  (let [g  (base-graph 2 "Fs")
        g' (g2/save-file-source g 2 {:connection_id "42"
                                     :base_path "/tmp"
                                     :file_configs [{:path "orders.jsonl"
                                                     :format "jsonl"
                                                     :primary_key_fields "id"
                                                     :bronze_table_name "bronze.orders_raw"}]})
        item (g2/get-file-source-item 2 g')]
    (is (= 42 (:connection_id (g2/getData g' 2))))
    (is (= "orders.jsonl" (get-in (g2/getData g' 2) [:file_configs 0 :path])))
    (is (= "local" (get-in (g2/getData g' 2) [:file_configs 0 :transport])))
    (is (= "orders.jsonl" (get-in item ["file_configs" 0 :endpoint_name])))))

(deftest save-file-source-rejects-multi-character-delimiter
  (let [g (base-graph 2 "Fs")]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"delimiter must be a single character"
                          (g2/save-file-source g 2 {:connection_id "42"
                                                    :base_path "/tmp"
                                                    :file_configs [{:path "orders.csv"
                                                                    :format "csv"
                                                                    :delimiter "||"}]})))))

(deftest save-api-rejects-missing-primary-key
  (let [g (base-graph 2 "Ap")]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"primary_key_fields"
                          (g2/save-api g 2 {:endpoint_configs [{:endpoint_url "/fleet/trips"
                                                                :load_type "full"
                                                                :pagination_strategy "none"
                                                                :primary_key_fields []}]})))))

(deftest save-api-rejects-unsupported-pagination-strategy
  (let [g (base-graph 2 "Ap")]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unsupported pagination_strategy"
                          (g2/save-api g 2 {:endpoint_configs [{:endpoint_url "/fleet/trips"
                                                                :load_type "full"
                                                                :pagination_strategy "graph"
                                                                :primary_key_fields ["id"]}]})))))

(deftest save-api-rejects-invalid-inferred-field-config
  (let [g (base-graph 2 "Ap")]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"column_name must be a valid identifier"
                          (g2/save-api g 2 {:endpoint_configs [{:endpoint_url "/fleet/trips"
                                                                :load_type "full"
                                                                :pagination_strategy "none"
                                                                :primary_key_fields ["id"]
                                                                :inferred_fields [{:path "$.data[].id"
                                                                                   :column_name "bad-name"}]}]})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"override_type"
                          (g2/save-api g 2 {:endpoint_configs [{:endpoint_url "/fleet/trips"
                                                                :load_type "full"
                                                                :pagination_strategy "none"
                                                                :primary_key_fields ["id"]
                                                                :inferred_fields [{:path "$.data[].id"
                                                                                   :column_name "good_name"
                                                                                  :override_type "STRING); DROP TABLE bronze;"}]}]})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"sample_records must be greater than 0"
                          (g2/save-api g 2 {:endpoint_configs [{:endpoint_url "/fleet/trips"
                                                                :load_type "full"
                                                                :pagination_strategy "none"
                                                                :sample_records 0
                                                                :primary_key_fields ["id"]}]})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid JSON array for inferred_fields"
                          (g2/save-api g 2 {:endpoint_configs [{:endpoint_url "/fleet/trips"
                                                                :load_type "full"
                                                                :pagination_strategy "none"
                                                                :primary_key_fields ["id"]
                                                                :inferred_fields "{bad json"}]})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unsupported schema_enforcement_mode"
                          (g2/save-api g 2 {:endpoint_configs [{:endpoint_url "/fleet/trips"
                                                                :load_type "full"
                                                                :pagination_strategy "none"
                                                                :primary_key_fields ["id"]
                                                                :schema_enforcement_mode "chaos"}]})))))

(deftest build-page-rows-extracts-records-and-promoted-columns
  (let [page {:body {:data [{:id "t1" :eventTime "2026-03-13T09:00:00Z" :vehicleId "v1"}
                            {:id "t2" :eventTime "2026-03-13T09:05:00Z" :vehicleId "v2"}]}
              :page 1
              :state {}
              :response {:status 200}}
        endpoint {:endpoint_name "trips"
                  :selected_nodes ["$.data[].id" "$.data[].eventTime" "$.data[].vehicleId"]
                  :json_explode_rules [{:path "$.data[]"}]
                  :primary_key_fields ["id"]
                  :watermark_column "eventTime"}
        out (bronze/build-page-rows page endpoint {:run-id "r1"
                                                   :source-system "samara"
                                                   :now (java.time.Instant/parse "2026-03-13T10:00:00Z")
                                                   :request-url "https://api.example.com/fleet/trips"})]
    (is (= 2 (count (:rows out))))
    (is (= "t1" ((keyword "data_items_id") (first (:rows out)))))
    (is (= "t1" (:source_record_id (first (:rows out)))))
    (is (= "2026-03-13T09:05:00Z" (:event_time_utc (second (:rows out)))))
    (is (empty? (:bad-records out)))))

(deftest build-page-rows-supports-inferred-field-descriptors
  (let [page {:body {:data [{:id "t1" :vehicle {:id "v1"} :speed 42 :active true}]}
              :page 1
              :state {}
              :response {:status 200}}
        endpoint {:endpoint_name "trips"
                  :schema_mode "infer"
                  :inferred_fields [{:path "$.data[].id"
                                     :column_name "trip_id"
                                     :enabled true
                                     :type "STRING"
                                     :nullable false}
                                    {:path "$.data[].vehicle.id"
                                     :column_name "vehicle_id"
                                     :enabled true
                                     :type "STRING"
                                     :nullable true}
                                    {:path "$.data[].speed"
                                     :column_name "speed"
                                     :enabled true
                                     :type "INT"
                                     :nullable false}
                                    {:path "$.data[].active"
                                     :column_name "active"
                                     :enabled true
                                     :type "BOOLEAN"
                                     :nullable false}]
                  :json_explode_rules [{:path "$.data[]"}]
                  :primary_key_fields ["id"]}
        out (bronze/build-page-rows page endpoint {:run-id "r1"
                                                   :source-system "samara"
                                                   :now (java.time.Instant/parse "2026-03-13T10:00:00Z")
                                                   :request-url "https://api.example.com/fleet/trips"})]
    (is (= "t1" (:trip_id (first (:rows out)))))
    (is (= "v1" (:vehicle_id (first (:rows out)))))
    (is (= 42 (:speed (first (:rows out)))))
    (is (= true (:active (first (:rows out)))))))

(deftest build-page-rows-coerces-common-type-aliases-and-keeps-ddl-aligned
  (let [page {:body {:data [{:id "t1"
                             :distance "42.5"
                             :counter "922337203685477580"
                             :note 99}]}
              :page 1
              :state {}
              :response {:status 200}}
        endpoint {:endpoint_name "trips"
                  :schema_mode "infer"
                  :inferred_fields [{:path "$.data[].distance"
                                     :column_name "distance"
                                     :enabled true
                                     :type "STRING"
                                     :override_type "FLOAT"
                                     :nullable false}
                                    {:path "$.data[].counter"
                                     :column_name "counter"
                                     :enabled true
                                     :type "STRING"
                                     :override_type "LONG"
                                     :nullable false}
                                    {:path "$.data[].note"
                                     :column_name "note"
                                     :enabled true
                                     :type "STRING"
                                     :override_type "VARCHAR"
                                     :nullable true}]
                  :json_explode_rules [{:path "$.data[]"}]
                  :primary_key_fields ["id"]}
        out (bronze/build-page-rows page endpoint {:run-id "r1"
                                                   :source-system "samara"
                                                   :now (java.time.Instant/parse "2026-03-13T10:00:00Z")
                                                   :request-url "https://api.example.com/fleet/trips"})
        columns (bronze/bronze-columns endpoint)]
    (is (= 42.5 (:distance (first (:rows out)))))
    (is (= 922337203685477580 (:counter (first (:rows out)))))
    (is (= "99" (:note (first (:rows out)))))
    (is (= "DOUBLE" (:data_type (first (filter #(= "distance" (:column_name %)) columns)))))
    (is (= "BIGINT" (:data_type (first (filter #(= "counter" (:column_name %)) columns)))))
    (is (= "STRING" (:data_type (first (filter #(= "note" (:column_name %)) columns)))))))

(deftest build-page-rows-routes-coercion-failures-to-bad-records
  (let [page {:body {:data [{:id "t1" :distance "not-a-number"}]}
              :page 1
              :state {}
              :response {:status 200}}
        endpoint {:endpoint_name "trips"
                  :schema_mode "infer"
                  :inferred_fields [{:path "$.data[].distance"
                                     :column_name "distance"
                                     :enabled true
                                     :type "DOUBLE"
                                     :nullable false}]
                  :json_explode_rules [{:path "$.data[]"}]
                  :primary_key_fields ["id"]}
        out (bronze/build-page-rows page endpoint {:run-id "r1"
                                                   :source-system "samara"
                                                   :now (java.time.Instant/parse "2026-03-13T10:00:00Z")
                                                   :request-url "https://api.example.com/fleet/trips"})]
    (is (empty? (:rows out)))
    (is (= 1 (count (:bad-records out))))
    (is (re-find #"For input string" (:error_message (first (:bad-records out)))))))

(deftest checkpoint-window-start-applies-overlap
  (let [row {:last_successful_watermark "2026-03-13T09:00:00Z"}
        endpoint {:watermark_column "eventTime"
                  :watermark_overlap_minutes 60}]
    (is (= "2026-03-13T08:00:00Z"
           (checkpoint/window-start row endpoint (java.time.Instant/parse "2026-03-13T10:00:00Z"))))))

(deftest create-dbspec-from-id-supports-databricks
  (with-redefs [sql/get-by-id (fn [& _]
                                {:dbtype "databricks"
                                 :host "dbc.example.com"
                                 :port 443
                                 :http_path "/sql/1.0/warehouses/abc"
                                 :token "tok"
                                 :catalog "sheetz_telematics"
                                 :schema "bronze"})]
    (let [spec (db/create-dbspec-from-id 42)]
      (is (= "databricks" (:dbtype spec)))
      (is (re-find #"jdbc:databricks://" (:jdbcUrl spec)))
      (is (re-find #"ConnCatalog=sheetz_telematics" (:jdbcUrl spec))))))

(deftest create-dbspec-from-id-supports-snowflake
  (with-redefs [sql/get-by-id (fn [& _]
                                {:dbtype "snowflake"
                                 :host "acme.snowflakecomputing.com"
                                 :dbname "BITOOL"
                                 :schema "BRONZE"
                                 :warehouse "INGEST_WH"
                                 :role "TRANSFORMER"
                                 :username "svc"
                                 :password "pw"})]
    (let [spec (db/create-dbspec-from-id 42)]
      (is (= "snowflake" (:dbtype spec)))
      (is (re-find #"jdbc:snowflake://acme\.snowflakecomputing\.com/" (:jdbcUrl spec)))
      (is (re-find #"warehouse=INGEST_WH" (:jdbcUrl spec)))
      (is (= "BITOOL" (:catalog spec))))))

(deftest runtime-rejects-unsupported-pagination-strategy
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unsupported pagination strategy"
                        (#'bitool.ingest.runtime/pagination-config {:pagination_strategy "graph"}))))

(deftest runtime-rejects-missing-secret-reference
  (let [secret-ref (str "missing_" (java.util.UUID/randomUUID))]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Secret reference could not be resolved from environment"
                          (#'bitool.ingest.runtime/resolve-secret-ref secret-ref)))))

(deftest runtime-rejects-invalid-checkpoint-table-name
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"Table name must be a valid qualified identifier"
                        (#'bitool.ingest.runtime/fetch-checkpoint 1 "audit.bad;drop" "samara" "trips"))))

(deftest runtime-rejects-invalid-replace-row-table-name
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"Table name must be a valid qualified identifier"
                        (#'bitool.ingest.runtime/replace-row! 1 "audit.bad;drop" [:source_system] {:source_system "samara"}))))

(deftest runtime-rejects-invalid-replace-row-column-name
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"Column name must be a valid identifier"
                        (#'bitool.ingest.runtime/replace-row! 1 "audit.good" [:source_system "bad;drop"] {:source_system "samara"}))))

(deftest runtime-supports-postgresql-targets
  (let [endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :pagination_strategy "none"
                  :selected_nodes []
                  :primary_key_fields ["id"]
                  :bronze_table_name "audit.samara_trips_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9 :schema "audit" :table_name "samara_trips_raw"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "postgresql" :schema "audit"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [{:body {:data []}
                                                                         :response {:status 200}}
                                                                        {:stop-reason :eof
                                                                         :state {}
                                                                         :http-status 200}])
                                                :errors (async/to-chan! [])
                                                :cancel (fn [] nil)})
                     #'bitool.ingest.runtime/with-batch-commit (fn [_ f] (f))
                     #'bronze/build-page-rows (fn [& _] {:rows [] :bad-records []})
                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [& _] nil)
                     #'bitool.ingest.runtime/load-rows! (fn [& _] nil)
                     #'bitool.ingest.runtime/replace-row! (fn [& _] nil)}
      (fn []
        (is (= "success" (get-in (runtime/run-api-node! 99 2) [:results 0 :status])))))))

(deftest run-api-node-treats-freshness-write-failures-as-non-fatal
  (let [endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :pagination_strategy "none"
                  :selected_nodes []
                  :primary_key_fields ["id"]
                  :bronze_table_name "audit.samara_trips_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9 :schema "audit" :table_name "samara_trips_raw"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "postgresql" :schema "audit"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [{:body {:data []}
                                                                         :response {:status 200}}
                                                                        {:stop-reason :eof
                                                                         :state {}
                                                                         :http-status 200}])
                                                :errors (async/to-chan! [])
                                                :cancel (fn [] nil)})
                     #'bitool.ingest.runtime/with-batch-commit (fn [_ f] (f))
                     #'bronze/build-page-rows (fn [& _] {:rows [] :bad-records []})
                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [& _] nil)
                     #'bitool.ingest.runtime/load-rows! (fn [& _] nil)
                     #'bitool.ingest.runtime/replace-row! (fn [& _] nil)
                     #'operations/record-endpoint-freshness! (fn [& _]
                                                               (throw (ex-info "freshness write failed" {})))}
      (fn []
        (is (= "success" (get-in (runtime/run-api-node! 99 2) [:results 0 :status])))))))

(deftest run-api-node-rejects-missing-endpoint-name
  (let [api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [{:endpoint_name "trips"
                                      :enabled true}
                                     {:endpoint_name "vehicles"
                                      :enabled true}]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9})}
      (fn []
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"No enabled endpoint config found"
                              (runtime/run-api-node! 99 2 {:endpoint-name "missing"})))))))

(deftest run-api-node-failure-preserves-successful-checkpoint
  (let [captured-checkpoint (atom nil)
        endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :pagination_strategy "cursor"
                  :cursor_field "endCursor"
                  :cursor_param "cursor"
                  :selected_nodes []
                  :primary_key_fields ["id"]
                  :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        existing-checkpoint {:source_system "samara"
                             :endpoint_name "trips"
                             :last_successful_watermark "2026-03-13T09:00:00Z"
                             :last_successful_cursor "prev-cursor"
                             :last_successful_run_id "run-old"
                             :rows_ingested 10}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9 :catalog "sheetz_telematics" :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] existing-checkpoint)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [{:stop-reason :server-error
                                                                         :state {:cursor "attempted-cursor"}
                                                                         :http-status 500}])
                                                :errors (async/to-chan! [{:type :server-error}])
                                                :cancel (fn [] nil)})
                     #'bronze/build-page-rows (fn [& _] {:rows [] :bad-records []})
                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [& _] nil)
                     #'bitool.ingest.runtime/load-rows! (fn [& _] nil)
                     #'bitool.ingest.runtime/replace-row! (fn [_ _ _ row] (reset! captured-checkpoint row))}
      (fn []
        (let [out (runtime/run-api-node! 99 2)
              row @captured-checkpoint]
          (is (= "failed" (get-in out [:results 0 :status])))
          (is (= "failed" (:last_status row)))
          (is (= "prev-cursor" (:last_successful_cursor row)))
          (is (= "2026-03-13T09:00:00Z" (:last_successful_watermark row)))
          (is (= "attempted-cursor" (:last_attempted_cursor row))))))))

(deftest run-api-node-all-endpoints-continues-when-one-endpoint-throws
  (let [endpoint-ok {:endpoint_name "trips"
                     :endpoint_url "/fleet/trips"
                     :enabled true
                     :pagination_strategy "none"
                     :selected_nodes []
                     :primary_key_fields ["id"]
                     :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        endpoint-fail {:endpoint_name "vehicles"
                       :endpoint_url "/fleet/vehicles"
                       :enabled true
                       :pagination_strategy "none"
                       :selected_nodes []
                       :primary_key_fields ["id"]
                       :bronze_table_name "sheetz_telematics.bronze.samara_vehicles_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint-ok endpoint-fail]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [])
                                                :errors (async/to-chan! [])
                                                :cancel (fn [] nil)})
                     #'bitool.ingest.runtime/collect-schema-sample! (fn [_ _]
                                                                      {:sample-pages []
                                                                       :terminal-msg nil})
                     #'bitool.ingest.runtime/maybe-infer-endpoint-fields (fn [endpoint _] endpoint)
                     #'bitool.ingest.runtime/ensure-unique-field-column-names! identity
                     #'bitool.ingest.runtime/process-endpoint-stream! (fn [_ _ endpoint _ _ _]
                                                                        (if (= "vehicles" (:endpoint_name endpoint))
                                                                          (throw (ex-info "boom"
                                                                                          {:failure_class "endpoint_boom"}))
                                                                          {:pages-fetched 1
                                                                           :rows-extracted 1
                                                                           :rows-written 1
                                                                           :bad-records-total 0
                                                                           :batch-seq 1
                                                                           :manifests []
                                                                           :retry-count 0
                                                                           :changed-partition-dates []}))
                     #'bitool.ingest.runtime/load-rows! (fn [& _] nil)
                     #'bitool.ingest.runtime/replace-row! (fn [& _] nil)
                     #'operations/record-endpoint-freshness! (fn [& _] nil)}
      (fn []
        (let [out (runtime/run-api-node! 99 2)
              statuses (into {} (map (juxt :endpoint_name :status) (:results out)))]
          (is (= "partial_success" (:status out)))
          (is (= "success" (get statuses "trips")))
          (is (= "failed" (get statuses "vehicles")))
          (is (= "endpoint_boom" (get-in (first (filter #(= "vehicles" (:endpoint_name %)) (:results out)))
                                         [:failure_class]))))))))

(deftest run-api-node-targeted-endpoint-throws-instead-of-isolating-failure
  (let [endpoint-ok {:endpoint_name "trips"
                     :endpoint_url "/fleet/trips"
                     :enabled true
                     :pagination_strategy "none"
                     :selected_nodes []
                     :primary_key_fields ["id"]
                     :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        endpoint-fail {:endpoint_name "vehicles"
                       :endpoint_url "/fleet/vehicles"
                       :enabled true
                       :pagination_strategy "none"
                       :selected_nodes []
                       :primary_key_fields ["id"]
                       :bronze_table_name "sheetz_telematics.bronze.samara_vehicles_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint-ok endpoint-fail]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [])
                                                :errors (async/to-chan! [])
                                                :cancel (fn [] nil)})
                     #'bitool.ingest.runtime/collect-schema-sample! (fn [_ _]
                                                                      {:sample-pages []
                                                                       :terminal-msg nil})
                     #'bitool.ingest.runtime/maybe-infer-endpoint-fields (fn [endpoint _] endpoint)
                     #'bitool.ingest.runtime/ensure-unique-field-column-names! identity
                     #'bitool.ingest.runtime/process-endpoint-stream! (fn [_ _ endpoint _ _ _]
                                                                        (if (= "vehicles" (:endpoint_name endpoint))
                                                                          (throw (ex-info "boom"
                                                                                          {:failure_class "endpoint_boom"}))
                                                                          {:pages-fetched 1
                                                                           :rows-extracted 1
                                                                           :rows-written 1
                                                                           :bad-records-total 0
                                                                           :batch-seq 1
                                                                           :manifests []
                                                                           :retry-count 0
                                                                           :changed-partition-dates []}))
                     #'bitool.ingest.runtime/load-rows! (fn [& _] nil)
                     #'bitool.ingest.runtime/replace-row! (fn [& _] nil)
                     #'operations/record-endpoint-freshness! (fn [& _] nil)}
      (fn []
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"boom"
                              (runtime/run-api-node! 99 2 {:endpoint-name "vehicles"})))))))

(deftest run-api-node-resumes-from-checkpoint-cursor-and-persists-last-cursor
  (let [captured-fetch-opts (atom nil)
        captured-checkpoint (atom nil)
        endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :pagination_strategy "cursor"
                  :cursor_field "endCursor"
                  :cursor_param "cursor"
                  :selected_nodes []
                  :primary_key_fields ["id"]
                  :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9 :catalog "sheetz_telematics" :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] {:source_system "samara"
                                                                          :endpoint_name "trips"
                                                                          :last_successful_cursor "resume-cursor"})
                     #'api/fetch-paged-async (fn [opts]
                                               (reset! captured-fetch-opts opts)
                                               {:pages (async/to-chan! [{:body {:data []}
                                                                         :state {:last-cursor "next-cursor"}
                                                                         :response {:status 200}}
                                                                        {:stop-reason :eof
                                                                         :state {:last-cursor "next-cursor"}
                                                                         :http-status 200}])
                                                :errors (async/to-chan! [])
                                                :cancel (fn [] nil)})
                     #'bronze/build-page-rows (fn [& _] {:rows [] :bad-records []})
                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [& _] nil)
                     #'bitool.ingest.runtime/load-rows! (fn [& _] nil)
                     #'bitool.ingest.runtime/replace-row! (fn [_ _ _ row] (reset! captured-checkpoint row))}
      (fn []
        (runtime/run-api-node! 99 2)
        (is (= "resume-cursor" (get-in @captured-fetch-opts [:query-builder :cursor])))
        (is (= "resume-cursor" (get-in @captured-fetch-opts [:initial-state :cursor])))
        (is (= "next-cursor" (:last_successful_cursor @captured-checkpoint)))
        (is (= "next-cursor" (:last_attempted_cursor @captured-checkpoint)))))))

(deftest run-api-node-opens-and-recovers-source-circuit-breaker
  (let [fetch-count (atom 0)
        endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :pagination_strategy "none"
                  :selected_nodes []
                  :primary_key_fields ["id"]
                  :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"
                  :circuit_breaker_failure_threshold 1
                  :circuit_breaker_reset_timeout_seconds 60}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}
        breaker-key "samara::trips"]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9 :catalog "sheetz_telematics" :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               (swap! fetch-count inc)
                                               (if (= 1 @fetch-count)
                                                 {:pages (async/to-chan! [{:stop-reason :server-error
                                                                           :state {}
                                                                           :http-status 503}])
                                                  :errors (async/to-chan! [{:type :server-error :status 503}])
                                                  :cancel (fn [] nil)}
                                                 {:pages (async/to-chan! [{:body {:data []}
                                                                           :response {:status 200}}
                                                                          {:stop-reason :eof
                                                                           :state {}
                                                                           :http-status 200}])
                                                  :errors (async/to-chan! [])
                                                  :cancel (fn [] nil)}))
                     #'bronze/build-page-rows (fn [& _] {:rows [] :bad-records []})
                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [& _] nil)
                     #'bitool.ingest.runtime/load-rows! (fn [& _] nil)
                     #'bitool.ingest.runtime/replace-row! (fn [& _] nil)}
      (fn []
        (let [first-run (runtime/run-api-node! 99 2)]
          (is (= "failed" (get-in first-run [:results 0 :status])))
          (is (= "open" (get-in (#'bitool.ingest.runtime/circuit-state-summary "samara" endpoint) [:state]))))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"circuit breaker is open"
                              (runtime/run-api-node! 99 2)))
        (is (= 1 @fetch-count))
        (swap! @#'bitool.ingest.runtime/source-circuit-breaker-state
               update breaker-key assoc :open-until-ms 0)
        (let [recovered-run (runtime/run-api-node! 99 2)]
          (is (= "success" (get-in recovered-run [:results 0 :status])))
          (is (= 2 @fetch-count))
          (is (= "closed" (get-in (#'bitool.ingest.runtime/circuit-state-summary "samara" endpoint) [:state]))))))))

(deftest api-observability-summary-includes-circuit-breaker-and-endpoint-thresholds
  (let [endpoint {:endpoint_name "trips"
                  :enabled true
                  :freshness_sla_seconds 100
                  :bad_record_alert_ratio 0.2
                  :retry_volume_alert_24h 7
                  :replay_failure_alert_7d 3}
        api-node {:source_system "samara"
                  :endpoint_configs [endpoint]}
        captured (atom {})
        checkpoint-row {:updated_at_utc (str (java.time.Instant/now))}
        latest-run {:run_id "run-1"
                    :status "succeeded"
                    :started_at_utc "2026-03-15T00:00:00Z"
                    :finished_at_utc "2026-03-15T00:10:00Z"
                    :rows_written 10
                    :retry_count 9}
        manifests [{:run_id "run-1"
                    :status "committed"
                    :row_count 8
                    :bad_record_count 4
                    :started_at_utc "2026-03-15T00:00:00Z"
                    :committed_at_utc "2026-03-15T00:01:00Z"}]]
    (reset! @#'bitool.ingest.runtime/source-circuit-breaker-state
            {"samara::trips" {:state :open
                               :events [{:at_ms (System/currentTimeMillis) :failure? true}]
                               :open-until-ms (+ (System/currentTimeMillis) 60000)
                               :last-failure-class "rate_limited"}})
    (with-redefs [db/getGraph (fn [_] {})
                  g2/getData (fn [_ _] api-node)
                  bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9 :catalog "sheetz_telematics" :schema "bronze"})
                  bitool.ingest.runtime/sql-opts (fn [_] :ignored)
                  bitool.ingest.runtime/fetch-checkpoint (fn [& _] checkpoint-row)
                  bitool.ingest.runtime/query-manifest-rows (fn [_ _ opts]
                                                              (swap! captured assoc :manifest-opts opts)
                                                              manifests)
                  jdbc/execute-one! (fn [_ [sql & params]]
                                      (cond
                                        (re-find #"ORDER BY started_at_utc DESC LIMIT 1" sql)
                                        (do
                                          (swap! captured assoc :latest-run-params params)
                                          latest-run)

                                        (re-find #"SUM\\(retry_count\\)" sql)
                                        {:retry_volume 9}

                                        (re-find #"COUNT\\(\\*\\) AS cnt" sql)
                                        {:cnt 0}

                                        :else nil))
                  jdbc/execute! (fn [_ _] [{:status "failed" :cnt 2}])]
      (let [summary (runtime/api-observability-summary 99 2 {:endpoint-name "trips"})]
        (is (= "samara" (:source-system (:manifest-opts @captured))))
        (is (= ["samara" "trips"] (:latest-run-params @captured)))
        (is (= "open" (get-in summary [:circuit_breaker :state])))
        (is (= 2 (count (:alerts summary))))
        (is (= #{"high_bad_record_ratio" "source_circuit_open"}
               (set (map :code (:alerts summary)))))))))

(deftest run-api-node-triggers-databricks-jobs-when-configured
  (let [captured-job-call (atom nil)
        endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :pagination_strategy "none"
                  :selected_nodes []
                  :primary_key_fields ["id"]
                  :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}
        target {:connection_id 9
                :catalog "sheetz_telematics"
                :schema "bronze"
                :silver_job_id "111"}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] target)
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [{:body {:data []}
                                                                         :response {:status 200}}
                                                                        {:stop-reason :eof
                                                                         :state {}
                                                                         :http-status 200}])
                                                :errors (async/to-chan! [])
                                                :cancel (fn [] nil)})
                     #'bronze/build-page-rows (fn [& _] {:rows [{:partition_date "2026-03-13"}] :bad-records []})
                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [& _] nil)
                     #'bitool.ingest.runtime/load-rows! (fn [& _] nil)
                     #'bitool.ingest.runtime/replace-row! (fn [& _] nil)
                     #'bitool.databricks.jobs/trigger-job! (fn [_ job-id params]
                                                             (reset! captured-job-call [job-id params])
                                                             {:job_id job-id :run_id 123})}
      (fn []
        (let [out (runtime/run-api-node! 99 2)]
          (is (= "111" (first @captured-job-call)))
          (is (= "sheetz_telematics.bronze.samara_trips_raw" (get-in @captured-job-call [1 :bronze_table])))
          (is (= 123 (get-in out [:results 0 :job_triggers :silver :run_id]))))))))

(deftest run-api-node-uses-committed-manifest-partitions-for-downstream-trigger
  (let [captured-job-call (atom nil)
        endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :pagination_strategy "none"
                  :selected_nodes []
                  :primary_key_fields ["id"]
                  :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}
        target {:connection_id 9
                :catalog "sheetz_telematics"
                :schema "bronze"
                :silver_job_id "111"}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] target)
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [])
                                                :errors (async/to-chan! [])
                                                :cancel (fn [] nil)})
                     #'bitool.ingest.runtime/collect-schema-sample! (fn [& _]
                                                                      {:sample-pages []
                                                                       :terminal-msg nil})
                     #'bitool.ingest.runtime/process-endpoint-stream! (fn [& _]
                                                                        {:batch-seq 1
                                                                         :checkpoint-row nil
                                                                         :max-watermark nil
                                                                         :next-cursor nil
                                                                         :rows-extracted 10
                                                                         :rows-written 10
                                                                         :bad-records-total 0
                                                                         :bad-records-written 0
                                                                         :pages-fetched 1
                                                                         :retry-count 0
                                                                         :last-http-status 200
                                                                         :changed-partition-dates ["2026-03-13"]
                                                                         :manifests [{:status "committed"
                                                                                      :active true
                                                                                      :partition_dates_json "[\"2026-03-14\"]"}]})
                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [& _] nil)
                     #'bitool.ingest.runtime/load-rows! (fn [& _] nil)
                     #'bitool.ingest.runtime/replace-row! (fn [& _] nil)
                     #'operations/record-endpoint-freshness! (fn [& _] nil)
                     #'bitool.databricks.jobs/trigger-job! (fn [_ job-id params]
                                                             (reset! captured-job-call [job-id params])
                                                             {:job_id job-id :run_id 123})}
      (fn []
        (runtime/run-api-node! 99 2)
        (is (= "111" (first @captured-job-call)))
        (is (= ["2026-03-14"] (get-in @captured-job-call [1 :changed_partition_dates])))))))

(deftest run-api-node-inferrs-fields-for-infer-schema-mode
  (let [captured-columns (atom nil)
        endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :schema_mode "infer"
                  :sample_records 10
                  :max_inferred_columns 10
                  :type_inference_enabled true
                  :pagination_strategy "none"
                  :json_explode_rules [{:path "$.data[]"}]
                  :primary_key_fields ["id"]
                  :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9 :catalog "sheetz_telematics" :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [_ table-name columns _]
                                                             (when (= table-name "sheetz_telematics.bronze.samara_trips_raw")
                                                               (reset! captured-columns columns)))
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [{:body {:data [{:id "t1" :vehicle {:id "v1"}}]}
                                                                         :response {:status 200}}
                                                                        {:stop-reason :eof
                                                                         :state {}
                                                                         :http-status 200}])
                                                :errors (async/to-chan! [])
                                                :cancel (fn [] nil)})
                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [& _] nil)
                     #'bitool.ingest.runtime/load-rows! (fn [& _] nil)
                     #'bitool.ingest.runtime/replace-row! (fn [& _] nil)}
      (fn []
        (let [out (runtime/run-api-node! 99 2)]
          (is (some #(= "data_items_id" (:column_name %)) @captured-columns))
          (is (some #(= "data_items_vehicle_id" (:column_name %)) @captured-columns))
          (is (= "data_items_id" (get-in out [:results 0 :inferred_fields 0 :column_name]))))))))

(deftest run-api-node-additive-schema-evolution-merges-new-fields
  (let [captured-columns (atom nil)
        endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :schema_mode "infer"
                  :schema_evolution_mode "additive"
                  :sample_records 10
                  :max_inferred_columns 10
                  :type_inference_enabled true
                  :pagination_strategy "none"
                  :json_explode_rules [{:path "$.data[]"}]
                  :primary_key_fields ["id"]
                  :inferred_fields [{:path "$.data[].id"
                                     :column_name "trip_id"
                                     :enabled true
                                     :type "STRING"
                                     :nullable false}]
                  :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9 :catalog "sheetz_telematics" :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [_ table-name columns _]
                                                             (when (= table-name "sheetz_telematics.bronze.samara_trips_raw")
                                                               (reset! captured-columns columns)))
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [{:body {:data [{:id "t1" :vehicle {:id "v1"}}]}
                                                                         :response {:status 200}}
                                                                        {:stop-reason :eof
                                                                         :state {}
                                                                         :http-status 200}])
                                                :errors (async/to-chan! [])
                                                :cancel (fn [] nil)})
                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [& _] nil)
                     #'bitool.ingest.runtime/load-rows! (fn [& _] nil)
                     #'bitool.ingest.runtime/replace-row! (fn [& _] nil)}
      (fn []
        (let [out (runtime/run-api-node! 99 2)]
          (is (some #(= "trip_id" (:column_name %)) @captured-columns))
          (is (some #(= "data_items_vehicle_id" (:column_name %)) @captured-columns))
          (is (some #(= "$.data[].vehicle.id" (:path %))
                    (get-in out [:results 0 :schema_drift :new_fields])))
          (is (some #(= "trip_id" (:column_name %))
                    (get-in out [:results 0 :inferred_fields]))))))))

(deftest run-api-node-strict-schema-enforcement-rejects-drift
  (let [endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :schema_mode "infer"
                  :schema_enforcement_mode "strict"
                  :sample_records 10
                  :max_inferred_columns 10
                  :type_inference_enabled true
                  :pagination_strategy "none"
                  :json_explode_rules [{:path "$.data[]"}]
                  :primary_key_fields ["id"]
                  :inferred_fields [{:path "$.data[].id"
                                     :column_name "trip_id"
                                     :enabled true
                                     :type "STRING"
                                     :nullable false}]
                  :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [{:body {:data [{:id "t1" :vehicle {:id "v1"}}]}
                                                                         :response {:status 200}}
                                                                        {:stop-reason :eof
                                                                         :state {}
                                                                         :http-status 200}])
                                                :errors (async/to-chan! [])
                                                :cancel (fn [] nil)})}
      (fn []
        (try
          (runtime/run-api-node! 99 2)
          (is false "Expected strict schema enforcement failure")
          (catch clojure.lang.ExceptionInfo e
            (is (= "schema_drift" (:failure_class (ex-data e))))
            (is (= "strict" (:schema_enforcement_mode (ex-data e))))
            (is (re-find #"strict enforcement" (ex-message e)))))))))

(deftest run-api-node-permissive-schema-enforcement-records-drift-without-changing-fields
  (let [captured-columns (atom nil)
        endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :schema_mode "infer"
                  :schema_enforcement_mode "permissive"
                  :sample_records 10
                  :max_inferred_columns 10
                  :type_inference_enabled true
                  :pagination_strategy "none"
                  :json_explode_rules [{:path "$.data[]"}]
                  :primary_key_fields ["id"]
                  :inferred_fields [{:path "$.data[].id"
                                     :column_name "trip_id"
                                     :enabled true
                                     :type "STRING"
                                     :nullable false}]
                  :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [_ table-name columns _]
                                                             (when (= table-name "sheetz_telematics.bronze.samara_trips_raw")
                                                               (reset! captured-columns columns)))
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [{:body {:data [{:id "t1" :vehicle {:id "v1"}}]}
                                                                         :response {:status 200}}
                                                                        {:stop-reason :eof
                                                                         :state {}
                                                                         :http-status 200}])
                                                :errors (async/to-chan! [])
                                                :cancel (fn [] nil)})
                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [& _] nil)
                     #'bitool.ingest.runtime/load-rows! (fn [& _] nil)
                     #'bitool.ingest.runtime/replace-row! (fn [& _] nil)}
      (fn []
        (let [out (runtime/run-api-node! 99 2)]
          (is (some #(= "trip_id" (:column_name %)) @captured-columns))
          (is (not-any? #(= "data_items_vehicle_id" (:column_name %)) @captured-columns))
          (is (= ["trip_id"]
                 (mapv :column_name (get-in out [:results 0 :inferred_fields]))))
          (is (some #(= "$.data[].vehicle.id" (:path %))
                    (get-in out [:results 0 :schema_drift :new_fields]))))))))

(deftest run-api-node-additive-schema-enforcement-widens-compatible-types
  (let [endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :schema_mode "infer"
                  :schema_enforcement_mode "additive"
                  :sample_records 10
                  :max_inferred_columns 10
                  :type_inference_enabled true
                  :pagination_strategy "none"
                  :json_explode_rules [{:path "$.data[]"}]
                  :primary_key_fields ["id"]
                  :inferred_fields [{:path "$.data[].speed"
                                     :column_name "speed"
                                     :enabled true
                                     :type "INT"
                                     :nullable false}]
                  :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [{:body {:data [{:id "t1" :speed 42.5}]}
                                                                         :response {:status 200}}
                                                                        {:stop-reason :eof
                                                                         :state {}
                                                                         :http-status 200}])
                                                :errors (async/to-chan! [])
                                                :cancel (fn [] nil)})
                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [& _] nil)
                     #'bitool.ingest.runtime/load-rows! (fn [& _] nil)
                     #'bitool.ingest.runtime/replace-row! (fn [& _] nil)}
      (fn []
        (let [out (runtime/run-api-node! 99 2)]
          (is (= "DOUBLE"
                 (->> (get-in out [:results 0 :inferred_fields])
                      (filter #(= "speed" (:column_name %)))
                      first
                      :type))))))))

(deftest run-api-node-additive-schema-enforcement-rejects-narrowing-type-changes
  (let [endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :schema_mode "infer"
                  :schema_enforcement_mode "additive"
                  :sample_records 10
                  :max_inferred_columns 10
                  :type_inference_enabled true
                  :pagination_strategy "none"
                  :json_explode_rules [{:path "$.data[]"}]
                  :primary_key_fields ["id"]
                  :inferred_fields [{:path "$.data[].speed"
                                     :column_name "speed"
                                     :enabled true
                                     :type "DOUBLE"
                                     :nullable false}]
                  :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [{:body {:data [{:id "t1" :speed 42}]}
                                                                         :response {:status 200}}
                                                                        {:stop-reason :eof
                                                                         :state {}
                                                                         :http-status 200}])
                                                :errors (async/to-chan! [])
                                                :cancel (fn [] nil)})}
      (fn []
        (try
          (runtime/run-api-node! 99 2)
          (is false "Expected additive narrowing rejection")
          (catch clojure.lang.ExceptionInfo e
            (is (= "schema_drift" (:failure_class (ex-data e))))
            (is (= "additive" (:schema_enforcement_mode (ex-data e))))
            (is (re-find #"incompatible type change" (ex-message e)))))))))

(deftest run-api-node-persists-endpoint-schema-snapshot
  (let [snapshot-rows (atom [])
        endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :schema_mode "infer"
                  :schema_enforcement_mode "additive"
                  :sample_records 10
                  :max_inferred_columns 10
                  :type_inference_enabled true
                  :pagination_strategy "none"
                  :json_explode_rules [{:path "$.data[]"}]
                  :primary_key_fields ["id"]
                  :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {:a {:id 99 :v 7}})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [{:body {:data [{:id "t1" :vehicle {:id "v1"}}]}
                                                                         :response {:status 200}}
                                                                        {:stop-reason :eof
                                                                         :state {}
                                                                         :http-status 200}])
                                                :errors (async/to-chan! [])
                                                :cancel (fn [] nil)})
                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [& _] nil)
                     #'bitool.ingest.runtime/load-rows! (fn [_ table rows]
                                                          (when (re-find #"endpoint_schema_snapshot$" table)
                                                            (swap! snapshot-rows into rows)))
                     #'bitool.ingest.runtime/replace-row! (fn [& _] nil)}
      (fn []
        (runtime/run-api-node! 99 2)
        (is (= 1 (count @snapshot-rows)))
        (let [row (first @snapshot-rows)
              inferred (json/parse-string (:inferred_fields_json row) true)]
          (is (= 99 (:graph_id row)))
          (is (= 2 (:api_node_id row)))
          (is (= 7 (:graph_version row)))
          (is (= "trips" (:endpoint_name row)))
          (is (= "additive" (:schema_enforcement_mode row)))
          (is (= 1 (:sample_record_count row)))
          (is (some #(= "$.data[].vehicle.id" (:path %)) inferred)))))))

(deftest run-api-node-streams-batches-and-persists-manifests
  (let [bronze-loads      (atom [])
        manifest-rows     (atom [])
        checkpoint-rows   (atom [])
        delete-calls      (atom [])
        bronze-table      "sheetz_telematics.bronze.samara_trips_raw"
        manifest-table    "sheetz_telematics.audit.run_batch_manifest"
        checkpoint-table  "sheetz_telematics.audit.ingestion_checkpoint"
        endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :pagination_strategy "none"
                  :selected_nodes []
                  :primary_key_fields ["id"]
                  :bronze_table_name bronze-table}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/parse-int-env (fn [k default]
                                                             (case k
                                                               :worker-batch-rows 1
                                                               :worker-max-batch-bytes 1048576
                                                               default))
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [{:body {:data [{:id "t1"}]}
                                                                         :page 1
                                                                         :state {}
                                                                         :response {:status 200}}
                                                                        {:body {:data [{:id "t2"}]}
                                                                         :page 2
                                                                         :state {:last-cursor "cursor-2"}
                                                                         :response {:status 200}}
                                                                        {:stop-reason :eof
                                                                         :state {:last-cursor "cursor-2"}
                                                                         :http-status 200}])
                                                :errors (async/to-chan! [])
                                                :cancel (fn [] nil)})
                     #'bronze/build-page-rows (fn [page _ _]
                                                {:rows [{:partition_date "2026-03-13"
                                                         :event_time_utc (if (= 1 (:page page))
                                                                           "2026-03-13T10:00:00Z"
                                                                           "2026-03-13T10:05:00Z")
                                                         :payload_json (str "{\"page\":" (:page page) "}")}]
                                                 :bad-records []})
                     #'bitool.ingest.runtime/persist-batch-artifact! (fn [_ _ _ batch-id _]
                                                                       {:artifact_path (str "tmp/" batch-id ".json")
                                                                        :artifact_checksum (str "sum-" batch-id)})
                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [_ table column value]
                                                                      (swap! delete-calls conj [table column value]))
                     #'bitool.ingest.runtime/load-rows! (fn [_ table rows]
                                                          (when (= table bronze-table)
                                                            (swap! bronze-loads conj rows)))
                     #'bitool.ingest.runtime/replace-row! (fn [_ table _ row]
                                                            (cond
                                                              (= table manifest-table) (swap! manifest-rows conj row)
                                                              (= table checkpoint-table) (swap! checkpoint-rows conj row)
                                                              :else nil))}
      (fn []
        (let [out (runtime/run-api-node! 99 2)]
          (is (= 2 (get-in out [:results 0 :batch_count])))
          (is (= 2 (count @bronze-loads)))
          (is (= 2 (count (filter #(= "committed" (:status %)) @manifest-rows))))
          (is (= 2 (count @checkpoint-rows)))
          (is (= [1 2]
                 (->> @manifest-rows
                      (filter #(= "committed" (:status %)))
                      (mapv :batch_seq))))
          (is (= nil (:last_successful_cursor (first @checkpoint-rows))))
          (is (= "cursor-2" (:last_successful_cursor (second @checkpoint-rows))))
          (is (= 4 (count @delete-calls))))))))

(deftest run-api-node-does-not-advance-zero-row-checkpoint-before-run-detail
  (let [checkpoint-rows  (atom [])
        run-detail-table "sheetz_telematics.audit.endpoint_run_detail"
        checkpoint-table "sheetz_telematics.audit.ingestion_checkpoint"
        endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :pagination_strategy "none"
                  :selected_nodes []
                  :primary_key_fields ["id"]
                  :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/ensure-checkpoint-columns! (fn [& _] nil)
                     #'bitool.ingest.runtime/ensure-batch-manifest-columns! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_] {:pages nil :errors nil :cancel (fn [] nil)})
                     #'bitool.ingest.runtime/collect-schema-sample! (fn [& _] {:sample-pages [] :terminal-msg nil})
                     #'bitool.ingest.runtime/collect-errors! (fn [& _] [])
                     #'bitool.ingest.runtime/abort-preparing-batches! (fn [& _] nil)
                     #'bitool.ingest.runtime/process-endpoint-stream! (fn [& _]
                                                                        {:batch-seq 0
                                                                         :checkpoint-row nil
                                                                         :max-watermark nil
                                                                         :next-cursor "cursor-1"
                                                                         :rows-extracted 0
                                                                         :rows-written 0
                                                                         :bad-records-total 0
                                                                         :bad-records-written 0
                                                                         :pages-fetched 1
                                                                         :retry-count 0
                                                                         :last-http-status 200
                                                                         :changed-partition-dates []
                                                                         :manifests []})
                     #'bitool.ingest.runtime/load-rows! (fn [_ table _]
                                                          (when (= table run-detail-table)
                                                            (throw (ex-info "run detail write failed" {:table table}))))
                     #'bitool.ingest.runtime/replace-row! (fn [_ table _ row]
                                                            (when (= table checkpoint-table)
                                                              (swap! checkpoint-rows conj row)))}
      (fn []
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"run detail write failed"
                              (runtime/run-api-node! 99 2)))
        (is (empty? @checkpoint-rows))))))

(deftest run-api-node-fails-fast-on-normalized-column-collisions
  (let [endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :schema_mode "infer"
                  :sample_records 10
                  :max_inferred_columns 10
                  :type_inference_enabled true
                  :pagination_strategy "none"
                  :json_explode_rules [{:path "$.data[]"}]
                  :primary_key_fields ["id"]
                  :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [{:body {:data [{"a-b" 1 "a_b" 2}]}
                                                                         :response {:status 200}}
                                                                        {:stop-reason :eof
                                                                         :state {}
                                                                         :http-status 200}])
                                                :errors (async/to-chan! [])
                                                :cancel (fn [] nil)})}
      (fn []
        (try
          (runtime/run-api-node! 99 2)
          (is false "Expected schema-drift duplicate column failure")
          (catch clojure.lang.ExceptionInfo e
            (is (= "schema_drift" (:failure_class (ex-data e))))
            (is (re-find #"duplicate enabled column_name values" (ex-message e)))))))))

(deftest run-api-node-fails-fast-on-reserved-bronze-column-collisions
  (let [endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :pagination_strategy "none"
                  :selected_nodes ["$.run_id"]
                  :primary_key_fields ["id"]
                  :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [{:body {:run_id "r1"}
                                                                         :response {:status 200}}
                                                                        {:stop-reason :eof
                                                                         :state {}
                                                                         :http-status 200}])
                                                :errors (async/to-chan! [])
                                                :cancel (fn [] nil)})}
      (fn []
        (try
          (runtime/run-api-node! 99 2)
          (is false "Expected reserved Bronze column_name failure")
          (catch clojure.lang.ExceptionInfo e
            (is (= "schema_drift" (:failure_class (ex-data e))))
            (is (re-find #"reserved Bronze column_name values" (ex-message e)))))))))

(deftest run-api-node-cancels-fetcher-when-post-fetch-validation-throws
  (let [cancelled? (atom false)
        endpoint   {:endpoint_name "trips"
                    :endpoint_url "/fleet/trips"
                    :enabled true
                    :pagination_strategy "none"
                    :selected_nodes ["$.data[].id"]
                    :json_explode_rules [{:path "$.data[]"}]
                    :primary_key_fields ["id"]
                    :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node   {:source_system "samara"
                    :base_url "https://api.example.com"
                    :auth_ref {}
                    :endpoint_configs [endpoint]}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [_ table-name _ _]
                                                             (when (= table-name "sheetz_telematics.bronze.samara_trips_raw")
                                                               (throw (ex-info "table create failed" {}))))
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [{:body {:data [{:id "t1"}]}
                                                                         :response {:status 200}}
                                                                        {:stop-reason :eof
                                                                         :state {}
                                                                         :http-status 200}])
                                                :errors (async/to-chan! [])
                                                :cancel (fn []
                                                          (reset! cancelled? true)
                                                          true)})}
      (fn []
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"table create failed"
                              (runtime/run-api-node! 99 2)))
        (is (true? @cancelled?))))))

(deftest run-api-node-drains-errors-when-post-fetch-validation-throws-early
  (let [cancelled? (atom false)
        errors-ch  (async/chan 2)
        endpoint   {:endpoint_name "trips"
                    :endpoint_url "/fleet/trips"
                    :enabled true
                    :pagination_strategy "none"
                    :selected_nodes ["$.data[].id"]
                    :json_explode_rules [{:path "$.data[]"}]
                    :primary_key_fields ["id"]
                    :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node   {:source_system "samara"
                    :base_url "https://api.example.com"
                    :auth_ref {}
                    :endpoint_configs [endpoint]}]
    (async/>!! errors-ch {:type :producer-warning :message "buffered error"})
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [_ table-name _ _]
                                                             (when (= table-name "sheetz_telematics.bronze.samara_trips_raw")
                                                               (throw (ex-info "table create failed" {}))))
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               {:pages (async/to-chan! [{:body {:data [{:id "t1"}]}
                                                                         :response {:status 200}}
                                                                        {:stop-reason :eof
                                                                         :state {}
                                                                         :http-status 200}])
                                                :errors errors-ch
                                                :cancel (fn []
                                                          (reset! cancelled? true)
                                                          true)})}
      (fn []
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"table create failed"
                              (runtime/run-api-node! 99 2)))
        (is (true? @cancelled?))
        (is (nil? (async/poll! errors-ch)))))))

(deftest persist-batch-artifact-supports-none-and-http-store-modes
  (let [artifact {:batch_id "b1" :run_id "r1" :pages []}]
    (with-redefs [config/env {:ingest-artifact-store "none"}]
      (let [out (#'bitool.ingest.runtime/persist-batch-artifact! "r1" "samara" "trips" "b1" artifact)]
        (is (nil? (:artifact_path out)))
        (is (string? (:artifact_checksum out)))))
    (with-redefs [config/env {:ingest-artifact-store "http"
                              :ingest-artifact-http-endpoint "https://artifact.example.com/store"}
                  api/do-request (fn [_]
                                   {:status 200
                                    :body {:artifact_path "s3://bucket/path/b1.json"
                                           :artifact_checksum "checksum-1"}})]
      (let [out (#'bitool.ingest.runtime/persist-batch-artifact! "r1" "samara" "trips" "b1" artifact)]
        (is (= "s3://bucket/path/b1.json" (:artifact_path out)))
        (is (= "checksum-1" (:artifact_checksum out)))))))

(deftest read-batch-artifact-supports-db-backed-store
  (let [artifact {:batch_id "b1" :run_id "r1" :pages [{:page 1 :body {:data [{:id "t1"}]}}]}
        payload  (json/generate-string artifact)
        checksum (#'bitool.ingest.runtime/sha256-hex payload)]
    (with-redefs [bitool.ingest.runtime/ensure-artifact-store-table! (fn [] true)
                  jdbc/execute-one! (fn [_ params]
                                      (if (re-find #"SELECT payload_json" (first params))
                                        {:payload_json payload
                                         :artifact_checksum checksum}
                                        {:artifact_path "db://ingest-batch-artifact/r1/b1"
                                         :artifact_checksum checksum}))]
      (let [out (#'bitool.ingest.runtime/read-batch-artifact! "db://ingest-batch-artifact/r1/b1" checksum)]
        (is (= artifact out))))))

(deftest read-batch-artifact-local-rejects-path-escape
  (with-redefs [config/env {:ingest-artifact-root "tmp/ingest-artifacts"}]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Artifact path escapes artifact root"
                          (#'bitool.ingest.runtime/read-batch-artifact-local! "../outside.json" nil)))))

(deftest cleanup-ingest-retention-is-disabled-without-manifest-coordination
  (with-redefs [config/env {:ingest-artifact-store "local"
                            :bitool-ingest-manifest-retention-enabled "false"
                            :bitool-enable-unsafe-artifact-cleanup "true"}]
    (let [result (runtime/cleanup-ingest-retention!)]
      (is (= false (:manifest_retention_enabled result)))
      (is (= false (:unsafe_cleanup_enabled result)))
      (is (= false (:unsafe_cleanup_supported result)))
      (is (= "disabled_without_manifest_coordination" (:maintenance_mode result)))
      (is (= 0 (:archived_count result)))
      (is (= 0 (:deleted_count result))))))

(deftest cleanup-ingest-retention-manifest-aware-sweeps-discovered-api-targets
  (with-redefs [config/env {:ingest-artifact-store "http"
                            :bitool-ingest-manifest-retention-enabled "true"
                            :ingest-artifact-archive-days "5"
                            :ingest-artifact-retention-days "30"}
                db/list-graph-ids (fn [] [99])
                db/getGraph (fn [_]
                              {:n {2 {:na {:btype "Ap"}}
                                   7 {:na {:btype "Fi"}}}})
                runtime/apply-api-retention! (fn [graph-id api-node-id opts]
                                               (is (= 99 graph-id))
                                               (is (= 2 api-node-id))
                                               (is (= 5 (:archive-days opts)))
                                               (is (= 30 (:retention-days opts)))
                                               {:archived_count 3
                                                :deleted_count 1})]
    (let [result (runtime/cleanup-ingest-retention!)]
      (is (= "manifest_aware" (:maintenance_mode result)))
      (is (= 1 (:targets_discovered result)))
      (is (= 3 (:archived_count result)))
      (is (= 1 (:deleted_count result)))
      (is (= 5 (:archive_days result)))
      (is (= 30 (:retention_days result))))))

(deftest read-batch-artifact-local-allows-archive-root-paths
  (let [root-dir (java.nio.file.Files/createTempDirectory "bitool-retention-hot-read" (make-array java.nio.file.attribute.FileAttribute 0))
        archive-dir (java.nio.file.Files/createTempDirectory "bitool-retention-archive-read" (make-array java.nio.file.attribute.FileAttribute 0))
        archived-file (.toFile (.resolve archive-dir "endpoint/run-1/b1.json"))
        artifact {:batch_id "b1" :run_id "r1" :pages [{:page 1 :body {:data [{:id "t1"}]}}]}
        payload (json/generate-string artifact)
        checksum (#'bitool.ingest.runtime/sha256-hex payload)]
    (.mkdirs (.getParentFile archived-file))
    (spit archived-file payload)
    (with-redefs [config/env {:ingest-artifact-root (.toString root-dir)
                              :ingest-artifact-archive-root (.toString archive-dir)}]
      (is (= artifact
             (#'bitool.ingest.runtime/read-batch-artifact-local! (.getPath archived-file) checksum))))))

(deftest delete-batch-artifact-local-rejects-path-escape
  (with-redefs [config/env {:ingest-artifact-root "tmp/ingest-artifacts"
                            :ingest-artifact-archive-root "tmp/ingest-artifacts-archive"}]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Artifact path escapes artifact root"
                          (#'bitool.ingest.runtime/delete-batch-artifact! "../outside.json")))))

(deftest checkpoint-covers-manifest-requires-explicit-cursor-or-watermark-match
  (is (false? (#'bitool.ingest.runtime/checkpoint-covers-manifest?
               {:last_successful_run_id "run-1"
                :last_successful_cursor nil
                :last_successful_watermark nil}
               {:run_id "run-1"
                :next_cursor nil
                :max_watermark nil})))
  (is (true? (#'bitool.ingest.runtime/checkpoint-covers-manifest?
              {:last_successful_run_id "run-1"
               :last_successful_cursor "cursor-1"
               :last_successful_watermark nil}
              {:run_id "run-1"
               :next_cursor "cursor-1"
               :max_watermark nil}))))

(deftest checkpoint-covers-manifest-requires-matching-batch-identity-for-full-load-runs
  (is (false? (#'bitool.ingest.runtime/checkpoint-covers-manifest?
               {:last_successful_run_id "run-1"
                :last_successful_batch_id "run-1-b000001"
                :last_successful_batch_seq 1
                :last_successful_cursor nil
                :last_successful_watermark nil}
               {:run_id "run-1"
                :batch_id "run-1-b000002"
                :batch_seq 2
                :next_cursor nil
                :max_watermark nil})))
  (is (true? (#'bitool.ingest.runtime/checkpoint-covers-manifest?
              {:last_successful_run_id "run-1"
               :last_successful_batch_id "run-1-b000002"
               :last_successful_batch_seq 2
               :last_successful_cursor nil
               :last_successful_watermark nil}
              {:run_id "run-1"
               :batch_id "run-1-b000002"
               :batch_seq 2
               :next_cursor nil
               :max_watermark nil}))))

(deftest checkpoint-covers-manifest-rejects-partial-batch-identity
  (is (false? (#'bitool.ingest.runtime/checkpoint-covers-manifest?
               {:last_successful_run_id "run-1"
                :last_successful_batch_seq 2
                :last_successful_cursor nil
                :last_successful_watermark nil}
               {:run_id "run-1"
                :batch_id "run-1-b000002"
                :batch_seq 2
                :next_cursor nil
                :max_watermark nil})))
  (is (false? (#'bitool.ingest.runtime/checkpoint-covers-manifest?
               {:last_successful_run_id "run-1"
                :last_successful_batch_id "run-1-b000002"
                :last_successful_cursor nil
                :last_successful_watermark nil}
               {:run_id "run-1"
                :batch_id "run-1-b000002"
                :batch_seq 2
                :next_cursor nil
                :max_watermark nil}))))

(deftest manifest-replayable-coerces-jdbc-boolean-variants
  (is (true? (#'bitool.ingest.runtime/manifest-replayable?
              {:status "committed"
               :active "true"
               :artifact_path "db://artifact"})))
  (is (true? (#'bitool.ingest.runtime/manifest-replayable?
              {:status "committed"
               :active 1
               :artifact_path "db://artifact"})))
  (is (false? (#'bitool.ingest.runtime/manifest-replayable?
               {:status "committed"
                :active "false"
                :artifact_path "db://artifact"}))))

(deftest flush-batch-wraps-writes-in-batch-commit-wrapper
  (let [wrapped?    (atom false)
        batch-opts  (atom [])
        manifests   (atom [])
        checkpoints (atom [])
        out         (with-redefs-fn {#'bitool.ingest.runtime/with-batch-commit (fn [_ f]
                                                                                  (reset! wrapped? true)
                                                                                  (binding [runtime/*batch-sql-opts* {:tx true}]
                                                                                    (f)))
                                     #'bitool.ingest.runtime/atomic-batch-commit? (fn [_] true)
                                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [& _] nil)
                                     #'bitool.ingest.runtime/load-rows! (fn [& _]
                                                                          (swap! batch-opts conj runtime/*batch-sql-opts*)
                                                                          nil)
                                     #'bitool.ingest.runtime/replace-row! (fn [_ table _ row]
                                                                            (cond
                                                                              (re-find #"run_batch_manifest$" table) (swap! manifests conj row)
                                                                              (re-find #"ingestion_checkpoint$" table) (swap! checkpoints conj row)
                                                                              :else nil))
                                     #'bitool.ingest.runtime/persist-batch-artifact! (fn [_ _ _ _ _]
                                                                                       {:artifact_path "tmp/b1.json"
                                                                                        :artifact_checksum "sum-1"})}
                      (fn []
                        (#'bitool.ingest.runtime/flush-batch!
                         9
                         {:table-name "audit.trips_raw"
                          :bad-records-table "audit.bad_records"
                          :manifest-table "audit.run_batch_manifest"
                          :checkpoint-table "audit.ingestion_checkpoint"
                          :source-system "samara"
                          :endpoint-name "trips"
                          :run-id "run-1"
                          :started-at (java.time.Instant/parse "2026-03-14T10:00:00Z")}
                         {:rows [{:partition_date "2026-03-14"
                                  :event_time_utc "2026-03-14T09:59:00Z"
                                  :payload_json "{\"id\":\"t1\"}"}]
                          :bad-records []
                          :page-artifacts []
                          :page-count 1
                          :byte-count 128
                          :last-state {:last-cursor "cursor-1"}
                          :last-http-status 200}
                         {:batch-seq 0
                          :checkpoint-row nil
                          :max-watermark nil
                          :next-cursor nil
                          :rows-extracted 1
                          :rows-written 0
                          :bad-records-total 0
                          :bad-records-written 0
                          :pages-fetched 1
                          :retry-count 0
                          :last-http-status nil
                          :changed-partition-dates []
                         :manifests []})))]
    (is (true? @wrapped?))
    (is (= [{:tx true}] @batch-opts))
    (is (= 1 (:batch-seq out)))
    (is (= 2 (count @manifests)))
    (is (= 1 (count @checkpoints)))))

(deftest flush-batch-uses-pending-checkpoint-manifest-for-non-transactional-targets
  (let [manifests   (atom [])
        checkpoints (atom [])
        out         (with-redefs-fn {#'bitool.ingest.runtime/atomic-batch-commit? (fn [_] false)
                                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [& _] nil)
                                     #'bitool.ingest.runtime/load-rows! (fn [& _] nil)
                                     #'bitool.ingest.runtime/replace-row! (fn [_ table _ row]
                                                                            (cond
                                                                              (re-find #"run_batch_manifest$" table) (swap! manifests conj row)
                                                                              (re-find #"ingestion_checkpoint$" table) (swap! checkpoints conj row)
                                                                              :else nil))
                                     #'bitool.ingest.runtime/persist-batch-artifact! (fn [_ _ _ _ _]
                                                                                       {:artifact_path "tmp/b1.json"
                                                                                        :artifact_checksum "sum-1"})}
                      (fn []
                        (#'bitool.ingest.runtime/flush-batch!
                         9
                         {:table-name "audit.trips_raw"
                          :bad-records-table "audit.bad_records"
                          :manifest-table "audit.run_batch_manifest"
                          :checkpoint-table "audit.ingestion_checkpoint"
                          :source-system "samara"
                          :endpoint-name "trips"
                          :run-id "run-1"
                          :started-at (java.time.Instant/parse "2026-03-14T10:00:00Z")}
                         {:rows [{:partition_date "2026-03-14"
                                  :event_time_utc "2026-03-14T09:59:00Z"
                                  :payload_json "{\"id\":\"t1\"}"}]
                          :bad-records []
                          :page-artifacts []
                          :page-count 1
                          :byte-count 128
                          :last-state {:last-cursor "cursor-1"}
                          :last-http-status 200}
                         {:batch-seq 0
                          :checkpoint-row nil
                          :max-watermark nil
                          :next-cursor nil
                          :rows-extracted 1
                          :rows-written 0
                          :bad-records-total 0
                          :bad-records-written 0
                          :pages-fetched 1
                          :retry-count 0
                          :last-http-status nil
                          :changed-partition-dates []
                          :manifests []})))]
    (is (= 1 (:batch-seq out)))
    (is (= ["preparing" "pending_checkpoint" "committed"] (mapv :status @manifests)))
    (is (= 1 (count @checkpoints)))))

(deftest abort-preparing-batches-promotes-pending-checkpoint-when-checkpoint-matches
  (let [manifest-updates (atom [])
        deletes          (atom [])]
    (with-redefs-fn {#'bitool.ingest.runtime/incomplete-manifest-rows (fn [& _]
                                                                        [{:batch_id "b1"
                                                                          :run_id "run-1"
                                                                          :source_system "samara"
                                                                          :endpoint_name "trips"
                                                                          :table_name "audit.trips_raw"
                                                                          :status "pending_checkpoint"
                                                                          :next_cursor "cursor-1"
                                                                          :max_watermark "2026-03-14T09:59:00Z"
                                                                          :active false}])
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _]
                                                               {:last_successful_run_id "run-1"
                                                                :last_successful_cursor "cursor-1"
                                                                :last_successful_watermark "2026-03-14T09:59:00Z"})
                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [& args]
                                                                      (swap! deletes conj args))
                     #'bitool.ingest.runtime/mark-manifest-row! (fn [_ _ row]
                                                                  (swap! manifest-updates conj row))}
      (fn []
        (#'bitool.ingest.runtime/abort-preparing-batches!
         9
         {:manifest-table "audit.run_batch_manifest"
          :checkpoint-table "audit.ingestion_checkpoint"
          :bad-records-table "audit.bad_records"
          :endpoint-name "trips"})
        (is (empty? @deletes))
        (is (= 1 (count @manifest-updates)))
        (is (= "committed" (:status (first @manifest-updates))))
        (is (true? (:active (first @manifest-updates))))))))

(deftest abort-preparing-batches-fails-closed-when-reconciliation-query-errors
  (with-redefs-fn {#'bitool.ingest.runtime/incomplete-manifest-rows (fn [& _]
                                                                      (throw (ex-info "db unavailable" {:status 503})))}
    (fn []
      (try
        (#'bitool.ingest.runtime/abort-preparing-batches!
         9
         {:manifest-table "audit.run_batch_manifest"
          :checkpoint-table "audit.ingestion_checkpoint"
          :bad-records-table "audit.bad_records"
          :endpoint-name "trips"})
        (is false "Expected reconciliation failure to abort the run")
        (catch clojure.lang.ExceptionInfo e
          (is (= "manifest_reconciliation" (:failure_class (ex-data e))))
          (is (= "trips" (:endpoint_name (ex-data e)))))))))

(deftest run-api-node-replays-deterministically-from-stored-batch-artifacts
  (let [bronze-loads     (atom [])
        manifest-rows    (atom [])
        checkpoint-rows  (atom [])
        delete-calls     (atom [])
        endpoint         {:endpoint_name "trips"
                          :endpoint_url "/fleet/trips"
                          :enabled true
                          :pagination_strategy "none"
                          :selected_nodes []
                          :primary_key_fields ["current_id"]
                          :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node         {:source_system "samara"
                          :base_url "https://api.example.com"
                          :auth_ref {}
                          :endpoint_configs [endpoint]}
        graph            {:a {:id 99 :v 7}
                          :n {2 {:na api-node}}}
        manifests        [{:batch_id "run-old-b000001"
                           :run_id "run-old"
                           :endpoint_name "trips"
                           :artifact_path "db://ingest-batch-artifact/run-old/b1"
                           :artifact_checksum "sum-1"}
                          {:batch_id "run-old-b000002"
                           :run_id "run-old"
                           :endpoint_name "trips"
                           :artifact_path "db://ingest-batch-artifact/run-old/b2"
                           :artifact_checksum "sum-2"}]]
    (with-redefs-fn {#'db/getGraph (fn [_] graph)
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                     #'api/fetch-paged-async (fn [_]
                                               (throw (ex-info "live fetch should not run during deterministic replay" {})))
                     #'bitool.ingest.runtime/committed-manifest-rows (fn [_ _ run-id endpoint-name]
                                                                       (is (= "run-old" run-id))
                                                                       (is (= "trips" endpoint-name))
                                                                       manifests)
                     #'bitool.ingest.runtime/read-batch-artifact! (fn [artifact-path _]
                                                                    (case artifact-path
                                                                      "db://ingest-batch-artifact/run-old/b1"
                                                                      {:endpoint_config {:endpoint_name "trips"
                                                                                         :endpoint_url "/fleet/trips"
                                                                                         :enabled true
                                                                                         :pagination_strategy "none"
                                                                                         :selected_nodes []
                                                                                         :primary_key_fields ["stored_id"]
                                                                                         :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
                                                                       :pages [{:page 1
                                                                                :state {}
                                                                                :response {:status 200}
                                                                                :body {:data [{:id "t1"}]}}]}
                                                                      "db://ingest-batch-artifact/run-old/b2"
                                                                      {:endpoint_config {:endpoint_name "trips"
                                                                                         :endpoint_url "/fleet/trips"
                                                                                         :enabled true
                                                                                         :pagination_strategy "none"
                                                                                         :selected_nodes []
                                                                                         :primary_key_fields ["stored_id"]
                                                                                         :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
                                                                       :pages [{:page 2
                                                                                :state {:last-cursor "cursor-2"}
                                                                                :response {:status 200}
                                                                                :body {:data [{:id "t2"}]}}]}))
                     #'bronze/bronze-columns (fn [replay-endpoint]
                                               (is (= ["stored_id"] (:primary_key_fields replay-endpoint)))
                                               [])
                     #'bronze/build-page-rows (fn [page replay-endpoint _]
                                                (is (= ["stored_id"] (:primary_key_fields replay-endpoint)))
                                                {:rows [{:partition_date "2026-03-13"
                                                         :event_time_utc (if (= 1 (:page page))
                                                                           "2026-03-13T10:00:00Z"
                                                                           "2026-03-13T10:05:00Z")
                                                         :payload_json (str "{\"page\":" (:page page) "}")}]
                                                 :bad-records []})
                     #'bitool.ingest.runtime/persist-batch-artifact! (fn [_ _ _ batch-id _]
                                                                       {:artifact_path (str "db://replayed/" batch-id)
                                                                        :artifact_checksum (str "sum-" batch-id)})
                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [_ table column value]
                                                                      (swap! delete-calls conj [table column value]))
                     #'bitool.ingest.runtime/load-rows! (fn [_ table rows]
                                                          (when (re-find #"samara_trips_raw$" table)
                                                            (swap! bronze-loads conj rows)))
                     #'bitool.ingest.runtime/replace-row! (fn [_ table _ row]
                                                            (cond
                                                              (re-find #"run_batch_manifest$" table) (swap! manifest-rows conj row)
                                                              (re-find #"ingestion_checkpoint$" table) (swap! checkpoint-rows conj row)
                                                              :else nil))
                     #'operations/record-endpoint-freshness! (fn [& _] nil)}
      (fn []
        (let [out (runtime/run-api-node! 99 2 {:endpoint-name "trips"
                                               :replay_source_run_id "run-old"
                                               :replay_source_graph_version 7})]
          (is (= "success" (get-in out [:results 0 :status])))
          (is (= "run-old" (get-in out [:results 0 :replay_source_run_id])))
          (is (= 2 (get-in out [:results 0 :batch_count])))
          (is (= ["run-old-b000001" "run-old-b000002"]
                 (->> @manifest-rows
                      (filter #(= "committed" (:status %)))
                      (mapv :batch_id))))
          (is (= ["run-old-b000001" "run-old-b000002"]
                 (->> @delete-calls
                      (filter #(re-find #"samara_trips_raw$" (first %)))
                      (mapv #(nth % 2)))))
          (is (= "cursor-2" (:last_successful_cursor (last @checkpoint-rows))))
          (is (= 2 (count @bronze-loads))))))))

(deftest run-api-node-rejects-deterministic-replay-when-graph-version-drifted
  (let [endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :pagination_strategy "none"
                  :selected_nodes []
                  :primary_key_fields ["id"]
                  :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}
        graph {:a {:id 99 :v 8}
               :n {2 {:na api-node}}}]
    (with-redefs-fn {#'db/getGraph (fn [_] graph)
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9})}
      (fn []
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"Deterministic replay requires the source graph version to match"
                              (runtime/run-api-node! 99 2 {:endpoint-name "trips"
                                                           :replay_source_run_id "run-old"
                                                           :replay_source_graph_version 7})))))))

(deftest run-api-node-rejects-invalid-replay-graph-version
  (let [endpoint {:endpoint_name "trips"
                  :endpoint_url "/fleet/trips"
                  :enabled true
                  :pagination_strategy "none"
                  :selected_nodes []
                  :primary_key_fields ["id"]
                  :bronze_table_name "sheetz_telematics.bronze.samara_trips_raw"}
        api-node {:source_system "samara"
                  :base_url "https://api.example.com"
                  :auth_ref {}
                  :endpoint_configs [endpoint]}
        graph {:a {:id 99 :v 8}
               :n {2 {:na api-node}}}]
    (with-redefs-fn {#'db/getGraph (fn [_] graph)
                     #'g2/getData (fn [_ _] api-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9})}
      (fn []
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"Replay source graph version must be an integer"
                              (runtime/run-api-node! 99 2 {:endpoint-name "trips"
                                                           :replay_source_run_id "run-old"
                                                           :replay_source_graph_version "abc"})))))))

(deftest rollback-api-batch-deletes-batch-and-marks-manifest-rolled-back
  (let [deletes (atom [])
        manifest-updates (atom [])]
    (with-redefs-fn {#'db/getGraph (fn [_] {:n {2 {:na {:btype "Ap"
                                                       :source_system "samara"
                                                       :endpoint_configs [{:endpoint_name "trips"
                                                                           :enabled true}]}}}})
                     #'g2/getData (fn [g id] (get-in g [:n id :na]))
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/ensure-batch-manifest-columns! (fn [& _] nil)
                     #'bitool.ingest.runtime/manifest-row-by-batch-id (fn [& _]
                                                                        {:batch_id "b1"
                                                                         :endpoint_name "trips"
                                                                         :table_name "sheetz_telematics.bronze.trips"
                                                                         :status "committed"
                                                                         :active true})
                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [_ table column value]
                                                                      (swap! deletes conj [table column value]))
                     #'bitool.ingest.runtime/mark-manifest-row! (fn [_ _ row]
                                                                  (swap! manifest-updates conj row))
                     #'bitool.ingest.runtime/with-batch-commit (fn [_ f] (f))}
      (fn []
        (let [result (runtime/rollback-api-batch! 99 2 "b1" {:endpoint-name "trips"
                                                              :rollback-reason "bad_batch"
                                                              :rolled-back-by "alice"})]
          (is (= "rolled_back" (:status result)))
          (is (= [["sheetz_telematics.bronze.trips" :batch_id "b1"]
                  ["sheetz_telematics.audit.bad_records" :batch_id "b1"]]
                 @deletes))
          (is (= "rolled_back" (:status (first @manifest-updates))))
          (is (false? (:active (first @manifest-updates))))
          (is (= "bad_batch" (:rollback_reason (first @manifest-updates))))
          (is (= "alice" (:rolled_back_by (first @manifest-updates)))))))))

(deftest rollback-api-batch-rejects-in-flight-manifests
  (with-redefs-fn {#'db/getGraph (fn [_] {:n {2 {:na {:btype "Ap"
                                                      :source_system "samara"
                                                      :endpoint_configs [{:endpoint_name "trips"
                                                                          :enabled true}]}}}})
                   #'g2/getData (fn [g id] (get-in g [:n id :na]))
                   #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                             :catalog "sheetz_telematics"
                                                                             :schema "bronze"})
                   #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                   #'bitool.ingest.runtime/ensure-batch-manifest-columns! (fn [& _] nil)
                   #'bitool.ingest.runtime/manifest-row-by-batch-id (fn [& _]
                                                                      {:batch_id "b1"
                                                                       :endpoint_name "trips"
                                                                       :table_name "sheetz_telematics.bronze.trips"
                                                                       :status "pending_checkpoint"
                                                                       :active false})}
    (fn []
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"In-flight batches cannot be rolled back manually"
                            (runtime/rollback-api-batch! 99 2 "b1" {:endpoint-name "trips"}))))))

(deftest list-api-batches-returns-manifest-summary-with-archive-state
  (with-redefs-fn {#'db/getGraph (fn [_] {:n {2 {:na {:btype "Ap"
                                                     :source_system "samara"
                                                     :endpoint_configs [{:endpoint_name "trips"
                                                                         :enabled true}]}}}})
                   #'g2/getData (fn [g id] (get-in g [:n id :na]))
                   #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                             :catalog "sheetz_telematics"
                                                                             :schema "bronze"})
                   #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                   #'bitool.ingest.runtime/ensure-batch-manifest-columns! (fn [& _] nil)
                   #'bitool.ingest.runtime/query-manifest-rows (fn [& _]
                                                                 [{:batch_id "b1"
                                                                   :run_id "run-1"
                                                                   :endpoint_name "trips"
                                                                   :table_name "sheetz_telematics.bronze.trips"
                                                                   :batch_seq 1
                                                                   :status "committed"
                                                                   :active true
                                                                   :row_count 10
                                                                   :bad_record_count 1
                                                                   :byte_count 100
                                                                   :page_count 2
                                                                   :partition_dates_json "[\"2026-03-14\"]"
                                                                   :artifact_path "db://ingest-batch-artifact/run-1/b1"
                                                                   :artifact_checksum "abc"
                                                                   :archived_at_utc "2026-03-15T00:00:00Z"
                                                                   :started_at_utc "2026-03-14T00:00:00Z"
                                                                   :committed_at_utc "2026-03-14T00:05:00Z"}])}
    (fn []
      (let [result (runtime/list-api-batches 99 2 {:endpoint-name "trips"
                                                   :archived-only true
                                                   :limit 25})
            batch (first (:batches result))]
        (is (= 1 (:batch_count result)))
        (is (= "archived" (:artifact_state batch)))
        (is (= true (:replayable batch)))
        (is (= {:rollback true :archive false :replay true}
               (:available_actions batch)))))))

(deftest list-api-bad-records-uses-conditional-payload-projection
  (let [row {:bad_record_id "br-1"
             :run_id "run-1"
             :batch_id "b-1"
             :source_system "samara"
             :endpoint_name "trips"
             :error_message "bad value"
             :replay_status "pending"
             :payload_json "{\"id\":1}"
             :row_json "{\"id\":1}"
             :created_at_utc "2026-03-15T00:00:00Z"}]
    (with-redefs-fn {#'bitool.ingest.runtime/api-endpoint-runtime-context (fn [& _]
                                                                             {:conn-id 9
                                                                              :bad-records-table "sheetz_telematics.audit.bad_records"
                                                                              :endpoint {:endpoint_name "trips"}})
                     #'bitool.ingest.runtime/query-bad-record-rows (fn [& _] [row])}
      (fn []
        (let [without-payloads (runtime/list-api-bad-records 99 2 {:endpoint-name "trips"
                                                                    :include-payloads false})
              with-payloads    (runtime/list-api-bad-records 99 2 {:endpoint-name "trips"
                                                                    :include-payloads true})
              bad-without      (first (:bad_records without-payloads))
              bad-with         (first (:bad_records with-payloads))]
          (is (= 1 (:bad_record_count without-payloads)))
          (is (nil? (:payload_json bad-without)))
          (is (nil? (:row_json bad-without)))
          (is (= "{\"id\":1}" (:payload_json bad-with)))
          (is (= "{\"id\":1}" (:row_json bad-with))))))))

(deftest aggregate-endpoint-status-treats-empty-results-as-failed
  (is (= "failed" (#'bitool.ingest.runtime/aggregate-endpoint-status []))))

(deftest committed-manifest-partition-dates-uses-only-active-committed-manifests
  (is (= ["2026-03-14" "2026-03-15" "2026-03-16"]
         (#'bitool.ingest.runtime/committed-manifest-partition-dates
          [{:status "committed"
            :active true
            :partition_dates_json "[\"2026-03-14\", \"2026-03-15\"]"}
           {:status "committed"
            :active "true"
            :partition_dates_json ["2026-03-16"]}
           {:status "pending_checkpoint"
            :active true
            :partition_dates_json "[\"2026-03-13\"]"}
           {:status "committed"
            :active false
            :partition_dates_json "[\"2026-03-12\"]"}]))))

(deftest ensure-final-run-manifest-invariants-fails-for-non-transactional-incomplete-manifest
  (with-redefs-fn {#'bitool.ingest.runtime/atomic-batch-commit? (fn [_] false)}
    (fn []
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"manifest commit closure is incomplete"
                            (#'bitool.ingest.runtime/ensure-final-run-manifest-invariants!
                             9
                             "trips"
                             "run-1"
                             [{:batch_id "run-1-b1"
                               :status "pending_checkpoint"
                               :active false}]))))))

(deftest replay-batch-id-is-stable-for-same-source-bad-record-set
  (is (= (#'bitool.ingest.runtime/replay-batch-id "trips" ["br-2" "br-1"])
         (#'bitool.ingest.runtime/replay-batch-id "trips" ["br-1" "br-2"]))))

(deftest replay-api-bad-records-skips-already-committed-source-bad-record-ids
  (let [captured-replay-input (atom nil)
        captured-replay-scope (atom nil)]
    (with-redefs-fn {#'bitool.ingest.runtime/api-endpoint-runtime-context (fn [& _]
                                                                             {:conn-id 9
                                                                              :target {:catalog "sheetz_telematics"
                                                                                       :schema "bronze"}
                                                                              :api-node {:source_system "samara"
                                                                                         :base_url "https://api.example.com"}
                                                                              :endpoint {:endpoint_name "trips"
                                                                                         :endpoint_url "/fleet/trips"
                                                                                         :selected_nodes []
                                                                                         :primary_key_fields ["id"]
                                                                                         :bronze_table_name "sheetz_telematics.bronze.trips"}
                                                                              :bad-records-table "sheetz_telematics.audit.bad_records"
                                                                              :manifest-table "sheetz_telematics.audit.run_batch_manifest"})
                     #'bitool.ingest.runtime/query-bad-record-rows (fn [& _]
                                                                     [{:bad_record_id "br-1" :payload_json "{\"id\":1}"}
                                                                      {:bad_record_id "br-2" :payload_json "{\"id\":2}"}])
                     #'bitool.ingest.runtime/committed-replayed-source-bad-record-ids (fn [_ _ source-system endpoint-name]
                                                                                        (reset! captured-replay-scope [source-system endpoint-name])
                                                                                        #{"br-1"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bronze/bronze-columns (fn [_] [])
                     #'bronze/replay-bad-records->rows (fn [rows _ _]
                                                         (reset! captured-replay-input (mapv :bad_record_id rows))
                                                         {:rows [{:partition_date "2026-03-16"
                                                                  :event_time_utc "2026-03-16T00:00:00Z"
                                                                  :payload_json "{\"id\":2}"}]
                                                          :bad-records []
                                                          :succeeded-source-bad-record-ids ["br-2"]
                                                          :failed-source-bad-record-ids []})
                     #'bitool.ingest.runtime/flush-bad-record-replay-batch! (fn [& _]
                                                                              {:batch_id "badreplay-trips-x"
                                                                               :run_id "run-1"
                                                                               :endpoint_name "trips"
                                                                               :table_name "sheetz_telematics.bronze.trips"
                                                                               :batch_seq 1
                                                                               :status "committed"
                                                                               :active true
                                                                               :row_count 1
                                                                               :bad_record_count 0
                                                                               :byte_count 10
                                                                               :page_count 0
                                                                               :partition_dates_json "[\"2026-03-16\"]"})
                     #'control-plane/record-audit-event! (fn [& _] nil)}
      (fn []
        (let [result (runtime/replay-api-bad-records! 99 2 {:endpoint-name "trips"})]
          (is (= ["samara" "trips"] @captured-replay-scope))
          (is (= ["br-2"] @captured-replay-input))
          (is (= 1 (:skipped_already_replayed_count result)))
          (is (= ["br-1"] (:skipped_already_replayed_bad_record_ids result)))
          (is (= 1 (:rows_replayed result))))))))

(deftest replay-api-bad-records-returns-noop-when-all-source-bad-records-were-already-replayed
  (with-redefs-fn {#'bitool.ingest.runtime/api-endpoint-runtime-context (fn [& _]
                                                                           {:conn-id 9
                                                                            :target {:catalog "sheetz_telematics"
                                                                                     :schema "bronze"}
                                                                            :api-node {:source_system "samara"
                                                                                       :base_url "https://api.example.com"}
                                                                            :endpoint {:endpoint_name "trips"
                                                                                       :endpoint_url "/fleet/trips"
                                                                                       :selected_nodes []
                                                                                       :primary_key_fields ["id"]
                                                                                       :bronze_table_name "sheetz_telematics.bronze.trips"}
                                                                            :bad-records-table "sheetz_telematics.audit.bad_records"
                                                                            :manifest-table "sheetz_telematics.audit.run_batch_manifest"})
                   #'bitool.ingest.runtime/query-bad-record-rows (fn [& _]
                                                                   [{:bad_record_id "br-1" :payload_json "{\"id\":1}"}])
                   #'bitool.ingest.runtime/committed-replayed-source-bad-record-ids (fn [& _] #{"br-1"})
                   #'bronze/replay-bad-records->rows (fn [& _]
                                                       (throw (ex-info "Replay coercion should not run for noop replay" {})))
                   #'bitool.ingest.runtime/flush-bad-record-replay-batch! (fn [& _]
                                                                            (throw (ex-info "Flush should not run for noop replay" {})))
                   #'control-plane/record-audit-event! (fn [& _] nil)}
    (fn []
      (let [result (runtime/replay-api-bad-records! 99 2 {:endpoint-name "trips"})]
        (is (= "noop_already_replayed" (:status result)))
        (is (= 1 (:skipped_already_replayed_count result)))
        (is (= ["br-1"] (:skipped_already_replayed_bad_record_ids result)))
        (is (= 0 (:rows_replayed result)))
        (is (nil? (:manifest result)))))))

(deftest flush-bad-record-replay-batch-uses-non-transactional-manifest-states
  (let [ops (atom [])]
    (with-redefs-fn {#'bitool.ingest.runtime/atomic-batch-commit? (fn [_] false)
                     #'bitool.ingest.runtime/persist-batch-artifact! (fn [& _]
                                                                       {:artifact_path "db://artifact/replay"
                                                                        :artifact_checksum "sum"})
                     #'bitool.ingest.runtime/delete-rows-by-column! (fn [_ table column value]
                                                                       (swap! ops conj [:delete table column value]))
                     #'bitool.ingest.runtime/load-rows! (fn [_ table rows]
                                                          (swap! ops conj [:load table (count rows)]))
                     #'bitool.ingest.runtime/mark-manifest-row! (fn [_ _ row]
                                                                  (swap! ops conj [:manifest (:status row)]))
                     #'bitool.ingest.runtime/mark-bad-record-replay-statuses! (fn [_ _ ids status _ _]
                                                                                (swap! ops conj [:status status (count ids)]))}
      (fn []
        (#'bitool.ingest.runtime/flush-bad-record-replay-batch!
         9
         {:table-name "sheetz_telematics.bronze.trips"
          :bad-records-table "sheetz_telematics.audit.bad_records"
          :manifest-table "sheetz_telematics.audit.run_batch_manifest"
          :source-system "samara"
          :endpoint-name "trips"
          :run-id "run-1"
          :started-at #inst "2026-03-15T00:00:00.000-00:00"
          :endpoint-config {:endpoint_name "trips"}}
         [{:partition_date "2026-03-15"
           :event_time_utc "2026-03-15T00:00:00Z"
           :payload_json "{\"id\":1}"}]
         []
         {:source-bad-record-ids ["br-1"]
          :succeeded-source-bad-record-ids ["br-1"]
          :failed-source-bad-record-ids []
          :replay-run-id "run-1"})
        (let [manifest-statuses (->> @ops
                                     (filter #(= :manifest (first %)))
                                     (mapv second))
              committed-idx (first (keep-indexed (fn [idx op]
                                                   (when (= op [:manifest "committed"]) idx))
                                                 @ops))
              status-idx (first (keep-indexed (fn [idx op]
                                                (when (and (= :status (first op))
                                                           (= "succeeded" (second op)))
                                                  idx))
                                              @ops))]
          (is (= ["preparing" "pending_checkpoint" "committed"] manifest-statuses))
          (is (number? committed-idx))
          (is (number? status-idx))
          (is (< committed-idx status-idx)))))))

(deftest archive-api-batch-updates-manifest-archive-state
  (let [manifest-updates (atom [])]
    (with-redefs-fn {#'db/getGraph (fn [_] {:n {2 {:na {:btype "Ap"
                                                       :source_system "samara"
                                                       :endpoint_configs [{:endpoint_name "trips"
                                                                           :enabled true}]}}}})
                     #'g2/getData (fn [g id] (get-in g [:n id :na]))
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/ensure-batch-manifest-columns! (fn [& _] nil)
                     #'bitool.ingest.runtime/manifest-row-by-batch-id (fn [& _]
                                                                        {:batch_id "b1"
                                                                         :endpoint_name "trips"
                                                                         :table_name "sheetz_telematics.bronze.trips"
                                                                         :status "committed"
                                                                         :active true
                                                                         :artifact_path "/tmp/b1.json"
                                                                         :artifact_checksum "old"
                                                                         :started_at_utc "2026-03-14T00:00:00Z"
                                                                         :committed_at_utc "2026-03-14T00:05:00Z"})
                     #'bitool.ingest.runtime/archive-batch-artifact! (fn [_ _]
                                                                       {:artifact_path "/tmp/archive/b1.json"
                                                                        :artifact_checksum "new"})
                     #'bitool.ingest.runtime/mark-manifest-row! (fn [_ _ row]
                                                                  (reset! manifest-updates [row]))}
      (fn []
        (let [result (runtime/archive-api-batch! 99 2 "b1" {:endpoint-name "trips"
                                                             :archived-by "alice"})
              updated (first @manifest-updates)]
          (is (= "archived" (:artifact_state result)))
          (is (= "/tmp/archive/b1.json" (:artifact_path updated)))
          (is (= "new" (:artifact_checksum updated)))
          (is (string? (:archived_at_utc updated))))))))

(deftest apply-api-retention-archives-then-deletes-manifest-artifacts
  (let [manifest-updates (atom [])
        archived (atom [])
        deleted (atom [])]
    (with-redefs-fn {#'db/getGraph (fn [_] {:n {2 {:na {:btype "Ap"
                                                       :source_system "samara"
                                                       :endpoint_configs [{:endpoint_name "trips"
                                                                           :enabled true}]}}}})
                     #'g2/getData (fn [g id] (get-in g [:n id :na]))
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/ensure-batch-manifest-columns! (fn [& _] nil)
                     #'bitool.ingest.runtime/query-manifest-rows (fn [& [_ _ {:keys [offset]}]]
                                                                   (if (zero? (or offset 0))
                                                                     [{:batch_id "archive-me"
                                                                       :endpoint_name "trips"
                                                                       :status "committed"
                                                                       :active true
                                                                       :artifact_path "/tmp/a.json"
                                                                       :artifact_checksum "a1"
                                                                       :started_at_utc "2026-02-01T00:00:00Z"
                                                                       :committed_at_utc "2026-02-01T00:10:00Z"}
                                                                      {:batch_id "delete-me"
                                                                       :endpoint_name "trips"
                                                                       :status "rolled_back"
                                                                       :active false
                                                                       :artifact_path "/tmp/d.json"
                                                                       :artifact_checksum "d1"
                                                                       :archived_at_utc "2026-02-10T00:00:00Z"
                                                                       :started_at_utc "2025-01-01T00:00:00Z"
                                                                       :committed_at_utc "2025-01-01T00:10:00Z"}]
                                                                     []))
                     #'bitool.ingest.runtime/query-bad-record-retention-rows (fn [& _] [])
                     #'bitool.ingest.runtime/archive-batch-artifact! (fn [path checksum]
                                                                       (swap! archived conj [path checksum])
                                                                       {:artifact_path (str path ".archived")
                                                                        :artifact_checksum (str checksum "-archived")})
                     #'bitool.ingest.runtime/delete-batch-artifact! (fn [path]
                                                                      (swap! deleted conj path)
                                                                      1)
                     #'bitool.ingest.runtime/mark-manifest-row! (fn [_ _ row]
                                                                  (swap! manifest-updates conj row))}
      (fn []
        (let [result (runtime/apply-api-retention! 99 2 {:endpoint-name "trips"
                                                         :archive-days 20
                                                         :retention-days 200
                                                         :dry-run false})]
          (is (= 1 (:archived_count result)))
          (is (= 1 (:deleted_count result)))
          (is (= [["/tmp/a.json" "a1"]] @archived))
          (is (= ["/tmp/d.json"] @deleted))
          (is (= #{"archive-me" "delete-me"}
                 (set (map :batch_id @manifest-updates))))
          (is (some #(nil? (:artifact_path %))
                    @manifest-updates)))))))

(deftest apply-api-retention-continues-after-per-batch-errors
  (let [manifest-updates (atom [])
        archived (atom [])
        deleted (atom [])]
    (with-redefs-fn {#'db/getGraph (fn [_] {:n {2 {:na {:btype "Ap"
                                                       :source_system "samara"
                                                       :endpoint_configs [{:endpoint_name "trips"
                                                                           :enabled true}]}}}})
                     #'g2/getData (fn [g id] (get-in g [:n id :na]))
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/ensure-batch-manifest-columns! (fn [& _] nil)
                     #'bitool.ingest.runtime/query-manifest-rows (fn [& [_ _ {:keys [offset]}]]
                                                                   (if (zero? (or offset 0))
                                                                     [{:batch_id "archive-fail"
                                                                       :endpoint_name "trips"
                                                                       :status "committed"
                                                                       :active true
                                                                       :artifact_path "/tmp/a.json"
                                                                       :artifact_checksum "a1"
                                                                       :started_at_utc "2026-02-01T00:00:00Z"
                                                                       :committed_at_utc "2026-02-01T00:10:00Z"}
                                                                      {:batch_id "delete-ok"
                                                                       :endpoint_name "trips"
                                                                       :status "rolled_back"
                                                                       :active false
                                                                       :artifact_path "/tmp/d.json"
                                                                       :artifact_checksum "d1"
                                                                       :archived_at_utc "2026-02-10T00:00:00Z"
                                                                       :started_at_utc "2025-01-01T00:00:00Z"
                                                                       :committed_at_utc "2025-01-01T00:10:00Z"}]
                                                                     []))
                     #'bitool.ingest.runtime/query-bad-record-retention-rows (fn [& _] [])
                     #'bitool.ingest.runtime/archive-batch-artifact! (fn [path checksum]
                                                                       (swap! archived conj [path checksum])
                                                                       (throw (ex-info "archive unavailable" {:path path})))
                     #'bitool.ingest.runtime/delete-batch-artifact! (fn [path]
                                                                      (swap! deleted conj path)
                                                                      1)
                     #'bitool.ingest.runtime/mark-manifest-row! (fn [_ _ row]
                                                                  (swap! manifest-updates conj row))}
      (fn []
        (let [result (runtime/apply-api-retention! 99 2 {:endpoint-name "trips"
                                                         :archive-days 20
                                                         :retention-days 200
                                                         :dry-run false
                                                         :limit 50})]
          (is (= 0 (:archived_count result)))
          (is (= 1 (:deleted_count result)))
          (is (= 1 (count (:errors result))))
          (is (= "archive-fail" (:batch_id (first (:errors result)))))
          (is (= ["/tmp/d.json"] @deleted))
          (is (= #{"delete-ok"} (set (map :batch_id @manifest-updates)))))))))

(deftest apply-api-retention-archives-bad-record-payloads-and-deletes-expired-metadata
  (let [archived-payloads (atom [])
        archived-rows (atom [])
        deleted-rows (atom [])]
    (with-redefs-fn {#'db/getGraph (fn [_] {:n {2 {:na {:btype "Ap"
                                                       :source_system "samara"
                                                       :endpoint_configs [{:endpoint_name "trips"
                                                                           :enabled true}]}}}})
                     #'g2/getData (fn [g id] (get-in g [:n id :na]))
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                               :catalog "sheetz_telematics"
                                                                               :schema "bronze"})
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/ensure-batch-manifest-columns! (fn [& _] nil)
                     #'bitool.ingest.runtime/ensure-bad-record-columns! (fn [& _] nil)
                     #'bitool.ingest.runtime/query-manifest-rows (fn [& _] [])
                     #'bitool.ingest.runtime/query-bad-record-retention-rows (fn [& [_ _ {:keys [offset]}]]
                                                                                (if (zero? (or offset 0))
                                                                                  [{:bad_record_id "br-archive"
                                                                                    :run_id "run-1"
                                                                                    :source_system "samara"
                                                                                   :endpoint_name "trips"
                                                                                   :payload_json "{\"id\":1}"
                                                                                   :row_json "{\"id\":1}"
                                                                                   :error_message "bad type"
                                                                                    :created_at_utc "2026-01-15T00:00:00Z"
                                                                                    :payload_archive_ref nil}
                                                                                   {:bad_record_id "br-delete"
                                                                                    :run_id "run-2"
                                                                                    :source_system "samara"
                                                                                    :endpoint_name "trips"
                                                                                    :payload_json nil
                                                                                    :row_json nil
                                                                                    :error_message "bad type"
                                                                                    :created_at_utc "2024-01-01T00:00:00Z"
                                                                                    :payload_archive_ref "db://archive/br-delete"}]
                                                                                  []))
                     #'bitool.ingest.runtime/persist-batch-artifact! (fn [& args]
                                                                        (swap! archived-payloads conj args)
                                                                        {:artifact_path "db://archive/br-archive"
                                                                         :artifact_checksum "chk"})
                     #'bitool.ingest.runtime/mark-bad-record-payload-archived! (fn [_ _ bad-record-id archive-ref _]
                                                                                  (swap! archived-rows conj [bad-record-id archive-ref]))
                     #'bitool.ingest.runtime/delete-bad-record! (fn [_ _ bad-record-id]
                                                                  (swap! deleted-rows conj bad-record-id)
                                                                  1)}
      (fn []
        (let [result (runtime/apply-api-retention! 99 2 {:endpoint-name "trips"
                                                         :archive-days 20
                                                         :retention-days 200
                                                         :bad-record-payload-archive-days 30
                                                         :bad-record-retention-days 90
                                                         :dry-run false})]
          (is (= 1 (:bad_record_payload_archived_count result)))
          (is (= 1 (:bad_record_metadata_deleted_count result)))
          (is (= [["br-archive" "db://archive/br-archive"]] @archived-rows))
          (is (= ["br-delete"] @deleted-rows))
          (is (= 1 (count @archived-payloads))))))))

(deftest target-partition-columns-enforces-databricks-policy
  (with-redefs [bitool.ingest.runtime/connection-dbtype (fn [_] "databricks")]
    (is (= ["partition_date"]
           (#'bitool.ingest.runtime/target-partition-columns 9 {} {:endpoint_name "trips"})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"partition policy requires partition_date"
                          (#'bitool.ingest.runtime/target-partition-columns 9 {:partition_columns ["load_date"]}
                                                                     {:endpoint_name "trips"}))))
  (with-redefs [bitool.ingest.runtime/connection-dbtype (fn [_] "postgresql")]
    (is (= []
           (#'bitool.ingest.runtime/target-partition-columns 9 {:partition_columns ["partition_date"]}
                                                              {:endpoint_name "trips"})))))

(deftest ensure-schema-approved-rejects-mismatched-promoted-schema
  (with-redefs [bitool.ingest.runtime/latest-promoted-schema-approval (fn [& _]
                                                                        {:schema_hash "different"})]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"does not match the promoted approved schema"
                          (#'bitool.ingest.runtime/ensure-schema-approved!
                           9
                           "audit.endpoint_schema_approval"
                           99
                           2
                           {:endpoint_name "trips"
                            :schema_review_state "required"
                            :inferred_fields [{:path "$.data[].id"
                                               :column_name "id"
                                               :type "STRING"}]})))))

(deftest promote-api-schema-runs-under-lock-and-transaction
  (let [calls (atom [])
        replacements (atom [])]
    (with-redefs-fn {#'bitool.ingest.runtime/api-endpoint-runtime-context (fn [_ _ _]
                                                                            {:conn-id 9
                                                                             :schema-approval-table "audit.endpoint_schema_approval"
                                                                             :schema-snapshot-table "audit.endpoint_schema_snapshot"
                                                                             :endpoint {:endpoint_name "trips"}})
                     #'bitool.ingest.runtime/query-schema-snapshot-rows (fn [& _]
                                                                          [{:inferred_fields_json [{:path "$.data[].id"
                                                                                                    :column_name "id"
                                                                                                    :type "STRING"}]}])
                     #'bitool.ingest.runtime/with-batch-commit (fn [_ f]
                                                                 (swap! calls conj :tx)
                                                                 (f))
                     #'bitool.ingest.runtime/with-schema-promotion-lock (fn [_ graph-id api-node-id endpoint-name f]
                                                                          (swap! calls conj [:lock graph-id api-node-id endpoint-name])
                                                                          (f))
                     #'bitool.ingest.runtime/query-schema-approval-rows (fn [& _]
                                                                          [{:graph_id 99
                                                                            :api_node_id 2
                                                                            :endpoint_name "trips"
                                                                            :schema_hash "old-hash"
                                                                            :promoted true}])
                     #'bitool.ingest.runtime/replace-row! (fn [_ _ _ row]
                                                            (swap! replacements conj row))}
      (fn []
        (let [result (runtime/promote-api-schema! 99 2 {:endpoint-name "trips"
                                                        :reviewed-by "alice"})]
          (is (= :tx (first @calls)))
          (is (= [:lock 99 2 "trips"] (second @calls)))
          (is (= 2 (count @replacements)))
          (is (false? (:promoted (first @replacements))))
          (is (true? (:promoted (second @replacements))))
          (is (= "trips" (:endpoint_name result))))))))

(deftest verify-api-commit-closure-flags-incomplete-manifests
  (with-redefs-fn {#'bitool.ingest.runtime/api-endpoint-runtime-context (fn [_ _ _]
                                                                          {:conn-id 9
                                                                           :source-system "samara"
                                                                           :manifest-table "audit.run_batch_manifest"
                                                                           :endpoint {:endpoint_name "trips"}})
                   #'bitool.ingest.runtime/query-manifest-closure-summary (fn [_ _ opts]
                                                                            (is (= "samara" (:source-system opts)))
                                                                            {:manifest-count 7
                                                                             :incomplete-batch-ids ["b1"]
                                                                             :active-non-committed-batch-ids []})}
    (fn []
      (let [result (runtime/verify-api-commit-closure 99 2 {:endpoint-name "trips"})]
        (is (= false (:ready? result)))
        (is (= 7 (:manifest_count result)))
        (is (= ["b1"] (:incomplete_batch_ids result)))))))

(deftest reset-api-checkpoint-requires-reason
  (with-redefs [db/getGraph (fn [_] {:n {2 {:na {:btype "Ap"
                                                 :source_system "samara"
                                                 :endpoint_configs [{:endpoint_name "trips"
                                                                     :enabled true}]}}}})
                g2/getData (fn [g id] (get-in g [:n id :na]))
                bitool.ingest.runtime/find-downstream-target (fn [_ _] {:connection_id 9
                                                                        :catalog "sheetz_telematics"
                                                                        :schema "bronze"})
                bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                bitool.ingest.runtime/ensure-batch-manifest-columns! (fn [& _] nil)
                bitool.ingest.runtime/ensure-bad-record-columns! (fn [& _] nil)
                bitool.ingest.runtime/fetch-checkpoint (fn [& _] nil)
                bitool.ingest.runtime/abort-preparing-batches! (fn [& _] nil)
                bitool.ingest.runtime/incomplete-manifest-rows (fn [& _] [])]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"reason is required"
                          (runtime/reset-api-checkpoint! 99 2 {:endpoint-name "trips"})))))

(deftest ensure-batch-manifest-columns-fails-closed-on-migration-error
  (let [ready (atom #{})
        calls (atom 0)]
    (with-redefs-fn {#'bitool.ingest.runtime/manifest-columns-ready? ready
                     #'bitool.ingest.runtime/sql-opts (fn [_] :fake-ds)
                     #'jdbc/execute! (fn [_ _]
                                       (swap! calls inc)
                                       (throw (ex-info "boom" {})))}
      (fn []
        (try
          (real-ensure-batch-manifest-columns! 9 "audit.run_batch_manifest")
          (is nil "Expected batch manifest column migration failure to abort")
          (catch clojure.lang.ExceptionInfo e
            (is (= "manifest_migration" (:failure_class (ex-data e))))
            (is (empty? @ready))
            (is (pos? @calls))))))))

(deftest ensure-checkpoint-columns-fails-closed-on-migration-error
  (let [ready (atom #{})
        calls (atom 0)]
    (with-redefs-fn {#'bitool.ingest.runtime/checkpoint-columns-ready? ready
                     #'bitool.ingest.runtime/sql-opts (fn [_] :fake-ds)
                     #'jdbc/execute! (fn [_ _]
                                       (swap! calls inc)
                                       (throw (ex-info "boom" {})))}
      (fn []
        (try
          (real-ensure-checkpoint-columns! 9 "audit.ingestion_checkpoint")
          (is nil "Expected checkpoint column migration failure to abort")
          (catch clojure.lang.ExceptionInfo e
            (is (= "checkpoint_migration" (:failure_class (ex-data e))))
            (is (empty? @ready))
            (is (pos? @calls))))))))

(deftest checkpoint-row-for-failure-preserves-last-successful-batch-identity
  (let [existing {:last_successful_watermark "2026-03-14T09:59:00Z"
                  :last_successful_cursor "cursor-1"
                  :last_successful_run_id "run-1"
                  :last_successful_batch_id "run-1-b000002"
                  :last_successful_batch_seq 2
                  :rows_ingested 25}
        failure  (checkpoint/failure-row
                  {:source_system "samara"
                   :endpoint_name "trips"
                   :run_id "run-2"
                   :error_message "boom"
                   :now #inst "2026-03-15T00:00:00.000-00:00"})]
    (is (= "run-1-b000002"
           (:last_successful_batch_id
            (#'bitool.ingest.runtime/checkpoint-row-for-failure existing failure))))
    (is (= 2
           (:last_successful_batch_seq
            (#'bitool.ingest.runtime/checkpoint-row-for-failure existing failure))))))

(deftest load-rows-in-batch-mode-validates-qualified-table-name
  (binding [runtime/*batch-sql-opts* :tx]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Table name must be a valid qualified identifier"
                          (#'bitool.ingest.runtime/load-rows!
                           9
                           "audit.trips_raw; DROP TABLE audit.bad_records"
                           [{:payload_json "{}"}])))))

(deftest load-rows-routes-file-snowflake-stage-copy-for-primary-bronze-table
  (let [stage-copy-call (atom nil)
        jdbc-call (atom nil)]
    (binding [runtime/*row-load-context* {:primary-table-name "lake.bronze.file_raw"
                                          :source-kind :file
                                          :target {:sf_load_method "stage_copy"
                                                   :sf_stage_name "@bitool_stage"
                                                   :sf_on_error "CONTINUE"
                                                   :sf_purge true}}]
      (with-redefs [bitool.ingest.runtime/connection-dbtype (fn [_] "snowflake")
                    db/load-rows-snowflake-stage-copy! (fn [conn-id db-name table-name rows key->col opts]
                                                         (reset! stage-copy-call [conn-id db-name table-name rows key->col opts])
                                                         {:status :ok})
                    db/load-rows! (fn [& args]
                                    (reset! jdbc-call args)
                                    {:status :jdbc})]
        (#'bitool.ingest.runtime/load-rows! 9 "lake.bronze.file_raw" [{:payload_json "{}" :row_json "{}"}])
        (is (= 9 (first @stage-copy-call)))
        (is (= "lake.bronze.file_raw" (nth @stage-copy-call 2)))
        (is (= {:sf_stage_name "@bitool_stage"
                :sf_on_error "CONTINUE"
                :sf_purge true}
               (nth @stage-copy-call 5)))
        (is (nil? @jdbc-call))))))

(deftest load-rows-does-not-route-audit-table-through-stage-copy
  (let [stage-copy-call (atom nil)
        jdbc-call (atom nil)]
    (binding [runtime/*row-load-context* {:primary-table-name "lake.bronze.file_raw"
                                          :source-kind :file
                                          :target {:sf_load_method "stage_copy"
                                                   :sf_stage_name "@bitool_stage"}}]
      (with-redefs [bitool.ingest.runtime/connection-dbtype (fn [_] "snowflake")
                    db/load-rows-snowflake-stage-copy! (fn [& args]
                                                         (reset! stage-copy-call args)
                                                         {:status :ok})
                    db/load-rows! (fn [& args]
                                    (reset! jdbc-call args)
                                    {:status :jdbc})]
        (#'bitool.ingest.runtime/load-rows! 9 "lake.audit.run_batch_manifest" [{:batch_id "b1"}])
        (is (nil? @stage-copy-call))
        (is (= 5 (count @jdbc-call)))))))

(deftest run-file-node-uses-transport-aware-checksum-detection
  (let [fetch-config (atom nil)
        source-node {:btype "Fs"
                     :source_system "file"
                     :base_path ""
                     :file_configs [{:endpoint_name "orders"
                                     :transport "s3"
                                     :format "jsonl"
                                     :paths ["s3://bucket/a.jsonl" "s3://bucket/b.jsonl"]
                                     :bronze_table_name "lake.bronze.file_raw"}]}
        target {:connection_id 9
                :catalog "lake"
                :schema "bronze"}]
    (with-redefs-fn {#'db/getGraph (fn [_] {})
                     #'g2/getData (fn [_ _] source-node)
                     #'bitool.ingest.runtime/find-downstream-target (fn [_ _] target)
                     #'bitool.ingest.runtime/ensure-table! (fn [& _] nil)
                     #'bitool.ingest.runtime/ensure-checkpoint-columns! (fn [& _] nil)
                     #'bitool.ingest.runtime/ensure-batch-manifest-columns! (fn [& _] nil)
                     #'bitool.ingest.runtime/ensure-bad-record-columns! (fn [& _] nil)
                     #'bitool.ingest.runtime/fetch-checkpoint (fn [& _]
                                                                {:last_successful_cursor "{\"s3://bucket/a.jsonl\":\"same\",\"s3://bucket/b.jsonl\":\"old\"}"})
                     #'bitool.ingest.runtime/abort-preparing-batches! (fn [& _] nil)
                     #'bitool.connector.file/file-checksum (fn [_ _ path]
                                                             (if (= path "s3://bucket/a.jsonl") "same" "new"))
                     #'bitool.connector.file/fetch-files-async (fn [{:keys [file-config]}]
                                                                 (reset! fetch-config file-config)
                                                                 {:pages (async/to-chan! [{:stop-reason :eof
                                                                                           :state nil
                                                                                           :http-status 200}])
                                                                  :errors (async/to-chan! [])
                                                                  :cancel (fn [] nil)})
                     #'bitool.ingest.runtime/process-source-stream! (fn [& _]
                                                                      {:batch-seq 0
                                                                       :checkpoint-row nil
                                                                       :max-watermark nil
                                                                       :next-cursor nil
                                                                       :rows-extracted 0
                                                                       :rows-written 0
                                                                       :bad-records-total 0
                                                                       :bad-records-written 0
                                                                       :pages-fetched 0
                                                                       :retry-count 0
                                                                       :last-http-status 200
                                                                       :changed-partition-dates []
                                                                       :manifests []})
                     #'bitool.ingest.runtime/load-rows! (fn [& _] nil)
                     #'bitool.ingest.runtime/replace-row! (fn [& _] nil)
                     #'bitool.ingest.runtime/connection-dbtype (fn [_] "snowflake")}
      (fn []
        (runtime/run-file-node! 99 2)
        (is (= ["s3://bucket/b.jsonl"] (:paths @fetch-config)))))))

(deftest preview-endpoint-schema-returns-inferred-fields
  (with-redefs [api/do-request (fn [_]
                                 {:status 200
                                  :body {:data [{:id "t1" :vehicle {:id "v1"}}]}})]
    (let [out (runtime/preview-endpoint-schema!
                {:base_url "https://api.example.com"
                 :auth_ref {}}
                {:endpoint_name "trips"
                 :endpoint_url "/fleet/trips"
                 :schema_mode "infer"
                 :sample_records 10
                 :max_inferred_columns 10
                 :type_inference_enabled true
                 :pagination_strategy "none"
                 :json_explode_rules [{:path "$.data[]"}]
                 :primary_key_fields ["id"]})]
      (is (= 200 (:http_status out)))
      (is (some #(= "$.data[].id" (:path %)) (:inferred_fields out)))))) 

(deftest preview-endpoint-schema-rejects-failed-response
  (with-redefs [api/do-request (fn [_]
                                 {:status 500
                                  :body {:error "bad gateway"}})]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Schema preview request failed"
                          (runtime/preview-endpoint-schema!
                            {:base_url "https://api.example.com"
                             :auth_ref {}}
                            {:endpoint_name "trips"
                             :endpoint_url "/fleet/trips"
                             :schema_mode "infer"
                             :sample_records 10
                             :max_inferred_columns 10
                             :type_inference_enabled true
                             :pagination_strategy "none"
                             :json_explode_rules [{:path "$.data[]"}]
                             :primary_key_fields ["id"]})))))
