(ns bitool.ingest-execution-test
  (:require [bitool.db :as db]
            [bitool.control-plane :as control-plane]
            [bitool.ingest.file-runtime]
            [bitool.ingest.kafka-runtime]
            [bitool.ingest.execution :as execution]
            [bitool.ingest.runtime :as runtime]
            [bitool.ingest.scheduler :as scheduler]
            [bitool.platform.plugins]
            [clojure.string :as string]
            [clojure.test :refer :all]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import [java.time Instant]
           [java.util UUID]
           [java.util.concurrent CountDownLatch]))

(defn- postgres-available? []
  (try
    (jdbc/execute-one! db/ds ["SELECT 1"])
    true
    (catch Exception _
      false)))

(defn- unique-table-name
  [base-name]
  (str "test_" (string/replace (str (UUID/randomUUID)) "-" "") "_" base-name))

(defn- drop-table-if-exists!
  [table-name]
  (let [opts (jdbc/with-options db/ds {:builder-fn rs/as-unqualified-lower-maps})]
    (jdbc/execute! opts [(str "DROP TABLE IF EXISTS " table-name " CASCADE")])))

(defn- with-isolated-execution-tables
  [f]
  (let [tables {:graph-version (unique-table-name "graph_version")
                :graph-release (unique-table-name "graph_release")
                :execution-request (unique-table-name "execution_request")
                :execution-run (unique-table-name "execution_run")
                :node-run (unique-table-name "node_run")
                :execution-lease-heartbeat (unique-table-name "execution_lease_heartbeat")
                :execution-dlq (unique-table-name "execution_dlq")
                :execution-orphan-recovery-event (unique-table-name "execution_orphan_recovery_event")}]
    (with-redefs-fn {#'bitool.ingest.execution/graph-version-table (:graph-version tables)
                     #'bitool.ingest.execution/graph-release-table (:graph-release tables)
                     #'bitool.ingest.execution/execution-request-table (:execution-request tables)
                     #'bitool.ingest.execution/execution-run-table (:execution-run tables)
                     #'bitool.ingest.execution/node-run-table (:node-run tables)
                     #'bitool.ingest.execution/execution-lease-heartbeat-table (:execution-lease-heartbeat tables)
                     #'bitool.ingest.execution/execution-dlq-table (:execution-dlq tables)
                     #'bitool.ingest.execution/execution-orphan-recovery-event-table (:execution-orphan-recovery-event tables)}
      (fn []
        (try
          (execution/ensure-execution-tables!)
          (f tables)
          (finally
            (doseq [table-name (reverse (vals tables))]
              (drop-table-if-exists! table-name))))))))

(use-fixtures
  :each
  (fn [f]
    (with-redefs [execution/api-request-concurrency-context (fn [& _]
                                                              {:source-system "samara"
                                                               :credential-ref "cred-ref"
                                                               :source-max-concurrency 8
                                                               :credential-max-concurrency 1})]
      (f))))

(deftest execute-request-api-forwards-endpoint-name-from-queued-row
  (let [captured (atom nil)]
    (with-redefs [runtime/run-api-node! (fn [graph-id node-id opts]
                                          (reset! captured [graph-id node-id opts])
                                          {:status "success"})]
      (is (= {:status "success"}
             (#'execution/execute-request*!
              {:request_kind "api"
               :graph_id 42
               :node_id 7
               :endpoint_name "drivers"})))
      (is (= [42 7 {:endpoint-name "drivers"}] @captured)))))

(deftest execute-request-api-throws-when-all-endpoints-fail
  (with-redefs [runtime/run-api-node! (fn [_ _ _]
                                        {:status "failed"
                                         :results [{:endpoint_name "trips" :status "failed"}]})]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"failed for all endpoints"
                          (#'execution/execute-request*!
                           {:request_kind "api"
                            :request_id "req-1"
                            :graph_id 42
                            :node_id 7
                            :endpoint_name nil})))))

(deftest execute-request-kafka-forwards-endpoint-name-from-queued-row
  (let [captured (atom nil)]
    (with-redefs [bitool.ingest.kafka-runtime/run-kafka-node! (fn [graph-id node-id opts]
                                                                (reset! captured [graph-id node-id opts])
                                                                {:status "success"})]
      (is (= {:status "success"}
             (#'execution/execute-request*!
              {:request_kind "kafka"
               :graph_id 42
               :node_id 7
               :endpoint_name "orders.events"})))
      (is (= [42 7 {:endpoint-name "orders.events"}] @captured)))))

(deftest execute-request-file-forwards-endpoint-name-from-queued-row
  (let [captured (atom nil)]
    (with-redefs [bitool.ingest.file-runtime/run-file-node! (fn [graph-id node-id opts]
                                                              (reset! captured [graph-id node-id opts])
                                                              {:status "success"})]
      (is (= {:status "success"}
             (#'execution/execute-request*!
              {:request_kind "file"
               :graph_id 42
               :node_id 7
               :endpoint_name "orders.jsonl"})))
      (is (= [42 7 {:endpoint-name "orders.jsonl"}] @captured)))))

(deftest execute-request-supports-plugin-registered-request-kind
  (let [captured (atom nil)]
    (bitool.platform.plugins/register-execution-handler!
     :custom-test
     {:workload-classifier (fn [_] "plugin")
      :execute (fn [request-row request-params]
                 (reset! captured [request-row request-params])
                 {:status "plugin-ok"})})
    (is (= {:status "plugin-ok"}
           (#'execution/execute-request*!
            {:request_kind "custom-test"
             :graph_id 42
             :node_id 7
             :request_params "{\"hello\":\"world\"}"})))
    (is (= "world" (get-in @captured [1 :hello])))))

(deftest execute-request-scheduler-completes-enqueued-slot
  (let [completed (atom nil)
        scheduled-for "2026-03-14T10:00:00Z"]
    (with-redefs [scheduler/execute-scheduler-node! (fn [graph-id node-id]
                                                      {:graph_id graph-id
                                                       :scheduler_node_id node-id
                                                       :status "success"})
                  scheduler/complete-enqueued-slot! (fn [graph-id node-id scheduled-for-utc status details]
                                                      (reset! completed [graph-id node-id scheduled-for-utc status details]))]
      (is (= {:graph_id 99
              :scheduler_node_id 5
              :status "success"}
             (#'execution/execute-request*!
              {:request_kind "scheduler"
               :graph_id 99
               :node_id 5
               :request_params (str "{\"scheduled_for_utc\":\"" scheduled-for "\"}")})))
      (is (= [99 5 scheduled-for "success"
              {:graph_id 99
               :scheduler_node_id 5
               :status "success"}]
             @completed)))))

(deftest enqueue-api-request-rejects-unmet-graph-dependencies
  (with-redefs [control-plane/dependency-blockers (fn [_]
                                                    [{:upstream_graph_id 10
                                                      :reason "missing_successful_run"}])
                control-plane/graph-workspace-context (fn [_]
                                                       {:workspace_key "default"
                                                        :tenant_key "default"
                                                        :max_queued_requests 100})]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"unmet upstream dependencies"
                          (execution/enqueue-api-request! 99 2 {:endpoint-name "trips"})))))

(deftest enqueue-api-request-rejects-inactive-workspace
  (with-redefs [control-plane/dependency-blockers (fn [_] [])
                control-plane/graph-workspace-context (fn [_]
                                                       {:workspace_key "ops"
                                                        :tenant_key "tenant-a"
                                                        :max_queued_requests 100
                                                        :active false})]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Workspace is inactive"
                          (execution/enqueue-api-request! 99 2 {:endpoint-name "trips"})))))

(deftest enqueue-api-request-persists-request-and-run-with-phase5-columns
  (if-not (postgres-available?)
    (is true "Skipping enqueue integration test; local Postgres is not available")
    (with-isolated-execution-tables
      (fn [tables]
        (let [opts             (jdbc/with-options db/ds {:builder-fn rs/as-unqualified-lower-maps})
              graph-id          (+ 700000 (rand-int 100000))
              graph-version-id  (:id (jdbc/execute-one!
                                      opts
                                      [(str "INSERT INTO " (:graph-version tables) "
                                             (graph_id, graph_version, graph_name, graph_definition, definition_checksum)
                                             VALUES (?, ?, ?, ?, ?)
                                             RETURNING id")
                                       graph-id 3 "enqueue-phase5" "{}" (apply str (repeat 64 "b"))]))]
          (with-redefs [control-plane/dependency-blockers (fn [_] [])
                        control-plane/graph-workspace-context (fn [_]
                                                               {:workspace_key "ops"
                                                                :tenant_key "tenant-a"
                                                                :max_queued_requests 100
                                                                :tenant_max_queued_requests 1000
                                                                :active true
                                                                :tenant_active true})
                        execution/ensure-active-release! (fn [_ _ _]
                                                           {:graph_version_id graph-version-id
                                                            :graph_version 3})
                        execution/queue-partition-for (fn [& _] "p00")
                        execution/request-workload-class (fn [& _] "api")]
            (let [result  (execution/enqueue-api-request! graph-id 2 {:endpoint-name "trips"})
                  run-row  (jdbc/execute-one! opts [(str "SELECT request_kind, queue_partition, workload_class, endpoint_name, status, source_system, credential_ref
                                                         FROM " (:execution-run tables) " WHERE run_id = ?")
                                                   (java.util.UUID/fromString (:run_id result))])
                  req-row  (jdbc/execute-one! opts [(str "SELECT request_kind, queue_partition, workload_class, endpoint_name, status, source_system, credential_ref
                                                         FROM " (:execution-request tables) " WHERE request_id = ?")
                                                   (java.util.UUID/fromString (:request_id result))])]
              (is (= true (:created? result)))
              (is (= "queued" (:status result)))
              (is (= "p00" (:queue_partition run-row)))
              (is (= "api" (:workload_class run-row)))
              (is (= "trips" (:endpoint_name run-row)))
              (is (= "queued" (:status run-row)))
              (is (= "samara" (:source_system run-row)))
              (is (= "cred-ref" (:credential_ref run-row)))
              (is (= "p00" (:queue_partition req-row)))
              (is (= "api" (:workload_class req-row)))
              (is (= "samara" (:source_system req-row)))
              (is (= "cred-ref" (:credential_ref req-row)))
              (is (= "queued" (:status req-row))))))))))

(deftest enqueue-api-request-reuses-recovering-orphan-request-key
  (if-not (postgres-available?)
    (is true "Skipping recovering_orphan enqueue test; local Postgres is not available")
    (with-isolated-execution-tables
      (fn [tables]
        (let [opts            (jdbc/with-options db/ds {:builder-fn rs/as-unqualified-lower-maps})
              graph-id         (+ 800000 (rand-int 100000))
              request-id       (UUID/randomUUID)
              run-id           (UUID/randomUUID)
              request-key      (str "api::" graph-id "::2::default::trips")
              graph-version-id (:id (jdbc/execute-one!
                                     opts
                                     [(str "INSERT INTO " (:graph-version tables) "
                                            (graph_id, graph_version, graph_name, graph_definition, definition_checksum)
                                            VALUES (?, ?, ?, ?, ?)
                                            RETURNING id")
                                      graph-id 3 "recovering-orphan" "{}" (apply str (repeat 64 "c"))]))]
          (jdbc/execute!
           opts
           [(str "INSERT INTO " (:execution-request tables) "
                  (request_id, request_key, request_kind, tenant_key, workspace_key, graph_id, graph_version_id, graph_version,
                   environment, node_id, trigger_type, endpoint_name, request_params, queue_partition, workload_class, status, max_retries)
                  VALUES (?, ?, 'api', 'tenant-a', 'ops', ?, ?, 3, 'default', 2, 'manual', 'trips', '{}', 'p00', 'api', 'recovering_orphan', 3)")
            request-id request-key graph-id graph-version-id])
          (jdbc/execute!
           opts
           [(str "INSERT INTO " (:execution-run tables) "
                  (run_id, request_id, tenant_key, workspace_key, graph_id, graph_version_id, graph_version, environment,
                   request_kind, queue_partition, workload_class, node_id, trigger_type, endpoint_name, status)
                  VALUES (?, ?, 'tenant-a', 'ops', ?, ?, 3, 'default', 'api', 'p00', 'api', 2, 'manual', 'trips', 'recovering_orphan')")
            run-id request-id graph-id graph-version-id])
          (with-redefs [control-plane/dependency-blockers (fn [_] [])
                        control-plane/graph-workspace-context (fn [_]
                                                               {:workspace_key "ops"
                                                                :tenant_key "tenant-a"
                                                                :max_queued_requests 100
                                                                :tenant_max_queued_requests 1000
                                                                :active true
                                                                :tenant_active true})]
            (let [result        (execution/enqueue-api-request! graph-id 2 {:endpoint-name "trips"})
                  request-count (:cnt (jdbc/execute-one!
                                       opts
                                       [(str "SELECT COUNT(*) AS cnt FROM " (:execution-request tables))]))]
              (is (= false (:created? result)))
              (is (= (str request-id) (:request_id result)))
              (is (= (str run-id) (:run_id result)))
              (is (= 1 (long request-count))))))))))

(deftest enqueue-api-request-reuses-all-endpoints-run-for-overlapping-specific-endpoint
  (if-not (postgres-available?)
    (is true "Skipping overlapping API enqueue test; local Postgres is not available")
    (with-isolated-execution-tables
      (fn [tables]
        (let [opts            (jdbc/with-options db/ds {:builder-fn rs/as-unqualified-lower-maps})
              graph-id         (+ 810000 (rand-int 100000))
              request-id       (UUID/randomUUID)
              run-id           (UUID/randomUUID)
              request-key      (str "api::" graph-id "::2::default::")
              graph-version-id (:id (jdbc/execute-one!
                                     opts
                                     [(str "INSERT INTO " (:graph-version tables) "
                                            (graph_id, graph_version, graph_name, graph_definition, definition_checksum)
                                            VALUES (?, ?, ?, ?, ?)
                                            RETURNING id")
                                      graph-id 3 "overlap-all-endpoints" "{}" (apply str (repeat 64 "d"))]))]
          (jdbc/execute!
           opts
           [(str "INSERT INTO " (:execution-request tables) "
                  (request_id, request_key, request_kind, tenant_key, workspace_key, graph_id, graph_version_id, graph_version,
                   environment, node_id, trigger_type, endpoint_name, request_params, queue_partition, workload_class, status, max_retries)
                  VALUES (?, ?, 'api', 'tenant-a', 'ops', ?, ?, 3, 'default', 2, 'manual', NULL, '{}', 'p00', 'api', 'running', 3)")
            request-id request-key graph-id graph-version-id])
          (jdbc/execute!
           opts
           [(str "INSERT INTO " (:execution-run tables) "
                  (run_id, request_id, tenant_key, workspace_key, graph_id, graph_version_id, graph_version, environment,
                   request_kind, queue_partition, workload_class, node_id, trigger_type, endpoint_name, status)
                  VALUES (?, ?, 'tenant-a', 'ops', ?, ?, 3, 'default', 'api', 'p00', 'api', 2, 'manual', NULL, 'running')")
            run-id request-id graph-id graph-version-id])
          (with-redefs [control-plane/dependency-blockers (fn [_] [])
                        control-plane/graph-workspace-context (fn [_]
                                                               {:workspace_key "ops"
                                                                :tenant_key "tenant-a"
                                                                :max_queued_requests 100
                                                                :tenant_max_queued_requests 1000
                                                                :active true
                                                                :tenant_active true})]
            (let [result        (execution/enqueue-api-request! graph-id 2 {:endpoint-name "trips"})
                  request-count (:cnt (jdbc/execute-one!
                                       opts
                                       [(str "SELECT COUNT(*) AS cnt FROM " (:execution-request tables))]))]
              (is (= false (:created? result)))
              (is (= (str request-id) (:request_id result)))
              (is (= (str run-id) (:run_id result)))
              (is (= 1 (long request-count))))))))))

(deftest enqueue-api-request-rejects-all-endpoints-run-when-scoped-endpoint-is-active
  (if-not (postgres-available?)
    (is true "Skipping all-endpoints overlap enqueue test; local Postgres is not available")
    (with-isolated-execution-tables
      (fn [tables]
        (let [opts            (jdbc/with-options db/ds {:builder-fn rs/as-unqualified-lower-maps})
              graph-id         (+ 820000 (rand-int 100000))
              request-id       (UUID/randomUUID)
              run-id           (UUID/randomUUID)
              request-key      (str "api::" graph-id "::2::default::trips")
              graph-version-id (:id (jdbc/execute-one!
                                     opts
                                     [(str "INSERT INTO " (:graph-version tables) "
                                            (graph_id, graph_version, graph_name, graph_definition, definition_checksum)
                                            VALUES (?, ?, ?, ?, ?)
                                            RETURNING id")
                                      graph-id 3 "overlap-scoped-endpoint" "{}" (apply str (repeat 64 "e"))]))]
          (jdbc/execute!
           opts
           [(str "INSERT INTO " (:execution-request tables) "
                  (request_id, request_key, request_kind, tenant_key, workspace_key, graph_id, graph_version_id, graph_version,
                   environment, node_id, trigger_type, endpoint_name, request_params, queue_partition, workload_class, status, max_retries)
                  VALUES (?, ?, 'api', 'tenant-a', 'ops', ?, ?, 3, 'default', 2, 'manual', 'trips', '{}', 'p00', 'api', 'leased', 3)")
            request-id request-key graph-id graph-version-id])
          (jdbc/execute!
           opts
           [(str "INSERT INTO " (:execution-run tables) "
                  (run_id, request_id, tenant_key, workspace_key, graph_id, graph_version_id, graph_version, environment,
                   request_kind, queue_partition, workload_class, node_id, trigger_type, endpoint_name, status)
                  VALUES (?, ?, 'tenant-a', 'ops', ?, ?, 3, 'default', 'api', 'p00', 'api', 2, 'manual', 'trips', 'leased')")
            run-id request-id graph-id graph-version-id])
          (with-redefs [control-plane/dependency-blockers (fn [_] [])
                        control-plane/graph-workspace-context (fn [_]
                                                               {:workspace_key "ops"
                                                                :tenant_key "tenant-a"
                                                                :max_queued_requests 100
                                                                :tenant_max_queued_requests 1000
                                                                :active true
                                                                :tenant_active true})]
            (try
              (execution/enqueue-api-request! graph-id 2 {})
              (is false "Expected all-endpoints enqueue to reject overlapping endpoint-scoped run")
              (catch clojure.lang.ExceptionInfo e
                (is (= 409 (:status (ex-data e))))
                (is (= "trips" (:active_endpoint_name (ex-data e))))))))))))

(deftest enqueue-kafka-request-reuses-all-endpoints-run-for-overlapping-specific-endpoint
  (if-not (postgres-available?)
    (is true "Skipping overlapping Kafka enqueue test; local Postgres is not available")
    (with-isolated-execution-tables
      (fn [tables]
        (let [opts            (jdbc/with-options db/ds {:builder-fn rs/as-unqualified-lower-maps})
              graph-id         (+ 830000 (rand-int 100000))
              request-id       (UUID/randomUUID)
              run-id           (UUID/randomUUID)
              request-key      (str "kafka::" graph-id "::2::default::")
              graph-version-id (:id (jdbc/execute-one!
                                     opts
                                     [(str "INSERT INTO " (:graph-version tables) "
                                            (graph_id, graph_version, graph_name, graph_definition, definition_checksum)
                                            VALUES (?, ?, ?, ?, ?)
                                            RETURNING id")
                                      graph-id 3 "overlap-all-topics" "{}" (apply str (repeat 64 "f"))]))]
          (jdbc/execute!
           opts
           [(str "INSERT INTO " (:execution-request tables) "
                  (request_id, request_key, request_kind, tenant_key, workspace_key, graph_id, graph_version_id, graph_version,
                   environment, node_id, trigger_type, endpoint_name, request_params, queue_partition, workload_class, status, max_retries)
                  VALUES (?, ?, 'kafka', 'tenant-a', 'ops', ?, ?, 3, 'default', 2, 'manual', NULL, '{}', 'p00', 'kafka', 'running', 3)")
            request-id request-key graph-id graph-version-id])
          (jdbc/execute!
           opts
           [(str "INSERT INTO " (:execution-run tables) "
                  (run_id, request_id, tenant_key, workspace_key, graph_id, graph_version_id, graph_version, environment,
                   request_kind, queue_partition, workload_class, node_id, trigger_type, endpoint_name, status)
                  VALUES (?, ?, 'tenant-a', 'ops', ?, ?, 3, 'default', 'kafka', 'p00', 'kafka', 2, 'manual', NULL, 'running')")
            run-id request-id graph-id graph-version-id])
          (with-redefs [control-plane/dependency-blockers (fn [_] [])
                        control-plane/graph-workspace-context (fn [_]
                                                               {:workspace_key "ops"
                                                                :tenant_key "tenant-a"
                                                                :max_queued_requests 100
                                                                :tenant_max_queued_requests 1000
                                                                :active true
                                                                :tenant_active true})]
            (let [result        (execution/enqueue-kafka-request! graph-id 2 {:endpoint-name "orders.events"})
                  request-count (:cnt (jdbc/execute-one!
                                       opts
                                       [(str "SELECT COUNT(*) AS cnt FROM " (:execution-request tables))]))]
              (is (= false (:created? result)))
              (is (= (str request-id) (:request_id result)))
              (is (= (str run-id) (:run_id result)))
              (is (= 1 (long request-count))))))))))

(deftest enqueue-file-request-rejects-all-endpoints-run-when-scoped-endpoint-is-active
  (if-not (postgres-available?)
    (is true "Skipping all-endpoints file overlap enqueue test; local Postgres is not available")
    (with-isolated-execution-tables
      (fn [tables]
        (let [opts            (jdbc/with-options db/ds {:builder-fn rs/as-unqualified-lower-maps})
              graph-id         (+ 840000 (rand-int 100000))
              request-id       (UUID/randomUUID)
              run-id           (UUID/randomUUID)
              request-key      (str "file::" graph-id "::2::default::orders.jsonl")
              graph-version-id (:id (jdbc/execute-one!
                                     opts
                                     [(str "INSERT INTO " (:graph-version tables) "
                                            (graph_id, graph_version, graph_name, graph_definition, definition_checksum)
                                            VALUES (?, ?, ?, ?, ?)
                                            RETURNING id")
                                      graph-id 3 "overlap-file-endpoint" "{}" (apply str (repeat 64 "g"))]))]
          (jdbc/execute!
           opts
           [(str "INSERT INTO " (:execution-request tables) "
                  (request_id, request_key, request_kind, tenant_key, workspace_key, graph_id, graph_version_id, graph_version,
                   environment, node_id, trigger_type, endpoint_name, request_params, queue_partition, workload_class, status, max_retries)
                  VALUES (?, ?, 'file', 'tenant-a', 'ops', ?, ?, 3, 'default', 2, 'manual', 'orders.jsonl', '{}', 'p00', 'file', 'leased', 3)")
            request-id request-key graph-id graph-version-id])
          (jdbc/execute!
           opts
           [(str "INSERT INTO " (:execution-run tables) "
                  (run_id, request_id, tenant_key, workspace_key, graph_id, graph_version_id, graph_version, environment,
                   request_kind, queue_partition, workload_class, node_id, trigger_type, endpoint_name, status)
                  VALUES (?, ?, 'tenant-a', 'ops', ?, ?, 3, 'default', 'file', 'p00', 'file', 2, 'manual', 'orders.jsonl', 'leased')")
            run-id request-id graph-id graph-version-id])
          (with-redefs [control-plane/dependency-blockers (fn [_] [])
                        control-plane/graph-workspace-context (fn [_]
                                                               {:workspace_key "ops"
                                                                :tenant_key "tenant-a"
                                                                :max_queued_requests 100
                                                                :tenant_max_queued_requests 1000
                                                                :active true
                                                                :tenant_active true})]
            (try
              (execution/enqueue-file-request! graph-id 2 {})
              (is false "Expected all-endpoints file enqueue to reject overlapping endpoint-scoped run")
              (catch clojure.lang.ExceptionInfo e
                (is (= 409 (:status (ex-data e))))
                (is (= "orders.jsonl" (:active_endpoint_name (ex-data e))))))))))))

(deftest classify-failure-maps-common-runtime-errors
  (is (= "rate_limited"
         (#'execution/classify-failure
          (ex-info "Too many requests" {:status 429}))))
  (is (= "schema_drift"
         (#'execution/classify-failure
          (ex-info "duplicate enabled column_name values" {}))))
  (is (= "config_error"
         (#'execution/classify-failure
          (ex-info "No downstream target connection found for API node" {}))))
  (is (= "transient_network"
         (#'execution/classify-failure
          (ex-info "Connection timed out" {}))))
  (is (= "config_error"
         (#'execution/classify-failure
          (ex-info "Column name must be a valid identifier" {}))))
  (is (= "unknown"
         (#'execution/classify-failure
          (ex-info "Failed to parse JDBC URL" {}))))
  (is (= "unknown"
         (#'execution/classify-failure
          (ex-info "Missing conflict_status column in configuration" {}))))
  (is (= "unknown"
         (#'execution/classify-failure
          (ex-info "Reference thereof is missing" {})))))

(deftest retry-decision-and-backoff-follow-request-budget
  (is (= {:retryable_failure? true
          :retry_count 1
          :max_retries 3
          :retry? true}
         (#'execution/retry-decision {:retry_count 1 :max_retries 3} "transient_network")))
  (is (= {:retryable_failure? true
          :retry_count 3
          :max_retries 3
          :retry? false}
         (#'execution/retry-decision {:retry_count 3 :max_retries 3} "rate_limited")))
  (is (= 30 (#'execution/retry-delay-seconds {:retry_count 0} "transient_network")))
  (is (= 120 (#'execution/retry-delay-seconds {:retry_count 1} "rate_limited"))))

(deftest heartbeat-stop-does-not-wait-for-full-interval
  (with-redefs-fn {#'execution/renew-request-lease! (fn [& _] true)}
    (fn []
      (let [heartbeat (#'execution/start-lease-heartbeat!
                       "request-id"
                       "run-id"
                       "worker-id"
                       300
                       10)
            start-ms  (System/currentTimeMillis)]
        ((:stop heartbeat))
        (is (< (- (System/currentTimeMillis) start-ms) 1000))))))

(deftest sweep-expired-leases-claims-each-orphan-before-processing
  (let [claimed      (atom [{:request_id "req-1" :request_kind "scheduler" :request_params "{\"scheduled_for_utc\":\"2026-03-14T10:00:00Z\"}" :graph_id 9 :node_id 5}
                            {:request_id "req-2" :request_kind "api" :graph_id 9 :node_id 6}])
        handled      (atom [])
        completed    (atom [])]
    (with-redefs [execution/claim-expired-request! (fn []
                                                     (let [next-row (first @claimed)]
                                                       (swap! claimed subvec (min 1 (count @claimed)))
                                                       next-row))
                  execution/find-run-id (fn [request-id] (str request-id "-run"))
                  execution/record-orphan-recovery-event! (fn [& _] true)
                  execution/handle-request-failure! (fn [request-row run-id _ request-guard]
                                                      (swap! handled conj [(:request_id request-row) run-id request-guard])
                                                      (if (= "req-1" (:request_id request-row))
                                                        {:status "timed_out" :failure_class "worker_orphaned" :error "expired"}
                                                        {:status "queued" :failure_class "worker_orphaned"}))
                  execution/maybe-complete-scheduler-slot! (fn [request-row status details]
                                                             (swap! completed conj [(:request_id request-row) status details]))]
      (#'execution/sweep-expired-leases!)
      (is (= [["req-1" "req-1-run" {:expected-statuses ["recovering_orphan"]}]
              ["req-2" "req-2-run" {:expected-statuses ["recovering_orphan"]}]]
             @handled))
      (is (= [["req-1" "failed" {:failure_class "worker_orphaned" :error "expired"}]]
             @completed)))))

(deftest process-next-request-caps-orphan-sweep-work-per-poll
  (let [sweep-limit (atom nil)]
    (with-redefs [execution/sweep-expired-leases! (fn [limit]
                                                    (reset! sweep-limit limit)
                                                    nil)
                  execution/claim-next-request! (fn [& _] nil)]
      (with-redefs [bitool.config/env {:execution-lease-sweeper-max-per-poll 3}]
        (is (nil? (execution/process-next-request!)))
        (is (= 3 @sweep-limit))))))

(deftest execution-status-counts-returns-status-indexed-map
  (let [sql-params (atom nil)]
    (with-redefs [jdbc/execute! (fn [_ params]
                                  (reset! sql-params params)
                                [{:status "queued" :cnt 3}
                                 {:status "recovering_orphan" :cnt 1}])]
      (is (= {"queued" 3
              "recovering_orphan" 1}
             (execution/execution-request-status-counts)))
      (is (re-find #"WHERE status IN \('queued', 'leased', 'running', 'recovering_orphan'\)"
                   (first @sql-params))))))

(deftest ensure-execution-tables-runs-ddl-once-per-table-set
  (let [sql-calls (atom [])]
    (with-redefs-fn {#'bitool.ingest.execution/execution-table-init-cache (atom #{})
                     #'jdbc/execute! (fn [_ params]
                                       (swap! sql-calls conj params)
                                       [{:next.jdbc/update-count 0}])}
      (fn []
        (execution/ensure-execution-tables!)
        (let [first-call-count (count @sql-calls)]
          (is (pos? first-call-count))
          (execution/ensure-execution-tables!)
          (is (= first-call-count (count @sql-calls))))
        (is (not-any? #(re-find #"DROP INDEX IF EXISTS" (first %)) @sql-calls))))))

(deftest claim-next-request-filters-inactive-workspaces
  (let [sql-params (atom nil)]
    (with-redefs [jdbc/execute-one! (fn [_ params]
                                      (reset! sql-params params)
                                      nil)]
      (#'execution/claim-next-request! "worker-1" 300 {:allowed-partitions #{"p00"}
                                                       :allowed-workloads #{"api"}})
      (is (re-find #"COALESCE\(w.active, TRUE\) = TRUE"
                   (first @sql-params))))))

(deftest execution-demand-snapshot-groups-by-partition-and-workload
  (let [sql-params (atom nil)]
    (with-redefs [jdbc/execute! (fn [_ params]
                                  (reset! sql-params params)
                                  [{:tenant_key "tenant-a"
                                    :workspace_key "ops"
                                    :queue_partition "p00"
                                    :workload_class "api"
                                    :queued_count 4
                                    :active_count 2
                                    :oldest_queued_age_seconds 120}])]
      (is (= [{:tenant_key "tenant-a"
               :workspace_key "ops"
               :queue_partition "p00"
               :workload_class "api"
               :queued_count 4
               :active_count 2
               :oldest_queued_age_seconds 120}]
             (execution/execution-demand-snapshot)))
      (is (re-find #"WHERE status IN \('queued', 'leased', 'running', 'recovering_orphan'\)"
                   (first @sql-params))))))

(deftest maybe-record-usage-skips-when-metering-disabled
  (let [calls (atom [])]
    (with-redefs [control-plane/workspace-context (fn [_ _]
                                                    {:workspace_key "ops"
                                                     :tenant_key "tenant-a"
                                                     :metering_enabled false})
                  bitool.operations/record-execution-usage! (fn [payload]
                                                              (swap! calls conj payload))]
      (#'execution/maybe-record-usage!
       {:request_id (UUID/randomUUID)
        :tenant_key "tenant-a"
        :workspace_key "ops"
        :graph_id 99
        :node_id 2
        :request_kind "api"
        :queue_partition "p00"
        :workload_class "api"
        :retry_count 0
        :started_at_utc (Instant/parse "2026-03-14T10:00:00Z")}
       (UUID/randomUUID)
       "succeeded"
       nil
       {:rows_written 10})
      (is (empty? @calls)))))

(deftest orphan-recovery-metrics-reads-db-backed-events
  (let [sql-params (atom nil)]
    (with-redefs [jdbc/execute! (fn [_ params]
                                  (reset! sql-params params)
                                  [{:metric_name "claimed" :cnt 2}
                                   {:metric_name "timed_out" :cnt 1}])]
      (is (= {:claimed 2
              :timed_out 1
              :failed 0
              :queued 0
              :stale 0}
             (execution/orphan-recovery-metrics {:since-hours 12})))
      (is (= "12" (second @sql-params))))))

(deftest mark-run-status-does-not-update-run-when-request-guard-fails
  (let [calls (atom [])]
    (with-redefs [jdbc/execute! (fn [_ params]
                                  (swap! calls conj params)
                                  (if (re-find #"UPDATE .*execution_request" (first params))
                                    [{:next.jdbc/update-count 0}]
                                    (throw (ex-info "Execution run row should not be updated on stale guard" {}))))]
      (is (nil? (#'execution/mark-run-status!
                 :conn
                 (UUID/randomUUID)
                 (UUID/randomUUID)
                 "succeeded"
                 {:ok true}
                 nil
                 nil
                 {:expected-statuses ["leased" "running"]
                  :expected-worker-id "worker-1"})))
      (is (= 1 (count @calls))))))

(deftest concurrent-orphan-sweep-does-not-clobber-worker-completion
  (if-not (postgres-available?)
    (is true "Skipping integration race test; local Postgres is not available")
    (with-isolated-execution-tables
      (fn [tables]
        (let [request-id       (UUID/randomUUID)
              run-id           (UUID/randomUUID)
              graph-id         (+ 300000 (rand-int 100000))
              graph-version    1
              worker-id        "integration-worker-1"
              request-key      (str "integration::race::" request-id)
              opts             (jdbc/with-options db/ds {:builder-fn rs/as-unqualified-lower-maps})
              gv-row           (jdbc/execute-one!
                                opts
                                [(str "INSERT INTO " (:graph-version tables) " (graph_id, graph_version, graph_name, graph_definition, definition_checksum)
                                       VALUES (?, ?, ?, ?, ?)
                                       RETURNING id")
                                 graph-id graph-version "integration-graph" "{}" (apply str (repeat 64 "a"))])
              graph-version-id (:id gv-row)]
          (jdbc/execute!
           opts
           [(str "INSERT INTO " (:execution-request tables) "
                  (request_id, request_key, request_kind, graph_id, graph_version_id, graph_version,
                   environment, node_id, trigger_type, endpoint_name, request_params, status,
                   worker_id, lease_expires_at_utc, max_retries)
                  VALUES (?, ?, 'api', ?, ?, ?, 'default', 9, 'manual', 'trips', '{}', 'running',
                          ?, now() - interval '5 minutes', 0)")
            request-id request-key graph-id graph-version-id graph-version worker-id])
          (jdbc/execute!
           opts
           [(str "INSERT INTO " (:execution-run tables) "
                  (run_id, request_id, graph_id, graph_version_id, graph_version, environment,
                   request_kind, node_id, trigger_type, endpoint_name, status, started_at_utc)
                  VALUES (?, ?, ?, ?, ?, 'default', 'api', 9, 'manual', 'trips', 'running', ?)")
            run-id request-id graph-id graph-version-id graph-version (java.sql.Timestamp/from (Instant/now))])
          (let [start-latch   (CountDownLatch. 1)
                worker-future (future
                                (.await start-latch)
                                (jdbc/with-transaction [tx db/ds]
                                  (#'execution/mark-run-status! tx
                                                                request-id
                                                                run-id
                                                                "succeeded"
                                                                {:ok true}
                                                                nil
                                                                nil
                                                                {:expected-statuses ["leased" "running"]
                                                                 :expected-worker-id worker-id})))
                sweeper-future (future
                                 (.await start-latch)
                                 (#'execution/sweep-expired-leases!))]
            (.countDown start-latch)
            @worker-future
            @sweeper-future
            (let [request-row (jdbc/execute-one! opts [(str "SELECT status, failure_class FROM " (:execution-request tables) " WHERE request_id = ?") request-id])
                  run-row     (jdbc/execute-one! opts [(str "SELECT status, failure_class FROM " (:execution-run tables) " WHERE run_id = ?") run-id])]
              (is (contains? #{"succeeded" "timed_out"} (:status request-row)))
              (is (= (:status request-row) (:status run-row)))
              (is (not= "recovering_orphan" (:status request-row))))))))))
