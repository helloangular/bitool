(ns bitool.ops-test
  (:require [bitool.ingest.execution :as ingest-execution]
            [bitool.ops.admin :as admin]
            [bitool.ops.alerts :as alerts]
            [bitool.ops.dashboard :as dashboard]
            [bitool.ops.routes :as ops-routes]
            [clojure.test :refer :all]
            [next.jdbc :as jdbc]))

(deftest list-alerts-supports-comma-separated-states
  (let [captured (atom nil)]
    (with-redefs [alerts/ensure-alert-tables! (fn [])
                  jdbc/execute! (fn [_ sql-vec]
                                  (reset! captured sql-vec)
                                  [])]
      (alerts/list-alerts {:workspace-key "ws1"
                           :state "fired,acknowledged,silenced"
                           :limit 10
                           :offset 0})
      (is (re-find #"state = ANY" (first @captured)))
      (is (= "ws1" (second @captured)))
      (is (= ["fired" "acknowledged" "silenced"]
             (vec (nth @captured 2)))))))

(deftest validate-config-supports-ops-console-shapes
  (is (empty? (admin/validate-config
               "retention_policies"
               {:manifest_retention 7
                :bad_record_retention 14
                :checkpoint_history 30
                :dlq_retention 30
                :archive_destination "s3://archive"})))
  (is (empty? (admin/validate-config
               "source_concurrency"
               {:sources [{:source_key "kafka::orders" :max_concurrent 2}]}))))

(deftest replay-from-checkpoint-enqueues-through-execution-api
  (let [query-step (atom 0)
        captured (atom nil)]
    (with-redefs [dashboard/safe-query-one
                  (fn [_ _]
                    (case (swap! query-step inc)
                      1 {:batch_id "b1"
                         :run_id "run-1"
                         :source_system "kafka"
                         :endpoint_name "orders.events"}
                      2 {:graph_id 12
                         :api_node_id 34
                         :source_system "kafka"
                         :endpoint_name "orders.events"}))
                  ingest-execution/enqueue-kafka-request!
                  (fn [graph-id node-id opts]
                    (reset! captured [graph-id node-id opts])
                    {:created? true
                     :graph_id graph-id
                     :node_id node-id
                     :status "queued"})]
      (let [result (dashboard/replay-from-checkpoint!
                    {:workspace-key "ws1"
                     :source-key "kafka::orders.events"
                     :from-batch "b1"
                     :operator "alice"})]
        (is (= [12
                34
                {:workspace-key "ws1"
                 :endpoint-name "orders.events"
                 :trigger-type "manual"
                 :request-params {:replay_source_run_id "run-1"
                                  :replay_source_batch_ids ["b1"]
                                  :endpoint-name "orders.events"}}]
               @captured))
        (is (= "queued" (:status result)))))))

(deftest replay-bad-records-collapses-to-unique-batches
  (let [replays (atom [])]
    (with-redefs [dashboard/safe-query
                  (fn [_ _]
                    [{:artifact_id 1 :batch_id "b1"}
                     {:artifact_id 2 :batch_id "b2"}
                     {:artifact_id 3 :batch_id "b1"}])
                  dashboard/replay-from-checkpoint!
                  (fn [{:keys [from-batch dry-run]}]
                    (swap! replays conj [from-batch dry-run])
                    {:status "queued" :batch_id from-batch})]
      (let [result (dashboard/replay-bad-records!
                    {:record-ids [1 2 3]
                     :workspace-key "ws1"})]
        (is (= 3 (:requested_records result)))
        (is (= 2 (:unique_batches result)))
        (is (= #{["b1" nil] ["b2" nil]} (set @replays)))))))

(deftest route-default-limit-is-applied
  (let [captured (atom nil)]
    (with-redefs [alerts/list-alerts (fn [opts] (reset! captured opts) [])]
      (ops-routes/list-alerts {:params {}})
      (is (= 100 (:limit @captured)))
      (is (= 0 (:offset @captured))))))

(deftest ops-route-table-loads
  (is (vector? (ops-routes/ops-routes))))
