(ns bitool.operations-test
  (:require [bitool.ingest.execution :as execution]
            [bitool.operations :as operations]
            [clojure.test :refer :all]
            [next.jdbc :as jdbc]))

(deftest ensure-operations-tables-runs-ddl-only-once-per-process
  (let [sql-calls (atom [])]
    (with-redefs-fn {#'bitool.operations/operations-ready? (atom false)
                     #'jdbc/execute! (fn [_ params]
                                       (swap! sql-calls conj params)
                                       [])}
      (fn []
        (operations/ensure-operations-tables!)
        (let [first-call-count (count @sql-calls)]
          (is (pos? first-call-count))
          (operations/ensure-operations-tables!)
          (is (= first-call-count (count @sql-calls))))))))

(deftest freshness-dashboard-marks-overdue-rows
  (with-redefs [jdbc/execute! (fn [_ _]
                                [{:graph_id 99
                                  :endpoint_name "trips"
                                  :freshness_lag_seconds 7200
                                  :freshness_sla_seconds 3600
                                  :updated_at_utc (java.time.Instant/now)}])]
    (let [rows (operations/freshness-dashboard {:workspace-key "ops"})]
      (is (= 1 (count rows)))
      (is (= true (:overdue? (first rows))))
      (is (= 7200 (:freshness_lag_seconds (first rows)))))))

(deftest freshness-dashboard-applies-limit
  (let [sql-params (atom nil)]
    (with-redefs [jdbc/execute! (fn [_ params]
                                  (reset! sql-params params)
                                  [])]
      (operations/freshness-dashboard {:workspace-key "ops" :limit 25})
      (is (= 25 (last @sql-params)))
      (is (re-find #"LIMIT \?" (first @sql-params))))))

(deftest record-endpoint-freshness-ignores-malformed-run-id
  (let [calls (atom [])]
    (with-redefs [jdbc/execute! (fn [_ params]
                                  (swap! calls conj params)
                                  [])]
      (operations/record-endpoint-freshness!
       {:graph-id 99
        :api-node-id 2
        :tenant-key "default"
        :workspace-key "default"
        :source-system "samara"
        :endpoint-name "trips"
        :target-table "audit.trips"
        :run-id "not-a-uuid"
        :status "success"
        :rows-written 10
        :finished-at (java.time.Instant/now)})
      (let [insert-call (last @calls)]
        (is (nil? (nth insert-call 8)))
        (is (nil? (nth insert-call 11)))))))

(deftest replay-execution-run-delegates-api-runs-to-enqueue
  (let [captured (atom nil)]
    (with-redefs [execution/get-execution-run (fn [_]
                                                {:run_id "run-1"
                                                 :request_kind "api"
                                                 :graph_id 99
                                                 :node_id 2
                                                 :endpoint_name "trips"})
                  execution/enqueue-api-request! (fn [graph-id node-id opts]
                                                   (reset! captured [graph-id node-id opts])
                                                   {:created? true
                                                    :request_id "req-2"
                                                    :run_id "run-2"
                                                    :status "queued"})]
      (is (= "run-2" (:run_id (operations/replay-execution-run! "run-1"))))
      (is (= [99 2 {:endpoint-name "trips"
                    :trigger-type "replay"
                    :request-params {:replay_source_run_id "run-1"
                                     :replay_mode "deterministic"
                                     :replay_source_graph_version nil
                                     :replay_source_status nil}}]
             @captured)))))

(deftest record-execution-usage-persists-aggregated-meter-row
  (let [calls (atom [])]
    (with-redefs [jdbc/execute! (fn [_ params]
                                  (swap! calls conj params)
                                  [])]
      (operations/record-execution-usage!
       {:tenant-key "tenant-a"
        :workspace-key "ops"
        :request-kind "api"
        :workload-class "replay"
        :queue-partition "p01"
        :status "succeeded"
        :rows-written 42
        :retry-count 2
        :started-at (java.time.Instant/parse "2026-03-14T10:00:00Z")
        :finished-at (java.time.Instant/parse "2026-03-14T10:00:05Z")})
      (let [insert-call (last @calls)]
        (is (re-find #"operations_usage_meter_daily" (first insert-call)))
        (is (= "tenant-a" (nth insert-call 2)))
        (is (= "ops" (nth insert-call 3)))
        (is (= 42 (nth insert-call 8)))))))

(deftest usage-dashboard-applies-filters-and-limit
  (let [sql-params (atom nil)]
    (with-redefs [jdbc/execute! (fn [_ params]
                                  (reset! sql-params params)
                                  [{:tenant_key "tenant-a" :request_count 2}])]
      (let [rows (operations/usage-dashboard {:tenant-key "tenant-a"
                                              :request-kind "api"
                                              :workload-class "replay"
                                              :limit 25})]
        (is (= 1 (count rows)))
        (is (re-find #"tenant_key = \?" (first @sql-params)))
        (is (re-find #"request_kind = \?" (first @sql-params)))
        (is (re-find #"workload_class = \?" (first @sql-params)))
        (is (= 25 (last @sql-params)))))))

(deftest usage-dashboard-parses-usage-date-before-binding
  (let [sql-params (atom nil)]
    (with-redefs [jdbc/execute! (fn [_ params]
                                  (reset! sql-params params)
                                  [])]
      (operations/usage-dashboard {:usage-date "2026-03-14"})
      (is (instance? java.sql.Date (second @sql-params))))))

(deftest usage-dashboard-rejects-invalid-usage-date
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"usage_date must be YYYY-MM-DD"
                        (operations/usage-dashboard {:usage-date "2026-14-99"}))))
