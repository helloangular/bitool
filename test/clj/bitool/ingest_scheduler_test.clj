(ns bitool.ingest-scheduler-test
  (:require [bitool.db :as db]
            [bitool.graph2 :as g2]
            [bitool.ingest.execution :as execution]
            [bitool.ingest.runtime :as runtime]
            [bitool.ingest.scheduler :as scheduler]
            [clojure.test :refer :all]))

(deftest run-scheduler-node-runs-reachable-api-nodes
  (let [graph {:a {:id 77}
               :n {1 {:na {:name "O" :btype "O"} :e {}}
                   2 {:na {:name "schedule" :btype "Sc" :enabled true :cron_expression "0 * * * *" :timezone "UTC"} :e {3 {} 4 {}}}
                   3 {:na {:name "api-1" :btype "Ap"} :e {1 {}}}
                   4 {:na {:name "bridge" :btype "Fi"} :e {5 {}}}
                   5 {:na {:name "api-2" :btype "Ap"} :e {1 {}}}}}
        calls (atom [])]
    (with-redefs [db/getGraph (fn [_] graph)
                  g2/getData (fn [g id] (get-in g [:n id :na]))
                  runtime/run-api-node! (fn [gid api-node-id]
                                          (swap! calls conj [gid api-node-id])
                                          {:graph_id gid :api_node_id api-node-id})]
      (let [out (scheduler/run-scheduler-node! 77 2)]
        (is (= [[77 3] [77 5]] @calls))
        (is (= [3 5] (mapv :api_node_id (:api_runs out))))))))

(deftest run-scheduler-node-rejects-disabled-node
  (with-redefs [db/getGraph (fn [_] {:n {2 {:na {:name "schedule" :btype "Sc" :enabled false} :e {}}}})
                g2/getData (fn [g id] (get-in g [:n id :na]))]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Scheduler node is disabled"
                          (scheduler/run-scheduler-node! 77 2)))))

(deftest process-due-schedulers-enqueues-work-instead-of-running-inline
  (let [graph {:a {:id 77}
               :n {1 {:na {:name "O" :btype "O"} :e {}}
                   2 {:na {:name "schedule" :btype "Sc" :enabled true :cron_expression "0 * * * *" :timezone "UTC"} :e {3 {}}}
                   3 {:na {:name "api-1" :btype "Ap"} :e {1 {}}}}}
        captured (atom nil)
        matched-instant (java.time.Instant/parse "2026-03-14T10:00:00Z")]
    (with-redefs [db/list-graph-ids (fn [] [77])
                  db/getGraph (fn [_] graph)
                  scheduler/claim-run-slot! (fn [_ _ _] true)
                  scheduler/mark-slot-enqueued! (fn [& _] nil)
                  scheduler/finish-run-slot! (fn [& _] nil)
                  execution/enqueue-scheduler-request! (fn [graph-id scheduler-node-id opts]
                                                         (reset! captured [graph-id scheduler-node-id opts])
                                                         {:request_id "req-1" :run_id "run-1"})
                  runtime/run-api-node! (fn [& _]
                                          (throw (ex-info "scheduler should not run API nodes inline" {})))]
      (let [out (scheduler/process-due-schedulers! matched-instant)]
        (is (= [77 2 {:trigger-type "scheduler"
                      :scheduled-for-utc (java.time.Instant/parse "2026-03-14T10:00:00Z")}]
               @captured))
        (is (= "run-1" (:run_id (first out))))))))

(deftest scheduler-loop-logs-top-level-poll-failures
  (let [captured (atom nil)]
    (with-redefs [scheduler/process-due-schedulers! (fn []
                                                      (throw (ex-info "boom" {:phase :poll})))
                  scheduler/log-scheduler-poll-failure! (fn [e]
                                                          (reset! captured {:message (.getMessage e)
                                                                            :data (ex-data e)}))]
      (#'scheduler/poll-schedulers-once!)
      (is (= "boom" (:message @captured)))
      (is (= {:phase :poll} (:data @captured))))))
