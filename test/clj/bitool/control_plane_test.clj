(ns bitool.control-plane-test
  (:require [bitool.control-plane :as control-plane]
            [bitool.db :as db]
            [clojure.test :refer :all]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

(defn- postgres-available? []
  (try
    (jdbc/execute-one! db/ds ["SELECT 1"])
    true
    (catch Exception _
      false)))

(deftest ensure-control-plane-tables-preserves-default-active-flags
  (let [sql-calls (atom [])]
    (with-redefs-fn {#'bitool.control-plane/control-plane-ready? (atom false)
                     #'jdbc/execute! (fn [_ params]
                                       (swap! sql-calls conj params)
                                       [])}
      (fn []
        (control-plane/ensure-control-plane-tables!)
        (is (some #(re-find #"ON CONFLICT \(tenant_key\) DO NOTHING" (first %)) @sql-calls))
        (is (some #(re-find #"ON CONFLICT \(workspace_key\) DO NOTHING" (first %)) @sql-calls)))))

(deftest ensure-control-plane-tables-runs-ddl-only-once-per-process
  (let [sql-calls (atom [])]
    (with-redefs-fn {#'bitool.control-plane/control-plane-ready? (atom false)
                     #'jdbc/execute! (fn [_ params]
                                       (swap! sql-calls conj params)
                                       [])}
      (fn []
        (control-plane/ensure-control-plane-tables!)
        (let [first-call-count (count @sql-calls)]
          (is (pos? first-call-count))
          (control-plane/ensure-control-plane-tables!)
          (is (= first-call-count (count @sql-calls))))))))

(deftest dependency-blockers-uses-configurable-execution-run-table
  (let [sql-params (atom nil)]
    (with-redefs-fn {#'bitool.control-plane/control-plane-ready? (atom true)
                     #'bitool.control-plane/execution-run-table "execution_run_test"
                     #'jdbc/execute! (fn [_ params]
                                       (reset! sql-params params)
                                       [])}
      (fn []
        (control-plane/dependency-blockers 99)
        (is (re-find #"FROM execution_run_test" (first @sql-params)))))))

(deftest persist-graph-persists-updated-version-and-workspace
  (if-not (postgres-available?)
    (is true "Skipping OCC happy-path integration test; local Postgres is not available")
    (let [graph-id   (+ 600000 (rand-int 100000))
          base-graph {:a {:id graph-id :v 1 :name "persist-success"}}]
      (control-plane/ensure-control-plane-tables!)
      (jdbc/execute! db/ds ["DELETE FROM control_plane_graph_workspace WHERE graph_id = ?" graph-id])
      (jdbc/execute! db/ds ["DELETE FROM graph WHERE id = ?" graph-id])
      (jdbc/execute!
       db/ds
       ["INSERT INTO graph (id, version, name, definition) VALUES (?, ?, ?, ?)"
        graph-id
        1
        "persist-success"
        (pr-str base-graph)])
      (try
        (let [saved         (control-plane/persist-graph!
                             (assoc-in base-graph [:a :name] "persist-success-v2")
                             {:expected-version 1
                              :workspace-key "default"
                              :updated-by "alice"})
              workspace-row (jdbc/execute-one!
                             (jdbc/with-options db/ds {:builder-fn rs/as-unqualified-lower-maps})
                             ["SELECT workspace_key, updated_by FROM control_plane_graph_workspace WHERE graph_id = ?" graph-id])]
          (is (= 2 (get-in saved [:a :v])))
          (is (= "persist-success-v2" (get-in saved [:a :name])))
          (is (= "default" (:workspace_key workspace-row)))
          (is (= "alice" (:updated_by workspace-row))))
        (finally
          (jdbc/execute! db/ds ["DELETE FROM control_plane_graph_workspace WHERE graph_id = ?" graph-id])
          (jdbc/execute! db/ds ["DELETE FROM graph WHERE id = ?" graph-id])))))))

(deftest persist-graph-in-tx-rejects-stale-expected-version-without-db
  (with-redefs-fn {#'db/insert-graph! (fn [_ _]
                                        (throw (ex-info "should not insert stale graph" {})))
                   #'bitool.control-plane/current-graph-version* (fn [_ _] 5)}
    (fn []
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Graph version conflict"
                            (#'bitool.control-plane/persist-graph-in-tx!
                             :tx
                             {:a {:id 99 :v 4 :name "test"}}
                             {:graph-id 99
                              :expected-version 4
                              :workspace-key "default"
                              :updated-by "alice"}))))))

(deftest graph-workspace-context-inherits-configured-default-workspace
  (with-redefs [control-plane/ensure-control-plane-tables! (fn [] true)
                jdbc/execute-one! (fn [_ [_ lookup-id]]
                                    (case lookup-id
                                      99 nil
                                      "default" {:workspace_key "default"
                                                 :tenant_key "tenant-a"
                                                 :max_concurrent_requests 7
                                                 :max_queued_requests 350
                                                 :weight 5
                                                 :active false
                                                 :tenant_max_concurrent_requests 11
                                                 :tenant_max_queued_requests 700
                                                 :tenant_weight 4
                                                 :tenant_active false
                                                 :metering_enabled true}
                                      nil))]
    (is (= {:graph_id 99
            :workspace_key "default"
            :tenant_key "tenant-a"
            :max_concurrent_requests 7
            :max_queued_requests 350
            :weight 5
            :active false
            :tenant_max_concurrent_requests 11
            :tenant_max_queued_requests 700
            :tenant_weight 4
            :tenant_active false
            :metering_enabled true}
           (control-plane/graph-workspace-context 99)))))

(deftest upsert-tenant-normalizes-tenant-quota-config
  (let [calls (atom [])]
    (with-redefs [control-plane/ensure-control-plane-tables! (fn [] true)
                  jdbc/execute! (fn [_ params]
                                  (swap! calls conj params)
                                  [])
                  jdbc/execute-one! (fn [_ _]
                                      {:tenant_key "tenant-a"
                                       :max_concurrent_requests 12
                                       :max_queued_requests 1500
                                       :weight 3
                                       :metering_enabled false
                                       :active false})]
      (is (= {:tenant_key "tenant-a"
              :max_concurrent_requests 12
              :max_queued_requests 1500
              :weight 3
              :metering_enabled false
              :active false}
             (control-plane/upsert-tenant! {:tenant_key "tenant-a"
                                            :max_concurrent_requests 12
                                            :max_queued_requests 1500
                                            :weight 3
                                            :metering_enabled false
                                            :active false})))
      (is (re-find #"metering_enabled" (ffirst @calls))))))

(deftest resolve-managed-secret-returns-value-when-audit-write-fails
  (with-redefs [control-plane/ensure-control-plane-tables! (fn [] true)
                jdbc/execute-one! (fn [_ _]
                                    {:secret_value "plain-secret"
                                     :secret_encoding "plaintext"})
                control-plane/record-audit-event! (fn [_]
                                                    (throw (ex-info "audit unavailable" {})))]
    (is (= "plain-secret"
           (control-plane/resolve-managed-secret "SAMSARA_TOKEN")))))

(deftest record-api-bronze-signoff-validates-proof-status
  (with-redefs [control-plane/ensure-control-plane-tables! (fn [] true)]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"proof_status must be passed or failed"
                          (control-plane/record-api-bronze-signoff!
                           {:release_tag "r2026.03.16"
                            :environment "stage"
                            :commit_sha "abc"
                            :proof_summary_path "tmp/sum.json"
                            :proof_results_path "tmp/res.ndjson"
                            :proof_log_path "tmp/log.txt"
                            :proof_status "unknown"
                            :operator_name "alice"
                            :reviewer_name "bob"})))))

(deftest record-api-bronze-signoff-requires-existing-proof-files
  (with-redefs [control-plane/ensure-control-plane-tables! (fn [] true)]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"must point to an existing file"
                          (control-plane/record-api-bronze-signoff!
                           {:release_tag "r2026.03.16"
                            :environment "stage"
                            :commit_sha "abc"
                            :proof_summary_path "tmp/missing-summary.json"
                            :proof_results_path "tmp/missing-results.ndjson"
                            :proof_log_path "tmp/missing-suite.log"
                            :proof_status "passed"
                            :operator_name "alice"
                            :reviewer_name "bob"})))))

(deftest list-api-bronze-signoffs-forwards-environment-filter
  (let [captured (atom nil)]
    (with-redefs [control-plane/ensure-control-plane-tables! (fn [] true)
                  jdbc/execute! (fn [_ params]
                                  (reset! captured params)
                                  [{:id 1 :environment "stage"}])]
      (is (= [{:id 1 :environment "stage"}]
             (control-plane/list-api-bronze-signoffs {:environment "stage" :limit 25})))
      (is (re-find #"environment = \?" (first @captured)))
      (is (= ["stage" 25] (subvec @captured 1))))))

(deftest persist-graph-rejects-stale-expected-version
  (if-not (postgres-available?)
    (is true "Skipping stale OCC integration test; local Postgres is not available")
    (let [graph-id   (+ 400000 (rand-int 100000))
          base-graph {:a {:id graph-id :v 5 :name "stale-occ"}}]
      (control-plane/ensure-control-plane-tables!)
      (jdbc/execute! db/ds ["DELETE FROM control_plane_graph_workspace WHERE graph_id = ?" graph-id])
      (jdbc/execute! db/ds ["DELETE FROM graph WHERE id = ?" graph-id])
      (jdbc/execute!
       db/ds
       ["INSERT INTO graph (id, version, name, definition) VALUES (?, ?, ?, ?)"
        graph-id
        5
        "stale-occ"
        (pr-str base-graph)])
      (try
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"Graph version conflict"
                              (control-plane/persist-graph!
                               {:a {:id graph-id :v 4 :name "test"}}
                               {:expected-version 4
                                :workspace-key "default"
                                :updated-by "alice"})))
        (finally
          (jdbc/execute! db/ds ["DELETE FROM control_plane_graph_workspace WHERE graph_id = ?" graph-id])
          (jdbc/execute! db/ds ["DELETE FROM graph WHERE id = ?" graph-id]))))))

(deftest persist-graph-rejects-second-concurrent-writer
  (if-not (postgres-available?)
    (is true "Skipping concurrent OCC integration test; local Postgres is not available")
    (let [graph-id    (+ 500000 (rand-int 100000))
          base-graph  {:a {:id graph-id :v 1 :name "control-plane-occ"}}
          start-latch (java.util.concurrent.CountDownLatch. 1)]
      (control-plane/ensure-control-plane-tables!)
      (jdbc/execute! db/ds ["DELETE FROM control_plane_graph_workspace WHERE graph_id = ?" graph-id])
      (jdbc/execute! db/ds ["DELETE FROM graph WHERE id = ?" graph-id])
      (jdbc/execute!
       db/ds
       ["INSERT INTO graph (id, version, name, definition) VALUES (?, ?, ?, ?)"
        graph-id
        1
        "control-plane-occ"
        (pr-str base-graph)])
      (try
        (let [writer-a (future
                         (.await start-latch)
                         (try
                           {:status :saved
                            :graph (control-plane/persist-graph!
                                    (assoc-in base-graph [:a :name] "writer-a")
                                    {:expected-version 1
                                     :workspace-key "default"
                                     :updated-by "writer-a"})}
                           (catch clojure.lang.ExceptionInfo e
                             {:status :error
                              :message (ex-message e)
                              :data (ex-data e)})))
              writer-b (future
                         (.await start-latch)
                         (try
                           {:status :saved
                            :graph (control-plane/persist-graph!
                                    (assoc-in base-graph [:a :name] "writer-b")
                                    {:expected-version 1
                                     :workspace-key "default"
                                     :updated-by "writer-b"})}
                           (catch clojure.lang.ExceptionInfo e
                             {:status :error
                              :message (ex-message e)
                              :data (ex-data e)})))]
          (.countDown start-latch)
          (let [results      [@writer-a @writer-b]
                saved-count  (count (filter #(= :saved (:status %)) results))
                error-result (first (filter #(= :error (:status %)) results))
                versions     (jdbc/execute!
                              (jdbc/with-options db/ds {:builder-fn rs/as-unqualified-lower-maps})
                              ["SELECT version FROM graph WHERE id = ? ORDER BY version ASC" graph-id])]
            (is (= 1 saved-count))
            (is (= "Graph version conflict" (:message error-result)))
            (is (= 409 (get-in error-result [:data :status])))
            (is (= 2 (get-in error-result [:data :current_version])))
            (is (= [1 2] (mapv :version versions)))))
        (finally
          (jdbc/execute! db/ds ["DELETE FROM control_plane_graph_workspace WHERE graph_id = ?" graph-id])
          (jdbc/execute! db/ds ["DELETE FROM graph WHERE id = ?" graph-id]))))))

(deftest dependency-blockers-flags-missing-and-stale-upstream-runs
  (let [rows [{:upstream_graph_id 10
               :freshness_window_seconds nil
               :latest_success_at_utc nil}
              {:upstream_graph_id 11
               :freshness_window_seconds 60
               :latest_success_at_utc (.minusSeconds (java.time.Instant/now) 3600)}
              {:upstream_graph_id 12
               :freshness_window_seconds 3600
               :latest_success_at_utc (java.time.Instant/now)}]]
    (with-redefs [jdbc/execute! (fn [_ _] rows)]
      (is (= [{:upstream_graph_id 10
               :reason "missing_successful_run"}
              {:upstream_graph_id 11
               :reason "stale_successful_run"
               :latest_success_at_utc (:latest_success_at_utc (second rows))
               :freshness_window_seconds 60}]
             (control-plane/dependency-blockers 99))))))
