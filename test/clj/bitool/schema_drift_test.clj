(ns bitool.schema-drift-test
  (:require [bitool.ops.schema-drift :as schema-drift]
            [bitool.ops.alerts :as alerts]
            [bitool.ops.routes :as ops-routes]
            [clojure.test :refer :all]
            [bitool.ingest.runtime :as ingest-runtime]
            [bitool.db :as db]
            [next.jdbc :as jdbc]))

;; ─── Severity Classification ──────────────────────────────────────────

(deftest classify-drift-severity-info-for-new-fields-only
  (is (= "info"
         (schema-drift/classify-drift-severity
          {:new_fields [{:path "driver_id" :type "STRING"}]
           :missing_fields []
           :type_changes []}))))

(deftest classify-drift-severity-warning-for-missing-fields
  (is (= "warning"
         (schema-drift/classify-drift-severity
          {:new_fields []
           :missing_fields [{:path "legacy_code" :type "STRING"}]
           :type_changes []}))))

(deftest classify-drift-severity-warning-for-compatible-type-changes
  (is (= "warning"
         (schema-drift/classify-drift-severity
          {:new_fields []
           :missing_fields []
           :type_changes [{:current_type "INT" :inferred_type "BIGINT"}]}))))

(deftest classify-drift-severity-breaking-for-incompatible-type-changes
  (is (= "breaking"
         (schema-drift/classify-drift-severity
          {:new_fields []
           :missing_fields []
           :type_changes [{:current_type "STRING" :inferred_type "INT"}]}))))

(deftest classify-drift-severity-nil-for-no-drift
  (is (nil? (schema-drift/classify-drift-severity
             {:new_fields [] :missing_fields [] :type_changes []}))))

(deftest classify-drift-severity-breaking-trumps-info
  (is (= "breaking"
         (schema-drift/classify-drift-severity
          {:new_fields [{:path "x" :type "STRING"}]
           :missing_fields []
           :type_changes [{:current_type "BOOLEAN" :inferred_type "INT"}]}))))

;; ─── DDL Generation ──────────────────────────────────────────────────

(deftest generate-schema-ddl-adds-new-columns
  (let [current  [{:column_name "id" :type "INT" :enabled true}]
        approved [{:column_name "id" :type "INT" :enabled true}
                  {:column_name "name" :type "STRING" :enabled true}]
        plan     (schema-drift/generate-schema-ddl "postgres" "my_table" current approved)]
    (is (= 1 (count (:add-columns plan))))
    (is (= "name" (get-in (first (:add-columns plan)) [:column :column_name])))
    (is (re-find #"ADD COLUMN" (:sql (first (:add-columns plan)))))))

(deftest generate-schema-ddl-widens-types
  (let [current  [{:column_name "age" :type "INT" :enabled true}]
        approved [{:column_name "age" :type "BIGINT" :enabled true}]
        plan     (schema-drift/generate-schema-ddl "postgres" "my_table" current approved)]
    (is (= 0 (count (:add-columns plan))))
    (is (= 1 (count (:widen-columns plan))))
    (is (= "INT" (:from_type (first (:widen-columns plan)))))
    (is (= "BIGINT" (:to_type (first (:widen-columns plan)))))))

(deftest generate-schema-ddl-skips-disabled-fields
  (let [current  [{:column_name "id" :type "INT" :enabled true}]
        approved [{:column_name "id" :type "INT" :enabled true}
                  {:column_name "secret" :type "STRING" :enabled false}]
        plan     (schema-drift/generate-schema-ddl "postgres" "my_table" current approved)]
    (is (= 0 (count (:add-columns plan))))))

(deftest generate-schema-ddl-rejects-narrowing
  (let [current  [{:column_name "age" :type "BIGINT" :enabled true}]
        approved [{:column_name "age" :type "INT" :enabled true}]
        plan     (schema-drift/generate-schema-ddl "postgres" "my_table" current approved)]
    (is (= 0 (count (:widen-columns plan))))))

(deftest generate-schema-ddl-snowflake-types
  (let [current  [{:column_name "id" :type "INT" :enabled true}]
        approved [{:column_name "id" :type "INT" :enabled true}
                  {:column_name "note" :type "STRING" :enabled true}]
        plan     (schema-drift/generate-schema-ddl "snowflake" "my_table" current approved)]
    (is (re-find #"VARCHAR" (:sql (first (:add-columns plan)))))))

;; ─── Event Persistence (with mocked DB) ──────────────────────────────

(deftest persist-drift-event-deduplicates
  (let [insert-count (atom 0)
        existing-event (atom nil)]
    (with-redefs [schema-drift/ensure-schema-drift-tables! (fn [])
                  jdbc/execute-one!
                  (fn [_ sql-vec]
                    (cond
                      ;; Dedup SELECT
                      (and (string? (first sql-vec))
                           (.contains ^String (first sql-vec) "SELECT event_id"))
                      @existing-event

                      ;; INSERT
                      (and (string? (first sql-vec))
                           (.contains ^String (first sql-vec) "INSERT INTO schema_drift_event"))
                      (do (swap! insert-count inc)
                          {:event_id 42})

                      :else nil))
                  jdbc/execute! (fn [_ _] nil)]
      ;; First call should insert
      (let [params {:workspace-key "ops" :graph-id 1 :api-node-id 2
                    :endpoint-name "test/ep" :source-system "test"
                    :run-id nil
                    :drift {:new_fields [{:path "x" :type "STRING"}]
                            :missing_fields [] :type_changes []}
                    :enforcement-mode "additive"
                    :schema-hash-before "aaa" :schema-hash-after "bbb"}]
        (schema-drift/persist-schema-drift-event! params)
        (is (= 1 @insert-count))

        ;; Simulate existing unacknowledged event
        (reset! existing-event {:event_id 42})
        (schema-drift/persist-schema-drift-event! params)
        ;; Should NOT have inserted again
        (is (= 1 @insert-count))))))

;; ─── Route Table ──────────────────────────────────────────────────────

(deftest schema-drift-routes-present-in-ops-route-table
  (let [routes (ops-routes/ops-routes)
        paths  (set (map first (rest routes)))]
    (is (contains? paths "/schema/driftEvents"))
    (is (contains? paths "/schema/driftEvents/:event_id"))
    (is (contains? paths "/schema/driftEvents/:event_id/ack"))
    (is (contains? paths "/schema/notifications"))
    (is (contains? paths "/schema/notifications/markRead"))
    (is (contains? paths "/schema/notifications/unreadCount"))
    (is (contains? paths "/schema/timeline"))
    (is (contains? paths "/schema/previewDdl"))
    (is (contains? paths "/schema/applyDdl"))
    (is (contains? paths "/schema/ddlHistory"))))

;; ─── Notification Creation via mock ───────────────────────────────────

(deftest notification-created-on-event-insert
  (let [notif-created (atom false)]
    (with-redefs [schema-drift/ensure-schema-drift-tables! (fn [])
                  alerts/fire! (fn [_] nil)
                  jdbc/execute-one!
                  (fn [_ sql-vec]
                    (cond
                      (.contains ^String (first sql-vec) "SELECT event_id") nil
                      (.contains ^String (first sql-vec) "INSERT INTO schema_drift_event")
                      {:event_id 99}
                      :else nil))
                  jdbc/execute!
                  (fn [_ sql-vec]
                    (when (and (string? (first sql-vec))
                               (.contains ^String (first sql-vec) "INSERT INTO schema_notification"))
                      (reset! notif-created true))
                    nil)]
      (schema-drift/persist-schema-drift-event!
       {:workspace-key "ops" :graph-id 1 :api-node-id 2
        :endpoint-name "test/ep" :source-system "test"
        :drift {:new_fields [{:path "x" :type "STRING"}]
                :missing_fields [] :type_changes []}
        :enforcement-mode "permissive"
        :schema-hash-before "a" :schema-hash-after "b"})
      (is @notif-created))))

(deftest ops-alert-fired-on-new-drift-event-only
  (let [fired (atom [])]
    (with-redefs [schema-drift/ensure-schema-drift-tables! (fn [])
                  jdbc/execute-one!
                  (fn [_ sql-vec]
                    (cond
                      (.contains ^String (first sql-vec) "SELECT event_id")
                      nil

                      (.contains ^String (first sql-vec) "INSERT INTO schema_drift_event")
                      {:event_id 123}

                      :else nil))
                  jdbc/execute! (fn [_ _] nil)
                  alerts/fire! (fn [alert]
                                 (swap! fired conj alert)
                                 {:alert_id "alert-1"})]
      (schema-drift/persist-schema-drift-event!
       {:workspace-key "ops"
        :graph-id 11
        :api-node-id 22
        :endpoint-name "fleet/vehicles"
        :source-system "samsara"
        :run-id "run-1"
        :drift {:new_fields [{:path "driver_id" :type "STRING"}]
                :missing_fields []
                :type_changes []}
        :enforcement-mode "additive"
        :schema-hash-before "before"
        :schema-hash-after "after"})
      (is (= 1 (count @fired)))
      (is (= "schema_drift" (:alert-type (first @fired))))
      (is (= "info" (:severity (first @fired))))
      (is (= "schema_drift:ops:11:22:fleet/vehicles:123"
             (:source-key (first @fired))))
      (is (= 123 (get-in (first @fired) [:detail :event_id]))))))

(deftest ops-alert-not-fired-when-drift-event-deduped
  (let [fire-count (atom 0)]
    (with-redefs [schema-drift/ensure-schema-drift-tables! (fn [])
                  jdbc/execute-one!
                  (fn [_ sql-vec]
                    (cond
                      (.contains ^String (first sql-vec) "SELECT event_id")
                      {:event_id 123}

                      (.contains ^String (first sql-vec) "INSERT INTO schema_drift_event")
                      (throw (ex-info "insert should not happen" {}))

                      :else nil))
                  jdbc/execute! (fn [_ _] nil)
                  alerts/fire! (fn [_]
                                 (swap! fire-count inc)
                                 nil)]
      (schema-drift/persist-schema-drift-event!
       {:workspace-key "ops"
        :graph-id 11
        :api-node-id 22
        :endpoint-name "fleet/vehicles"
        :source-system "samsara"
        :drift {:new_fields [{:path "driver_id" :type "STRING"}]
                :missing_fields []
                :type_changes []}
        :enforcement-mode "additive"
        :schema-hash-before "before"
        :schema-hash-after "after"})
      (is (= 0 @fire-count)))))

(deftest get-and-ack-drift-event-are-workspace-scoped
  (let [seen (atom [])]
    (with-redefs [schema-drift/ensure-schema-drift-tables! (fn [])
                  jdbc/execute-one!
                  (fn [_ sql-vec]
                    (swap! seen conj sql-vec)
                    (cond
                      (.contains ^String (first sql-vec) "SELECT * FROM schema_drift_event")
                      {:event_id 7 :workspace_key "ops"}

                      (.contains ^String (first sql-vec) "UPDATE schema_drift_event")
                      {:event_id 7}

                      :else nil))]
      (is (= {:event_id 7 :workspace_key "ops"}
             (schema-drift/get-drift-event 7 "ops")))
      (is (= {:acknowledged true :event_id 7}
             (schema-drift/acknowledge-drift-event! 7 "ops" "reviewer")))
      (is (= "ops" (nth (first @seen) 2)))
      (is (= "ops" (nth (second @seen) 3))))))

(deftest preview-schema-ddl-uses-promoted-approval-and-field-decisions
  (with-redefs [schema-drift/get-drift-event
                (fn [_ workspace-key]
                  {:event_id 99
                   :workspace_key workspace-key
                   :graph_id 1
                   :api_node_id 2
                   :endpoint_name "fleet/vehicles"
                   :schema_hash_after "hash-2"
                   :drift_json {:new_fields [{:column_name "new_col" :type "STRING"}]}})
                ingest-runtime/schema-drift-target-context
                (fn [_ _ _]
                  {:workspace_key "ops"
                   :graph_id 1
                   :api_node_id 2
                   :endpoint_name "fleet/vehicles"
                   :conn_id 11
                   :warehouse "postgresql"
                   :target_table "public.fleet_vehicles"})
                ingest-runtime/resolve-api-schema-approval
                (fn [_ _ {:keys [schema-hash]}]
                  (when (= schema-hash "hash-2")
                    {:schema_hash "hash-2"
                     :review_state "approved"
                     :promoted true
                     :field_decisions {"existing_col" {:decision "exclude"}}
                     :inferred_fields [{:column_name "existing_col" :type "INT"}
                                       {:column_name "new_col" :type "STRING"}]}))
                db/create-dbspec-from-id (fn [_] {:dbtype "postgresql" :dbname "bitool" :schema "public"})
                db/get-columns (fn [_ _ _ _]
                                 [{:column_name "id" :data_type "integer"}
                                  {:column_name "existing_col" :data_type "integer"}])]
    (let [preview (schema-drift/preview-schema-ddl
                   {:workspace-key "ops"
                    :event-id 99})]
      (is (= "public.fleet_vehicles" (:table_name preview)))
      (is (= "postgres" (:warehouse preview)))
      (is (= 1 (count (get-in preview [:ddl_plan :add-columns]))))
      (is (= "new_col" (get-in preview [:ddl_plan :add-columns 0 :column :column_name])))
      (is (empty? (get-in preview [:ddl_plan :widen-columns]))))))

(deftest apply-schema-ddl-records-failed-audit-rows
  (let [executed (atom [])
        audit-sql (atom [])
        failing-ex (ex-info "boom" {:status 500})]
    (with-redefs [schema-drift/ensure-schema-drift-tables! (fn [])
                  db/get-opts (fn [_ _] db/ds)
                  jdbc/execute!
                  (fn [conn sql-vec]
                    (if (.contains ^String (first sql-vec) "ALTER TABLE public.fleet_vehicles ADD COLUMN")
                      (do
                        (swap! executed conj sql-vec)
                        (throw failing-ex))
                      (do
                        (swap! audit-sql conj sql-vec)
                        nil)))]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"boom"
                            (schema-drift/apply-schema-ddl!
                             11
                             "public.fleet_vehicles"
                             {:add-columns [{:sql "ALTER TABLE public.fleet_vehicles ADD COLUMN IF NOT EXISTS \"new_col\" TEXT"
                                             :column {:column_name "new_col"}}]
                              :widen-columns []}
                             {:workspace-key "ops"
                              :graph-id 1
                              :endpoint-name "fleet/vehicles"
                              :event-id 55
                              :schema-hash "hash-2"
                              :applied-by "reviewer"})))
      (is (= 1 (count @executed)))
      (is (= 1 (count @audit-sql)))
      (is (= "ops" (nth (first @audit-sql) 1)))
      (is (= "failed" (nth (first @audit-sql) 12))))))
