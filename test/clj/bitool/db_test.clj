(ns bitool.db-test
  (:require [clojure.test :refer :all]
            [bitool.db :as db]
            [next.jdbc :as jdbc]))

(deftest insert-rows-databricks-inlines-literals-instead-of-binding-params
  (let [calls (atom [])]
    (with-redefs [db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                  db/get-opts (fn [_ _] :fake-ds)
                  jdbc/execute! (fn [_ sqlvec]
                                  (swap! calls conj sqlvec)
                                  {:next.jdbc/update-count 1})]
      (db/insert-rows!
       9
       nil
       "main.audit.run_batch_manifest"
       [{:column_name "batch_id"}
        {:column_name "status"}
        {:column_name "active"}
        {:column_name "partition_dates_json"}
        {:column_name "committed_at_utc"}]
       [["batch-1"
         "committed"
         true
         ["2026-03-22"]
         #inst "2026-03-22T22:20:00.000-00:00"]])
      (let [[sqlvec] @calls
            [sql] sqlvec]
        (is (= 1 (count sqlvec)))
        (is (.contains sql "INSERT INTO main.audit.run_batch_manifest"))
        (is (.contains sql "'batch-1'"))
        (is (.contains sql "'committed'"))
        (is (.contains sql "TRUE"))
        (is (.contains sql "'[\"2026-03-22\"]'"))
        (is (.contains sql "'2026-03-22T22:20:00Z'"))))))

(deftest insert-rows-databricks-escapes-single-quotes-and-nulls
  (let [calls (atom [])]
    (with-redefs [db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                  db/get-opts (fn [_ _] :fake-ds)
                  jdbc/execute! (fn [_ sqlvec]
                                  (swap! calls conj sqlvec)
                                  {:next.jdbc/update-count 1})]
      (db/insert-rows!
       9
       nil
       "main.audit.run_batch_manifest"
       [{:column_name "rollback_reason"}
        {:column_name "rolled_back_by"}]
       [["driver's note" nil]])
      (let [[sqlvec] @calls
            [sql] sqlvec]
        (is (.contains sql "'driver''s note'"))
        (is (.contains sql "NULL"))))))

(deftest insert-rows-databricks-inlines-small-non-audit-batches
  (let [calls (atom [])]
    (with-redefs [db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                  db/get-opts (fn [_ _] :fake-ds)
                  jdbc/execute! (fn [_ sqlvec]
                                  (swap! calls conj sqlvec)
                                  {:next.jdbc/update-count 1})]
      (db/insert-rows!
       9
       nil
       "main.bronze.fleet_vehicles"
       [{:column_name "payload_json"}
        {:column_name "load_date"}]
       [[{:id "1"} (java.sql.Date/valueOf "2026-03-22")]])
      (let [[sqlvec] @calls
            [sql] sqlvec]
        (is (= 1 (count sqlvec)))
        (is (.contains sql "INSERT INTO main.bronze.fleet_vehicles"))
        (is (.contains sql "'{\"id\":\"1\"}'"))
        (is (.contains sql "'2026-03-22'"))))))

(deftest insert-rows-databricks-keeps-parameterized-path-for-large-non-audit-batches
  (let [calls (atom [])
        rows  (vec (repeat 251 [{:id "1"} (java.sql.Date/valueOf "2026-03-22")]))]
    (with-redefs [db/create-dbspec-from-id (fn [_] {:dbtype "databricks"})
                  db/get-opts (fn [_ _] :fake-ds)
                  jdbc/execute! (fn [_ sqlvec]
                                  (swap! calls conj sqlvec)
                                  {:next.jdbc/update-count 1})]
      (db/insert-rows!
       9
       nil
       "main.bronze.fleet_vehicles"
       [{:column_name "payload_json"}
        {:column_name "load_date"}]
       rows)
      (let [[sqlvec] @calls
            [sql payload load-date] sqlvec]
        (is (.contains sql "VALUES (?, ?)"))
        (is (= "{\"id\":\"1\"}" payload))
        (is (= "2026-03-22" load-date))))))
