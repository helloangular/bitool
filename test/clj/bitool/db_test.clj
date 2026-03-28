(ns bitool.db-test
  (:require [clojure.test :refer :all]
            [bitool.db :as db]
            [next.jdbc :as jdbc]))

(deftest insert-rows-databricks-uses-parameterized-insert
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
            [sql batch-id status active partition-dates committed-at] sqlvec]
        (is (= 6 (count sqlvec)))
        (is (.contains sql "INSERT INTO main.audit.run_batch_manifest"))
        (is (.contains sql "VALUES (?, ?, ?, ?, ?)"))
        (is (= "batch-1" batch-id))
        (is (= "committed" status))
        (is (= true active))
        (is (= ["2026-03-22"] partition-dates))
        (is (= #inst "2026-03-22T22:20:00.000-00:00" committed-at))))))

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
            [sql rollback-reason rolled-back-by] sqlvec]
        (is (.contains sql "VALUES (?, ?)"))
        (is (= "driver's note" rollback-reason))
        (is (nil? rolled-back-by))))))

(deftest insert-rows-databricks-uses-parameterized-non-audit-batches
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
            [sql payload load-date] sqlvec]
        (is (= 3 (count sqlvec)))
        (is (.contains sql "INSERT INTO main.bronze.fleet_vehicles"))
        (is (.contains sql "VALUES (?, ?)"))
        (is (= {:id "1"} payload))
        (is (= (java.sql.Date/valueOf "2026-03-22") load-date))))))

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
        (is (= {:id "1"} payload))
        (is (= (java.sql.Date/valueOf "2026-03-22") load-date))))))

(deftest load-rows-databricks-copy-into-emits-copy-sql
  (let [calls (atom [])]
    (with-redefs [db/get-opts (fn [_ _] :fake-ds)
                  jdbc/execute! (fn [_ sqlvec]
                                  (swap! calls conj sqlvec)
                                  {:next.jdbc/update-count 1})]
      (db/load-rows-databricks-copy-into!
       9
       nil
       "main.bronze.file_raw"
       {:source_uri "s3://bucket/path/"
        :file_format "json"
        :files ["part-0001.json" "part-0002.json"]
        :pattern ".*\\.json"
        :format_options {:inferSchema false}
        :copy_options {:mergeSchema true}})
      (let [[sqlvec] @calls
            [sql] sqlvec]
        (is (.contains sql "COPY INTO `main`.`bronze`.`file_raw`"))
        (is (.contains sql "FROM 's3://bucket/path/'"))
        (is (.contains sql "FILEFORMAT = JSON"))
        (is (.contains sql "FILES = ('part-0001.json', 'part-0002.json')"))
        (is (.contains sql "PATTERN = '.*\\.json'"))
        (is (.contains sql "FORMAT_OPTIONS (inferSchema = false)"))
        (is (.contains sql "COPY_OPTIONS (mergeSchema = true)"))))))

(deftest load-rows-databricks-copy-into-rejects-missing-source-uri
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"requires source_uri"
                        (db/load-rows-databricks-copy-into!
                         9
                         nil
                         "main.bronze.file_raw"
                         {:file_format "json"}))))

(deftest load-rows-databricks-copy-into-rejects-invalid-file-format
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"supported file_format"
                        (db/load-rows-databricks-copy-into!
                         9
                         nil
                         "main.bronze.file_raw"
                         {:source_uri "s3://bucket/path/"
                          :file_format "exe"}))))

(deftest load-rows-databricks-copy-into-rejects-invalid-credential
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"credential must be a valid identifier"
                        (db/load-rows-databricks-copy-into!
                         9
                         nil
                         "main.bronze.file_raw"
                         {:source_uri "s3://bucket/path/"
                          :file_format "json"
                          :credential "cred); DROP TABLE t;"}))))
