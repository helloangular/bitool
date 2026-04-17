(ns bitool.db-test
  (:require [clojure.test :refer :all]
            [bitool.db :as db]
            [next.jdbc :as jdbc]))

(deftest discover-joins-uses-mysql-compatible-query
  (let [calls (atom [])]
    (with-redefs [db/get-dbspec (fn [_ _] {:dbtype "mysql" :schema "analytics"})
                  db/get-opts (fn [_ _] :fake-ds)
                  jdbc/execute! (fn [_ sqlvec]
                                  (swap! calls conj sqlvec)
                                  [{:from_table "orders"
                                    :from_column "customer_id"
                                    :to_table "customers"
                                    :to_column "id"}])]
      (let [joins (db/discover-joins 9 :force-refresh true)]
        (is (= [{:from_table "orders"
                 :from_column "customer_id"
                 :to_table "customers"
                 :to_column "id"}]
               joins))
        (is (= "analytics" (second (first @calls))))
        (is (re-find #"referenced_table_name" (ffirst @calls)))))))

(deftest discover-joins-returns-empty-for-unsupported-fk-introspection-engines
  (with-redefs [db/get-dbspec (fn [_ _] {:dbtype "snowflake" :schema "PUBLIC"})
                db/get-opts (fn [_ _] (throw (ex-info "should not query" {})))]
    (is (= [] (db/discover-joins 9 :force-refresh true)))))

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

(deftest create-table-databricks-ensures-schema-from-qualified-table-name
  (let [calls (atom [])]
    (with-redefs [db/create-dbspec-from-id (fn [_]
                                             {:dbtype "databricks"
                                              :catalog "main"
                                              :schema "bronze"})
                  db/get-opts (fn [_ _] :fake-ds)
                  jdbc/execute! (fn [_ sqlvec]
                                  (swap! calls conj sqlvec)
                                  {:next.jdbc/update-count 1})]
      (db/create-table!
       9
       nil
       "main.audit.ingestion_checkpoint"
       [{:column_name "endpoint_name" :data_type "STRING"}]
       {})
      (let [[[schema-sql] [table-sql]] @calls]
        (is (= "CREATE SCHEMA IF NOT EXISTS main.audit" schema-sql))
        (is (.contains table-sql "CREATE TABLE IF NOT EXISTS main.audit.ingestion_checkpoint"))))))

(deftest create-table-postgres-ensures-schema-from-qualified-table-name
  (let [calls (atom [])]
    (with-redefs [db/create-dbspec-from-id (fn [_]
                                             {:dbtype "postgresql"
                                             :schema "public"})
                  db/get-opts (fn [_ _] :fake-ds)
                  jdbc/execute! (fn [_ sqlvec]
                                  (swap! calls conj sqlvec)
                                  {:next.jdbc/update-count 1})]
      (db/create-table!
       9
       nil
       "audit.ingestion_checkpoint"
       [{:column_name "endpoint_name" :data_type "STRING"}]
       {})
      (let [[[schema-sql] [table-sql]] @calls]
        (is (= "CREATE SCHEMA IF NOT EXISTS \"audit\"" schema-sql))
        (is (.contains table-sql "CREATE TABLE IF NOT EXISTS \"audit\".\"ingestion_checkpoint\""))))))

(deftest create-table-postgres-adds-missing-columns-on-existing-table
  (let [calls (atom [])]
    (with-redefs [db/create-dbspec-from-id (fn [_]
                                             {:dbtype "postgresql"
                                              :dbname "bitool"
                                              :schema "public"})
                  db/get-opts (fn [_ _] :fake-ds)
                  jdbc/execute! (fn
                                  ([_ sqlvec]
                                   (swap! calls conj sqlvec)
                                   (let [sql (first sqlvec)]
                                     (cond
                                       (.contains sql "SELECT column_name FROM information_schema.columns")
                                       [{:column_name "id"}
                                        {:column_name "created_at"}
                                        {:column_name "updated_at"}
                                        {:column_name "created_by"}
                                        {:column_name "updated_by"}
                                        {:column_name "ingestion_id"}]
                                       :else
                                       {:next.jdbc/update-count 1})))
                                  ([_ sqlvec opts]
                                   (swap! calls conj [sqlvec opts])
                                   (let [sql (first sqlvec)]
                                     (if (.contains sql "SELECT column_name FROM information_schema.columns")
                                       [{:column_name "id"}
                                        {:column_name "created_at"}
                                        {:column_name "updated_at"}
                                        {:column_name "created_by"}
                                        {:column_name "updated_by"}
                                        {:column_name "ingestion_id"}]
                                       {:next.jdbc/update-count 1}))))]
      (db/create-table!
       9
       nil
       "public.fleet_vehicles"
       [{:column_name "ingestion_id" :data_type "STRING" :is_nullable "NO"}
        {:column_name "data_items_id" :data_type "STRING" :is_nullable "YES"}]
       {})
      (let [sqls (map (fn [entry]
                        (let [sqlvec (if (vector? (first entry)) (first entry) entry)]
                          (first sqlvec)))
                      @calls)]
        (is (some #(.contains % "CREATE TABLE IF NOT EXISTS \"public\".\"fleet_vehicles\"") sqls))
        (is (some #(.contains % "SELECT column_name FROM information_schema.columns") sqls))
        (is (some #(.contains % "ALTER TABLE \"public\".\"fleet_vehicles\" ADD COLUMN IF NOT EXISTS \"data_items_id\" TEXT NULL") sqls))))))

(deftest get-ds-reuses-pool-when-db-spec-is-unchanged
  (let [builds (atom 0)]
    (reset! @#'bitool.db/ds-pool-cache {})
    (with-redefs [db/get-dbspec (fn [_ _] {:dbtype "postgresql" :host "localhost" :schema "public"})
                  bitool.db/make-hikari-ds (fn [_]
                                             (swap! builds inc)
                                             {:pool @builds})]
      (let [first-ds (db/get-ds 478 nil)
            second-ds (db/get-ds 478 nil)]
        (is (= {:pool 1} first-ds))
        (is (= first-ds second-ds))
        (is (= 1 @builds))))))

(deftest get-ds-refreshes-pool-when-db-spec-changes
  (let [builds     (atom 0)
        current-db (atom {:dbtype "postgresql" :host "localhost" :schema "public"})]
    (reset! @#'bitool.db/ds-pool-cache {})
    (with-redefs [db/get-dbspec (fn [_ _] @current-db)
                  bitool.db/make-hikari-ds (fn [spec]
                                             (swap! builds inc)
                                             {:pool @builds
                                              :dbtype (:dbtype spec)
                                              :host (:host spec)})]
      (let [first-ds (db/get-ds 478 nil)]
        (reset! current-db {:dbtype "databricks" :jdbcUrl "jdbc:databricks://workspace" :schema "bronze"})
        (let [second-ds (db/get-ds 478 nil)]
          (is (= {:pool 1 :dbtype "postgresql" :host "localhost"} first-ds))
          (is (= {:pool 2 :dbtype "databricks" :host nil} second-ds))
          (is (not= first-ds second-ds))
          (is (= 2 @builds)))))))
