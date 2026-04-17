(ns bitool.db
        (:require [next.jdbc :as jdbc]
        [bitool.bigquery :as bigquery]
        [bitool.macros :refer :all]
        [bitool.config :refer [env]]
        [clojure.string :as str]
        [next.jdbc.sql :as sql]
        [next.jdbc.result-set :as rs]
        [com.rpl.specter :as sp]
        [clojure.data.codec.base64 :as b64]
        [cheshire.core :as json]
        [clojure.edn :as edn]
        [clojure.pprint :as pp]
        [clojure.java.io :as io])
(:import [com.zaxxer.hikari HikariConfig HikariDataSource]
         [java.util UUID]
         [java.util.zip GZIPOutputStream GZIPInputStream]
           [java.io ByteArrayOutputStream ByteArrayInputStream]
         [java.util Base64]))

(declare get-dbspec get-opts default-introspection-schema)
       
(comment 
(defn load-edn
  "Load edn from an io/reader source (filename or io/resource)."
  [source]
  (try
    (with-open [r (io/reader source)]
      (edn/read (java.io.PushbackReader. r)))

    (catch java.io.IOException e
      (printf "Couldn't open '%s': %s\n" source (.getMessage e)))
    (catch RuntimeException e
      (printf "Error parsing edn file '%s': %s\n" source (.getMessage e)))))
)

(defn load-edn [resource-name]
  (if-let [res (io/resource resource-name)]
    (-> res slurp edn/read-string)
    (throw
      (ex-info
        (str "EDN resource not found on classpath: " resource-name)
        {:resource resource-name
         :classpath (System/getProperty "java.class.path")}))))

(def sql (load-edn "sql.edn"))

(def admin-db-spec
  {:dbtype "postgresql"
   :dbname "bitool"
   :host "localhost"
   :user "postgres"
   :password "postgres"})

(def ds (jdbc/get-datasource admin-db-spec))

(def user-db-spec
  {:dbtype "mysql"
   :dbname "lake"
   :host "localhost"
   :user "merico"
   :password "merico"})

(def ds-user (jdbc/get-datasource user-db-spec))

(def ^:private ds-pool-cache (atom {}))
(def ^:private schema-cache (atom {}))
(def ^:private join-cache (atom {}))

(defn- ds-fingerprint
  [db-spec]
  (-> db-spec
      (dissoc :password :token)
      pr-str))

(defn compress [s]
  (let [baos (ByteArrayOutputStream.)]
    (with-open [gzip (GZIPOutputStream. baos)]
      (.write gzip (.getBytes s "UTF-8")))
    (.toByteArray baos)))

(defn decompress [compressed-bytes]
  (let [bais (ByteArrayInputStream. compressed-bytes)
        buffer (byte-array 1024)]
    (with-open [gzip (GZIPInputStream. bais)
                baos (ByteArrayOutputStream.)]
      (loop []
        (let [len (.read gzip buffer)]
          (when (pos? len)
            (.write baos buffer 0 len)
            (recur))))
      (.toString baos "UTF-8"))))

(defn bytes>hex[bytes]
     (apply str (map #(format "%02x" %) bytes)))

(defn hex>bytes[hex]
  (map #(Integer/parseInt (subs hex % (+ % 2)) 16)
                   (range 0 (count hex) 2))) ;; Split hex into byte-sized chunks

(defn str>hex [s]
  (let [bytes (.getBytes s "UTF-8")]
    (apply str (map #(format "%02x" %) bytes))))

(defn hex>str [hex]
  (let [bytes (byte-array (map #(Integer/parseInt (subs hex % (+ % 2)) 16)
                               (range 0 (count hex) 2)))]
    (String. bytes "UTF-8")))

(defn map>hex[m]
      (str>hex (pr-str m)))

(defn hex>map[hex]
      (read-string (hex>str hex)))

;; To Encode String -> Base64 (String):
(defn encode [s]
  (.encodeToString (Base64/getEncoder) (.getBytes s)))

;; To Decode Base64 (byte[] or String) -> String:
(defn decode [to-decode]
  (String. (.decode (Base64/getDecoder) to-decode)))

(defn map-to-str[g]
    (String. (b64/encode (compress (pr-str g)))))

(defn str-to-map[s]
    (String. (read-string (decompress (b64/decode s)))))

(defn id[g] (get-in g [:a :id]))

(defn version[g] (get-in g [:a :v]))

(defn name[g] (get-in g [:a :name]))

(defn insert-data [obj json-params]
    (sql/insert! ds obj json-params))

(defn update-data [obj json-params where-clause]
  (sql/update! ds obj json-params where-clause))

(defn delete-data [obj where-clause]
  (sql/delete! ds obj where-clause))

(defn seq-next-val*
  [conn name]
  (let [query (str "SELECT nextval('" name "') AS next_val")]
    (-> (jdbc/execute-one! conn [query])
        :next_val)))

(defn seq-next-val
  [name]
  (seq-next-val* ds name))

(defn getGraph[gid] (read-string (:graph/definition (jdbc/execute-one! ds ["select definition from graph where id = ? and version = (select max(version) from graph where id = ?)" gid gid]))))

(defn list-graph-ids
  "Return all distinct graph IDs from the graph table."
  []
  (mapv :graph/id (jdbc/execute! ds ["select distinct id from graph"])))

(defn list-models
  "Return the latest saved version for each graph id."
  []
  (jdbc/execute! ds
                 ["select distinct on (id) id, version, coalesce(name, '') as name
                   from graph
                   order by id desc, version desc"]
                 {:builder-fn rs/as-unqualified-lower-maps}))

(defn version+[g] (update-in g [:a :v] inc))

(defn insert-graph!
  [conn g]
  (let [_          (pp/pprint g)
        g          (version+ g)
        id         (id g)
        gid        (if (= 0 id) (seq-next-val* conn "sqlgraph_id_seq") id)
        definition (pr-str (assoc-in g [:a :id] gid))]
    (read-string
     (:graph/definition
      (sql/insert! conn :graph {:id gid
                                :version (version g)
                                :name (name g)
                                :definition definition})))))

(defn insertGraph
  [g]
  (insert-graph! ds g))

(defn list-all-connections
  "Return id, connection_name, dbtype for all connections."
  []
  (jdbc/execute! ds ["SELECT id, connection_name, dbtype FROM connection ORDER BY id"]))

(defn get-connection [conn-id]
  (jdbc/execute! ds ["SELECT * FROM connection WHERE id = ?" conn-id]))

(defn list-all-connections-summary
  []
  (jdbc/execute! ds
                 ["SELECT id, connection_name, dbtype, host, dbname, schema
                   FROM connection
                   ORDER BY id"]
                 {:builder-fn rs/as-unqualified-lower-maps}))

(defn get-connection-detail
  [conn-id]
  (sql/get-by-id ds :connection conn-id {:builder-fn rs/as-unqualified-lower-maps}))

(def ^:private reporting-ready? (atom false))
(def ^:private saved-report-table "saved_report")

(defn ensure-reporting-tables!
  []
  (locking reporting-ready?
    (when-not @reporting-ready?
      (doseq [sql-str
              [(str "CREATE TABLE IF NOT EXISTS " saved-report-table " ("
                    "report_id BIGSERIAL PRIMARY KEY, "
                    "conn_id INTEGER NULL REFERENCES connection(id), "
                    "schema_name TEXT NULL, "
                    "name TEXT NOT NULL, "
                    "description TEXT NULL, "
                    "scope_tables_json TEXT NULL, "
                    "isl_json TEXT NOT NULL, "
                    "created_by TEXT NULL, "
                    "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                    "updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
               (str "CREATE INDEX IF NOT EXISTS idx_saved_report_conn_created "
                    "ON " saved-report-table " (conn_id, created_at_utc DESC)")
               ;; Phase 4: semantic context columns (safe ADD COLUMN IF NOT EXISTS)
               (str "ALTER TABLE " saved-report-table
                    " ADD COLUMN IF NOT EXISTS semantic_model_id BIGINT NULL")
               (str "ALTER TABLE " saved-report-table
                    " ADD COLUMN IF NOT EXISTS perspective_name TEXT NULL")
               (str "CREATE INDEX IF NOT EXISTS idx_saved_report_model "
                    "ON " saved-report-table " (semantic_model_id)")]]
        (jdbc/execute! ds [sql-str]))
      (reset! reporting-ready? true))))

;; ── Schema Context / Training ──────────────────────────────────────────────
;; Stores human or AI-generated descriptions for tables and columns so
;; the ISL prompt has business-level context (the "Train" button).

(def ^:private schema-context-ready? (atom false))

(defn ensure-schema-context-tables!
  []
  (locking schema-context-ready?
    (when-not @schema-context-ready?
      (doseq [ddl
              [(str "CREATE TABLE IF NOT EXISTS schema_context ("
                    "context_id BIGSERIAL PRIMARY KEY, "
                    "conn_id INTEGER NOT NULL REFERENCES connection(id), "
                    "schema_name TEXT NOT NULL DEFAULT 'public', "
                    "table_name TEXT NOT NULL, "
                    "column_name TEXT NULL, "
                    "description TEXT NOT NULL, "
                    "sample_values_json TEXT NULL, "
                    "source TEXT NOT NULL DEFAULT 'manual', "
                    "created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(), "
                    "updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now())")
               "CREATE UNIQUE INDEX IF NOT EXISTS idx_schema_context_uniq ON schema_context (conn_id, schema_name, table_name, COALESCE(column_name, ''))"
               "CREATE INDEX IF NOT EXISTS idx_schema_context_conn ON schema_context (conn_id, schema_name)"]]
        (jdbc/execute! ds [ddl]))
      (reset! schema-context-ready? true))))

(defn upsert-schema-context!
  "Insert or update a table/column description. column_name nil = table-level."
  [{:keys [conn_id schema_name table_name column_name description sample_values source]}]
  (ensure-schema-context-tables!)
  (let [schema-name (or schema_name "public")
        col-val     (or column_name "")
        samples-json (when (seq sample_values) (json/generate-string sample_values))
        src         (or source "manual")]
    (jdbc/execute!
     ds
     [(str "INSERT INTO schema_context (conn_id, schema_name, table_name, column_name, description, sample_values_json, source) "
           "VALUES (?, ?, ?, NULLIF(?, ''), ?, ?, ?) "
           "ON CONFLICT (conn_id, schema_name, table_name, COALESCE(column_name, '')) "
           "DO UPDATE SET description = EXCLUDED.description, "
           "sample_values_json = EXCLUDED.sample_values_json, "
           "source = EXCLUDED.source, "
           "updated_at_utc = now()")
      conn_id schema-name table_name col-val description samples-json src])))

(defn get-schema-context
  "Return all context entries for a connection+schema, optionally filtered to tables."
  [conn-id & {:keys [schema table-filter]}]
  (ensure-schema-context-tables!)
  (let [schema-name (or schema "public")
        rows (jdbc/execute!
              ds
              [(str "SELECT context_id, conn_id, schema_name, table_name, column_name, "
                    "description, sample_values_json, source, created_at_utc, updated_at_utc "
                    "FROM schema_context "
                    "WHERE conn_id = ? AND schema_name = ? "
                    "ORDER BY table_name, column_name NULLS FIRST")
               conn-id schema-name]
              {:builder-fn rs/as-unqualified-lower-maps})]
    (->> rows
         (filter (fn [row]
                   (or (nil? table-filter)
                       (contains? table-filter (:table_name row)))))
         (mapv (fn [row]
                 (cond-> row
                   (:sample_values_json row)
                   (assoc :sample_values
                          (try (json/parse-string (:sample_values_json row) true)
                               (catch Exception _ nil)))))))))

(defn delete-schema-context!
  [context-id]
  (ensure-schema-context-tables!)
  (sql/delete! ds :schema_context {:context_id context-id}))

(defn sample-table-data
  "Fetch a small sample of rows from a table for the LLM to learn from.
   Returns up to n rows as maps."
  [conn-id table-name & {:keys [schema n] :or {n 5}}]
  (let [db-spec     (get-dbspec conn-id false)
        dbtype      (:dbtype db-spec)
        schema-name (or schema (:schema db-spec) (default-introspection-schema dbtype))
        qualified   (str (when schema-name (str schema-name ".")) table-name)
        limit-sql   (str "SELECT * FROM " qualified " LIMIT " n)]
    (jdbc/execute!
     (get-opts conn-id false)
     [limit-sql])))

(defn table-stats
  "Get row count and basic column stats for a single table (with timeout protection)."
  [conn-id table-name & {:keys [schema]}]
  (let [db-spec     (get-dbspec conn-id false)
        dbtype      (:dbtype db-spec)
        schema-name (or schema (:schema db-spec) (default-introspection-schema dbtype))
        qualified   (str (when schema-name (str schema-name ".")) table-name)
        count-sql   (str "SELECT COUNT(*) AS row_count FROM " qualified)]
    (try
      (let [result (jdbc/execute-one!
                    (get-opts conn-id false)
                    [count-sql]
                    {:timeout 5})]
        {:table_name table-name
         :row_count  (:row_count result)})
      (catch Exception e
        {:table_name table-name
         :row_count  nil
         :error      (.getMessage e)}))))

(defn save-report!
  [{:keys [conn_id schema_name name description scope_tables isl created_by
           semantic_model_id perspective_name]}]
  (ensure-reporting-tables!)
  (let [row (cond-> {:conn_id conn_id
                     :schema_name schema_name
                     :name name
                     :description description
                     :scope_tables_json (when (seq scope_tables) (json/generate-string (vec scope_tables)))
                     :isl_json (json/generate-string isl)
                     :created_by created_by}
              semantic_model_id  (assoc :semantic_model_id semantic_model_id)
              perspective_name   (assoc :perspective_name perspective_name))]
    (sql/insert! ds :saved_report row {:builder-fn rs/as-unqualified-lower-maps})))

(defn list-saved-reports
  [conn-id]
  (ensure-reporting-tables!)
  (let [rows (jdbc/execute!
              ds
              (if (some? conn-id)
                [(str "SELECT report_id, conn_id, schema_name, name, description, "
                      "scope_tables_json, isl_json, semantic_model_id, perspective_name, "
                      "created_by, created_at_utc, updated_at_utc "
                      "FROM " saved-report-table " WHERE conn_id = ? ORDER BY created_at_utc DESC")
                 conn-id]
                [(str "SELECT report_id, conn_id, schema_name, name, description, "
                      "scope_tables_json, isl_json, semantic_model_id, perspective_name, "
                      "created_by, created_at_utc, updated_at_utc "
                      "FROM " saved-report-table " WHERE conn_id IS NULL ORDER BY created_at_utc DESC")])
              {:builder-fn rs/as-unqualified-lower-maps})]
    (mapv (fn [row]
            (assoc row
                   :scope_tables (when-let [raw (:scope_tables_json row)]
                                   (try
                                     (json/parse-string raw true)
                                     (catch Exception _ nil)))))
          rows)))

(defn get-saved-report
  [report-id]
  (ensure-reporting-tables!)
  (when-let [row (sql/get-by-id ds :saved_report report-id {:builder-fn rs/as-unqualified-lower-maps})]
    (assoc row
           :scope_tables (when-let [raw (:scope_tables_json row)]
                           (try
                             (json/parse-string raw true)
                             (catch Exception _ nil)))
           :isl (try
                  (json/parse-string (:isl_json row) true)
                  (catch Exception _ {})))))

(defn delete-saved-report!
  [report-id]
  (ensure-reporting-tables!)
  (sql/delete! ds :saved_report {:report_id report-id}))

(defn update-connection!
  [conn-id attrs]
  (sql/update! ds :connection attrs {:id conn-id}))

(defn delete-connection!
  [conn-id]
  (sql/delete! ds :connection {:id conn-id}))

(defn- backend-debug-logging-enabled? []
  (contains? #{"true" "1" "yes" "on"}
             (some-> (get env :bitool_backend_debug_logs) str str/lower-case)))
   
;; (defn updateGraph[g]
;;    (sql/update! ds :graph {:version (version g) :name (name g) :definition (definition g)} {:id (id g)})) 

(defn save-conn [ args ]
   (let [payload (sp/transform ["dbtype"] first
                               (sp/transform ["port"] #(Integer/parseInt %) (:multipart-params args)))
         ins     (insert-data :connection payload)]
     (when (backend-debug-logging-enabled?)
       (println "SAVE-CONN")
       (println ins))
     ins))

;;

(defn tableColumns1 [ conn t ] 
    (sql/get-by-id ds :connection 2))

;; Function to dynamically create a db-spec from the connection table
(defn create-dbspec-from-id [id]
  (let [connection (sql/get-by-id ds :connection id {:builder-fn rs/as-unqualified-lower-maps})
     ;;  _ (println connection) 
     ] ; Retrieve connection data by ID
    (when connection
      (let [{:keys [dbtype dbname schema host port username password schema sid service]} connection
         ;;  _ (println (str "DBTYPE:" dbtype )) 
   ]
        (case dbtype
          "postgresql" {:dbtype "postgresql"
                        :dbname dbname
                        :schema schema
                        :host host
                        :port port
                        :user username
                        :password password}
          "mysql"      {:dbtype "mysql"
                        :dbname dbname
                        :host host
                        :port port
                        :user username
                        :password password}
          "oracle"     (cond-> {:dbtype "oracle"
                                :host host
                                :port port
                                :user username
                                :password password}
                         sid     (assoc :sid sid)
                         service (assoc :service-name service))
          "db2"        (cond-> {:dbtype "db2"
                        	:dbname dbname
                        	:schema schema
                                :host host
                                :port port
                                :user username
                                :password password}
                         schema  (assoc :schema schema))
          "sqlserver"  (cond-> {:dbtype "sqlserver"
                        	:dbname dbname
                        	:schema schema
                                :host host
                                :port port
                                :user username
                                :password password}
                         dbname  (assoc :database-name dbname))
          "databricks" (let [host       (or host "")
                             port       (or port 443)
                             http-path  (or (:http_path connection) "")
                             token      (or (:token connection) password "")
                             catalog    (or (:catalog connection) dbname)
                             schema     (or schema (:schema connection) "default")
                             jdbc-url   (format "jdbc:databricks://%s:%s/default;transportMode=http;ssl=1;AuthMech=3;httpPath=%s;UID=token;PWD=%s;ConnCatalog=%s;ConnSchema=%s"
                                                host port http-path token catalog schema)]
                         {:jdbcUrl jdbc-url
                          :dbtype "databricks"})
          "snowflake"  (let [account-host (or host "")
                             warehouse    (or (:warehouse connection) service)
                             role         (or (:role connection) sid)
                             params       (remove str/blank?
                                                  [(when dbname (str "db=" dbname))
                                                   (when schema (str "schema=" schema))
                                                   (when warehouse (str "warehouse=" warehouse))
                                                   (when role (str "role=" role))])
                             jdbc-url     (str "jdbc:snowflake://" account-host "/"
                                               (when (seq params)
                                                 (str "?" (str/join "&" params))))]
                         (cond-> {:jdbcUrl jdbc-url
                                  :dbtype "snowflake"
                                  :catalog dbname
                                  :user username
                                  :password password}
                           warehouse (assoc :warehouse warehouse)
                           dbname (assoc :dbname dbname)
                           schema (assoc :schema schema)
                           role (assoc :role role)))
          "bigquery"   {:dbtype "bigquery"
                        :project-id host
                        :dataset dbname
                        :location (or schema "US")
                        :token (:token connection)}
          ;; Add other db types if needed
          (throw (ex-info "Unsupported database type" {:dbtype dbtype})))))))

;; Example usage
(defn get-dbspec[conn-id db-name]
     (let [db-spec (create-dbspec-from-id  conn-id)] 
     	(if db-name (assoc db-spec :dbname db-name) db-spec)))

(defn get-dbtype [conn-id]
      (keyword (:dbtype (get-dbspec conn-id false))))

(defn- hikari-driver-class
  [db-spec]
  (case (:dbtype db-spec)
    "databricks" "com.databricks.client.jdbc.Driver"
    "snowflake" "net.snowflake.client.jdbc.SnowflakeDriver"
    nil))

(defn- make-hikari-ds
  [db-spec]
  (let [cfg (HikariConfig.)]
    (.setMaximumPoolSize cfg 4)
    (.setMinimumIdle cfg 0)
    (.setAutoCommit cfg true)
    (.setPoolName cfg (str "bitool-" (or (:dbtype db-spec) "jdbc") "-" (System/currentTimeMillis)))
    (if-let [jdbc-url (:jdbcUrl db-spec)]
      (do
        (.setJdbcUrl cfg jdbc-url)
        (when-let [driver-class (hikari-driver-class db-spec)]
          (.setDriverClassName cfg driver-class))
        (when-let [user (:user db-spec)]
          (.setUsername cfg user))
        (when-let [password (:password db-spec)]
          (.setPassword cfg password)))
      (let [raw-spec (cond-> db-spec
                       (:dbname db-spec) (assoc :dbname (:dbname db-spec))
                       true (dissoc :catalog :warehouse :role))]
        (.setDataSource cfg (jdbc/get-datasource raw-spec))))
    (HikariDataSource. cfg)))

(defn invalidate-ds-cache!
  [conn-id db-name]
  (let [cache-key [conn-id db-name]]
    (when-let [existing (get @ds-pool-cache cache-key)]
      (when-let [ds (:ds existing)]
        (when (instance? HikariDataSource ds)
          (.close ^HikariDataSource ds))))
    (swap! ds-pool-cache dissoc cache-key)))

(defn get-ds[conn-id db-name]
   (let [cache-key    [conn-id db-name]
         db-spec      (get-dbspec conn-id db-name)
         fingerprint  (ds-fingerprint db-spec)]
     (when (= "bigquery" (:dbtype db-spec))
       (throw (ex-info "BigQuery connections use native API execution, not JDBC datasources"
                       {:dbtype "bigquery"
                        :conn_id conn-id})))
     (let [cached (get @ds-pool-cache cache-key)]
       (if (= fingerprint (:fingerprint cached))
         (:ds cached)
         (let [ds* (make-hikari-ds db-spec)]
           (when-let [old-ds (:ds cached)]
             (when (instance? HikariDataSource old-ds)
               (.close ^HikariDataSource old-ds)))
           (:ds (get (swap! ds-pool-cache assoc cache-key {:fingerprint fingerprint
                                                           :ds ds*})
                     cache-key)))))))

(defn get-opts[conn-id db-name]
      	(jdbc/with-options (get-ds conn-id db-name) {:builder-fn rs/as-unqualified-lower-maps}))

(defn invalidate-schema-cache!
  [conn-id schema]
  (swap! schema-cache dissoc [conn-id schema]))

(defn invalidate-join-cache!
  [conn-id schema]
  (swap! join-cache dissoc [conn-id schema]))

(defn- default-introspection-schema
  [dbtype]
  (case dbtype
    "postgresql" "public"
    "snowflake" "PUBLIC"
    "databricks" "default"
    "sqlserver" "dbo"
    "public"))

(defn- normalize-schema-type
  [raw]
  (let [t (str/lower-case (str (or raw "text")))]
    (cond
      (or (str/includes? t "int")
          (str/includes? t "long")
          (str/includes? t "bigint")) "int8"
      (or (str/includes? t "float")
          (str/includes? t "double")
          (str/includes? t "numeric")
          (str/includes? t "decimal")
          (str/includes? t "real")) "float8"
      (str/includes? t "bool") "boolean"
      (and (str/includes? t "timestamp")
           (not (str/includes? t "with time zone"))) "timestamp"
      (str/includes? t "time") "timestamp"
      (str/includes? t "date") "date"
      :else "text")))

(defn introspect-schema
  "Query information_schema to build a runtime schema registry.
   Returns {\"table_name\" [{:col \"col_name\" :type \"varchar\" :quoted? bool} ...]}."
  [conn-id & {:keys [schema table-filter]}]
  (let [db-spec      (get-dbspec conn-id false)
        dbtype       (:dbtype db-spec)
        schema-name  (or schema (:schema db-spec) (default-introspection-schema dbtype))
        raw-cols     (jdbc/execute!
                      (get-opts conn-id false)
                      ["SELECT table_name, column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = ?
                        ORDER BY table_name, ordinal_position"
                       schema-name])]
    (->> raw-cols
         (filter (fn [row]
                   (or (nil? table-filter)
                       (contains? table-filter (:table_name row)))))
         (group-by :table_name)
         (reduce-kv
          (fn [acc tbl cols]
            (assoc acc tbl
                   (mapv (fn [col]
                           (cond-> {:col (:column_name col)
                                    :type (normalize-schema-type (:data_type col))}
                             (or (re-find #"[A-Z]" (str (:column_name col)))
                                 (re-find #"[^A-Za-z0-9_]" (str (:column_name col))))
                             (assoc :quoted? true)))
                         cols)))
          {}))))

(defn get-schema-registry
  "Return schema registry for a connection, optionally filtered to a table subset.
   Caches the full schema per [conn-id, schema]."
  [conn-id & {:keys [schema table-filter force-refresh]}]
  (let [db-spec     (get-dbspec conn-id false)
        schema-name (or schema (:schema db-spec) (default-introspection-schema (:dbtype db-spec)))
        cache-key   [conn-id schema-name]
        cached      (get @schema-cache cache-key)
        ttl-ms      600000
        stale?      (or force-refresh
                        (nil? cached)
                        (> (- (System/currentTimeMillis) (:ts cached 0)) ttl-ms))
        full-registry
        (if stale?
          (let [registry (introspect-schema conn-id :schema schema-name)]
            (swap! schema-cache assoc cache-key {:registry registry
                                                 :ts (System/currentTimeMillis)})
            registry)
          (:registry cached))]
    (if (seq table-filter)
      (select-keys full-registry table-filter)
      full-registry)))

(defn discover-joins
  "Return FK-backed join metadata for a connection schema."
  [conn-id & {:keys [schema force-refresh]}]
  (let [db-spec     (get-dbspec conn-id false)
        dbtype      (:dbtype db-spec)
        schema-name (or schema (:schema db-spec) (default-introspection-schema dbtype))
        cache-key   [conn-id schema-name]
        cached      (get @join-cache cache-key)
        ttl-ms      600000
        stale?      (or force-refresh
                        (nil? cached)
                        (> (- (System/currentTimeMillis) (:ts cached 0)) ttl-ms))
        joins       (if stale?
                      (let [rows (case dbtype
                                   ("postgresql" "sqlserver")
                                   (jdbc/execute!
                                    (get-opts conn-id false)
                                    ["SELECT
                                        tc.table_name AS from_table,
                                        kcu.column_name AS from_column,
                                        ccu.table_name AS to_table,
                                        ccu.column_name AS to_column
                                      FROM information_schema.table_constraints tc
                                      JOIN information_schema.key_column_usage kcu
                                        ON tc.constraint_name = kcu.constraint_name
                                       AND tc.table_schema = kcu.table_schema
                                      JOIN information_schema.constraint_column_usage ccu
                                        ON ccu.constraint_name = tc.constraint_name
                                       AND ccu.table_schema = tc.table_schema
                                      WHERE tc.constraint_type = 'FOREIGN KEY'
                                        AND tc.table_schema = ?
                                      ORDER BY tc.table_name, kcu.column_name"
                                     schema-name])

                                   "mysql"
                                   (jdbc/execute!
                                    (get-opts conn-id false)
                                    ["SELECT
                                        table_name AS from_table,
                                        column_name AS from_column,
                                        referenced_table_name AS to_table,
                                        referenced_column_name AS to_column
                                      FROM information_schema.key_column_usage
                                      WHERE table_schema = ?
                                        AND referenced_table_name IS NOT NULL
                                      ORDER BY table_name, ordinal_position"
                                     schema-name])

                                   ;; Snowflake, Databricks, Oracle, and DB2 often do not expose
                                   ;; reliable FK metadata through a portable query path.
                                   [])
                            normalized (mapv (fn [row]
                                               {:from_table (:from_table row)
                                                :from_column (:from_column row)
                                                :to_table (:to_table row)
                                                :to_column (:to_column row)})
                                             rows)]
                        (swap! join-cache assoc cache-key {:joins normalized
                                                           :ts (System/currentTimeMillis)})
                        normalized)
                      (:joins cached))]
    joins))

(defn test-connection [conn-id]
  (let [db-spec (create-dbspec-from-id conn-id)
        dbtype  (:dbtype db-spec)]
    (if (= "bigquery" dbtype)
      (bigquery/test-connection! db-spec)
      (let [test-ds (make-hikari-ds db-spec)]
        (jdbc/execute! test-ds ["SELECT 1"])
        (.close ^HikariDataSource test-ds)
        true))))

(defn get-columns-old [ dbtype db_opts table-name ] 
          (case dbtype
            "postgresql"
            (jdbc/execute! db_opts
                           ["SELECT column_name as name , data_type as type , character_maximum_length as length , is_nullable as empty
                             FROM information_schema.columns
                             WHERE table_name = ?"
                            table-name])

            "mysql"
            (jdbc/execute! db_opts
                           ["SELECT COLUMN_NAME as name , DATA_TYPE as type , CHARACTER_MAXIMUM_LENGTH as length , IS_NULLABLE as empty
                             FROM INFORMATION_SCHEMA.COLUMNS
                             WHERE TABLE_NAME = ?"
                            table-name])

            "oracle"
            (jdbc/execute! db_opts
                           ["SELECT column_name as name , data_type as type, data_length as length , nullable as empty
                             FROM all_tab_columns
                             WHERE table_name = UPPER(?)"
                            table-name])

            "db2"
            (jdbc/execute! db_opts
                           ["SELECT colname AS name, typename AS type, length AS length, nulls AS empty
                             FROM syscat.columns
                             WHERE tabname = ?"
                            table-name])

            "sqlserver"
            (jdbc/execute! db_opts
                           ["SELECT COLUMN_NAME as name, DATA_TYPE as type , CHARACTER_MAXIMUM_LENGTH as length , IS_NULLABLE as empty
                             FROM INFORMATION_SCHEMA.COLUMNS
                             WHERE TABLE_NAME = ?"
                            table-name])

            (throw (ex-info "Unsupported database type" {:dbtype dbtype}))))

       
(defn column-names [columns]
      (map :name columns))
 

(defn get-sql [dbtype sql-name] (sql-name (dbtype sql)))

;; (defn get-metadata[conn-id db-name sql-name & params]
(defn get-metadata[conn-id db-name sql-name & params]
        (let [
              sql (get-sql (get-dbtype conn-id) sql-name)
              fx (if (= sql-name :columns) #(identity %) #(vals %))
              _ (when (backend-debug-logging-enabled?)
                  (println (str "SQL : " sql " DBNAME : " db-name " Params : " params)))
               ]
      		(let
                     [
                       opts (get-opts conn-id db-name)
                       tables (map fx (jdbc/execute! opts (into [sql] params)))
                     ]
                     tables )))

(comment
(defn get-metadata[conn-id db-name sql-name & params]
        (let [sql (get-sql (get-dbtype conn-id) sql-name)
              fx (if (= sql-name :columns) #(map %  [:column_name :data_type :is_nullable :character_maximum_length :numeric_precision :numeric_scale :is_primary_key :exp :tfm :agg :excluded :alias]) #(vals %))
              _ (println (str "SQL : " sql " DBNAME : " db-name " Params : " params))
               ]
      		(map fx (jdbc/execute! (get-opts conn-id db-name) (into [sql] params)))))
)

(defn get-databases [conn-id]
      (map first (get-metadata conn-id false :databases)))

(defn get-database [conn-id]
      (conj [] (:dbname (create-dbspec-from-id conn-id))))

(defn get-schemas [conn-id db-name]
       (map first (get-metadata conn-id db-name :schemas)))
      
(defn get-tables [conn-id db-name schema-name]
       (map first (get-metadata conn-id db-name :tables schema-name)))

(defn join-column [ coldetails ]
  (cond
    (map? coldetails) (or (:column_name coldetails)
                          (:name coldetails)
                          (some-> coldetails vals first str)
                          "")
    :else
    (->> coldetails
         (remove nil?)
         (map #(if (number? %) (str %) %))
         (clojure.string/join "-")))) 


(defn get-columns [conn-id db-name schema-name table-name]
      (get-metadata conn-id db-name :columns schema-name table-name))
      ;; (map vec (get-metadata conn-id db-name :columns schema-name table-name)))
      ;; (map #(join-column %) (get-metadata conn-id db-name :columns schema-name table-name)))

(defn get-table-columns [conn-id table-name]
  (let [db-spec (create-dbspec-from-id conn-id)
        dbname (:dbname db-spec) 
        schema (if (nil? (:schema db-spec)) "public"  (:schema db-spec)) 
       ]
       (get-columns conn-id dbname schema table-name)))

(defn get-table [connection-id table-name]
      (let [columns (get-table-columns connection-id table-name)
           ]
           {:name table-name :btype "T" :tcols columns}))            

(defn select-columns-id [ds table cols id]
  (let [col-str   (str/join ", " (map clojure.core/name cols))
        table-str (clojure.core/name table)
        sql       (format "select %s from %s where id = ?" col-str table-str)]
    (when (backend-debug-logging-enabled?)
      (prn :col-str col-str)
      (prn :sql sql))
    (jdbc/execute-one! ds [sql id])))

(comment
(defn select-columns [ds table cols]
  (let [col-str (clojure.string/join ", " (map clojure.core/name cols))
        sql (format "select %s from %s" col-str (clojure.core/name table))]
    (jdbc/execute-one! ds [sql])))
)

(defn select-columns [table cols]
  (let [col-str (str/join ", " (map clojure.core/name cols))
        sql     (format "select %s from %s" col-str (clojure.core/name table))]
    (jdbc/execute! ds [sql] {:builder-fn rs/as-unqualified-lower-maps})))

(defn- snowflake-variant-column?
  [column]
  (#{"payload_json" "row_json"} (str/lower-case (str (:column_name column)))))

(defn- make-insert-sql-for-dbtype
  [dbtype table-name columns n-rows]
  (let [col-names        (map :column_name columns)
        cols-str         (str/join ", " col-names)
        row-placeholders (str/join ", "
                                   (map (fn [column]
                                          (if (and (= "snowflake" dbtype)
                                                   (snowflake-variant-column? column))
                                            "PARSE_JSON(?)"
                                            "?"))
                                        columns))
        all-placeholders (->> (repeat n-rows (str "(" row-placeholders ")"))
                              (str/join ", "))]
    (format "INSERT INTO %s (%s) VALUES %s"
            table-name cols-str all-placeholders)))

(defn make-insert-sql
  "Build INSERT ... VALUES ... SQL.
   Supports multiple rows by repeating the VALUES tuple N times:
   n-rows = 1  -> VALUES (?, ?, ?)
   n-rows = 2  -> VALUES (?, ?, ?), (?, ?, ?)"
  [table-name columns n-rows]
  (make-insert-sql-for-dbtype nil table-name columns n-rows))

(defn- assert-row-shape! [columns rows']
  (let [col-count (count columns)]
    (doseq [[idx row] (map-indexed vector rows')]
      (when (not= col-count (count row))
        (throw (ex-info "Row value count does not match column count"
                        {:row-index idx
                         :columns col-count
                         :values  (count row)
                         :row      row}))))))

(defn- databricks-coerce-param-literal
  "Convert a value to a SQL literal string safe for Databricks INSERT."
  [value]
  (cond
    (nil? value)                          "NULL"
    (instance? java.sql.Date value)       (str "'" (.toLocalDate ^java.sql.Date value) "'")
    (instance? java.sql.Timestamp value)  (str "'" (.toInstant ^java.sql.Timestamp value) "'")
    (instance? java.util.Date value)      (str "'" (.toInstant ^java.util.Date value) "'")
    (instance? java.time.Instant value)   (str "'" value "'")
    (instance? java.time.OffsetDateTime value) (str "'" value "'")
    (instance? java.time.ZonedDateTime value)  (str "'" value "'")
    (instance? java.time.LocalDate value)      (str "'" value "'")
    (instance? java.time.LocalDateTime value)  (str "'" value "'")
    (instance? java.util.UUID value)      (str "'" value "'")
    (string? value)                       (str "'" (str/replace value "'" "''") "'")
    (instance? java.lang.Boolean value)   (if value "TRUE" "FALSE")
    (number? value)                       (str value)
    (map? value)                          (str "'" (str/replace (json/generate-string value) "'" "''") "'")
    (vector? value)                       (str "'" (str/replace (json/generate-string value) "'" "''") "'")
    :else                                 (str "'" (str/replace (str value) "'" "''") "'")))

(defn insert-rows!
  "Insert one or many rows.
   - rows can be a single row vector: [v1 v2 v3]
   - or a seq of row vectors: [[v1 v2 v3] [v1' v2' v3'] ...]"
  [conn-id db-name table-name columns rows]
  (let [opts (get-opts conn-id db-name)
        dbtype (:dbtype (create-dbspec-from-id conn-id))
        ;; normalize rows to a seq of row-vectors
        rows' (cond
                ;; seq-of-rows
                (and (sequential? rows)
                     (sequential? (first rows)))
                rows

                ;; single row vector/list
                (sequential? rows)
                [rows]

                :else
                (throw (ex-info "rows must be a vector or seq of vectors"
                                {:rows rows})))
        ]
	(assert-row-shape! columns rows')

    	(if (= "databricks" dbtype)
          ;; Databricks: use literal SQL to avoid JDBC param type errors
          (let [col-names (mapv #(str "`" (:column_name %) "`") columns)
                row-literal (fn [row]
                              (str "(" (str/join ", "
                                         (map (fn [v]
                                                (databricks-coerce-param-literal v))
                                              row))
                                   ")"))
                sql (str "INSERT INTO " table-name
                         " (" (str/join ", " col-names) ") VALUES "
                         (str/join ", " (map row-literal rows')))]
            (when (backend-debug-logging-enabled?) (prn-v sql))
            (jdbc/execute! opts [sql]))
          ;; Other warehouses: parameterized insert
          (let [n-rows      (count rows')
                sql         (make-insert-sql-for-dbtype dbtype table-name columns n-rows)
                _ (when (backend-debug-logging-enabled?) (prn-v sql))
                flat-params (vec (mapcat (fn [row]
                                          (map (fn [column value]
                                                 (if (and (= "snowflake" dbtype)
                                                          (snowflake-variant-column? column)
                                                          (or (map? value) (vector? value)))
                                                   (json/generate-string value)
                                                   value))
                                               columns
                                               row))
                                        rows'))]
            (jdbc/execute! opts (into [sql] flat-params))))))

(defn insert-row!
  "Convenience: insert a single row."
  [conn-id db-name table-name columns values]
  (insert-rows! conn-id db-name table-name columns values))

;; ---------- helper: quote Postgres identifiers ----------

(defn pg-ident
  "Quote a Postgres identifier (table or column name). Accepts string or keyword."
  [x]
  (when (nil? x)
    (throw (ex-info "Identifier cannot be nil" {})))
  (let [s (cond
            (keyword? x) (name x)
            (string?  x) x
            :else (throw (ex-info "Identifier must be string or keyword"
                                  {:value x})))]
    (format "\"%s\"" s)))

(defn- pg-qualified-ident [x]
  (->> (str/split (str x) #"\.")
       (map pg-ident)
       (str/join ".")))

(defn- postgres-type [data-type]
  (let [t (-> (or data-type "varchar(300)") str str/upper-case)]
    (case t
      "STRING" "TEXT"
      "INT" "INTEGER"
      "BIGINT" "BIGINT"
      "BOOLEAN" "BOOLEAN"
      "DATE" "DATE"
      "TIMESTAMP" "TIMESTAMPTZ"
      t)))

;; ---------- column DDL ----------

(defn make-column-ddl
  "Build a column DDL fragment.
   Defaults:
   - data_type: varchar(300)
   - nullable unless :is_nullable \"NO\""
  [{:keys [column_name data_type is_nullable]
    :or   {data_type   "varchar(300)"
           is_nullable "YES"}}]
  (when (nil? column_name)
    (throw (ex-info ":column_name is mandatory in column spec" {})))
  (str (pg-ident column_name)
       " " (postgres-type data_type)
       " "
       (if (= is_nullable "NO") "NOT NULL" "NULL")))

;; ---------- CREATE TABLE DDL (Postgres) ----------

(defn make-create-table-sql-postgres
  [table-name columns]
  (when (nil? table-name)
    (throw (ex-info "table-name cannot be nil" {})))
  (let [audit-ddl
        (str (pg-ident "id")          " BIGSERIAL PRIMARY KEY, "
             (pg-ident "created_at")  " TIMESTAMPTZ NOT NULL DEFAULT now(), "
             (pg-ident "updated_at")  " TIMESTAMPTZ NOT NULL DEFAULT now(), "
             (pg-ident "created_by")  " INTEGER NULL, "
             (pg-ident "updated_by")  " INTEGER NULL")

        cols-ddl
        (->> columns
             (map make-column-ddl)
             (str/join ", "))]
    (format "CREATE TABLE IF NOT EXISTS %s (%s%s%s);"
            (pg-qualified-ident table-name)
            audit-ddl
            (when (seq columns) ", ")
            cols-ddl)))

(defn- dbx-ident [x]
  (when (nil? x)
    (throw (ex-info "Identifier cannot be nil" {})))
  (let [s (cond
            (keyword? x) (name x)
            (string? x) x
            :else (str x))]
    (format "`%s`" s)))

(defn- dbx-qualified-ident [x]
  (->> (str/split (str x) #"\.")
       (remove str/blank?)
       (map dbx-ident)
       (str/join ".")))

(def ^:private databricks-file-format-pattern
  #"(?i)^(CSV|JSON|PARQUET|AVRO|ORC|TEXT|BINARYFILE|XML)$")

(def ^:private databricks-option-name-pattern
  #"^[A-Za-z_][A-Za-z0-9_]*$")

(defn- sql-string-literal
  [value]
  (str "'" (str/replace (str value) "'" "''") "'"))

(defn- databricks-copy-into-option-sql
  [[k v]]
  (let [k (clojure.core/name k)]
    (when-not (re-matches databricks-option-name-pattern k)
      (throw (ex-info "Databricks COPY INTO option name must be a valid identifier"
                      {:option_name k})))
    (str k " = "
         (cond
           (nil? v) "NULL"
           (true? v) "true"
           (false? v) "false"
           (number? v) (str v)
           :else (sql-string-literal v)))))

(defn- databricks-copy-into-options-clause
  [clause-name opts]
  (when (seq opts)
    (str clause-name " ("
         (str/join ", " (map databricks-copy-into-option-sql opts))
         ")")))

(defn- build-databricks-copy-into-sql
  [table-name {:keys [source_uri file_format files pattern format_options copy_options credential]}]
  (let [source-uri  (some-> source_uri str str/trim not-empty)
        file-format (some-> file_format str str/trim str/upper-case)
        files       (vec (or files []))]
    (when-not source-uri
      (throw (ex-info "Databricks COPY INTO requires source_uri"
                      {:field :source_uri})))
    (when-not (and file-format (re-matches databricks-file-format-pattern file-format))
      (throw (ex-info "Databricks COPY INTO requires a supported file_format"
                      {:field :file_format
                       :value file_format})))
    (when (and credential (not (re-matches databricks-option-name-pattern (str credential))))
      (throw (ex-info "Databricks COPY INTO credential must be a valid identifier"
                      {:field :credential
                       :value credential})))
    (str "COPY INTO " (dbx-qualified-ident table-name)
         " FROM " (sql-string-literal source-uri)
         (when credential
           (str " WITH (CREDENTIAL " credential ")"))
         " FILEFORMAT = " file-format
         (when (seq files)
           (str " FILES = ("
                (str/join ", " (map sql-string-literal files))
                ")"))
         (when (some-> pattern str str/trim not-empty)
           (str " PATTERN = " (sql-string-literal pattern)))
         (when-let [clause (databricks-copy-into-options-clause "FORMAT_OPTIONS" format_options)]
           (str " " clause))
         (when-let [clause (databricks-copy-into-options-clause "COPY_OPTIONS" copy_options)]
           (str " " clause)))))

(defn- databricks-col-type
  [{:keys [column_name data_type]}]
  (if (#{"payload_json" "row_json"} (str/lower-case (str column_name)))
    "STRING"
    (case (-> (or data_type "STRING") str str/upper-case)
      "STRING" "STRING"
      "TEXT" "STRING"
      "INT" "INT"
      "INTEGER" "INT"
      "BIGINT" "BIGINT"
      "DOUBLE" "DOUBLE"
      "FLOAT" "DOUBLE"
      "BOOLEAN" "BOOLEAN"
      "DATE" "DATE"
      "TIMESTAMP" "TIMESTAMP"
      "DECIMAL" "DECIMAL(38, 18)"
      (str/upper-case (str data_type)))))

(defn fully-qualified-table-name
  [{:keys [catalog schema dbtype target_kind]} table-name]
  (let [table-name (cond
                     (keyword? table-name) (name table-name)
                     :else (str table-name))
        dbtype     (some-> (or dbtype target_kind) str str/lower-case)]
    (cond
      (re-find #"\." table-name) table-name
      (contains? #{"postgresql" "mysql" "oracle" "sqlserver"} dbtype)
      (if (seq schema) (str schema "." table-name) table-name)
      (and (seq catalog) (seq schema)) (str catalog "." schema "." table-name)
      (seq schema) (str schema "." table-name)
      :else table-name)))

(defn- qualified-table-parts
  [table-name]
  (let [table-name (cond
                     (keyword? table-name) (name table-name)
                     :else (some-> table-name str str/trim))]
    (when (seq table-name)
      (let [parts (->> (str/split table-name #"\.")
                       (remove str/blank?)
                       vec)]
        (case (count parts)
          1 {:table (nth parts 0)}
          2 {:schema (nth parts 0)
             :table  (nth parts 1)}
          3 {:catalog (nth parts 0)
             :schema  (nth parts 1)
             :table   (nth parts 2)}
          nil)))))

(defn make-create-table-sql-databricks
  [table-name columns {:keys [partition-columns]}]
  (when (nil? table-name)
    (throw (ex-info "table-name cannot be nil" {})))
  (let [cols-ddl (->> columns
                      (map (fn [{:keys [column_name data_type is_nullable]
                                 :or {data_type "STRING" is_nullable "YES"}}]
                             (str (dbx-ident column_name)
                                  " "
                                  (databricks-col-type {:column_name column_name
                                                        :data_type data_type})
                                  (when (= is_nullable "NO") " NOT NULL"))))
                      (str/join ", "))
        partition-ddl (when (seq partition-columns)
                        (str " PARTITIONED BY ("
                             (str/join ", " partition-columns)
                             ")"))]
    (format "CREATE TABLE IF NOT EXISTS %s (%s) USING DELTA%s"
            table-name
            cols-ddl
            (or partition-ddl ""))))

(defn- snowflake-ident [x]
  (when (nil? x)
    (throw (ex-info "Identifier cannot be nil" {})))
  (let [s (cond
            (keyword? x) (name x)
            (string? x) x
            :else (str x))]
    (format "\"%s\"" s)))

(defn- snowflake-qualified-ident [x]
  (->> (str x)
       (str/split #"\.")
       (map snowflake-ident)
       (str/join ".")))

(defn- snowflake-column-type
  [{:keys [column_name data_type]}]
  (if (#{"payload_json" "row_json"} (str/lower-case (str column_name)))
    "VARIANT"
    (case (-> (or data_type "STRING") str str/upper-case)
      "STRING" "STRING"
      "INT" "NUMBER"
      "BIGINT" "NUMBER"
      "DOUBLE" "DOUBLE"
      "BOOLEAN" "BOOLEAN"
      "DATE" "DATE"
      "TIMESTAMP" "TIMESTAMP_TZ"
      (str/upper-case (str data_type)))))

(defn make-create-table-sql-snowflake
  [table-name columns]
  (when (nil? table-name)
    (throw (ex-info "table-name cannot be nil" {})))
  (let [cols-ddl (->> columns
                      (map (fn [{:keys [column_name is_nullable] :as col}]
                             (str (snowflake-ident column_name)
                                  " "
                                  (snowflake-column-type col)
                                  (when (= is_nullable "NO") " NOT NULL"))))
                      (str/join ", "))]
    (format "CREATE TABLE IF NOT EXISTS %s (%s)"
            (snowflake-qualified-ident table-name)
            cols-ddl)))

;; ---------------------------
;; Runner
;; ---------------------------

(defn run-ddl!
  [conn-id db-name ddl-sql]
  (let [opts (get-opts conn-id db-name)]
    (jdbc/execute! opts [ddl-sql])))

(defn ensure-schema-exists!
  ([conn-id db-name]
   (ensure-schema-exists! conn-id db-name nil))
  ([conn-id db-name table-name]
   (let [db-spec        (create-dbspec-from-id conn-id)
         table-parts    (qualified-table-parts table-name)
         default-catalog (or (:catalog db-spec) (:dbname db-spec))
         target-catalog  (or (:catalog table-parts) default-catalog)
         target-schema   (or (:schema table-parts) (:schema db-spec))]
     (when (seq target-schema)
       (case (:dbtype db-spec)
         "databricks" (run-ddl! conn-id db-name (format "CREATE SCHEMA IF NOT EXISTS %s.%s"
                                                        target-catalog
                                                        target-schema))
         "snowflake"  (run-ddl! conn-id db-name (format "CREATE SCHEMA IF NOT EXISTS %s.%s"
                                                        target-catalog
                                                        target-schema))
         "postgresql" (run-ddl! conn-id db-name (format "CREATE SCHEMA IF NOT EXISTS %s"
                                                        (pg-ident target-schema)))
         nil)))))

;; ---------------------------
;; Convenience wrapper
;; ---------------------------

(defn create-table!
  ([conn-id db-name table-name columns]
   (create-table! conn-id db-name table-name columns {}))
  ([conn-id db-name table-name columns opts]
   (let [db-spec (create-dbspec-from-id conn-id)
         _       (ensure-schema-exists! conn-id db-name table-name)
         ddl     (case (:dbtype db-spec)
                   "databricks" (make-create-table-sql-databricks
                                  (fully-qualified-table-name db-spec table-name)
                                  columns
                                  opts)
                   "snowflake" (make-create-table-sql-snowflake
                                 (fully-qualified-table-name db-spec table-name)
                                 columns)
                   (make-create-table-sql-postgres table-name columns))]
     (run-ddl! conn-id db-name ddl)
     (let [table-parts      (qualified-table-parts table-name)
           default-catalog  (or (:catalog db-spec) (:dbname db-spec))
           target-catalog   (or (:catalog table-parts) default-catalog)
           target-schema    (or (:schema table-parts) (:schema db-spec) "public")
           target-table     (:table table-parts)
           existing-columns (->> (jdbc/execute!
                                  (get-opts conn-id db-name)
                                  (into [(str "SELECT column_name FROM information_schema.columns "
                                              "WHERE table_schema = ? AND table_name = ?"
                                              (when (contains? #{"databricks" "snowflake"} (:dbtype db-spec))
                                                " AND table_catalog = ?"))]
                                        (cond-> [target-schema target-table]
                                          (contains? #{"databricks" "snowflake"} (:dbtype db-spec))
                                          (conj target-catalog)))
                                  {:builder-fn rs/as-unqualified-lower-maps})
                                 (map :column_name)
                                 (remove nil?)
                                 set)
           missing-columns  (->> columns
                                 (remove #(contains? existing-columns (:column_name %)))
                                 vec)]
       (when (seq missing-columns)
         (case (:dbtype db-spec)
           "databricks"
           (run-ddl! conn-id db-name
                     (str "ALTER TABLE " (fully-qualified-table-name db-spec table-name)
                          " ADD COLUMNS ("
                          (str/join ", "
                                    (map (fn [{:keys [column_name data_type]}]
                                           (str (dbx-ident column_name) " "
                                                (databricks-col-type {:column_name column_name
                                                                      :data_type data_type})))
                                         missing-columns))
                          ")"))
           "snowflake"
           (doseq [{:keys [column_name] :as col} missing-columns]
             (run-ddl! conn-id db-name
                       (str "ALTER TABLE " (snowflake-qualified-ident (fully-qualified-table-name db-spec table-name))
                            " ADD COLUMN IF NOT EXISTS "
                            (snowflake-ident column_name) " "
                            (snowflake-column-type col))))
           (doseq [col missing-columns]
             (run-ddl! conn-id db-name
                       (str "ALTER TABLE " (pg-qualified-ident table-name)
                            " ADD COLUMN IF NOT EXISTS "
                            (make-column-ddl col))))))))))

;; ... all your existing code ...

(defn load-rows!
  "Insert a seq of row maps into a table.

   Arguments:
     conn-id       - connection id (from :connection table)
     db-name       - logical db name (e.g. \"lake\"), or nil to use default from connection
     table-name    - string or keyword for target table (e.g. \"github_pull_requests\")
     rows          - seq of maps, e.g.
                     [{:number 8657
                       :title  \"...\"
                       :state  \"OPEN\"
                       :updated_at \"2025-12-07T02:00:24Z\"
                       :created_at \"2025-12-07T01:12:29Z\"}
                      ...]
     key->col      - map from row keys to DB column names, e.g.
                     {:number     \"number\"
                      :title      \"title\"
                      :state      \"state\"
                      :updated_at \"updated_at\"
                      :created_at \"created_at\"}

   Behavior:
     - Uses the order of keys in `key->col` to determine column order.
     - Builds a minimal `columns` vector for insert-rows!:
         [{:column_name \"number\"} {:column_name \"title\"} ...]
     - Converts each row map to a value vector [v1 v2 ...] matching that order.
     - Calls insert-rows! to actually insert the data."
  [conn-id db-name table-name rows key->col]
  (if (or (nil? rows) (empty? rows))
    {:status :no-op
     :reason :empty-rows}
    (let [;; preserve insertion order of the mapping keys
          col-keys  (vec (keys key->col))

          ;; build column specs that insert-rows! expects
          columns   (mapv (fn [k]
                            (let [col-name (get key->col k)]
                              {:column_name (cond
                                              (keyword? col-name) (name col-name)
                                              (string?  col-name) col-name
                                              :else (str col-name))}))
                          col-keys)

          ;; turn each row map into a value vector in the same order
          row-vals  (mapv (fn [row]
                            (mapv (fn [k] (get row k))
                                  col-keys))
                          rows)]
      ;; delegate to your existing bulk insert
      (insert-rows! conn-id db-name table-name columns row-vals))))

(def ^:private snowflake-stage-name-pattern #"^@?[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$")

(defn- csv-cell
  [value]
  (let [value (cond
                (nil? value) "__BITOOL_NULL__"
                (map? value) (json/generate-string value)
                (vector? value) (json/generate-string value)
                :else (str value))
        escaped (-> value
                    (str/replace "\"" "\"\"")
                    (str/replace "\r\n" "\n")
                    (str/replace "\r" "\n"))]
    (str "\"" escaped "\"")))

(defn- normalize-snowflake-stage-ref
  [stage-name]
  (let [stage-name (or (some-> stage-name str str/trim not-empty)
                       (str "BITOOL_STAGE_" (str/replace (str (UUID/randomUUID)) "-" "_")))]
    (when-not (re-matches snowflake-stage-name-pattern stage-name)
      (throw (ex-info "Snowflake stage name must be a valid identifier"
                      {:stage_name stage-name})))
    {:stage-ref (if (str/starts-with? stage-name "@") stage-name (str "@" stage-name))
     :stage-ident (str/replace stage-name #"^@" "")}))

(defn- snowflake-copy-value-expr
  [column idx]
  (let [raw-expr  (str "$" idx)
        value-expr (str "IFF(" raw-expr " = '__BITOOL_NULL__', NULL, " raw-expr ")")]
    (if (snowflake-variant-column? column)
      (str "PARSE_JSON(" value-expr ")")
      value-expr)))

(defn- write-snowflake-stage-csv!
  [columns rows]
  (let [file (java.io.File/createTempFile "bitool-sf-stage-" ".csv")
        row-keys (mapv :row-key columns)]
    (.deleteOnExit file)
    (with-open [writer (io/writer file)]
      (.write writer (str (str/join "," (map #(csv-cell (:column_name %)) columns)) "\n"))
      (doseq [row rows]
        (.write writer
                (str (str/join ","
                               (map (fn [k]
                                      (csv-cell (get row k)))
                                    row-keys))
                     "\n"))))
    file))

(defn load-rows-snowflake-stage-copy!
  [conn-id db-name table-name rows key->col {:keys [sf_stage_name sf_on_error sf_purge]}]
  (if (or (nil? rows) (empty? rows))
    {:status :no-op
     :reason :empty-rows}
    (let [col-keys      (vec (keys key->col))
          columns       (mapv (fn [k]
                                {:row-key k
                                 :column_name (let [col-name (get key->col k)]
                                                (cond
                                                  (keyword? col-name) (name col-name)
                                                  (string? col-name) col-name
                                                  :else (str col-name)))})
                              col-keys)
          file          (write-snowflake-stage-csv! columns rows)
          {:keys [stage-ref stage-ident]} (normalize-snowflake-stage-ref sf_stage_name)
          generated-stage? (str/blank? (str sf_stage_name))
          put-path      (str "file://" (.getAbsolutePath file))
          table-ident   (snowflake-qualified-ident table-name)
          column-idents (mapv (comp snowflake-ident :column_name) columns)
          select-exprs  (map-indexed (fn [idx column]
                                       (snowflake-copy-value-expr column (inc idx)))
                                     columns)
          put-sql       (str "PUT '" put-path "' " stage-ref " AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
          copy-sql      (str "COPY INTO " table-ident
                             " (" (str/join ", " column-idents) ") "
                             "FROM (SELECT " (str/join ", " select-exprs) " FROM " stage-ref ") "
                             "FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1 NULL_IF = ('__BITOOL_NULL__')) "
                             "ON_ERROR = '" (or (some-> sf_on_error str str/trim not-empty) "ABORT_STATEMENT") "' "
                             "PURGE = " (if (false? sf_purge) "FALSE" "TRUE"))
          create-stage-sql (str "CREATE OR REPLACE TEMP STAGE " (snowflake-ident stage-ident))]
      (with-open [conn (jdbc/get-connection (get-ds conn-id db-name))]
        (when generated-stage?
          (jdbc/execute! conn [create-stage-sql]))
        (jdbc/execute! conn [put-sql])
        (jdbc/execute! conn [copy-sql]))
      {:status :ok
       :load_method :snowflake_stage_copy
       :row_count (count rows)
       :table_name table-name})))

(defn load-rows-databricks-copy-into!
  [conn-id db-name table-name {:keys [source_uri file_format files pattern format_options copy_options credential]}]
  (let [sql (build-databricks-copy-into-sql table-name {:source_uri source_uri
                                                        :file_format file_format
                                                        :files files
                                                        :pattern pattern
                                                        :format_options format_options
                                                        :copy_options copy_options
                                                        :credential credential})]
    (jdbc/execute! (get-opts conn-id db-name) [sql])
    {:status :ok
     :load_method :databricks_copy_into
     :table_name table-name
     :source_uri source_uri}))
	
