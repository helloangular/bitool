(ns bitool.db
        (:require [next.jdbc :as jdbc]
        [next.jdbc.sql :as sql]
        [next.jdbc.result-set :as rs]
        [com.rpl.specter :as sp]
        [clojure.data.codec.base64 :as b64]
        [cheshire.core :as json]
        [clojure.edn :as edn]
        [clojure.pprint :as pp]
        [clojure.java.io :as io])
(:import [java.util.zip GZIPOutputStream GZIPInputStream]
           [java.io ByteArrayOutputStream ByteArrayInputStream]
           [java.util Base64]))
        
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

(defn seq-next-val[name] 
     (let [query (str "SELECT nextval('" name "') AS next_val")]
    (-> (jdbc/execute-one! ds [query])
        :next_val))) 

(defn getGraph[gid] (read-string (:graph/definition (jdbc/execute-one! ds ["select definition from graph where id = ? and version = (select max(version) from graph where id = ?)" gid gid])))) 

(defn version+[g] (update-in g [:a :v] inc))

(defn insertGraph [ g ] 
    (let [
           _ (pp/pprint g)
           g (version+ g) 
	   id (id g) 
           gid (if (= 0 id) (seq-next-val "sqlgraph_id_seq") id) 
           definition (pr-str (assoc-in g [:a :id] gid)) ]
    	(read-string (:graph/definition (sql/insert! ds :graph { :id gid :version (version g) :name (name g) :definition definition })))))
   
;; (defn updateGraph[g]
;;    (sql/update! ds :graph {:version (version g) :name (name g) :definition (definition g)} {:id (id g)})) 

(defn save-conn [ args ]
   (do
      (println "-------------------DB Inside -------------------")
      (println args)
      (println (class (:json-params args)))
      (println "-------------------DB1 Inside -------------------")
      (println (:multipart-params args))
      (println "-------------------DB2 Inside -------------------")
      (println  (sp/transform ["dbtype"] first (sp/transform ["port"] #(Integer/parseInt %) (:multipart-params args))))
    (def ins (insert-data :connection (sp/transform ["dbtype"] first (sp/transform ["port"] #(Integer/parseInt %) (:multipart-params args)))))
    (println "SAVE-CONN")
    (println ins)
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
          ;; Add other db types if needed
          (throw (ex-info "Unsupported database type" {:dbtype dbtype})))))))

;; Example usage
(defn get-dbspec[conn-id db-name]
     (let [db-spec (create-dbspec-from-id  conn-id)] 
     	(if db-name (assoc db-spec :dbname db-name) db-spec)))

(defn get-dbtype [conn-id]
      (keyword (:dbtype (get-dbspec conn-id false))))

(defn get-ds[conn-id db-name]
   (jdbc/get-datasource (get-dbspec conn-id db-name)))

(defn get-opts[conn-id db-name]
      	(jdbc/with-options (get-ds conn-id db-name) {:builder-fn rs/as-unqualified-lower-maps}))

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

(defn get-metadata[conn-id db-name sql-name & params]
        (let [sql (get-sql (get-dbtype conn-id) sql-name)
              fx (if (= sql-name :columns) #(map %  [:column_name :data_type :is_nullable :character_maximum_length :numeric_precision :numeric_scale :is_primary_key :exp :tfm :agg :excluded]) #(vals %))
              _ (println (str "SQL : " sql " DBNAME : " db-name " Params : " params))
               ]
      		(map fx (jdbc/execute! (get-opts conn-id db-name) (into [sql] params)))))

(defn get-databases [conn-id]
      (map first (get-metadata conn-id false :databases)))

(defn get-database [conn-id]
      (conj [] (:dbname (create-dbspec-from-id conn-id))))

(defn get-schemas [conn-id db-name]
       (map first (get-metadata conn-id db-name :schemas)))
      
(defn get-tables [conn-id db-name schema-name]
       (map first (get-metadata conn-id db-name :tables schema-name)))

(defn join-column [ coldetails ]
  (->> coldetails
       (remove nil?)               
       (map #(if (number? %) (str %) %)) 
       (clojure.string/join "-"))) 


(defn get-columns [conn-id db-name schema-name table-name]
      (map vec (get-metadata conn-id db-name :columns schema-name table-name)))
      ;; (map #(join-column %) (get-metadata conn-id db-name :columns schema-name table-name)))

(defn get-table-columns [conn-id table-name]
  (let [db-spec (create-dbspec-from-id conn-id)
        dbname (:dbname db-spec) 
        _ (pp/pprint db-spec)
        _ (println dbname)
        schema (if (nil? (:schema db-spec)) "public"  (:schema db-spec)) 
        _ (println schema)
        _ (println table-name)
       ]
       (get-columns conn-id dbname schema table-name)))

(defn get-table [connection-id table-name]
      (let [columns (get-table-columns connection-id table-name)
            _ (println "--------------COLUMNS----------------")  
            _ (println columns)
           ]
           {:name table-name :btype "T" :tcols columns}))            
