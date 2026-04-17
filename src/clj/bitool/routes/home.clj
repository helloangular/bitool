  (ns bitool.routes.home
  (:require
   [bitool.layout :as layout]
   [bitool.db :as db]
   [bitool.api.schema :as sc]
   [bitool.macros :refer :all]
   [ubergraph.core :as uber]
   [bitool.graph2 :as g2]
   [bitool.graph :as graph]
   [clojure.string :as string]
   [clojure.set :as set]
   [clojure.java.io :as io]
   [clojure.edn :as edn]
   [bitool.middleware :as middleware]
   [selmer.parser :as selmer]
   [selmer.filters :as filters]
   [ring.util.response :as response ]
   [ring.middleware.session :as session]
   [ring.util.http-response :as http-response]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [next.jdbc.sql :as sql]
   [cheshire.core :as json]
   [clj-http.client :as client]
   [buddy.core.crypto :as crypto]
   [buddy.core.codecs :as codecs]
   [buddy.core.keys :as keys]
   [buddy.core.mac :as mac]
   [buddy.core.nonce :as nonce]
   [buddy.core.hash :as hash]
   [clojure.pprint :as pp]
   [clojure.walk :as walk]
   [taoensso.telemere :as tel]
   [bitool.config :as config :refer [env]]
   [bitool.endpoint :as endpoint]
   [bitool.api.openapi :as openapi]
   [bitool.control-plane :as control-plane]
   [bitool.ingest.execution :as ingest-execution]
   [bitool.ingest.runtime :as ingest-runtime]
   [bitool.ingest.scheduler :as ingest-scheduler]
   [bitool.modeling.automation :as modeling-automation]
   [bitool.operations :as operations]
   [bitool.gil.api :as gil-api]
   [bitool.ai.assistant :as ai-assistant]
   [bitool.ops.schema-drift :as schema-drift]
   [bitool.schema-fingerprints :as fingerprints]
   [bitool.semantic.model :as semantic-model]
   [bitool.semantic.expression :as sem-expr]
   [bitool.semantic.association :as sem-assoc]
   [bitool.semantic.perspective :as sem-persp]
   [bitool.semantic.governance :as sem-gov]
   )
  (:import [java.net URI InetAddress]
           [java.time LocalDate YearMonth]))

(filters/add-filter! :second second)

(defn- backend-debug-logging-enabled? []
  (contains? #{"true" "1" "yes" "on"}
             (some-> (get env :bitool_backend_debug_logs) str string/lower-case)))

(defn- dbg [& args]
  (when (backend-debug-logging-enabled?)
    (apply println args)))

(defn- dbg-pp [value]
  (when (backend-debug-logging-enabled?)
    (pp/pprint value)))

(defn- tree-debug
  [stage data]
  (println "[TreeLazyDebug]" stage (pr-str data)))

(defn updateCoordinates [coordV] (map #(update % :y (fn[x] (+ (* x 50) 150)) ) (map #(update % :x (fn[x] (+ (* x 100) 150)) ) coordV)))

(defn rename-op [m]
  (into {} (map (fn [[k v]]
                       (cond 
                           (and (= k :alias) (= v "O")) [:alias "Output"]
                           :else [k v])) m)))

(defn getCoordinates[g] 
	(let [
		n (g2/nodecount g)
	     ]
	     (if (= n 0) [] (map rename-op (updateCoordinates (g2/createCoordinates g))))))

(defn splitCpSp[g]
      (let [
             cp (g2/connected-graph g)
             sp (g2/unconnected-graph g)
           ]
           {:cp cp :sp sp}))

(declare persist-graph! graph-save-error-response
         parse-required-int parse-optional-int parse-optional-bool)

(defn mapCoordinates[g]
      (let [
		retmap (splitCpSp g)
                _ (when (backend-debug-logging-enabled?)
                    (tel/log! {:level :debug :msg "mapCoordinates" :data {:retmap retmap}}))

           ]
             (into (getCoordinates (:cp retmap)) (g2/getOrphanAttrs (:sp retmap)))))
      ;;     (into (getCoordinates (:cp retmap)) (:sp retmap))))
  ;;         (assoc {} :cp (getCoordinates (:cp retmap)) :sp (:sp retmap))))

(defn about-page [{session :session}]
  (-> (http-response/ok "Home Page Setting session")
      (assoc :session (assoc session :user "Harish"))
      (assoc :headers {"Content-Type" "text/plain"})))

(defn home-page [request]
  (let [_ (dbg (str "Session :" (:session request)))]
  (layout/render request "index.html" {:name "Harish"})))

(defn custom [request]
  (let [_ (dbg (str "Session :" (:session request)))]
  (layout/render request "customWebComponent.html" {:name "Harish"})))


(declare initial-connection-items)

(defn testReq [request] (let [x (:params request)]
					(dbg (:table1 x))
					(dbg (:table2 x))
					(+ 1 1)))
(defn get-conn[request]
  (let [conn-id-str (:conn-id (:params request))]
    (if (or (nil? conn-id-str) (= "" conn-id-str))
      (http-response/ok
       (->> (db/list-all-connections-summary)
            (mapv (fn [conn]
                    {:id (:id conn)
                     :name (or (:connection_name conn)
                               (str "Connection " (:id conn)))
                     :dbtype (:dbtype conn)}))))
      (let [conn-id (Integer. conn-id-str)
            detail  (db/get-connection-detail conn-id)
            items   (when detail
                      (initial-connection-items conn-id (:dbtype detail)))]
        (http-response/ok {:items (or items [])})))))

(defn get-btype[alias]
      (if (some #{"join" "union" "projection" "aggregation" "sorter" "filter" "function" "target" "output" "api-connection" "api" "kafka-source" "file-source" "graphql-builder" "mapping" "conditionals" "endpoint" "response-builder" "validator" "auth" "db-execute" "rate-limiter" "cors" "logger" "cache" "event-emitter" "circuit-breaker" "scheduler" "webhook"} [alias])
          alias
          "T"))

(defn addSingle[request] (let [ _ (dbg-pp request)
                                 params (:params request)
                              _ (dbg (str "BTYPE : " (:alias params)))
                              _ (dbg (str "ALIAS class: " (class (:alias params))))
                              _ (dbg (str "ALIAS count: " (count (:alias params))))
                              gid (:gid (:session request))
                              alias (:alias params)
 			      _ (dbg (str "gid : " gid))
			      g (g2/add-single-node gid (Integer. (anil? (:conn_id params) "123")) alias (get-btype alias) (:x params) (:y params))
                               ]
                               g))

(defn connectSingle[request] (let [ _ (dbg-pp request)
                                 params (:params request)
                              gid (:gid (:session request))
 			      _ (dbg (str "gid : " gid))
			      g (g2/connect-single-node gid (Integer. (:conn_id params)) (Integer. (:src params)) (Integer. (:dest params)))
                               ]
			       g))

(defn saveRectJoin[request] (let [
                              ;;  _ (pp/pprint request)
                                 params (:params request)
                              gid (:gid (:session request))
 			      _ (dbg (str "gid : " gid))
                              _ (dbg "Inside save-rect-join")
                              g (g2/rect-join gid (Integer. (:src params)) (Integer. (:dest params)))
                              _ (dbg "FINAL GRAPH")
                                _ (dbg-pp g)
                               ]
                               g))

(defn addTable [request] (let [ _ (dbg-pp request)
                              params (:params request)
			      t1 (:table1 params) 
			      t2 (Integer. (:table2 params))
			      c1 (Integer. (:conn_id params))
                              gid (:gid (:session request))
                              jtype (:action params)
 			      _ (dbg (str "gid : " gid))
                              _ (dbg (str "t1 -home: " t1))
                              _ (dbg (str "t2 -home: " t2))
                              _ (dbg (str "c2 -home: " c1))
                              t1-map (db/get-table c1 t1)
			      g (g2/processAddTable gid t2 c1 t1-map (g2/btype-codes jtype))
                              _ (dbg "AFTER processAddTable")
                        ;;      _ (pp/pprint g)
                              session (:session request)
                              _ (dbg "Session " session)
                              _ (dbg "Printing Graph")
			      _ (dbg (class g))
			      ]
                              g))
                              ;; (g2/createCoordinates g)))

(defn deleteTable [request] (let [params (:params request)
			      t1 (:item params)
                              gid (:gid (:session request))
 			      _ (dbg (str "gid : " gid))
                              _ (dbg (str "t1 -home: " t1))
			      g (graph/deleteTable gid t1)
                              _ (dbg g)
			      ]
                              (dbg "Printing Graph")
			      (dbg (class g))
                              g))
                              ;; (g2/createCoordinates g)))


(defn addFilter [request]
  (let [params (:params request)
        item (:item params)
        gid (:gid (:session request))
        ftype (:label params)
        _ (dbg (str "ITEM " item))
        _ (dbg (str "ftype " ftype))
        retmap (g2/addRightClickNode gid (Integer. item) (g2/btype-codes ftype))]
                              (assoc retmap :cp (getCoordinates (:cp retmap)))))

(defn graph-page [request]
  (let [
         params (:params request)
         gid (:gid params)
         _ (dbg (str "home gid : " gid))
         graph (db/getGraph (Integer. gid))
         ver (get-in graph [:a :v])
         session (:session request)
       ]
     ;; (http-response/ok { "alias" "Output" "btype" "O" "x" "250" "y" "250" "id" 1 "parent" 0 })))
     (-> (http-response/ok (mapCoordinates graph))
         (assoc :session (assoc session :gid (Integer. gid) :ver ver)))))

(defn list-models [_request]
  (http-response/ok (db/list-models)))

(defn- connection-tree-parent
  [dbtype]
  (case (some-> dbtype string/lower-case)
    "api" "API"
    "databricks" "Databricks"
    "bigquery" "GCP"
    "snowflake" "Snowflake"
    ("kafka_stream" "connector") "Kafka"
    "postgresql" "PostgreSQL"
    "oracle" "Oracle"
    "sqlserver" "SQL Server"
    ("mysql" "db2") "RDBMS"
    nil))

(defn- db-type?
  [dbtype]
  (contains? #{"postgresql" "oracle" "sqlserver" "mysql" "db2"
               "snowflake" "databricks" "bigquery"}
             (some-> dbtype string/lower-case)))

(defn- loading-tree-item
  []
  {:label "Loading..."
   :disabled true
   :placeholder true})

(defn- postgres-connection-items
  []
  [(loading-tree-item)])

(defn- postgres-database-items
  [conn-id]
  (let [dbs (->> (or (seq (db/get-databases conn-id))
                     (db/get-database conn-id))
                 distinct
                 (remove nil?)
                 (map str)
                 (remove string/blank?)
                 vec)
        items (mapv (fn [db-name]
                      {:label db-name
                       :conn_id conn-id
                       :db_name db-name
                       :lazy true
                       :lazy_kind "database"
                       :items [(loading-tree-item)]})
                    dbs)]
    (tree-debug "postgres-database-items"
                {:conn-id conn-id
                 :db-count (count items)
                 :dbs (take 10 dbs)})
    items))

(defn- postgres-database-children
  [conn-id db-name]
  (let [schemas (->> (db/get-schemas conn-id db-name) vec)
        items   (mapv (fn [schema-name]
                        {:label schema-name
                         :conn_id conn-id
                         :db_name db-name
                         :schema schema-name
                         :lazy true
                         :lazy_kind "schema"
                         :items [(loading-tree-item)]})
                      schemas)]
    (tree-debug "postgres-database-children"
                {:conn-id conn-id
                 :db-name db-name
                 :schema-count (count items)
                 :schemas schemas})
    items))

(defn- postgres-schema-children
  [conn-id db-name schema-name]
  (let [tables (->> (db/get-tables conn-id db-name schema-name) vec)
        items  (mapv (fn [table-name]
                       {:label table-name
                        :conn_id conn-id
                        :db_name db-name
                        :schema schema-name
                        :table_name table-name
                        :nodetype "table"
                        :lazy true
                        :lazy_kind "table"
                        :items [(loading-tree-item)]})
                     tables)]
    (tree-debug "postgres-schema-children"
                {:conn-id conn-id
                 :db-name db-name
                 :schema schema-name
                 :table-count (count items)
                 :tables (take 20 tables)})
    items))

(defn- postgres-table-children
  [conn-id db-name schema-name table-name]
  (let [columns (->> (db/get-columns conn-id db-name schema-name table-name)
                     (map (fn [col]
                            {:label (db/join-column col)}))
                     vec)]
    (tree-debug "postgres-table-children"
                {:conn-id conn-id
                 :db-name db-name
                 :schema schema-name
                 :table-name table-name
                 :column-count (count columns)
                 :columns (take 20 (map :label columns))})
    columns))

(defn- annotate-db-tree-items
  ([items conn-id]
   (annotate-db-tree-items items conn-id {}))
  ([items conn-id {:keys [db-name schema level] :or {level 0}}]
   (mapv (fn [item]
           (let [label        (:label item)
                 next-context (case level
                                0 {:db-name label :schema schema :level (inc level)}
                                1 {:db-name db-name :schema label :level (inc level)}
                                {:db-name db-name :schema schema :level (inc level)})
                 annotated    (cond-> item
                                (= level 2)
                                (assoc :conn_id conn-id
                                       :schema schema
                                       :db_name db-name
                                       :nodetype "table"))]
             (if (seq (:items item))
               (assoc annotated :items (annotate-db-tree-items (:items item) conn-id next-context))
               annotated)))
         (or items []))))

(defn- initial-connection-items
  [conn-id dbtype]
  (let [kind (some-> dbtype string/lower-case)
        items (case kind
                "postgresql" (postgres-connection-items)
                (when-let [tree (try
                                  (let [f (future (g2/getDBTree conn-id))]
                                    (deref f 5000 nil))
                                  (catch Exception _
                                    nil))]
                  (annotate-db-tree-items (:items tree) conn-id)))]
    (tree-debug "initial-connection-items"
                {:conn-id conn-id
                 :dbtype dbtype
                 :kind kind
                 :item-count (count (or items []))
                 :labels (mapv :label (take 10 (or items [])))})
    items))

(defn- connection->tree-item
  [conn]
  (let [dbtype  (:dbtype conn)
        conn-id (:id conn)
        postgres? (= "postgresql" (some-> dbtype string/lower-case))
        items   (when (db-type? dbtype)
                  (initial-connection-items conn-id dbtype))]
    (cond-> {:conn_id conn-id
             :label   (or (:connection_name conn) (str "Connection " (:id conn)))
             :dbtype  dbtype
             :treeParent (connection-tree-parent dbtype)}
      postgres?
      (assoc :lazy true
             :lazy_kind "connection")
      (= "api" (some-> dbtype string/lower-case))
      (assoc :nodetype "api-connection")
      (some? items)
      (assoc :items items))))

(defn get-connection-tree [request]
  (try
    (let [params (:params request)
          conn-id (parse-required-int (or (:conn_id params) (:conn-id params)) :conn_id)
          detail  (db/get-connection-detail conn-id)
          dbtype  (:dbtype detail)
          items   (initial-connection-items conn-id dbtype)]
      (tree-debug "get-connection-tree"
                  {:conn-id conn-id
                   :dbtype dbtype
                   :item-count (count (or items []))})
      (http-response/ok {:conn_id conn-id
                         :tree {:items items}}))
    (catch Exception e
      (http-response/ok {:conn_id nil :tree nil :error (.getMessage e)}))))

(defn get-connection-tree-children [request]
  (try
    (let [params      (:params request)
          conn-id     (parse-required-int (or (:conn_id params) (:conn-id params)) :conn_id)
          lazy-kind   (some-> (or (:lazy_kind params) (:lazy-kind params)) str string/lower-case)
          db-name     (or (:db_name params) (:db-name params))
          schema-name (or (:schema params) (:schema-name params))
          table-name  (or (:table_name params) (:table-name params))
          _           (tree-debug "get-connection-tree-children-request"
                                  {:conn-id conn-id
                                   :lazy-kind lazy-kind
                                   :db-name db-name
                                   :schema schema-name
                                   :table-name table-name})
          items       (case lazy-kind
                        "connection" (postgres-database-items conn-id)
                        "database" (postgres-database-children conn-id db-name)
                        "schema" (postgres-schema-children conn-id db-name schema-name)
                        "table" (postgres-table-children conn-id db-name schema-name table-name)
                        (throw (ex-info "Unsupported lazy_kind"
                                        {:status 400
                                         :lazy_kind lazy-kind})))]
      (tree-debug "get-connection-tree-children-response"
                  {:conn-id conn-id
                   :lazy-kind lazy-kind
                   :item-count (count (or items []))
                   :labels (mapv :label (take 20 (or items [])))})
      (http-response/ok {:conn_id conn-id
                         :items items}))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn list-connections [_request]
  (let [connections (->> (db/list-all-connections-summary)
                         (map (fn [conn]
                                (let [health (control-plane/get-connection-health (:id conn))]
                                  (-> (connection->tree-item conn)
                                      (assoc :health_status (:status health))
                                      (assoc :health_checked_at (:checked_at_utc health))
                                      (assoc :health_error (:error_message health))))))
                         (remove #(nil? (:treeParent %)))
                         vec)]
    (http-response/ok {:connections connections})))

(defn get-connection-detail [request]
  (try
    (let [params (:params request)
          conn-id (parse-required-int (or (:conn_id params) (:conn-id params)) :conn_id)
          detail (db/get-connection-detail conn-id)]
      (if detail
        (http-response/ok (assoc detail :health (control-plane/get-connection-health conn-id)))
        (http-response/not-found {:error "Connection not found" :conn_id conn-id})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn create-api-connection [request]
  (try
    (let [params (:params request)
          connection-name (or (:api_name params) (:connection_name params))
          conn-id-param (or (:conn_id params) (:conn-id params))
          spec-url (:specification_url params)
          uri (when (seq (str spec-url))
                (try
                  (URI. (str spec-url))
                  (catch Exception _
                    nil)))
          port (let [explicit (some-> uri .getPort)]
                 (cond
                   (and explicit (not= -1 explicit)) explicit
                   (= "https" (some-> uri .getScheme string/lower-case)) 443
                   (= "http" (some-> uri .getScheme string/lower-case)) 80
                   :else 443))
          conn-row {:connection_name connection-name
                    :dbtype "api"
                    :host spec-url
                    :port port
                    :schema (:authentication params)
                    :username (:username params)
                    :password (:password params)}
          conn-id (when (some? conn-id-param)
                    (parse-required-int conn-id-param :conn_id))
          _       (when conn-id
                    (db/update-connection! conn-id conn-row))
          inserted (when-not conn-id
                     (db/insert-data :connection conn-row))
          conn-id (or conn-id (:connection/id inserted))]
      (http-response/ok
       {"conn-id" conn-id
        "tree-data" {:label (or connection-name "API Connection")
                     :items []
                     :nodetype "api-connection"}}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn update-db-connection [request]
  (try
    (let [params (:params request)
          conn-id (parse-required-int (or (:conn_id params) (:conn-id params)) :conn_id)
          existing (db/get-connection-detail conn-id)]
      (when-not existing
        (throw (ex-info "Connection not found" {:status 404 :conn_id conn-id})))
      (let [attrs (cond-> {}
                    (contains? params :connection_name) (assoc :connection_name (:connection_name params))
                    (contains? params :dbtype) (assoc :dbtype (:dbtype params))
                    (contains? params :host) (assoc :host (:host params))
                    (contains? params :dbname) (assoc :dbname (:dbname params))
                    (contains? params :schema) (assoc :schema (:schema params))
                    (contains? params :username) (assoc :username (:username params))
                    (contains? params :password) (assoc :password (:password params))
                    (contains? params :sid) (assoc :sid (:sid params))
                    (contains? params :service) (assoc :service (:service params))
                    (contains? params :token) (assoc :token (:token params))
                    (contains? params :http_path) (assoc :http_path (:http_path params))
                    (contains? params :catalog) (assoc :catalog (:catalog params))
                    (contains? params :warehouse) (assoc :warehouse (:warehouse params))
                    (contains? params :role) (assoc :role (:role params))
                    (some-> (:port params) str not-empty) (assoc :port (Integer/parseInt (str (:port params)))))]
        (db/update-connection! conn-id attrs)
        (when-let [db-name (:dbname existing)]
          (db/invalidate-ds-cache! conn-id db-name))
        (http-response/ok {:status "ok" :conn_id conn-id})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch NumberFormatException e
      (http-response/bad-request {:error (.getMessage e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn delete-connection [request]
  (try
    (let [params (:params request)
          conn-id (parse-required-int (or (:conn_id params) (:conn-id params)) :conn_id)
          existing (db/get-connection-detail conn-id)]
      (when-not existing
        (throw (ex-info "Connection not found" {:status 404 :conn_id conn-id})))
      (db/delete-connection! conn-id)
      (when-let [db-name (:dbname existing)]
        (db/invalidate-ds-cache! conn-id db-name))
      (http-response/ok {:status "ok" :conn_id conn-id}))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn test-connection-by-id [request]
  (try
    (let [params (:params request)
          conn-id (parse-required-int (or (:conn_id params) (:conn-id params)) :conn_id)]
      (db/test-connection conn-id)
      (control-plane/record-connection-health! {:conn_id conn-id :status "ok"})
      (http-response/ok {:status "ok"
                         :conn_id conn-id
                         :health (control-plane/get-connection-health conn-id)}))
    (catch clojure.lang.ExceptionInfo e
      (let [conn-id (parse-optional-int (or (get-in request [:params :conn_id])
                                            (get-in request [:params :conn-id]))
                                        :conn_id)]
        (when conn-id
          (control-plane/record-connection-health!
           {:conn_id conn-id
            :status "error"
            :error_message (ex-message e)}))
        (http-response/bad-request {:error (ex-message e) :data (ex-data e)})))
    (catch Exception e
      (let [conn-id (parse-optional-int (or (get-in request [:params :conn_id])
                                            (get-in request [:params :conn-id]))
                                        :conn_id)]
        (when conn-id
          (control-plane/record-connection-health!
           {:conn_id conn-id
            :status "error"
            :error_message (.getMessage e)}))
        (http-response/ok {:status "error"
                           :conn_id (or (some-> request :params :conn_id)
                                        (some-> request :params :conn-id))
                           :error (.getMessage e)
                           :health (when conn-id (control-plane/get-connection-health conn-id))})))))

(defn save-db-connection [request]
  (try
    (let [params (:params request)
          conn-row (cond-> {:connection_name (:connection_name params)
                            :dbtype (:dbtype params)
                            :host (:host params)
                            :dbname (:dbname params)
                            :schema (:schema params)
                            :username (:username params)
                            :password (:password params)}
                     (some-> (:token params) str not-empty)
                     (assoc :token (:token params))
                     (some-> (:http_path params) str not-empty)
                     (assoc :http_path (:http_path params))
                     (some-> (:catalog params) str not-empty)
                     (assoc :catalog (:catalog params))
                     (some-> (:warehouse params) str not-empty)
                     (assoc :warehouse (:warehouse params))
                     (some-> (:role params) str not-empty)
                     (assoc :role (:role params))
                     (some-> (:port params) str not-empty)
                     (assoc :port (Integer/parseInt (str (:port params))))
                     (some-> (:sid params) str not-empty)
                     (assoc :sid (:sid params))
                     (some-> (:service params) str not-empty)
                     (assoc :service (:service params)))
          inserted (db/insert-data :connection conn-row)
          conn-id (:connection/id inserted)
          tree-data (try
                      (connection->tree-item {:id conn-id
                                              :connection_name (:connection_name params)
                                              :dbtype (:dbtype params)})
                      (catch Exception _
                        {:label (or (:connection_name params) "DB Connection")
                         :items []}))]
      (http-response/ok {"conn-id" conn-id
                         "tree-data" (assoc tree-data
                                            :conn_id conn-id
                                            :dbtype (:dbtype params))}))
    (catch NumberFormatException e
      (http-response/bad-request {:error (.getMessage e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn save-connector-connection [request]
  (try
    (let [params (:params request)
          connector-type (:connector_type params)
          conn-row (cond-> {:connection_name (:connection_name params)
                            :dbtype (or connector-type "connector")}
                     (some-> (:host params) str not-empty)
                     (assoc :host (:host params))
                     (some-> (:port params) str not-empty)
                     (assoc :port (Integer/parseInt (str (:port params))))
                     (some-> (:dbname params) str not-empty)
                     (assoc :dbname (:dbname params))
                     (some-> (:schema params) str not-empty)
                     (assoc :schema (:schema params))
                     (some-> (:username params) str not-empty)
                     (assoc :username (:username params))
                     (some-> (:password params) str not-empty)
                     (assoc :password (:password params))
                     (some-> (:token params) str not-empty)
                     (assoc :token (:token params))
                     ;; Store connector-specific config as JSON in the host field
                     (some-> (:bootstrap_servers params) str not-empty)
                     (assoc :host (:bootstrap_servers params))
                     (some-> (:base_path params) str not-empty)
                     (assoc :host (:base_path params))
                     (some-> (:access_key params) str not-empty)
                     (assoc :username (:access_key params))
                     (some-> (:secret_key params) str not-empty)
                     (assoc :password (:secret_key params)))
          inserted (db/insert-data :connection conn-row)
          conn-id (:connection/id inserted)
          nodetype (cond
                     (some? (#{"kafka_stream" "kafka_consumer"} connector-type)) "kafka-source"
                     (some? (#{"local_files" "remote_files" "mainframe_files"} connector-type)) "file-source"
                     :else "connector")]
      (http-response/ok {"conn-id" conn-id
                         "tree-data" {:label (or (:connection_name params) "Connection")
                                      :items []
                                      :nodetype nodetype}}))
    (catch NumberFormatException e
      (http-response/bad-request {:error (.getMessage e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn test-db-connection [request]
  (try
    (let [params (:params request)
          conn-row (cond-> {:connection_name (:connection_name params)
                            :dbtype (:dbtype params)
                            :host (:host params)
                            :dbname (:dbname params)
                            :schema (:schema params)
                            :username (:username params)
                            :password (:password params)}
                     (some-> (:token params) str not-empty)
                     (assoc :token (:token params))
                     (some-> (:http_path params) str not-empty)
                     (assoc :http_path (:http_path params))
                     (some-> (:catalog params) str not-empty)
                     (assoc :catalog (:catalog params))
                     (some-> (:warehouse params) str not-empty)
                     (assoc :warehouse (:warehouse params))
                     (some-> (:role params) str not-empty)
                     (assoc :role (:role params))
                     (some-> (:port params) str not-empty)
                     (assoc :port (Integer/parseInt (str (:port params))))
                     (some-> (:sid params) str not-empty)
                     (assoc :sid (:sid params))
                     (some-> (:service params) str not-empty)
                     (assoc :service (:service params)))
          inserted (db/insert-data :connection conn-row)
          conn-id (:connection/id inserted)]
      (db/test-connection conn-id)
      (control-plane/record-connection-health! {:conn_id conn-id :status "ok"})
      (http-response/ok {:status "ok"
                         :conn_id conn-id
                         :health (control-plane/get-connection-health conn-id)}))
    (catch NumberFormatException e
      (http-response/bad-request {:error (.getMessage e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn get-connection-health [request]
  (try
    (let [params (:params request)
          conn-id (parse-required-int (or (:conn_id params) (:conn-id params)) :conn_id)
          detail  (db/get-connection-detail conn-id)]
      (when-not detail
        (throw (ex-info "Connection not found" {:status 404 :conn_id conn-id})))
      (http-response/ok {:conn_id conn-id
                         :health (or (control-plane/get-connection-health conn-id)
                                     {:conn_id conn-id :status "unknown"})}))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

     ;; (layout/render request "graphbody.html" {:items  (g2/displayGraph graph) :gid gid  })))
      ;; (http-response/ok (mapCoordinates (db/getGraph gid)))))

(defn new-graph [request]
  (let [
	graphname (:graphname (:params request))
        _ (dbg (str "Params : " (:params request))) 
        _ (dbg (str "Graph name : " graphname)) 
	graph (g2/createGraph (:graphname (:params request)))
        gid (get-in graph [:a :id])
        ver (get-in graph [:a :v])
        _ (dbg (str "home gid : " gid))
        _ (dbg (str "home ver : " ver))
        _ (dbg (str "home graph : " graph))
        session (:session request)
        _ (dbg (str "Session Contents :" session ))
        _ (dbg "DISPLAYGRAPH")
        _ (dbg (g2/displayGraph graph))
        _ (dbg "DISPLAYGRAPH")
        ]
  (->
    ;; (layout/render request "graphbody.html" {:items  (g2/displayGraph graph) :gid gid  })
    ;; (http-response/ok [{"alias" "Output","y" 250,"x" 250,"btype" "O","parent" 0,"id" 1},{"alias" "sqldi","y" 250,"x" 150,"btype" "T","parent" 1,"id" 2}])
     (http-response/ok { "alias" "Output" "btype" "O" "x" "250" "y" "250" "id" 1 "parent" 0 })
     (assoc :session (assoc session :gid gid :ver ver)))))

(defn getHtml [item] (let [cat (first (str item))
                           _ (dbg (str "cat " cat))
                           _ (dbg (str "class " (class cat)))
                           subcat (subs (str item) 2 3) ]
                        (case cat 
                         \j "j.html"
                         \p "t.html"
                         \f "filter.html"
                         \c "calculated_column.html"
                         \i "i.html"
                         \o "t.html"
                         \u "q.html"
                         \t "t.html" )))

(defn getFunc [function] (case (str function))
                         "j" "j.html"
                         "p" "t.html"
                         "f" "f.html"
                         "i" "i.html"
                         "o" "t.html"
                         "u" "q.html"
                         "t" "t.html" )

(defn readItemFile [item] (graph/getTableFromDB item))

(defn getOutputData [id] (readItemFile id)) 

(defn getItemName[sitem] (string/replace (str sitem) #":" ""))

(defn getItemData [gid itemid btype] 
                        (let [id (Integer/parseInt itemid)
                              _ (dbg (str "btype : " btype)) 
                             ] 
			(case btype
                         ("J" "U" "P" "T" "O" "Fi" "Fu" "A" "S" "Tg") (assoc (g2/getData gid id) :id id)
                      ;;   "mapping" (assoc (g2/getMapping gid id) :id id)
                      ;;   "target" (assoc (g2/getTarget gid id) :id id)
                      ;;   "f" (graph/getFilterData id)
                      ;;   "c" (graph/getCalculatedColumn id)
                         "query" (graph/getQuery)
                        ;;  "o" (getOutputData id)
                         )))

(defn getFunctionHtml [funct] (case funct
				   "CONCAT" "CONCAT.html"
				   "CHARINDEX" "CHARINDEX.html"
				   "COALESCE" "COALESCE.html"
				   "NULLIF" "NULLIF.html"
				   "REPLACE" "COALESCE.html"
                                   "SUBSTRING" "SUBSTRING.html"
 				   "Column_Function.html" ))

(defn rename-keys [m]
  (into {} (map (fn [[k v]]
                  (cond
                    (= k :name) [:alias v]
                    (= k :tid) [:id v]
                    :else [k v])) m)))

(defn get-fx-from-btype[btype]
	(case btype
		("J" "association" "U") (partial g2/get-unijoin-item btype)
                "Mp" g2/get-mapping-item
                "Tg" g2/get-target-item
                "Ap" g2/get-api-item
                "Kf" g2/get-kafka-source-item
                "Fs" g2/get-file-source-item
                "Ep" g2/get-endpoint-item
                "Rb" g2/get-response-builder-item
                "Vd" g2/get-validator-item
                "Au" g2/get-auth-item
                "Dx" g2/get-dx-item
                "Rl" g2/get-rl-item
                "Cr" g2/get-cr-item
                "Lg" g2/get-lg-item
                "Cq" g2/get-cq-item
                "Ev" g2/get-ev-item
                "Ci" g2/get-ci-item
                "Sc" g2/get-sc-item
                "Wh" g2/get-wh-item
                "C"  g2/get-conditional-item
                "Fu" g2/get-logic-item
                g2/get-item))

(defn get-item [request]
  (try
    (let [ params (:params request)
           id-str (:id params)
           gid (:gid (:session request))
           _ (when (or (nil? id-str) (= "null" id-str) (= "" id-str))
               (throw (ex-info "Invalid item id" {:status 400})))
           _ (when (or (nil? gid) (= "" (str gid)))
               (throw (ex-info "No graph selected" {:status 400})))
           id (Integer. id-str)
           _ (dbg (str "id: " id))
           _ (dbg (str "gid: " gid))
           g (db/getGraph gid)
           _ (dbg-pp g)
           btype (g2/getBtype g id)
           fx (get-fx-from-btype btype)
           _ (dbg "ITEM------")
           _ (dbg fx)
           _ (dbg (class btype))
           _ (dbg id)
           _ (dbg (str "btype " btype))]
     (http-response/ok (apply fx [id g])))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e)}))
    (catch Exception e
      (when (backend-debug-logging-enabled?)
        (println "get-item error:" (.getMessage e))
        (.printStackTrace e))
      (http-response/internal-server-error {:error (str "Failed to get item: " (.getMessage e))}))))

 ;;(http-response/ok (apply fx [id (getItemData gid id btype) g]))))
 ;; (layout/render request (getHtml btype) (assoc (getItemData gid id btype) :gid gid) )))


(defn endpoint-json[endpoints]
  (json/generate-string
    (for [[path method] endpoints]
      {:endpoint path
       :method   (-> method name .toUpperCase)})
    {:pretty true}))

(defn get-endpoints[request]
      (let [
             url (:url (:params request))
             _ (dbg url)
           ] 
      (http-response/ok (endpoint-json (sc/list-endpoints-from-url url)))))

(defn get-endpoint-schema[request]
      (let [
             url (:url (:params request))
             _ (dbg url)
             specurl (:spec (:params request))
             _ (dbg specurl)
             method (:method (:params request))
             _ (dbg method)
           ] 
           (http-response/ok (sc/endpoint-nodes-from-url specurl url (keyword (string/lower-case method))))))

(defn get-function [request] 
  (let [function (:function (:params request))]
  (layout/render request (getFunctionHtml function)  )))

(defn add-filter [request]
  (http-response/ok (addFilter request)))

(defn createWhere[join_count params] (loop [n 0 
                           		    retvec []]
                          		(if (< n join_count)
                              			(recur (inc n) (conj retvec  [((keyword (str "join" (str (+ 1 n)) "_1")) params) ((keyword (str "join" (str (+ 1 n)) "_2")) params) ])) retvec)))

(defn getJoinCount[params] 
    (reduce max (map #(Integer/parseInt %) (map #(first (clojure.string/split (subs % 5)  #"_")) (map str (filter #(clojure.string/starts-with? % ":join") (keys params)))))))

(defn save-aggregation [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          cp      (persist-graph! request (g2/update-agg g id params))]
      (http-response/ok (g2/get-item id cp)))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))


(defn save-union [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          cp      (persist-graph! request (g2/update-agg g id params))]
      (http-response/ok (g2/get-item id cp)))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn save-function [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          cp      (persist-graph! request (g2/update-projections g id params))]
      (http-response/ok (g2/get-item id cp)))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn save-mapping [request]
  (try
    (let [params  (:params request)
          g       (db/getGraph (:gid (:session request)))
          id      (Integer. (anil? (:id params) (first (first (filter (fn [[k,v]] (= "mapping" (get-in v [:na :btype]))) (:n g))))))
          cp      (persist-graph! request (g2/update-mapping g id (:mapping params)))]
      (http-response/ok (g2/get-mapping-item id cp)))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))


(defn save-target [request]
  (try
    (let [params (:params request)
          g      (db/getGraph (:gid (:session request)))
          id     (Integer. (str (:id params)))
          cp     (persist-graph! request (g2/save-target g id params))]
      (http-response/ok (g2/get-target-item id cp)))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn run-target [request]
  (let [params (:params request)
        g (db/getGraph (:gid params))
        tid (:tid params)]
       ;; (http-response/ok (g2/run-target g tid))))
        (http-response/ok (g2/get-target-item g tid))))

(defn save-column [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          cp      (persist-graph! request (g2/append-nodes-tcols g (conj [] id) (conj '() (g2/create-column params))))]
      (http-response/ok (g2/get-item id cp)))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))


(defn save-join [request]
  (try
    (let [params (:params request)
          id     (Integer. (:id params))
          g      (db/getGraph (:gid (:session request)))
          cp     (persist-graph! request (g2/save-join g id params))]
      (http-response/ok (g2/get-item id cp)))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn save-filter [request]
  (try
    (let [params (:params request)
          id     (Integer. (:id params))
          g      (db/getGraph (:gid (:session request)))
          cp     (persist-graph! request (g2/save-filter g id (set/rename-keys params {:having :where})))]
      (http-response/ok (g2/get-item id cp)))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))


(defn save-sorter [request]
  (try
    (let [params (:params request)
          id     (Integer. (:id params))
          g      (db/getGraph (:gid (:session request)))
          cp     (persist-graph! request (g2/save-sorter g id params))]
      (http-response/ok (g2/get-item id cp)))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn save-api [request]
  (try
    (let [params (:params request)
          id     (Integer. (:id params))
          g      (db/getGraph (:gid (:session request)))
          cp     (persist-graph! request (g2/save-api g id params))]
      (http-response/ok (mapCoordinates cp)))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn save-kafka-source [request]
  (try
    (let [params (:params request)
          id     (Integer. (:id params))
          g      (db/getGraph (:gid (:session request)))
          cp     (persist-graph! request (g2/save-kafka-source g id params))]
      (http-response/ok (g2/get-kafka-source-item id cp)))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn save-file-source [request]
  (try
    (let [params (:params request)
          id     (Integer. (:id params))
          g      (db/getGraph (:gid (:session request)))
          cp     (persist-graph! request (g2/save-file-source g id params))]
      (http-response/ok (g2/get-file-source-item id cp)))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn- parse-required-int
  [value field]
  (try
    (Integer/parseInt (str value))
    (catch Exception _
      (throw (ex-info (str "Invalid or missing " (name field))
                      {:field field :value value})))))

(defn- parse-optional-int
  [value field]
  (when (some? value)
    (parse-required-int value field)))

(defn- parse-optional-bool
  [value]
  (cond
    (nil? value) nil
    (instance? Boolean value) value
    :else
    (let [normalized (some-> value str string/trim string/lower-case)]
      (cond
        (#{"true" "1" "yes" "on"} normalized) true
        (#{"false" "0" "no" "off"} normalized) false
        :else (throw (ex-info "Invalid boolean value" {:value value}))))))

(defn- parse-source-kind
  [value]
  (let [normalized (some-> value str string/trim string/lower-case)]
    (case normalized
      "kafka" :kafka
      "file" :file
      (throw (ex-info "Invalid or missing source_kind"
                      {:field :source_kind
                       :value value})))))

(defn- rbac-enabled?
  []
  (let [raw (get env :bitool-rbac-enabled)]
    (if (nil? raw)
      false
      (contains? #{"1" "true" "yes" "on"}
                 (some-> raw str string/trim string/lower-case)))))

(defn- request-roles
  [request]
  (let [session-roles (or (get-in request [:session :roles])
                          (some-> (get-in request [:session :role]) vector))]
    (->> (if (sequential? session-roles) session-roles [session-roles])
         (map #(some-> % str string/trim string/lower-case))
         (remove string/blank?)
         set)))

(defn- bad-request
  ([message]
   (-> (http-response/bad-request (json/generate-string {:error message}))
       (response/content-type "application/json; charset=utf-8")))
  ([message data]
   (-> (http-response/bad-request (json/generate-string {:error message :data data}))
       (response/content-type "application/json; charset=utf-8"))))

(defn- with-bad-request-number-format
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch NumberFormatException e
        (bad-request "Invalid numeric parameter" {:message (.getMessage e)}))
      (catch IllegalArgumentException e
        (bad-request (.getMessage e))))))

(def ^:private allowed-legacy-save-functions
  {"save-conn" g2/save-conn})

(defn- safe-template-name
  [value]
  (let [template (some-> value str string/trim)]
    (when-not (and (seq template)
                   (re-matches #"[A-Za-z0-9_-]+" template))
      (throw (ex-info "Invalid template name"
                      {:status 400
                       :template value})))
    template))

(defn- local-host?
  [host]
  (let [normalized (some-> host str string/trim string/lower-case)]
    (contains? #{"localhost" "127.0.0.1" "::1" "0.0.0.0"} normalized)))

(defn- private-ip-literal?
  [host]
  (try
    (let [addr (.getAddress (InetAddress/getByName host))]
      (or (.isAnyLocalAddress addr)
          (.isLoopbackAddress addr)
          (.isLinkLocalAddress addr)
          (.isSiteLocalAddress addr)))
    (catch Exception _
      false)))

(defn- allowed-local-preview-uri?
  [^URI uri]
  (let [scheme (some-> (.getScheme uri) string/lower-case)
        host   (some-> (.getHost uri) string/trim string/lower-case)
        port   (let [explicit (.getPort uri)]
                 (if (neg? explicit)
                   (case scheme
                     "https" 443
                     "http" 80
                     explicit)
                   explicit))]
    (and (= "http" scheme)
         (#{"localhost" "127.0.0.1" "::1"} host)
         (= 3001 port))))

(defn- ensure-preview-base-url-safe!
  [raw-url]
  (let [uri    (URI. (or (some-> raw-url str string/trim) ""))
        scheme (some-> (.getScheme uri) string/lower-case)
        host   (.getHost uri)]
    (when-not (and (#{"http" "https"} scheme) (seq host))
      (throw (ex-info "Preview base_url must be an absolute http(s) URL"
                      {:status 400
                       :field :base_url})))
    (when (and (not (allowed-local-preview-uri? uri))
               (or (local-host? host)
                   (private-ip-literal? host)))
      (throw (ex-info "Preview base_url must not target local or private addresses"
                      {:status 400
                       :field :base_url
                       :host host})))
    (str uri)))

(defn- ensure-authorized!
  [request required-role]
  (when (rbac-enabled?)
    (let [required-role (some-> required-role name string/lower-case)
          admin-role    (or (some-> (get env :bitool-admin-role) str string/trim string/lower-case)
                            "admin")
          roles         (request-roles request)]
      (when-not (or (contains? roles admin-role)
                    (contains? roles required-role))
        (throw (ex-info "Forbidden"
                        {:status 403
                         :required_role required-role
                         :roles (vec roles)}))))))

(def ^:private workspace-role-rank
  {"viewer" 1
   "editor" 2
   "admin" 3})

(defn- workspace-role>=?
  [actual required]
  (>= (get workspace-role-rank (some-> actual str string/lower-case) 0)
      (get workspace-role-rank (some-> required str string/lower-case) 0)))

(defn- request-workspace-key
  [request]
  (or (some-> (get-in request [:params :workspace_key]) str string/trim not-empty)
      (some-> (get-in request [:session :workspace_key]) str string/trim not-empty)
      "default"))

(defn- session-user-profile
  [request]
  (let [username (some-> (get-in request [:session :user]) str string/trim not-empty)
        profile  (get-in request [:session :user_profile])]
    (when (and username
               (map? profile)
               (= username (:username profile)))
      profile)))

(defn- session-workspace-memberships
  [request]
  (let [username    (some-> (get-in request [:session :user]) str string/trim not-empty)
        memberships (get-in request [:session :workspace_memberships])]
    (when (and username
               (vector? memberships))
      memberships)))

(defn- ensure-workspace-access!
  [request required-role]
  (let [username       (some-> (get-in request [:session :user]) str string/trim not-empty)
        workspace-key  (request-workspace-key request)
        global-roles   (request-roles request)
        admin-role     (or (some-> (get env :bitool-admin-role) str string/trim string/lower-case)
                           "admin")
        session-workspace (some-> (get-in request [:session :workspace_key]) str string/trim not-empty)
        cached-role    (when (and username (= workspace-key session-workspace))
                         (some-> (get-in request [:session :workspace_role]) str string/lower-case))
        member         (when (and username (nil? cached-role))
                         (control-plane/workspace-member workspace-key username))
        member-role    (or cached-role (some-> (:role member) str string/lower-case))
        member-active? (if cached-role
                         true
                         (if (contains? member :active) (boolean (:active member)) true))]
    (when-not username
      (throw (ex-info "Authentication required"
                      {:status 401
                       :workspace_key workspace-key})))
    (when-not (or (contains? global-roles admin-role)
                  (contains? global-roles "api.ops")
                  (and member-active?
                       (workspace-role>=? member-role required-role)))
      (throw (ex-info "Workspace access denied"
                      {:status 403
                       :workspace_key workspace-key
                       :required_role required-role
                       :user username
                       :member_role member-role})))))

(defn- sanitize-workspace-member
  [member]
  (-> member
      (assoc :global_roles (or (:global_roles member)
                               (when-let [raw (:global_roles_json member)]
                                 (try
                                   (json/parse-string raw true)
                                   (catch Exception _
                                     [])))))
      (dissoc :global_roles_json)))

(defn- session-response-payload
  [request]
  (let [username      (some-> (get-in request [:session :user]) str string/trim not-empty)
        workspace-key (request-workspace-key request)
        user          (or (session-user-profile request)
                          (when username (control-plane/get-user username)))
        memberships   (or (session-workspace-memberships request)
                          (if username
                            (mapv sanitize-workspace-member (control-plane/user-workspace-memberships username))
                            []))
        current       (when username
                        (some #(when (= workspace-key (:workspace_key %)) %) memberships))
        roles         (->> (or (:global_roles user)
                               (get-in request [:session :roles])
                               [])
                           (map str)
                           distinct
                           vec)]
    {:authenticated (boolean username)
     :user (when username
             {:username username
              :display_name (or (:display_name user) username)
              :email (:email user)
              :global_roles roles})
     :workspace_key workspace-key
     :workspace_role (:role current)
     :memberships memberships}))

(defn- record-audit-event!
  [request event-type details]
  (try
    (control-plane/record-audit-event!
     {:event_type event-type
      :actor (or (get-in request [:session :user])
                 (get-in request [:params :updated_by])
                 "system")
      :graph_id (or (parse-optional-int (or (get-in request [:params :gid])
                                            (get-in request [:params :graph_id]))
                                        :gid)
                    (parse-optional-int (get-in request [:session :gid]) :gid))
      :node_id (or (parse-optional-int (or (get-in request [:params :id])
                                           (get-in request [:params :api_node_id]))
                                       :id)
                   nil)
      :details details})
    (catch Exception e
      (tel/log! {:level :warn
                 :msg "failed to persist audit event"
                 :error (.getMessage e)
                 :event_type event-type}))))

(defn- request-graph-save-context
  [request]
  (let [params (:params request)]
    {:expected-version (parse-optional-int (or (:expected_version params)
                                               (get-in request [:session :graph_expected_version]))
                                           :expected_version)
     :workspace-key (or (:workspace_key params)
                        (get-in request [:session :workspace_key]))
     :updated-by (or (:updated_by params)
                     (get-in request [:session :user])
                     "system")}))

(defn- persist-graph!
  [request graph]
  (let [context (request-graph-save-context request)
        saved   (control-plane/persist-graph! graph context)]
    (try
      (modeling-automation/refresh-proposals-for-graph-target! (get-in saved [:a :id])
                                                               {:updated_by (:updated-by context)})
      (catch Exception e
        (tel/log! {:level :warn
                   :msg "Failed to refresh model proposals after graph save"
                   :data {:graph_id (get-in saved [:a :id])
                          :error (ex-message e)}})))
    saved))

(defn- graph-save-error-response
  [e]
  ((if (= 409 (:status (ex-data e)))
     http-response/conflict
     http-response/bad-request)
   {:error (ex-message e) :field (:field (ex-data e)) :data (ex-data e)}))

(defn- queue-response
  [result]
  (-> ((if (:created? result)
         http-response/accepted
         http-response/ok)
       result)
      (assoc-in [:headers "Location"] (str "/executionRuns/" (:run_id result)))))

(defn run-api-ingestion [request]
  (try
    (ensure-authorized! request :api.execute)
    (let [params         (:params request)
          gid            (parse-required-int (or (:gid params) (:gid (:session request))) :gid)
          node-id        (parse-required-int (:id params) :id)
          endpoint-name  (:endpoint_name params)
          force-new?     (parse-optional-bool (:force_new params))
          _              (tel/log! {:level :error
                                    :msg "API ingestion enqueue requested"
                                    :data {:graph_id gid
                                           :api_node_id node-id
                                           :endpoint_name endpoint-name
                                           :force_new force-new?
                                           :trigger_type "manual"}})
          result         (ingest-execution/enqueue-api-request! gid node-id {:endpoint-name endpoint-name
                                                                             :force-new? force-new?
                                                                             :trigger-type "manual"})]
      (tel/log! {:level :error
                 :msg "API ingestion enqueued"
                 :data {:graph_id gid
                        :api_node_id node-id
                        :endpoint_name endpoint-name
                        :force_new force-new?
                        :request_id (:request_id result)
                        :run_id (:run_id result)
                        :created? (:created? result)
                        :request_status (:status result)}})
      (record-audit-event! request
                           "api.enqueue"
                           {:graph_id gid
                            :api_node_id node-id
                            :endpoint_name endpoint-name
                            :request_id (:request_id result)
                            :run_id (:run_id result)})
      (queue-response result))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn run-kafka-ingestion [request]
  (try
    (ensure-authorized! request :api.execute)
    (let [params        (:params request)
          gid           (parse-required-int (or (:gid params) (:gid (:session request))) :gid)
          node-id       (parse-required-int (:id params) :id)
          endpoint-name (:endpoint_name params)
          result        (ingest-execution/enqueue-kafka-request! gid node-id {:endpoint-name endpoint-name
                                                                              :trigger-type "manual"})]
      (record-audit-event! request
                           "kafka.enqueue"
                           {:graph_id gid
                            :kafka_node_id node-id
                            :endpoint_name endpoint-name
                            :request_id (:request_id result)
                            :run_id (:run_id result)})
      (queue-response result))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn run-file-ingestion [request]
  (try
    (ensure-authorized! request :api.execute)
    (let [params        (:params request)
          gid           (parse-required-int (or (:gid params) (:gid (:session request))) :gid)
          node-id       (parse-required-int (:id params) :id)
          endpoint-name (:endpoint_name params)
          result        (ingest-execution/enqueue-file-request! gid node-id {:endpoint-name endpoint-name
                                                                             :trigger-type "manual"})]
      (record-audit-event! request
                           "file.enqueue"
                           {:graph_id gid
                            :file_node_id node-id
                            :endpoint_name endpoint-name
                            :request_id (:request_id result)
                            :run_id (:run_id result)})
      (queue-response result))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn bronze-source-batches-route [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params      (:params request)
          graph-id    (parse-required-int (or (:gid params) (:graph_id params)) :graph_id)
          node-id     (parse-required-int (or (:source_node_id params) (:id params)) :source_node_id)
          source-kind (parse-source-kind (:source_kind params))]
      (http-response/ok
       (ingest-runtime/list-bronze-source-batches
        graph-id
        node-id
        {:source-kind source-kind
         :endpoint-name (:endpoint_name params)
         :run-id (:run_id params)
         :status (:status params)
         :active-only (parse-optional-bool (:active_only params))
         :replayable-only (parse-optional-bool (:replayable_only params))
         :archived-only (parse-optional-bool (:archived_only params))
         :limit (parse-optional-int (:limit params) :limit)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         429 http-response/too-many-requests
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn bronze-source-observability-summary-route [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params      (:params request)
          graph-id    (parse-required-int (or (:gid params) (:graph_id params)) :graph_id)
          node-id     (parse-required-int (or (:source_node_id params) (:id params)) :source_node_id)
          source-kind (parse-source-kind (:source_kind params))]
      (http-response/ok
       (ingest-runtime/bronze-source-observability-summary
        graph-id
        node-id
        {:source-kind source-kind
         :endpoint-name (:endpoint_name params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         429 http-response/too-many-requests
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn bronze-source-observability-alerts-route [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params      (:params request)
          graph-id    (parse-required-int (or (:gid params) (:graph_id params)) :graph_id)
          node-id     (parse-required-int (or (:source_node_id params) (:id params)) :source_node_id)
          source-kind (parse-source-kind (:source_kind params))]
      (http-response/ok
       (ingest-runtime/bronze-source-observability-alerts
        graph-id
        node-id
        {:source-kind source-kind
         :endpoint-name (:endpoint_name params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         429 http-response/too-many-requests
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn preview-copybook-schema-route [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)]
      (http-response/ok
       (ingest-runtime/preview-copybook-schema
        {:copybook (:copybook params)
         :encoding (:encoding params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn preview-api-schema-inference [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          api-node {:base_url (ensure-preview-base-url-safe! (:base_url params))
                    :auth_ref (:auth_ref params)}
          endpoint (:endpoint_config params)
          result (ingest-runtime/preview-endpoint-schema! api-node endpoint)]
      (http-response/ok result))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

;; ---------------------------------------------------------------------------
;; Embedded AI assistant routes (P1)
;; ---------------------------------------------------------------------------

(defn ai-explain-preview-schema [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)]
      (when-not (:endpoint_config params)
        (throw (ex-info "endpoint_config is required" {:status 400 :field :endpoint_config})))
      (when-not (:inferred_fields params)
        (throw (ex-info "inferred_fields is required" {:status 400 :field :inferred_fields})))
      (http-response/ok
       (ai-assistant/explain-preview-schema!
        {:endpoint_config (:endpoint_config params)
         :inferred_fields (:inferred_fields params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn ai-suggest-bronze-keys [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)]
      (when-not (:endpoint_config params)
        (throw (ex-info "endpoint_config is required" {:status 400 :field :endpoint_config})))
      (when-not (:inferred_fields params)
        (throw (ex-info "inferred_fields is required" {:status 400 :field :inferred_fields})))
      (http-response/ok
       (ai-assistant/suggest-bronze-keys!
        {:endpoint_config (:endpoint_config params)
         :inferred_fields (:inferred_fields params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn ai-explain-model-proposal [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (when-not (:proposal_json params)
        (throw (ex-info "proposal_json is required" {:status 400 :field :proposal_json})))
      (http-response/ok
       (ai-assistant/explain-model-proposal!
        {:proposal_id    proposal-id
         :proposal_json  (:proposal_json params)
         :compile_result (:compile_result params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn ai-explain-proposal-validation [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (when-not (:validation_result params)
        (throw (ex-info "validation_result is required" {:status 400 :field :validation_result})))
      (http-response/ok
       (ai-assistant/explain-proposal-validation!
        {:proposal_id       proposal-id
         :validation_result (:validation_result params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

;; ---------------------------------------------------------------------------
;; Embedded AI assistant routes (P2)
;; ---------------------------------------------------------------------------

(defn ai-suggest-silver-transforms [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (when-not (:proposal params)
        (throw (ex-info "proposal is required" {:status 400 :field :proposal})))
      (http-response/ok
       (ai-assistant/suggest-silver-transforms!
        {:proposal_id proposal-id
         :proposal    (:proposal params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn ai-generate-silver-from-brd [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)]
      (when-not (:brd_text params)
        (throw (ex-info "brd_text is required" {:status 400 :field :brd_text})))
      (when-not (:source_columns params)
        (throw (ex-info "source_columns is required" {:status 400 :field :source_columns})))
      (http-response/ok
       (ai-assistant/generate-silver-proposal-from-brd!
        {:brd_text        (:brd_text params)
         :source_columns  (:source_columns params)
         :endpoint_config (:endpoint_config params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn ai-generate-gold-from-brd [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)]
      (when-not (:brd_text params)
        (throw (ex-info "brd_text is required" {:status 400 :field :brd_text})))
      (when-not (:source_columns params)
        (throw (ex-info "source_columns is required" {:status 400 :field :source_columns})))
      (http-response/ok
       (ai-assistant/generate-gold-proposal-from-brd!
        {:brd_text           (:brd_text params)
         :source_columns     (:source_columns params)
         :silver_proposal_id (:silver_proposal_id params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn ai-suggest-gold-mart-design [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (when-not (:proposal params)
        (throw (ex-info "proposal is required" {:status 400 :field :proposal})))
      (http-response/ok
       (ai-assistant/suggest-gold-mart-design!
        {:proposal_id  proposal-id
         :proposal     (:proposal params)
         :source_table (:source_table params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn ai-explain-schema-drift [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          event-id (parse-required-int (:event_id params) :event_id)]
      (http-response/ok
       (ai-assistant/explain-schema-drift!
        {:event_id      event-id
         :workspace_key (:workspace_key params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn ai-suggest-drift-remediation [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          event-id (parse-required-int (:event_id params) :event_id)]
      (http-response/ok
       (ai-assistant/suggest-drift-remediation!
        {:event_id      event-id
         :workspace_key (:workspace_key params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn ai-explain-endpoint-business-shape [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)]
      (when-not (:endpoint_config params)
        (throw (ex-info "endpoint_config is required" {:status 400 :field :endpoint_config})))
      (when-not (:inferred_fields params)
        (throw (ex-info "inferred_fields is required" {:status 400 :field :inferred_fields})))
      (http-response/ok
       (ai-assistant/explain-endpoint-business-shape!
        {:endpoint_config (:endpoint_config params)
         :inferred_fields (:inferred_fields params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn ai-explain-target-strategy [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)]
      (when-not (:target_config params)
        (throw (ex-info "target_config is required" {:status 400 :field :target_config})))
      (http-response/ok
       (ai-assistant/explain-target-strategy!
        {:target_config (:target_config params)
         :proposal      (:proposal params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn ai-generate-metric-glossary [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (when-not (:proposal params)
        (throw (ex-info "proposal is required" {:status 400 :field :proposal})))
      (http-response/ok
       (ai-assistant/generate-metric-glossary!
        {:proposal_id proposal-id
         :proposal    (:proposal params)
         :brd_text    (:brd_text params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn ai-explain-run-or-kpi-anomaly [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (http-response/ok
       (ai-assistant/explain-run-or-kpi-anomaly!
        {:proposal_id   proposal-id
         :workspace_key (:workspace_key params)
         :kpi_delta     (:kpi_delta params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn propose-silver-schema [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          gid (parse-required-int (or (:gid params) (:gid (:session request))) :gid)
          node-id (parse-required-int (:id params) :id)
          result (modeling-automation/propose-silver-schema!
                  {:graph-id gid
                   :api-node-id node-id
                   :endpoint-name (:endpoint_name params)
                   :created-by (or (:created_by params)
                                   (get-in request [:session :user])
                                   "system")})]
      (http-response/ok result))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn list-silver-proposals [request]
  (try
    (ensure-authorized! request :api.audit)
    (let [params (:params request)]
      (http-response/ok
       (modeling-automation/list-silver-proposals
        {:graph-id (parse-optional-int (or (:gid params) (:graph_id params)) :gid)
         :status (:status params)
         :limit (parse-optional-int (:limit params) :limit)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn get-silver-proposal [request]
  (try
    (ensure-authorized! request :api.audit)
    (let [proposal-id (parse-required-int (or (get-in request [:path-params :proposal_id])
                                              (get-in request [:params :proposal_id]))
                                          :proposal_id)
          proposal    (modeling-automation/get-silver-proposal proposal-id)]
      (if proposal
        (http-response/ok proposal)
        (http-response/not-found {:error "Silver proposal not found" :proposal_id proposal-id})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn update-silver-proposal [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (http-response/ok
       (modeling-automation/update-silver-proposal!
        proposal-id
        {:proposal (:proposal params)
         :created_by (or (:created_by params)
                         (get-in request [:session :user])
                         "system")})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         429 http-response/too-many-requests
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn preview-target-data [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)
          limit       (parse-optional-int (:limit params) :limit)]
      (http-response/ok
       (modeling-automation/preview-target-data
        proposal-id
        (cond-> {} limit (assoc :limit limit)))))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn compile-silver-proposal [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (http-response/ok (modeling-automation/compile-silver-proposal! proposal-id)))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn synthesize-silver-graph [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (http-response/ok
       (modeling-automation/synthesize-silver-graph!
        proposal-id
        {:created_by (or (:created_by params)
                         (get-in request [:session :user])
                         "system")})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         429 http-response/too-many-requests
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn validate-silver-proposal [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (http-response/ok
       (modeling-automation/validate-silver-proposal!
        proposal-id
        {:sample_limit (parse-optional-int (:sample_limit params) :sample_limit)
         :created_by (or (:created_by params)
                         (get-in request [:session :user])
                         "system")})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn publish-silver-proposal [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (http-response/ok
       (modeling-automation/publish-silver-proposal!
        proposal-id
        {:sample_limit (parse-optional-int (:sample_limit params) :sample_limit)
         :created_by (or (:created_by params)
                         (get-in request [:session :user])
                         "system")})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         429 http-response/too-many-requests
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn execute-silver-release [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          release-id (parse-required-int (:release_id params) :release_id)]
      (http-response/ok
       (modeling-automation/execute-silver-release!
        release-id
        {:created_by (or (:created_by params)
                         (get-in request [:session :user])
                         "system")})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         429 http-response/too-many-requests
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn review-silver-proposal [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (http-response/ok
       (modeling-automation/review-silver-proposal!
        proposal-id
        {:review_state (:review_state params)
         :review_notes (:review_notes params)
         :reviewed_by (or (:reviewed_by params)
                          (get-in request [:session :user])
                          "system")})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn validate-silver-proposal-warehouse [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (http-response/ok
       (modeling-automation/validate-silver-proposal-warehouse!
        proposal-id
        {:created_by (or (:created_by params)
                         (get-in request [:session :user])
                         "system")})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn poll-silver-model-run [request]
  (try
    (ensure-authorized! request :api.audit)
    (let [params (:params request)
          model-run-id (parse-required-int (:model_run_id params) :model_run_id)]
      (http-response/ok
       (modeling-automation/poll-silver-model-run! model-run-id)))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn propose-gold-schema [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          silver-proposal-id (parse-required-int (:silver_proposal_id params) :silver_proposal_id)
          result (modeling-automation/propose-gold-schema!
                  {:silver_proposal_id silver-proposal-id
                   :created_by (or (:created_by params)
                                   (get-in request [:session :user])
                                   "system")})]
      (http-response/ok result))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn list-gold-proposals [request]
  (try
    (ensure-authorized! request :api.audit)
    (let [params (:params request)]
      (http-response/ok
       (modeling-automation/list-gold-proposals
        {:graph-id (parse-optional-int (or (:gid params) (:graph_id params)) :gid)
         :status (:status params)
         :limit (parse-optional-int (:limit params) :limit)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn get-gold-proposal [request]
  (try
    (ensure-authorized! request :api.audit)
    (let [proposal-id (parse-required-int (or (get-in request [:path-params :proposal_id])
                                              (get-in request [:params :proposal_id]))
                                          :proposal_id)
          proposal    (modeling-automation/get-gold-proposal proposal-id)]
      (if proposal
        (http-response/ok proposal)
        (http-response/not-found {:error "Gold proposal not found" :proposal_id proposal-id})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn update-gold-proposal [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (http-response/ok
       (modeling-automation/update-gold-proposal!
        proposal-id
        {:proposal (:proposal params)
         :created_by (or (:created_by params)
                         (get-in request [:session :user])
                         "system")})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn compile-gold-proposal [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (http-response/ok (modeling-automation/compile-gold-proposal! proposal-id)))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn synthesize-gold-graph [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (http-response/ok
       (modeling-automation/synthesize-gold-graph!
        proposal-id
        {:created_by (or (:created_by params)
                         (get-in request [:session :user])
                         "system")})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn validate-gold-proposal [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (http-response/ok
       (modeling-automation/validate-gold-proposal!
        proposal-id
        {:sample_limit (parse-optional-int (:sample_limit params) :sample_limit)
         :created_by (or (:created_by params)
                         (get-in request [:session :user])
                         "system")})))
    (catch clojure.lang.ExceptionInfo e
      ((if (= 404 (:status (ex-data e)))
         http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn publish-gold-proposal [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (http-response/ok
       (modeling-automation/publish-gold-proposal!
        proposal-id
        {:sample_limit (parse-optional-int (:sample_limit params) :sample_limit)
         :created_by (or (:created_by params)
                         (get-in request [:session :user])
                         "system")})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn execute-gold-release [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          release-id (parse-required-int (:release_id params) :release_id)]
      (http-response/ok
       (modeling-automation/execute-gold-release!
        release-id
        {:created_by (or (:created_by params)
                         (get-in request [:session :user])
                         "system")})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         429 http-response/too-many-requests
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn review-gold-proposal [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (http-response/ok
       (modeling-automation/review-gold-proposal!
        proposal-id
        {:review_state (:review_state params)
         :review_notes (:review_notes params)
         :reviewed_by (or (:reviewed_by params)
                          (get-in request [:session :user])
                          "system")})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn validate-gold-proposal-warehouse [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          proposal-id (parse-required-int (:proposal_id params) :proposal_id)]
      (http-response/ok
       (modeling-automation/validate-gold-proposal-warehouse!
        proposal-id
        {:created_by (or (:created_by params)
                         (get-in request [:session :user])
                         "system")})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn poll-gold-model-run [request]
  (try
    (let [params (:params request)
          model-run-id (parse-required-int (:model_run_id params) :model_run_id)]
      (http-response/ok
       (modeling-automation/poll-gold-model-run! model-run-id)))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn rollback-api-batch [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          graph-id (parse-required-int (or (:gid params) (:graph_id params)) :graph_id)
          api-node-id (parse-required-int (:api_node_id params) :api_node_id)
          batch-id (:batch_id params)]
      (let [result (ingest-runtime/rollback-api-batch!
                    graph-id
                    api-node-id
                    batch-id
                    {:endpoint-name (:endpoint_name params)
                     :rollback-reason (:rollback_reason params)
                     :rolled-back-by (or (:rolled_back_by params)
                                         (get-in request [:session :user])
                                         "system")})]
        (record-audit-event! request
                             "api.rollback_batch"
                             {:graph_id graph-id
                              :api_node_id api-node-id
                              :batch_id batch-id
                              :endpoint_name (:endpoint_name params)})
        (http-response/ok result)))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn list-api-batches [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          graph-id (parse-required-int (or (:gid params) (:graph_id params)) :graph_id)
          api-node-id (parse-required-int (:api_node_id params) :api_node_id)]
      (http-response/ok
       (ingest-runtime/list-api-batches
        graph-id
        api-node-id
        {:endpoint-name (:endpoint_name params)
         :run-id (:run_id params)
         :status (:status params)
         :active-only (parse-optional-bool (:active_only params))
         :replayable-only (parse-optional-bool (:replayable_only params))
         :archived-only (parse-optional-bool (:archived_only params))
         :limit (parse-optional-int (:limit params) :limit)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn list-api-bad-records-route [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          graph-id (parse-required-int (or (:gid params) (:graph_id params)) :graph_id)
          api-node-id (parse-required-int (:api_node_id params) :api_node_id)]
      (http-response/ok
       (ingest-runtime/list-api-bad-records
        graph-id
        api-node-id
        {:endpoint-name (:endpoint_name params)
         :run-id (:run_id params)
         :batch-id (:batch_id params)
         :include-succeeded (parse-optional-bool (:include_succeeded params))
         :include-payloads (parse-optional-bool (:include_payloads params))
         :limit (parse-optional-int (:limit params) :limit)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn archive-api-batch [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          graph-id (parse-required-int (or (:gid params) (:graph_id params)) :graph_id)
          api-node-id (parse-required-int (:api_node_id params) :api_node_id)
          batch-id (:batch_id params)]
      (let [result (ingest-runtime/archive-api-batch!
                    graph-id
                    api-node-id
                    batch-id
                    {:endpoint-name (:endpoint_name params)
                     :archived-by (or (:archived_by params)
                                      (get-in request [:session :user])
                                      "system")})]
        (record-audit-event! request
                             "api.archive_batch"
                             {:graph_id graph-id
                              :api_node_id api-node-id
                              :batch_id batch-id
                              :endpoint_name (:endpoint_name params)})
        (http-response/ok result)))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn apply-api-retention [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          graph-id (parse-required-int (or (:gid params) (:graph_id params)) :graph_id)
          api-node-id (parse-required-int (:api_node_id params) :api_node_id)]
      (let [result (ingest-runtime/apply-api-retention!
                    graph-id
                    api-node-id
                    {:endpoint-name (:endpoint_name params)
                     :archive-days (parse-optional-int (:archive_days params) :archive_days)
                     :retention-days (parse-optional-int (:retention_days params) :retention_days)
                     :bad-record-payload-archive-days (parse-optional-int (:bad_record_payload_archive_days params)
                                                                          :bad_record_payload_archive_days)
                     :bad-record-retention-days (parse-optional-int (:bad_record_retention_days params)
                                                                    :bad_record_retention_days)
                     :dry-run (parse-optional-bool (:dry_run params))
                     :limit (parse-optional-int (:limit params) :limit)
                     :archived-by (or (:archived_by params)
                                      (get-in request [:session :user])
                                      "system")})]
        (record-audit-event! request
                             "api.apply_retention"
                             {:graph_id graph-id
                              :api_node_id api-node-id
                              :endpoint_name (:endpoint_name params)
                              :archive_days (parse-optional-int (:archive_days params) :archive_days)
                              :retention_days (parse-optional-int (:retention_days params) :retention_days)
                              :bad_record_payload_archive_days (parse-optional-int (:bad_record_payload_archive_days params)
                                                                 :bad_record_payload_archive_days)
                              :bad_record_retention_days (parse-optional-int (:bad_record_retention_days params)
                                                                              :bad_record_retention_days)
                              :dry_run (parse-optional-bool (:dry_run params))
                              :result (select-keys result [:archived_count
                                                           :deleted_count
                                                           :bad_record_payload_archived_count
                                                           :bad_record_metadata_deleted_count])})
        (http-response/ok result)))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn replay-api-bad-records [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          graph-id (parse-required-int (or (:gid params) (:graph_id params)) :graph_id)
          api-node-id (parse-required-int (:api_node_id params) :api_node_id)
          result (ingest-runtime/replay-api-bad-records!
                  graph-id
                  api-node-id
                  {:endpoint-name (:endpoint_name params)
                   :batch-id (:batch_id params)
                   :source-run-id (:run_id params)
                   :limit (parse-optional-int (:limit params) :limit)
                   :include-succeeded? (parse-optional-bool (:include_succeeded params))
                   :replayed-by (or (:replayed_by params)
                                    (get-in request [:session :user])
                                    "system")})]
      (record-audit-event! request
                           "api.replay_bad_records"
                           {:graph_id graph-id
                            :api_node_id api-node-id
                            :endpoint_name (:endpoint_name params)
                            :batch_id (:batch_id params)
                            :source_run_id (:run_id params)
                            :replay_run_id (:run_id result)})
      (http-response/ok result))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn list-api-schema-approvals-route [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          graph-id (parse-required-int (or (:gid params) (:graph_id params)) :graph_id)
          api-node-id (parse-required-int (:api_node_id params) :api_node_id)]
      (http-response/ok
       (ingest-runtime/list-api-schema-approvals
        graph-id
        api-node-id
        {:endpoint-name (:endpoint_name params)
         :include-snapshots (parse-optional-bool (:include_snapshots params))
         :promoted-only (parse-optional-bool (:promoted_only params))
         :limit (parse-optional-int (:limit params) :limit)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn review-api-schema-route [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          graph-id (parse-required-int (or (:gid params) (:graph_id params)) :graph_id)
          api-node-id (parse-required-int (:api_node_id params) :api_node_id)
          result (ingest-runtime/review-api-schema!
                  graph-id
                  api-node-id
                  {:endpoint-name (:endpoint_name params)
                   :schema-hash (:schema_hash params)
                   :review-state (:review_state params)
                   :review-notes (:review_notes params)
                   :promote? (parse-optional-bool (:promote params))
                   :reviewed-by (or (:reviewed_by params)
                                    (get-in request [:session :user])
                                    "system")})]
      (record-audit-event! request
                           "api.review_schema"
                           {:graph_id graph-id
                            :api_node_id api-node-id
                            :endpoint_name (:endpoint_name params)
                            :schema_hash (:schema_hash result)
                            :review_state (:review_state result)
                            :promoted (:promoted result)})
      (http-response/ok result))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn promote-api-schema-route [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          graph-id (parse-required-int (or (:gid params) (:graph_id params)) :graph_id)
          api-node-id (parse-required-int (:api_node_id params) :api_node_id)
          result (ingest-runtime/promote-api-schema!
                  graph-id
                  api-node-id
                  {:endpoint-name (:endpoint_name params)
                   :schema-hash (:schema_hash params)
                   :review-notes (:review_notes params)
                   :reviewed-by (or (:reviewed_by params)
                                    (get-in request [:session :user])
                                    "system")})]
      (record-audit-event! request
                           "api.promote_schema"
                           {:graph_id graph-id
                            :api_node_id api-node-id
                            :endpoint_name (:endpoint_name params)
                            :schema_hash (:schema_hash result)})
      (http-response/ok result))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn verify-api-commit-closure-route [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          graph-id (parse-required-int (or (:gid params) (:graph_id params)) :graph_id)
          api-node-id (parse-required-int (:api_node_id params) :api_node_id)]
      (http-response/ok
       (ingest-runtime/verify-api-commit-closure
        graph-id
        api-node-id
        {:endpoint-name (:endpoint_name params)
         :limit (parse-optional-int (:limit params) :limit)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn reset-api-checkpoint-route [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          graph-id (parse-required-int (or (:gid params) (:graph_id params)) :graph_id)
          api-node-id (parse-required-int (:api_node_id params) :api_node_id)
          result (ingest-runtime/reset-api-checkpoint!
                  graph-id
                  api-node-id
                  {:endpoint-name (:endpoint_name params)
                   :reset-to-cursor (:reset_to_cursor params)
                   :reset-to-watermark (:reset_to_watermark params)
                   :reason (:reason params)
                   :requested-by (or (:requested_by params)
                                     (get-in request [:session :user])
                                     "system")})]
      (record-audit-event! request
                           "api.reset_checkpoint"
                           {:graph_id graph-id
                            :api_node_id api-node-id
                            :endpoint_name (:endpoint_name result)
                            :reason (:reason params)
                            :reset_to_cursor (:reset_to_cursor params)
                            :reset_to_watermark (:reset_to_watermark params)})
      (http-response/ok result))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn api-observability-summary-route [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          graph-id (parse-required-int (or (:gid params) (:graph_id params)) :graph_id)
          api-node-id (parse-required-int (:api_node_id params) :api_node_id)]
      (http-response/ok
       (ingest-runtime/api-observability-summary
        graph-id
        api-node-id
        {:endpoint-name (:endpoint_name params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn api-observability-alerts-route [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          graph-id (parse-required-int (or (:gid params) (:graph_id params)) :graph_id)
          api-node-id (parse-required-int (:api_node_id params) :api_node_id)]
      (http-response/ok
       (ingest-runtime/api-observability-alerts
        graph-id
        api-node-id
        {:endpoint-name (:endpoint_name params)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn record-api-bronze-proof-signoff [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          result (control-plane/record-api-bronze-signoff!
                  {:release_tag (:release_tag params)
                   :environment (:environment params)
                   :commit_sha (:commit_sha params)
                   :proof_summary_path (:proof_summary_path params)
                   :proof_results_path (:proof_results_path params)
                   :proof_log_path (:proof_log_path params)
                   :proof_status (:proof_status params)
                   :soak_iterations (parse-optional-int (:soak_iterations params) :soak_iterations)
                   :operator_name (or (:operator_name params)
                                      (get-in request [:session :user])
                                      "system")
                   :reviewer_name (:reviewer_name params)
                   :operator_notes (:operator_notes params)
                   :created_by (or (:created_by params)
                                   (get-in request [:session :user])
                                   "system")})]
      (record-audit-event! request
                           "api.proof_signoff"
                           {:release_tag (:release_tag params)
                            :environment (:environment params)
                            :proof_status (:proof_status params)})
      (http-response/ok result))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn list-api-bronze-proof-signoffs [request]
  (try
    (ensure-authorized! request :api.audit)
    (let [params (:params request)]
      (http-response/ok
       (control-plane/list-api-bronze-signoffs
        {:environment (:environment params)
         :limit (parse-optional-int (:limit params) :limit)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn run-scheduler-ingestion [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params        (:params request)
          gid           (parse-required-int (or (:gid params) (:gid (:session request))) :gid)
          scheduler-id  (parse-required-int (:id params) :id)
          result        (ingest-execution/enqueue-scheduler-request! gid scheduler-id {:trigger-type "manual"})]
      (queue-response result))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn list-execution-runs [request]
  (try
    (let [params (:params request)
          result (ingest-execution/list-execution-runs
                  {:graph-id (parse-optional-int (or (:gid params) (:graph_id params)) :gid)
                   :status (:status params)
                   :workspace-key (:workspace_key params)
                   :tenant-key (:tenant_key params)
                   :endpoint-name (:endpoint_name params)
                   :request-kind (:request_kind params)
                   :workload-class (:workload_class params)
                   :queue-partition (:queue_partition params)
                   :limit (parse-optional-int (:limit params) :limit)})]
      (http-response/ok result))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn get-execution-run [request]
  (try
    (let [run-id (or (get-in request [:path-params :run_id])
                     (get-in request [:params :run_id]))
          run    (ingest-execution/get-execution-run run-id)]
      (if run
        (http-response/ok run)
        (http-response/not-found {:error "Execution run not found"
                                  :run_id run-id})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch IllegalArgumentException e
      (http-response/bad-request {:error (.getMessage e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn replay-execution-run [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [run-id (or (get-in request [:path-params :run_id])
                     (get-in request [:params :run_id]))
          result (operations/replay-execution-run! run-id)]
      (record-audit-event! request
                           "execution.replay_run"
                           {:source_run_id run-id
                            :request_id (:request_id result)
                            :run_id (:run_id result)})
      (queue-response result))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         409 http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn list-workspaces [request]
  (try
    (ensure-authorized! request :api.audit)
    (let [workspaces (control-plane/list-workspaces)]
      (http-response/ok workspaces))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn list-users [request]
  (try
    (ensure-authorized! request :api.audit)
    (http-response/ok (mapv #(select-keys % [:username :display_name :email :active :global_roles])
                            (control-plane/list-users)))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn save-user [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          roles  (or (:global_roles params)
                     (:roles params)
                     [])
          user   (control-plane/upsert-user!
                  {:username (:username params)
                   :display_name (:display_name params)
                   :email (:email params)
                   :global_roles roles
                   :active (if (contains? params :active)
                             (parse-optional-bool (:active params))
                             true)})]
      (http-response/ok (select-keys user [:username :display_name :email :active :global_roles])))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn list-workspace-members [request]
  (try
    (ensure-workspace-access! request "viewer")
    (let [workspace-key (request-workspace-key request)]
      (http-response/ok
       {:workspace_key workspace-key
        :members (mapv sanitize-workspace-member
                       (control-plane/list-workspace-members workspace-key))}))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         401 http-response/unauthorized
         403 http-response/forbidden
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn save-workspace-member [request]
  (try
    (ensure-workspace-access! request "admin")
    (let [params (:params request)
          workspace-key (request-workspace-key request)
          member (control-plane/upsert-workspace-member!
                  {:workspace_key workspace-key
                   :username (:username params)
                   :role (:role params)
                   :active (if (contains? params :active)
                             (parse-optional-bool (:active params))
                             true)})]
      (http-response/ok {:workspace_key workspace-key
                         :member (sanitize-workspace-member member)}))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         401 http-response/unauthorized
         403 http-response/forbidden
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn control-plane-login [request]
  (try
    (let [params          (:params request)
          username        (some-> (:username params) str string/trim not-empty)
          requested-ws    (some-> (:workspace_key params) str string/trim not-empty)
          user            (control-plane/get-user username)
          global-roles    (set (:global_roles user))
          memberships     (if username
                            (mapv sanitize-workspace-member
                                  (control-plane/user-workspace-memberships username))
                            [])
          workspace-key   (or requested-ws
                              (some-> memberships first :workspace_key)
                              "default")
          current-member  (some #(when (= workspace-key (:workspace_key %)) %) memberships)
          privileged?     (boolean (some global-roles ["admin" "api.ops" "api.audit" "secrets.write"]))]
      (when-not username
        (throw (ex-info "username is required" {:status 400})))
      (when-not user
        (throw (ex-info "Unknown user" {:status 404 :username username})))
      (when-not (:active user)
        (throw (ex-info "User is inactive" {:status 403 :username username})))
      (when (and (nil? current-member) (not privileged?))
        (throw (ex-info "User is not assigned to the requested workspace"
                        {:status 403
                         :username username
                         :workspace_key workspace-key})))
      (let [session (assoc (:session request)
                           :user username
                           :user_profile (select-keys user [:username :display_name :email :global_roles])
                           :roles (vec global-roles)
                           :workspace_memberships memberships
                           :workspace_key workspace-key
                           :workspace_role (:role current-member))
            response-body {:ok true
                           :session (session-response-payload {:session session :params {}})}]
        (-> (http-response/ok response-body)
            (assoc :session session))))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         401 http-response/unauthorized
         403 http-response/forbidden
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn control-plane-logout [request]
  (-> (http-response/ok {:ok true})
      (assoc :session {})))

(defn control-plane-session [request]
  (http-response/ok (session-response-payload request)))

(defn list-tenants [request]
  (try
    (ensure-authorized! request :api.audit)
    (http-response/ok (control-plane/list-tenants))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn save-tenant [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          tenant (control-plane/upsert-tenant!
                  {:tenant_key (:tenant_key params)
                   :tenant_name (:tenant_name params)
                   :max_concurrent_requests (or (parse-optional-int (:max_concurrent_requests params) :max_concurrent_requests) 10)
                   :max_queued_requests (or (parse-optional-int (:max_queued_requests params) :max_queued_requests) 1000)
                   :weight (or (parse-optional-int (:weight params) :weight) 1)
                   :metering_enabled (if (contains? params :metering_enabled)
                                       (parse-optional-bool (:metering_enabled params))
                                       true)
                   :active (if (contains? params :active)
                             (parse-optional-bool (:active params))
                             true)})]
      (http-response/ok tenant))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn save-workspace [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          workspace (control-plane/upsert-workspace!
                     {:workspace_key (:workspace_key params)
                      :tenant_key (:tenant_key params)
                      :workspace_name (:workspace_name params)
                      :max_concurrent_requests (or (parse-optional-int (:max_concurrent_requests params) :max_concurrent_requests) 2)
                      :max_queued_requests (or (parse-optional-int (:max_queued_requests params) :max_queued_requests) 100)
                      :weight (or (parse-optional-int (:weight params) :weight) 1)
                      :active (if (contains? params :active)
                                (parse-optional-bool (:active params))
                                true)})]
      (http-response/ok workspace))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn save-managed-secret [request]
  (try
    (ensure-authorized! request :secrets.write)
    (let [params (:params request)
          result (control-plane/put-secret!
                  {:secret_ref (:secret_ref params)
                   :secret_value (:secret_value params)
                   :updated_by (or (:updated_by params)
                                   (get-in request [:session :user])
                                   "system")})]
      (record-audit-event! request
                           "secret.write"
                           {:secret_ref (:secret_ref params)})
      (http-response/ok result))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn list-audit-events [request]
  (try
    (ensure-authorized! request :api.audit)
    (let [params (:params request)]
      (http-response/ok
       (control-plane/list-audit-events
        {:event_type (:event_type params)
         :limit (parse-optional-int (:limit params) :limit)})))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         403 http-response/forbidden
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn assign-graph-workspace [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          graph-id (parse-required-int (or (:gid params) (:graph_id params)) :gid)
          workspace-key (:workspace_key params)
          result (control-plane/assign-graph-workspace! graph-id workspace-key (:updated_by params))]
      (http-response/ok result))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn save-graph-dependencies [request]
  (try
    (ensure-authorized! request :api.ops)
    (let [params (:params request)
          downstream-graph-id (parse-required-int (or (:gid params) (:downstream_graph_id params)) :gid)
          upstream-graph-ids  (or (:upstream_graph_ids params) [])
          freshness-window    (parse-optional-int (:freshness_window_seconds params) :freshness_window_seconds)
          deps                (mapv (fn [upstream-graph-id]
                                      {:upstream_graph_id (parse-required-int upstream-graph-id :upstream_graph_id)
                                       :freshness_window_seconds freshness-window})
                                    upstream-graph-ids)]
      (http-response/ok (control-plane/set-graph-dependencies! downstream-graph-id deps)))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn graph-lineage [request]
  (try
    (let [params (:params request)
          graph-id (parse-required-int (or (:gid params) (:graph_id params)) :gid)]
      (http-response/ok (operations/graph-lineage graph-id)))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn freshness-dashboard [request]
  (try
    (let [params (:params request)]
      (http-response/ok
       (operations/freshness-dashboard
        {:graph-id (parse-optional-int (or (:gid params) (:graph_id params)) :gid)
         :workspace-key (:workspace_key params)
         :tenant-key (:tenant_key params)
         :limit (parse-optional-int (:limit params) :limit)})))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn freshness-alerts [request]
  (try
    (let [params (:params request)]
      (http-response/ok
       (operations/freshness-alerts
        {:graph-id (parse-optional-int (or (:gid params) (:graph_id params)) :gid)
         :workspace-key (:workspace_key params)
         :tenant-key (:tenant_key params)
         :limit (parse-optional-int (:limit params) :limit)})))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn usage-dashboard [request]
  (try
    (let [params (:params request)]
      (http-response/ok
       (operations/usage-dashboard
        {:tenant-key (:tenant_key params)
         :workspace-key (:workspace_key params)
         :request-kind (:request_kind params)
         :workload-class (:workload_class params)
         :usage-date (:usage_date params)
         :limit (parse-optional-int (:limit params) :limit)})))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

;; ── Fleet Intelligence Reporting Dashboard ──────────────────────

(defn reporting-page [request]
  (layout/render request "reporting.html"))

(defn- rpt-q
  "Run a reporting SQL query, returning unqualified keyword maps."
  [sql-vec]
  (jdbc/execute! db/ds sql-vec {:builder-fn rs/as-unqualified-lower-maps}))

(defn- rpt-q1 [sql-vec]
  (jdbc/execute-one! db/ds sql-vec {:builder-fn rs/as-unqualified-lower-maps}))

(defn reporting-data [_request]
  (try
    (let [fuel-trend       (rpt-q
                             ["SELECT event_date::text as event_date,
                                      AVG(avg_fuel_economy_mpg) as avg_mpg,
                                      SUM(total_fuel_gallons) as total_fuel,
                                      SUM(total_idle_hours) as idle_hours,
                                      SUM(total_distance_miles) as distance
                               FROM gold_fuel_efficiency_daily
                               GROUP BY event_date ORDER BY event_date"])
          fuel-by-vehicle  (rpt-q
                             ["SELECT vehicle_id,
                                      SUM(total_fuel_gallons) as total_fuel,
                                      AVG(avg_fuel_economy_mpg) as avg_mpg,
                                      SUM(total_idle_hours) as idle_hours
                               FROM gold_fuel_efficiency_daily
                               GROUP BY vehicle_id ORDER BY total_fuel DESC LIMIT 10"])
          util-trend       (rpt-q
                             ["SELECT event_date::text as event_date,
                                      AVG(avg_speed_mph) as avg_speed,
                                      AVG(avg_fuel_percent) as avg_fuel_pct,
                                      SUM(record_count) as total_records
                               FROM gold_fleet_utilization_daily
                               GROUP BY event_date ORDER BY event_date"])
          safety-trend     (rpt-q
                             ["SELECT event_date::text as event_date,
                                      SUM(total_events) as total_events,
                                      SUM(major_or_severe_events) as major_events,
                                      SUM(coachable_events) as coachable
                               FROM gold_driver_safety_daily
                               GROUP BY event_date ORDER BY event_date"])
          safety-drivers   (rpt-q
                             ["SELECT driver_id,
                                      SUM(total_events) as total_events,
                                      SUM(major_or_severe_events) as major_events,
                                      SUM(coachable_events) as coachable
                               FROM gold_driver_safety_daily
                               GROUP BY driver_id ORDER BY total_events DESC LIMIT 15"])
          cold-trend       (rpt-q
                             ["SELECT event_date::text as event_date,
                                      AVG(avg_ambient_temperature_c) as avg_ambient,
                                      AVG(avg_probe_temperature_c) as avg_probe,
                                      SUM(high_temp_breach_count) as high_breaches,
                                      SUM(low_temp_breach_count) as low_breaches
                               FROM gold_cold_chain_compliance_daily
                               GROUP BY event_date ORDER BY event_date"])
          asset-trend      (rpt-q
                             ["SELECT event_date::text as event_date,
                                      SUM(dvir_count) as dvirs,
                                      SUM(open_issue_count) as open_issues,
                                      SUM(completed_issue_count) as completed
                               FROM gold_asset_health_daily
                               GROUP BY event_date ORDER BY event_date"])
          safety-scores    (rpt-q
                             ["SELECT \"sum_data_items_safetyScore\" as safety_score,
                                      \"sum_data_items_totalEvents\" as total_events
                               FROM gold_fleet_safety_scores_drivers"])
          alert-cnt        (rpt-q1
                             ["SELECT COUNT(*) as cnt FROM gold_alerts"])
          job-cnt          (rpt-q1
                             ["SELECT COUNT(*) as cnt FROM gold_dispatch_job"])
          hos-data         (rpt-q
                             ["SELECT event_date::text as event_date,
                                      SUM(\"sum_data_items_durationMs\") as total_duration_ms,
                                      COUNT(*) as violation_count
                               FROM gold_fleet_hos_violations
                               GROUP BY event_date ORDER BY event_date"])
          ifta-data        (rpt-q
                             ["SELECT event_date::text as event_date,
                                      SUM(\"sum_data_items_taxableGallons\") as taxable_gallons,
                                      SUM(\"sum_data_items_taxableMiles\") as taxable_miles,
                                      SUM(\"sum_data_items_totalGallons\") as total_gallons,
                                      SUM(\"sum_data_items_totalMiles\") as total_miles
                               FROM gold_fleet_ifta_summary
                               GROUP BY event_date ORDER BY event_date"])

          ;; ── Fleet Operations ──
          fuel-by-vehicle-daily (rpt-q
                            ["SELECT vehicle_id, event_date::text as event_date,
                                     total_fuel_gallons, total_distance_miles,
                                     avg_fuel_economy_mpg, total_idle_hours
                              FROM gold_fuel_efficiency_daily
                              ORDER BY event_date, vehicle_id"])
          util-by-vehicle  (rpt-q
                            ["SELECT vehicle_id,
                                     SUM(record_count) as total_records,
                                     AVG(avg_speed_mph) as avg_speed,
                                     AVG(avg_fuel_percent) as avg_fuel_pct,
                                     MAX(max_odometer_meters) as max_odometer
                              FROM gold_fleet_utilization_daily
                              GROUP BY vehicle_id ORDER BY vehicle_id"])
          util-by-vehicle-daily (rpt-q
                            ["SELECT vehicle_id, event_date::text as event_date,
                                     avg_speed_mph, avg_fuel_percent,
                                     max_odometer_meters, record_count
                              FROM gold_fleet_utilization_daily
                              ORDER BY event_date, vehicle_id"])
          dispatch-trend   (rpt-q
                            ["SELECT event_date::text as event_date,
                                     COUNT(DISTINCT job_id) as jobs,
                                     SUM(row_count) as total_rows
                              FROM gold_dispatch_job
                              GROUP BY event_date ORDER BY event_date"])
          route-trend      (rpt-q
                            ["SELECT event_date::text as event_date,
                                     COUNT(DISTINCT data_items_id) as routes,
                                     AVG(sum_data_items_stops_items_latitude) as avg_lat,
                                     AVG(sum_data_items_stops_items_longitude) as avg_lng
                              FROM gold_fleet_routes
                              GROUP BY event_date ORDER BY event_date"])
          vehicle-fleet    (rpt-q
                            ["SELECT data_items_id as vehicle_id,
                                     event_date::text as event_date,
                                     sum_data_items_year as year,
                                     row_count
                              FROM gold_fleet_vehicles
                              ORDER BY event_date"])

          ;; ── Fuel & Emissions ──
          emissions-trend  (rpt-q
                            ["SELECT event_date::text as event_date,
                                     SUM(\"sum_data_items_co2EmissionsKg\") as co2_kg,
                                     SUM(\"sum_data_items_distanceMiles\") as distance_mi,
                                     SUM(\"sum_data_items_engineHours\") as engine_hrs,
                                     SUM(\"sum_data_items_fuelConsumedGallons\") as fuel_gal,
                                     SUM(\"sum_data_items_fuelEconomyMpg\") as mpg,
                                     SUM(\"sum_data_items_idleFuelGallons\") as idle_fuel,
                                     SUM(\"sum_data_items_idleHours\") as idle_hrs
                              FROM gold_fleet_vehicles_fuel_energy
                              GROUP BY event_date ORDER BY event_date"])

          ;; ── Driver Safety detail ──
          safety-by-driver-daily (rpt-q
                            ["SELECT driver_id, event_date::text as event_date,
                                     total_events, major_or_severe_events, coachable_events
                              FROM gold_driver_safety_daily
                              ORDER BY event_date, driver_id"])

          ;; ── Cold Chain detail ──
          cold-by-sensor   (rpt-q
                            ["SELECT sensor_id,
                                     AVG(avg_ambient_temperature_c) as avg_ambient,
                                     AVG(avg_probe_temperature_c) as avg_probe,
                                     SUM(high_temp_breach_count) as high_breaches,
                                     SUM(low_temp_breach_count) as low_breaches
                              FROM gold_cold_chain_compliance_daily
                              GROUP BY sensor_id ORDER BY high_breaches DESC"])
          cold-by-sensor-daily (rpt-q
                            ["SELECT sensor_id, event_date::text as event_date,
                                     avg_ambient_temperature_c, avg_probe_temperature_c,
                                     high_temp_breach_count, low_temp_breach_count
                              FROM gold_cold_chain_compliance_daily
                              ORDER BY event_date, sensor_id"])

          ;; ── Asset / Maintenance detail ──
          asset-by-vehicle (rpt-q
                            ["SELECT vehicle_id,
                                     SUM(dvir_count) as dvirs,
                                     SUM(open_issue_count) as open_issues,
                                     SUM(completed_issue_count) as completed
                              FROM gold_asset_health_daily
                              GROUP BY vehicle_id ORDER BY open_issues DESC"])
          trailer-stats    (rpt-q
                            ["SELECT data_items_id as trailer_id,
                                     event_date::text as event_date,
                                     \"sum_data_items_engineHours\" as engine_hours,
                                     \"sum_data_items_odometerMeters\" as odometer_m,
                                     sum_data_items_gps_latitude as lat,
                                     sum_data_items_gps_longitude as lng
                              FROM gold_fleet_trailers_stats
                              ORDER BY event_date"])
          equipment-stats  (rpt-q
                            ["SELECT data_items_id as equip_id,
                                     event_date::text as event_date,
                                     \"sum_data_items_engineHours\" as engine_hours,
                                     \"sum_data_items_fuelPercent\" as fuel_pct
                              FROM gold_fleet_equipment_stats
                              ORDER BY event_date"])

          ;; ── Alerts detail ──
          alert-trend      (rpt-q
                            ["SELECT event_date::text as event_date,
                                     COUNT(DISTINCT data_items_id) as alerts,
                                     SUM(row_count) as total_rows
                              FROM gold_alerts
                              GROUP BY event_date ORDER BY event_date"])

          ;; ── HOS detail ──
          hos-by-driver    (rpt-q
                            ["SELECT data_items_id as driver_id,
                                     SUM(\"sum_data_items_durationMs\") as total_duration_ms,
                                     COUNT(*) as violations
                              FROM gold_fleet_hos_violations
                              GROUP BY data_items_id ORDER BY violations DESC"])

          ;; ── Sensor / Industrial ──
          door-trend       (rpt-q
                            ["SELECT event_date::text as event_date,
                                     COUNT(DISTINCT data_items_id) as sensors,
                                     SUM(row_count) as door_events
                              FROM gold_sensors_door
                              GROUP BY event_date ORDER BY event_date"])
          industrial-trend (rpt-q
                            ["SELECT event_date::text as event_date,
                                     COUNT(DISTINCT data_items_id) as assets,
                                     SUM(\"sum_data_items_dataPoints_items_value\") as total_value
                              FROM gold_industrial_data
                              GROUP BY event_date ORDER BY event_date"])]
      {:status 200
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string
               {:fuel_trend        fuel-trend
                :fuel_by_vehicle   fuel-by-vehicle
                :utilization_trend util-trend
                :safety_trend      safety-trend
                :safety_by_driver  safety-drivers
                :cold_chain_trend  cold-trend
                :asset_health_trend asset-trend
                :safety_scores     safety-scores
                :alert_count       (:cnt alert-cnt)
                :job_count         (:cnt job-cnt)
                :hos_violations    hos-data
                :ifta_summary      ifta-data
                :fuel_by_vehicle_daily fuel-by-vehicle-daily
                :util_by_vehicle    util-by-vehicle
                :util_by_vehicle_daily util-by-vehicle-daily
                :dispatch_trend     dispatch-trend
                :route_trend        route-trend
                :vehicle_fleet      vehicle-fleet
                :emissions_trend    emissions-trend
                :safety_by_driver_daily safety-by-driver-daily
                :cold_by_sensor     cold-by-sensor
                :cold_by_sensor_daily cold-by-sensor-daily
                :asset_by_vehicle   asset-by-vehicle
                :trailer_stats      trailer-stats
                :equipment_stats    equipment-stats
                :alert_trend        alert-trend
                :hos_by_driver      hos-by-driver
                :door_trend         door-trend
                :industrial_trend   industrial-trend})})
    (catch Exception e
      {:status 500
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string {:error (.getMessage e)})})))

;; ── ISL (Intelligent Structured Language) Pipeline ─────────────
;; NL → ISL (constrained tool-use) → Validate → Compile SQL → Execute
;; Prevents hallucination by never letting the LLM write raw SQL.

(def ^:private gold-table-registry
  "Schema registry for all 17 gold tables. Each entry maps table name
   to its columns with types. Columns needing double-quote wrapping
   in PostgreSQL are marked with :quoted? true."
  {"gold_fuel_efficiency_daily"
   [{:col "vehicle_id"          :type "text"}
    {:col "event_date"          :type "date"}
    {:col "total_fuel_gallons"  :type "float8"}
    {:col "total_distance_miles" :type "float8"}
    {:col "avg_fuel_economy_mpg" :type "float8"}
    {:col "total_idle_hours"    :type "float8"}]

   "gold_fleet_utilization_daily"
   [{:col "vehicle_id"          :type "text"}
    {:col "event_date"          :type "date"}
    {:col "avg_speed_mph"       :type "float8"}
    {:col "avg_fuel_percent"    :type "float8"}
    {:col "max_odometer_meters" :type "float8"}
    {:col "record_count"        :type "int8"}]

   "gold_driver_safety_daily"
   [{:col "driver_id"              :type "text"}
    {:col "event_date"             :type "date"}
    {:col "total_events"           :type "int8"}
    {:col "major_or_severe_events" :type "int8"}
    {:col "coachable_events"       :type "int8"}]

   "gold_cold_chain_compliance_daily"
   [{:col "sensor_id"                  :type "text"}
    {:col "event_date"                 :type "date"}
    {:col "avg_ambient_temperature_c"  :type "float8"}
    {:col "avg_probe_temperature_c"    :type "float8"}
    {:col "high_temp_breach_count"     :type "int8"}
    {:col "low_temp_breach_count"      :type "int8"}]

   "gold_asset_health_daily"
   [{:col "vehicle_id"             :type "text"}
    {:col "event_date"             :type "date"}
    {:col "dvir_count"             :type "int8"}
    {:col "open_issue_count"       :type "int8"}
    {:col "completed_issue_count"  :type "int8"}]

   "gold_fleet_vehicles"
   [{:col "data_items_id"         :type "text"}
    {:col "event_date"            :type "date"}
    {:col "sum_data_items_year"   :type "int8"}
    {:col "row_count"             :type "int8"}]

   "gold_fleet_vehicles_fuel_energy"
   [{:col "event_date"                              :type "date"}
    {:col "sum_data_items_co2EmissionsKg"            :type "float8" :quoted? true}
    {:col "sum_data_items_distanceMiles"              :type "float8" :quoted? true}
    {:col "sum_data_items_engineHours"                :type "float8" :quoted? true}
    {:col "sum_data_items_fuelConsumedGallons"        :type "float8" :quoted? true}
    {:col "sum_data_items_fuelEconomyMpg"             :type "float8" :quoted? true}
    {:col "sum_data_items_idleFuelGallons"            :type "float8" :quoted? true}
    {:col "sum_data_items_idleHours"                  :type "float8" :quoted? true}]

   "gold_dispatch_job"
   [{:col "job_id"       :type "text"}
    {:col "event_date"   :type "date"}
    {:col "row_count"    :type "int8"}]

   "gold_fleet_routes"
   [{:col "data_items_id"                          :type "text"}
    {:col "event_date"                             :type "date"}
    {:col "sum_data_items_stops_items_latitude"    :type "float8"}
    {:col "sum_data_items_stops_items_longitude"   :type "float8"}]

   "gold_fleet_safety_scores_drivers"
   [{:col "sum_data_items_safetyScore"  :type "int8" :quoted? true}
    {:col "sum_data_items_totalEvents"  :type "int8" :quoted? true}]

   "gold_alerts"
   [{:col "data_items_id" :type "text"}
    {:col "event_date"    :type "date"}
    {:col "row_count"     :type "int8"}]

   "gold_sensors_door"
   [{:col "data_items_id" :type "text"}
    {:col "event_date"    :type "date"}
    {:col "row_count"     :type "int8"}]

   "gold_fleet_trailers_stats"
   [{:col "data_items_id"                    :type "text"}
    {:col "event_date"                       :type "date"}
    {:col "sum_data_items_engineHours"       :type "int8" :quoted? true}
    {:col "sum_data_items_odometerMeters"    :type "int8" :quoted? true}
    {:col "sum_data_items_gps_latitude"      :type "float8"}
    {:col "sum_data_items_gps_longitude"     :type "float8"}]

   "gold_fleet_equipment_stats"
   [{:col "data_items_id"                  :type "text"}
    {:col "event_date"                     :type "date"}
    {:col "sum_data_items_engineHours"     :type "int8" :quoted? true}
    {:col "sum_data_items_fuelPercent"     :type "int8" :quoted? true}]

   "gold_fleet_hos_violations"
   [{:col "data_items_id"                 :type "text"}
    {:col "event_date"                    :type "date"}
    {:col "sum_data_items_durationMs"     :type "int8" :quoted? true}]

   "gold_fleet_ifta_summary"
   [{:col "event_date"                          :type "date"}
    {:col "sum_data_items_taxableGallons"       :type "float8" :quoted? true}
    {:col "sum_data_items_taxableMiles"          :type "float8" :quoted? true}
    {:col "sum_data_items_totalGallons"          :type "float8" :quoted? true}
    {:col "sum_data_items_totalMiles"            :type "float8" :quoted? true}]

   "gold_industrial_data"
   [{:col "data_items_id"                              :type "text"}
    {:col "event_date"                                 :type "date"}
    {:col "sum_data_items_dataPoints_items_value"      :type "float8" :quoted? true}]})

(def ^:private reporting-dbtypes
  #{"postgresql" "mysql" "oracle" "sqlserver" "db2" "snowflake" "databricks"})

(defn- reporting-registry
  [conn-id schema & {:keys [force-refresh table-filter]}]
  (if conn-id
    (db/get-schema-registry conn-id :schema schema :force-refresh force-refresh :table-filter table-filter)
    gold-table-registry))

(defn- reporting-joins
  [conn-id schema registry & {:keys [force-refresh]}]
  (if conn-id
    (let [allowed (set (keys registry))]
      (->> (db/discover-joins conn-id :schema schema :force-refresh force-refresh)
           (filter (fn [j]
                     (and (contains? allowed (:from_table j))
                          (contains? allowed (:to_table j)))))
           vec))
    []))

(defn- split-qualified-ident
  [value]
  (let [s (some-> value str)]
    (when (and s (string/includes? s "."))
      (let [[l r] (string/split s #"\." 2)]
        [l r]))))

(defn- normalize-column-ref
  [ref base-table]
  (if-let [[tbl col] (split-qualified-ident ref)]
    {:table tbl :column col :qualified? true}
    {:table base-table :column ref :qualified? false}))

(declare isl-quote-ident isl-quote-col)

(defn- column-sql
  [ref base-table]
  (let [{:keys [table column qualified?]} (normalize-column-ref ref base-table)]
    (if qualified?
      (str (isl-quote-ident table) "." (isl-quote-col column))
      (isl-quote-col column))))

(defn- select-column-alias
  [ref]
  (if-let [[tbl col] (split-qualified-ident ref)]
    (str (string/replace tbl #"[^A-Za-z0-9_]" "_")
         "__"
         (string/replace col #"[^A-Za-z0-9_]" "_"))
    ref))

(defn- join-edges-by-pair
  [joins]
  (reduce (fn [acc {:keys [from_table from_column to_table to_column]}]
            (-> acc
                (assoc [from_table to_table] {:left-table from_table
                                              :left-column from_column
                                              :right-table to_table
                                              :right-column to_column})
                (assoc [to_table from_table] {:left-table to_table
                                              :left-column to_column
                                              :right-table from_table
                                              :right-column from_column})))
          {}
          joins))

(defn- isl-quote-ident
  "Quote an SQL identifier when needed and escape embedded quotes."
  [ident]
  (let [ident-str (str ident)]
    (if (re-matches #"[A-Za-z_][A-Za-z0-9_]*" ident-str)
      ident-str
      (str "\"" (string/replace ident-str "\"" "\"\"") "\""))))

(defn- isl-quote-col
  [col-name]
  (isl-quote-ident col-name))

(defn- semantic-measure-names
  "Extract all measure names from a semantic model (calculated + restricted)."
  [sem-model]
  (when sem-model
    (let [calc  (or (:calculated_measures sem-model) [])
          restr (or (:restricted_measures sem-model) [])]
      (vec (sort (distinct (concat (map :name calc) (map :name restr))))))))

(defn- model-entity-tables
  "When a semantic model is present, build a mapping of entity physical table names
   and entity-level column documentation for the tool schema."
  [sem-model]
  (when-let [entities (seq (:entities sem-model))]
    (let [entity-map (into {} entities)]
      {:tables     (vec (sort (map (fn [[_ e]] (or (:table e) "")) entity-map)))
       :entity-map entity-map
       :col-doc    (string/join "; "
                     (map (fn [[ename entity]]
                            (str (or (:table entity) (clojure.core/name ename))
                                 " (" (clojure.core/name ename) " " (or (:kind entity) "entity") ") -> "
                                 (string/join ", " (map :name (:columns entity)))))
                          (sort-by key entity-map)))})))

(defn- semantic-hierarchy-names
  "Extract hierarchy names from a semantic model."
  [sem-model]
  (when sem-model
    (vec (sort (distinct (keep :name (or (:hierarchies sem-model) [])))))))

(defn- find-hierarchy
  "Look up a hierarchy by name in the semantic model. Returns nil if not found."
  [sem-model hname]
  (when (and sem-model hname)
    (some (fn [h] (when (= (:name h) hname) h))
          (or (:hierarchies sem-model) []))))

(defn- coerce-drill-level
  "Coerce a drill-down level to a positive integer. Accepts nil (full depth),
   integers, or numeric strings. Returns:
     {:ok true :level <int-or-nil>}   — valid (nil means full depth)
     {:ok false :reason <string>}      — invalid input
   Never throws."
  [raw]
  (cond
    (nil? raw)
    {:ok true :level nil}

    (integer? raw)
    (if (pos? raw)
      {:ok true :level (int raw)}
      {:ok false :reason (str "drill_down.level must be a positive integer, got " raw)})

    (and (number? raw) (== raw (long raw)))
    (let [n (long raw)]
      (if (pos? n)
        {:ok true :level (int n)}
        {:ok false :reason (str "drill_down.level must be a positive integer, got " raw)}))

    (string? raw)
    (let [trimmed (string/trim raw)]
      (if (string/blank? trimmed)
        {:ok true :level nil}
        (try
          (let [n (Long/parseLong trimmed)]
            (if (pos? n)
              {:ok true :level (int n)}
              {:ok false :reason (str "drill_down.level must be a positive integer, got " raw)}))
          (catch NumberFormatException _
            {:ok false :reason (str "drill_down.level must be an integer, got " (pr-str raw))}))))

    :else
    {:ok false :reason (str "drill_down.level must be an integer, got " (pr-str raw))}))

(defn- hierarchy-entity-table
  "Look up the physical table for a hierarchy's entity. Returns nil if not resolvable."
  [sem-model hname]
  (when-let [h (find-hierarchy sem-model hname)]
    (let [ent-key   (:entity h)
          entities  (or (:entities sem-model) {})
          entity    (or (get entities ent-key)
                        (get entities (keyword ent-key))
                        (get entities (name (or ent-key ""))))]
      (or (:table entity) ent-key))))

(defn- hierarchy-entity-name
  "Resolve the entity key for a hierarchy's entity field. Returns the entity
   name as a string when the hierarchy binds to a known entity, else nil."
  [sem-model hname]
  (when-let [h (find-hierarchy sem-model hname)]
    (let [ent-key  (:entity h)
          entities (or (:entities sem-model) {})]
      (cond
        (contains? entities ent-key)             (name ent-key)
        (contains? entities (keyword ent-key))   (name (keyword ent-key))
        (and (string? ent-key)
             (contains? entities ent-key))       ent-key
        :else                                    (some-> ent-key name)))))

(defn- hierarchy-level-columns
  "Return the first `level` column refs for a hierarchy (1-based).
   Clamps level to [1, total-levels]. If `base-table` is provided and the
   hierarchy's entity maps to a different physical table, columns are
   qualified with the hierarchy entity name (e.g. `drivers.region`) so the
   lazy-join pipeline can resolve the join. Returns nil if hierarchy not found.
   `level` must already be coerced to a positive integer or nil."
  ([sem-model hname level]
   (hierarchy-level-columns sem-model hname level nil))
  ([sem-model hname level base-table]
   (when-let [h (find-hierarchy sem-model hname)]
     (let [levels     (or (:levels h) [])
           total      (count levels)
           effective  (if (nil? level) total (min level total))
           effective  (max 1 effective)
           cols       (keep :column (take effective levels))
           hier-table (hierarchy-entity-table sem-model hname)
           ent-name   (hierarchy-entity-name sem-model hname)
           qualify?   (and base-table hier-table ent-name
                           (not= hier-table base-table))]
       (vec (if qualify?
              (map (fn [c] (str ent-name "." c)) cols)
              cols))))))

(defn- build-isl-tool
  ([registry] (build-isl-tool registry [] nil))
  ([registry joins] (build-isl-tool registry joins nil))
  ([registry joins sem-model]
  (let [;; Model-scoped ISL: use entity tables when semantic model is present
        model-info  (model-entity-tables sem-model)
        tables      (if model-info
                      ;; Union entity tables + registry tables (entity tables are primary)
                      (vec (sort (distinct (concat (:tables model-info) (keys registry)))))
                      (vec (sort (keys registry))))
        table-col-doc (if model-info
                        (:col-doc model-info)
                        (string/join "; "
                                     (map (fn [[tbl cols]]
                                            (str tbl " -> " (string/join ", " (map :col cols))))
                                          (sort-by key registry))))
        join-doc (when (and (seq joins) (not sem-model))
                   ;; When semantic model present, lazy associations replace manual join docs
                   (string/join "; "
                                (map (fn [{:keys [from_table from_column to_table to_column]}]
                                       (str from_table "." from_column " -> " to_table "." to_column))
                                     joins)))
        assoc-doc (when sem-model
                    (let [rels (or (:relationships sem-model) [])]
                      (when (seq rels)
                        (string/join "; "
                          (map (fn [r]
                                 (str (:from r) "." (:from_column r) " -> "
                                      (:to r) "." (:to_column r)))
                               rels)))))
        measure-names (semantic-measure-names sem-model)
        hierarchy-names (semantic-hierarchy-names sem-model)]
    (when (> (count tables) 50)
      (throw (ex-info "Schema too large for AI reporting. Narrow the connection schema first."
                      {:table-count (count tables)})))
    {:name "build_query"
     :description (str "Build a structured query against the connected database. "
                       "Output an ISL document - never raw SQL. "
                       "Only use columns that belong to the chosen table. "
                       "Available columns per table: " table-col-doc
                       (when join-doc
                         (str ". Available join relationships: " join-doc))
                       (when assoc-doc
                         (str ". Lazy associations (use entity.column for cross-entity refs, JOINs resolved automatically): "
                              assoc-doc))
                       (when (seq measure-names)
                         (str ". Semantic measures available: " (string/join ", " measure-names)
                              ". Use the 'measures' array to reference these by name instead of "
                              "building raw aggregate expressions.")))
     :input_schema
     (cond->
      {:type "object"
       :required ["table" "intent"]
       :properties
       {"table"
        {:type "string"
         :enum tables
         :description (if model-info
                        "Table to query (mapped from semantic model entities)"
                        "Table to query")}

        "columns"
        {:type "array"
         :items {:type "string"}
         :description (if sem-model
                        "Columns to SELECT. Use entity.column syntax for cross-entity refs. Omit for all columns."
                        "Columns to SELECT. Must belong to the chosen table. Omit for all columns.")}

        "aggregates"
        {:type "array"
         :items {:type "object"
                 :required ["fn" "column"]
                 :properties {"fn" {:type "string" :enum ["SUM" "AVG" "MIN" "MAX" "COUNT"]}
                              "column" {:type "string"}
                              "alias" {:type "string"}}}
         :description "Aggregate functions to apply. Column must belong to the chosen table."}

        "filters"
        {:type "array"
         :items {:type "object"
                 :required ["column" "op" "value"]
                 :properties {"column" {:type "string"}
                              "op" {:type "string" :enum ["=" "!=" ">" "<" ">=" "<=" "LIKE" "IN" "BETWEEN"]}
                              "value" {:oneOf [{:type "string"}
                                               {:type "number"}
                                               {:type "array"
                                                :items {:oneOf [{:type "string"} {:type "number"}]}}]}}}
         :description "WHERE conditions (ANDed together)"}

        "group_by"
        {:type "array"
         :items {:type "string"}
         :description "GROUP BY columns"}

        "order_by"
        {:type "array"
         :items {:type "object"
                 :required ["column"]
                 :properties {"column" {:type "string"}
                              "direction" {:type "string" :enum ["ASC" "DESC"]}}}
         :description "ORDER BY clauses"}

        "date_trunc"
        {:type "string"
         :enum ["day" "week" "month" "quarter" "year"]
         :description "Truncate the selected date column to this granularity"}

        "date_column"
        {:type "string"
         :description "Date column to use for date_trunc. Must belong to the chosen table."}

        "join"
        {:type "array"
         :items {:type "object"
                 :required ["table" "on"]
                 :properties {"table" {:type "string" :enum tables}
                              "on" {:type "object"
                                    :description "Column mappings such as {\"orders.customer_id\":\"customers.id\"}"}
                              "type" {:type "string" :enum ["LEFT" "INNER" "RIGHT"]}}}
         :description (if sem-model
                        "Explicit table joins. Usually unnecessary — lazy associations resolve JOINs from entity.column refs automatically."
                        "Optional table joins using discovered foreign-key relationships. Use qualified columns when joining.")}

        "limit"
        {:type "integer" :minimum 1 :maximum 500
         :description "Max rows to return (default 100)"}

        "intent"
        {:type "string"
         :description "One-line description of what the user is asking for"}}}

      ;; Add measures property when semantic model provides named measures
      (seq measure-names)
      (assoc-in [:properties "measures"]
                {:type "array"
                 :items {:type "string" :enum measure-names}
                 :description (str "Named semantic measures to include in the query. "
                                   "Each is expanded into the correct SQL expression. "
                                   "Prefer these over raw aggregates when available.")})

      ;; Add drill_down property when semantic model provides named hierarchies
      (seq hierarchy-names)
      (assoc-in [:properties "drill_down"]
                {:type "object"
                 :required ["hierarchy"]
                 :properties {"hierarchy" {:type "string" :enum hierarchy-names
                                           :description "Named hierarchy to drill into"}
                              "level"     {:type "integer" :minimum 1
                                           :description (str "How many levels to expand (1 = top only). "
                                                             "Omit for full depth. "
                                                             "Columns are added to group_by automatically.")}}
                 :description (str "Drill into a named hierarchy instead of listing columns in group_by. "
                                   "Available hierarchies: " (string/join ", " hierarchy-names)
                                   ". Works whether the hierarchy is defined on the base table or a "
                                   "related entity — cross-entity drills are resolved through lazy "
                                   "associations automatically. "
                                   "Use this when the user asks to drill down, break down, or slice "
                                   "by a known hierarchy.")}))})))

(defn- build-context-docs
  "Format schema context entries into prompt-friendly documentation."
  [context-entries]
  (when (seq context-entries)
    (let [by-table (group-by :table_name context-entries)]
      (string/join "\n"
                   (for [[tbl entries] (sort-by key by-table)]
                     (let [table-desc  (first (filter #(nil? (:column_name %)) entries))
                           col-descs   (filter :column_name entries)]
                       (str "  " tbl
                            (when table-desc (str " — " (:description table-desc)))
                            (when (seq col-descs)
                              (str "\n"
                                   (string/join "\n"
                                                (map (fn [c]
                                                       (str "    " (:column_name c) ": " (:description c)
                                                            (when-let [sv (:sample_values c)]
                                                              (str " (e.g. " (string/join ", " (take 5 sv)) ")"))))
                                                     col-descs)))))))))))

(defn- build-isl-prompt
  ([registry] (build-isl-prompt registry [] nil))
  ([registry joins] (build-isl-prompt registry joins nil))
  ([registry joins schema-context]
  (let [table-docs (string/join "\n"
                                (map (fn [[tbl cols]]
                                       (str "  " tbl ": " (string/join ", " (map :col cols))))
                                     (sort-by key registry)))
        context-docs (build-context-docs schema-context)
        join-docs  (when (seq joins)
                     (string/join "\n"
                                  (map (fn [{:keys [from_table from_column to_table to_column]}]
                                         (str "  " from_table "." from_column " -> " to_table "." to_column))
                                       joins)))
        date-cols (->> (vals registry)
                       (mapcat identity)
                       (filter #(contains? #{"date" "timestamp"} (:type %)))
                       (map :col)
                       distinct
                       vec)]
    (str
     "You are an ISL (Intelligent Structured Language) translator.\n"
     "The user asks natural-language questions about their data.\n"
     "You MUST respond by calling the build_query tool with a valid ISL document.\n"
     "NEVER write SQL. NEVER respond with plain text. ALWAYS use the tool.\n\n"
     "IMPORTANT - Each table has specific columns. Only reference columns that belong to the chosen table:\n"
     table-docs "\n\n"
     (when context-docs
       (str "BUSINESS CONTEXT — Use this to understand what each table/column means:\n"
            context-docs "\n\n"))
     (when join-docs
       (str "Available join relationships (use qualified columns like orders.total):\n"
            join-docs "\n\n"))
     "Rules:\n"
     "- Use BETWEEN for date ranges.\n"
     "- Use date_trunc for trends and specify date_column if needed.\n"
     "- For top-N questions use order_by + limit.\n"
     "- For totals/averages use aggregates + group_by.\n"
     "- Only emit joins when they match one of the available join relationships.\n"
     "- Column and table identifiers may need quoting; the compiler will handle that.\n"
     (when (seq date-cols)
       (str "- Date-like columns detected: " (string/join ", " date-cols) "\n"))))))

(defn- build-isl-prompt-from-model
  "Build an ISL prompt enriched with Semantic Model intelligence.
   Uses entities, calculated/restricted measures, and hierarchies from the model
   to generate enum-locked tool options for the LLM."
  [model-json registry joins schema-context]
  (let [entities     (or (:entities model-json) {})
        calc-m       (or (:calculated_measures model-json) [])
        restr-m      (or (:restricted_measures model-json) [])
        hierarchies  (or (:hierarchies model-json) [])
        ;; Build table/column docs from model entities (richer than raw registry)
        entity-docs  (string/join "\n"
                       (for [[ename entity] (sort-by key entities)]
                         (let [cols (:columns entity)
                               col-names (map :name cols)]
                           (str "  " (name ename) " (" (or (:kind entity) "entity") ")"
                                " — table: " (or (:table entity) (name ename))
                                "\n    Columns: " (string/join ", " col-names)
                                (when (:description entity)
                                  (str "\n    " (:description entity)))))))
        ;; Build measure docs
        measure-docs (when (or (seq calc-m) (seq restr-m))
                       (str "SEMANTIC MEASURES (use these names in aggregates instead of raw expressions):\n"
                            (string/join "\n"
                              (concat
                               (map (fn [m]
                                      (str "  " (:name m) " [calculated, " (or (:aggregation m) "row")
                                           "] on " (:entity m) ": " (:expression m)
                                           (when (:description m) (str " — " (:description m)))))
                                    calc-m)
                               (map (fn [m]
                                      (str "  " (:name m) " [restricted] on " (:entity m)
                                           ": " (:base_measure m)
                                           " WHERE " (:filter_column m) " IN "
                                           (pr-str (:filter_values m))
                                           (when (:description m) (str " — " (:description m)))))
                                    restr-m)))))
        ;; Build hierarchy docs
        hier-docs    (when (seq hierarchies)
                       (str "DRILL HIERARCHIES (use the 'drill_down' param with hierarchy+level instead of listing columns in group_by):\n"
                            (string/join "\n"
                              (map (fn [h]
                                     (str "  " (:name h) " on " (:entity h) ": "
                                          (string/join " → " (map :column (:levels h)))
                                          " (levels: 1=" (:column (first (:levels h)))
                                          ", " (count (:levels h)) "=full)"))
                                   hierarchies))))
        ;; Build association docs — tell the LLM it can use qualified refs
        rels         (or (:relationships model-json) [])
        assoc-docs   (when (seq rels)
                       (str "LAZY ASSOCIATIONS (use entity.column syntax to reference columns from related entities — JOINs are added automatically):\n"
                            (string/join "\n"
                              (map (fn [r]
                                     (str "  " (:from r) "." (:from_column r) " → "
                                          (:to r) "." (:to_column r)
                                          " (" (or (:type r) "many_to_one") ", " (or (:join r) "LEFT") " JOIN)"))
                                   rels))))
        ;; Fall through to raw registry for join and context docs
        base-prompt  (build-isl-prompt registry joins schema-context)]
    (str base-prompt
         (when entity-docs
           (str "\nSEMANTIC MODEL ENTITIES:\n" entity-docs "\n\n"))
         (when assoc-docs
           (str "\n" assoc-docs "\n\n"))
         (when measure-docs
           (str "\n" measure-docs "\n\n"))
         (when hier-docs
           (str "\n" hier-docs "\n\n"))
         "When a semantic measure matches the user's request, prefer it over building raw aggregate expressions.\n"
         "When referencing columns from related entities, use entity.column syntax (e.g., drivers.region). JOINs will be resolved automatically from declared associations.\n"
         "When the user asks to drill down, break down, or slice by a known hierarchy, use 'drill_down' with hierarchy+level instead of listing columns in group_by. Drill hierarchies can live on the base table or on a related entity — cross-entity drills ride the same lazy-association pipeline, so no explicit join is needed.\n")))

(defn- parse-reporting-table-filter
  [params]
  (let [raw (or (:tables params) (:table_filter params) (:table-filter params))]
    (cond
      (nil? raw) nil
      (set? raw) (not-empty raw)
      (sequential? raw) (not-empty (set (keep #(some-> % str string/trim not-empty) raw)))
      (string? raw)
      (let [trimmed (string/trim raw)]
        (cond
          (string/blank? trimmed) nil
          (string/starts-with? trimmed "[")
          (try
            (->> (json/parse-string trimmed)
                 (keep #(some-> % str string/trim not-empty))
                 set
                 not-empty)
            (catch Exception _
              nil))
          :else
          (->> (string/split trimmed #",")
               (keep #(some-> % string/trim not-empty))
               set
               not-empty)))
      :else nil)))

(defn- history-key
  [conn-id table-filter]
  (let [scope-id (if (seq table-filter)
                   (hash (vec (sort table-filter)))
                   "all")]
    (keyword (str "reporting-history-" (or conn-id "default") "-" scope-id))))

(defn- append-history-context
  [base-prompt session conn-id table-filter]
  (let [history (get session (history-key conn-id table-filter))]
    (if (empty? history)
      base-prompt
      (str base-prompt "\n\n"
           "Recent queries on THIS connection/scope for context:\n"
           (string/join "\n"
                        (map-indexed
                         (fn [i h]
                           (str (inc i) ". "
                                (:intent (:isl h))
                                " -> table: " (get-in h [:isl :table])
                                (when-let [f (seq (get-in h [:isl :filters]))]
                                  (str " filters: " (pr-str f)))))
                         (reverse history)))
           "\n\nIf the user says 'now filter that', 'same but', or 'drill in', "
           "reuse the most recent query in this same scope and modify it.\n"))))

(defn- follow-up?
  [question]
  (boolean (re-find #"(?i)^(now |also |same |but |and |filter|add|remove|change|show me that|drill)" question)))

(defn- update-reporting-history
  [session conn-id table-filter isl-doc sql]
  (let [hkey (history-key conn-id table-filter)
        entry {:isl isl-doc
               :sql sql
               :intent (or (get isl-doc :intent) (get isl-doc "intent"))
               :conn-id conn-id
               :table-filter (some->> table-filter sort vec)
               :ts (System/currentTimeMillis)}]
    (update session hkey
            (fn [history]
              (->> (conj (vec (or history [])) entry)
                   (take-last 5)
                   vec)))))

(defn- nl->isl
  "Step 1: Call LLM with forced tool-use to produce an ISL document."
  [question system-prompt tool]
  (let [call-llm (requiring-resolve 'bitool.ai.llm/call-llm)
        result   (call-llm system-prompt question
                           :anthropic-model "claude-sonnet-4-6"
                           :openai-model "gpt-4.1"
                           :max-tokens 1024
                           :temperature 0
                           :tools [tool]
                           :tool-choice {:type "tool" :name (:name tool)})]
    (when-not (map? result)
      (throw (ex-info "LLM failed to produce ISL document" {:question question :result result})))
    result))

(defn- valid-column-ref?
  [registry allowed-tables base-table ref]
  (let [{:keys [table column]} (normalize-column-ref ref base-table)]
    (and (contains? allowed-tables table)
         (some #(= column (:col %)) (get registry table)))))

(defn- column-type
  [registry base-table ref]
  (let [{:keys [table column]} (normalize-column-ref ref base-table)]
    (some (fn [c] (when (= column (:col c)) (:type c)))
          (get registry table))))

(defn- validate-join-spec
  [join-spec registry base-table known-tables join-lookup]
  (let [errors     (atom [])
        join-table (or (:table join-spec) (get join-spec "table"))
        join-type  (string/upper-case (str (or (:type join-spec) (get join-spec "type") "LEFT")))
        on-map     (or (:on join-spec) (get join-spec "on"))
        on-entries (seq (cond
                          (map? on-map) on-map
                          :else nil))]
    (when-not (contains? registry join-table)
      (swap! errors conj (str "Join table '" join-table "' is not available in the selected schema")))
    (when-not (#{"LEFT" "INNER" "RIGHT"} join-type)
      (swap! errors conj (str "Join type '" join-type "' is not supported")))
    (when-not on-entries
      (swap! errors conj (str "Join '" join-table "' must provide an 'on' mapping")))
    (doseq [[left-ref right-ref] on-entries]
      (let [left-ref  (str left-ref)
            right-ref (str right-ref)
            left      (normalize-column-ref left-ref base-table)
            right     (normalize-column-ref right-ref base-table)
            left-edge (get join-lookup [(:table left) (:table right)])]
        (when-not (and (:qualified? left) (:qualified? right))
          (swap! errors conj (str "Join columns must be qualified: " left-ref " -> " right-ref)))
        (when-not (or (= (:table left) join-table) (= (:table right) join-table))
          (swap! errors conj (str "Join mapping must include joined table '" join-table "'")))
        (doseq [[side-name side-ref] [["left" left] ["right" right]]]
          (when-not (valid-column-ref? registry (conj known-tables join-table) base-table
                                       (str (:table side-ref) "." (:column side-ref)))
            (swap! errors conj (str "Unknown " side-name " join column '" (:table side-ref) "." (:column side-ref) "'"))))
        (when-not (or (contains? known-tables (:table left))
                      (contains? known-tables (:table right)))
          (swap! errors conj (str "Join '" join-table "' must connect from an already selected table")))
        (when-not (and left-edge
                       (= (:left-column left-edge) (:column left))
                       (= (:right-column left-edge) (:column right)))
          (swap! errors conj (str "Join mapping '" left-ref " -> " right-ref
                                  "' does not match a discovered relationship")))))
    {:valid (empty? @errors)
     :errors @errors}))

(defn- validate-isl
  "Step 2: Validate ISL document against the provided registry.
   Optional sem-model param enables validation of named semantic measures."
  ([isl] (validate-isl isl gold-table-registry))
  ([isl registry] (validate-isl isl registry []))
  ([isl registry joins-meta] (validate-isl isl registry joins-meta nil))
  ([isl registry joins-meta sem-model]
   (let [errors      (atom [])
         table       (get isl :table (get isl "table"))
         cols        (get isl :columns (get isl "columns"))
         aggs        (get isl :aggregates (get isl "aggregates"))
         measures    (vec (or (get isl :measures (get isl "measures")) []))
         group-by    (get isl :group_by (get isl "group_by"))
         filters     (get isl :filters (get isl "filters"))
         order-by    (get isl :order_by (get isl "order_by"))
         date-trunc  (get isl :date_trunc (get isl "date_trunc"))
         date-column (get isl :date_column (get isl "date_column"))
         join-specs  (vec (or (get isl :join (get isl "join")) []))
         tbl-cols    (get registry table)
         ;; Merge semantic model relationships into join lookup so that
         ;; lazy-join-injected specs pass the FK-matching validation check.
         ;; Model relationships use entity names as table refs (which map to
         ;; physical tables via the :table field on each entity).
         sem-join-entries (when sem-model
                           (let [entities (or (:entities sem-model) {})]
                             (->> (or (:relationships sem-model) [])
                                  (mapv (fn [r]
                                          (let [from-entity (get entities (keyword (:from r))
                                                                 (get entities (:from r)))
                                                to-entity   (get entities (keyword (:to r))
                                                                  (get entities (:to r)))]
                                            {:from_table  (or (:table from-entity) (:from r))
                                             :from_column (:from_column r)
                                             :to_table    (or (:table to-entity) (:to r))
                                             :to_column   (:to_column r)}))))))
         join-lookup (join-edges-by-pair (concat joins-meta (or sem-join-entries [])))
         known-measure-names (set (semantic-measure-names sem-model))]
     (when-not tbl-cols
       (swap! errors conj (str "Unknown table: " table)))
     (when (and date-trunc (or (nil? date-column)
                               (string/blank? (str date-column))))
       (swap! errors conj "date_column is required when date_trunc is specified"))
     ;; Validate named measures
     (when (seq measures)
       (if (empty? known-measure-names)
         (swap! errors conj "Named measures are not available (no semantic model)")
         (doseq [m measures]
           (when-not (contains? known-measure-names m)
             (swap! errors conj (str "Unknown semantic measure: '" m "'"))))))
     ;; Validate drill_down references a real hierarchy with a well-formed level.
     ;; Columns on the hierarchy's entity are validated against the registry.
     ;; Cross-entity drills resolve through the same lazy-join pipeline as
     ;; cross-entity group_by refs, so we do not require the hierarchy's entity
     ;; to match the base :table.
     (let [drill     (get isl :drill_down (get isl "drill_down"))
           hname     (when drill (or (get drill :hierarchy) (get drill "hierarchy")))
           raw-level (when drill (or (get drill :level) (get drill "level")))]
       (when drill
         (if-not sem-model
           (swap! errors conj "drill_down requires a semantic model")
           (let [hier   (find-hierarchy sem-model hname)
                 coerce (when hier (coerce-drill-level raw-level))]
             (cond
               (nil? hname)
               (swap! errors conj "drill_down.hierarchy is required")

               (nil? hier)
               (swap! errors conj (str "Unknown hierarchy: '" hname "'"))

               (not (:ok coerce))
               (swap! errors conj (:reason coerce))

               :else
               (let [hier-table (hierarchy-entity-table sem-model hname)
                     hier-cols  (hierarchy-level-columns sem-model hname (:level coerce))]
                 (cond
                   (empty? hier-cols)
                   (swap! errors conj (str "Hierarchy '" hname "' has no levels"))

                   :else
                   (doseq [hc hier-cols]
                     (let [{:keys [column]} (normalize-column-ref hc table)
                           resolve-table    (or hier-table table)]
                       (when-not (valid-column-ref? registry #{resolve-table}
                                                    resolve-table column)
                         (swap! errors conj
                                (str "Hierarchy level column '" column
                                     "' not found on table " resolve-table))))))))))))
     (let [joined-tables (atom #{table})]
       (doseq [join-spec join-specs]
         (let [{valid :valid join-errors :errors} (validate-join-spec join-spec registry table @joined-tables join-lookup)
               join-table (or (:table join-spec) (get join-spec "table"))]
           (when-not valid
             (swap! errors into join-errors))
           (swap! joined-tables conj join-table)))
       (let [allowed-tables @joined-tables]
         (when (and tbl-cols cols)
           (doseq [c cols]
             (when-not (valid-column-ref? registry allowed-tables table c)
               (swap! errors conj (str "Column '" c "' is not available for table " table)))))
         (when (and tbl-cols aggs)
           (doseq [a aggs]
             (let [ac (get a :column (get a "column"))]
               (when-not (valid-column-ref? registry allowed-tables table ac)
                 (swap! errors conj (str "Aggregate column '" ac "' is not available for table " table))))))
         (when (and tbl-cols filters)
           (doseq [f filters]
             (let [fc (get f :column (get f "column"))]
               (when-not (valid-column-ref? registry allowed-tables table fc)
                 (swap! errors conj (str "Filter column '" fc "' is not available for table " table))))))
         (when (and tbl-cols group-by)
           (doseq [g group-by]
             (when-not (valid-column-ref? registry allowed-tables table g)
               (swap! errors conj (str "Group-by column '" g "' is not available for table " table)))))
         (when (and tbl-cols order-by)
           (doseq [o order-by]
             (let [oc (get o :column (get o "column"))]
               (when-not (or (= oc "period")
                             (valid-column-ref? registry allowed-tables table oc))
                 (swap! errors conj (str "Order-by column '" oc "' is not available for table " table))))))
         (when (and tbl-cols date-column
                    (not (valid-column-ref? registry allowed-tables table date-column)))
           (swap! errors conj (str "Date column '" date-column "' is not available for table " table)))))
     (if (seq @errors)
       {:valid false :errors @errors}
       {:valid true :isl isl}))))

(defn- clamp-iso-date-string
  "Normalize impossible ISO dates like 2026-02-29 to the last valid day of the month.
   Leaves non-ISO strings unchanged."
  [value]
  (if-not (string? value)
    value
    (let [trimmed (string/trim value)]
      (if-let [[_ y m d] (re-matches #"(\d{4})-(\d{2})-(\d{2})" trimmed)]
        (try
          (let [year      (Long/parseLong y)
                month     (Long/parseLong m)
                day       (Long/parseLong d)
                ym        (YearMonth/of (int year) (int month))
                safe-day  (min (int day) (.lengthOfMonth ym))]
            (str (LocalDate/of (int year) (int month) safe-day)))
          (catch Exception _
            value))
        value))))

(defn- normalize-filter-value
  "Repair filter values before binding them. Currently clamps invalid ISO dates."
  [col-type value]
  (if (= col-type "date")
    (if (sequential? value)
      (mapv clamp-iso-date-string value)
      (clamp-iso-date-string value))
    value))

(defn- date-placeholder
  [dbtype col-type]
  (if (= col-type "date")
    (case (some-> dbtype string/lower-case)
      ("postgresql" "snowflake") "?::date"
      "CAST(? AS DATE)")
    "?"))

(defn- compile-isl
  "Step 3: Deterministic compilation of validated ISL → parameterized SQL.
   Optional sem-model param enables expansion of named semantic measures."
  ([isl] (compile-isl isl gold-table-registry nil []))
  ([isl registry] (compile-isl isl registry nil []))
  ([isl registry schema] (compile-isl isl registry schema []))
  ([isl registry schema joins-meta] (compile-isl isl registry schema joins-meta nil))
  ([isl registry schema joins-meta dbtype] (compile-isl isl registry schema joins-meta dbtype nil))
  ([isl registry schema joins-meta dbtype sem-model]
   (let [table          (get isl :table (get isl "table"))
         cols           (get isl :columns (get isl "columns"))
         aggs           (get isl :aggregates (get isl "aggregates"))
         measures       (vec (or (get isl :measures (get isl "measures")) []))
         filters        (get isl :filters (get isl "filters"))
         raw-group-by   (vec (or (get isl :group_by (get isl "group_by")) []))
         drill-down     (get isl :drill_down (get isl "drill_down"))
         drill-cols     (when (and drill-down sem-model)
                          (let [hname  (or (get drill-down :hierarchy)
                                           (get drill-down "hierarchy"))
                                raw    (or (get drill-down :level)
                                           (get drill-down "level"))
                                {:keys [ok level]} (coerce-drill-level raw)]
                            (when ok
                              (hierarchy-level-columns sem-model hname level table))))
         order-by       (get isl :order_by (get isl "order_by"))
         date-trunc     (get isl :date_trunc (get isl "date_trunc"))
         date-col       (get isl :date_column (get isl "date_column"))
         join-specs     (vec (or (get isl :join (get isl "join")) []))
         limit          (min 500 (max 1 (or (get isl :limit) (get isl "limit") 100)))
         params         (atom [])
         ;; Resolve semantic measures into SQL expressions
         measure-exprs  (when (and (seq measures) sem-model)
                          (let [calc   (or (:calculated_measures sem-model) [])
                                restr  (or (:restricted_measures sem-model) [])
                                ;; Validate and get sorted order for dependency resolution
                                entity-cols (some->> (:entities sem-model)
                                                     vals
                                                     (mapcat :columns))
                                sorted (some-> (sem-expr/validate-calculated-measures calc (vec entity-cols))
                                               :sorted)
                                {:keys [select-exprs]} (sem-expr/resolve-measures-for-query
                                                        (set measures) calc restr sorted)]
                            select-exprs))
         aggregate-mode? (boolean (or (seq aggs) date-trunc (seq raw-group-by)
                                      (seq measures) (seq drill-cols)))
         table-sql      (fn [table-name]
                          (str (when schema (str (isl-quote-ident schema) "."))
                               (isl-quote-ident table-name)))
         inferred-group-by
         (if (and aggregate-mode? (seq cols) (empty? raw-group-by) (empty? drill-cols))
           (->> cols
                (remove #(and date-trunc (= % date-col)))
                vec)
           [])
         group-by       (vec (distinct (concat raw-group-by drill-cols inferred-group-by)))
         grouped-cols   (set group-by)
         select-expr    (fn [ref]
                          (let [expr (column-sql ref table)]
                            (if (split-qualified-ident ref)
                              (str expr " AS " (isl-quote-ident (select-column-alias ref)))
                              expr)))
         selected-cols  (cond
                          (empty? cols) []

                          aggregate-mode?
                          (->> cols
                               (keep (fn [c]
                                       (cond
                                         (and date-trunc (= c date-col)) nil
                                         (grouped-cols c) (select-expr c)
                                         :else (str "SUM(" (column-sql c table) ") AS "
                                                    (isl-quote-ident (select-column-alias c))))))
                               vec)

                          :else
                          (mapv select-expr cols))
         aggregate-cols (mapv (fn [a]
                                (let [f  (get a :fn (get a "fn"))
                                      c  (get a :column (get a "column"))
                                      al (or (get a :alias) (get a "alias")
                                             (str (string/lower-case f) "_" c))]
                                  (str f "(" (column-sql c table) ") AS " (isl-quote-ident al))))
                              aggs)
         drill-select   (when (and (seq drill-cols) (empty? cols))
                          (mapv select-expr drill-cols))
         select-parts   (vec (concat (when date-trunc
                                       [(str "date_trunc('" date-trunc "', " (column-sql date-col table) ") AS period")])
                                     selected-cols
                                     drill-select
                                     aggregate-cols
                                     measure-exprs
                                     (when (and (empty? cols) (empty? aggs) (not date-trunc)
                                                (empty? measures) (empty? drill-cols))
                                       ["*"])))
         select-str     (if (seq select-parts)
                          (string/join ", " select-parts)
                          "*")
         cast-ph        (fn [col-ref]
                          (let [t (column-type registry table col-ref)]
                            (date-placeholder dbtype t)))
         join-clauses   (mapv (fn [join-spec]
                                (let [join-table (or (:table join-spec) (get join-spec "table"))
                                      join-type  (string/upper-case (str (or (:type join-spec) (get join-spec "type") "LEFT")))
                                      on-map     (or (:on join-spec) (get join-spec "on"))
                                      conditions (->> on-map
                                                      (map (fn [[left-ref right-ref]]
                                                             (str (column-sql (str left-ref) table)
                                                                  " = "
                                                                  (column-sql (str right-ref) table))))
                                                      (string/join " AND "))]
                                  (str " " join-type " JOIN " (table-sql join-table) " ON " conditions)))
                              join-specs)
         where-clauses  (when (seq filters)
                          (mapv (fn [f]
                                  (let [raw-col (get f :column (get f "column"))
                                        col     (column-sql raw-col table)
                                        op      (get f :op (get f "op"))
                                        val     (normalize-filter-value (column-type registry table raw-col)
                                                                        (get f :value (get f "value")))
                                        ph      (cast-ph raw-col)]
                                    (cond
                                      (= op "BETWEEN")
                                      (let [[v1 v2] (if (sequential? val) val [val val])]
                                        (swap! params conj v1 v2)
                                        (str col " BETWEEN " ph " AND " ph))

                                      (= op "IN")
                                      (let [vs (if (sequential? val) val [val])
                                            placeholders (string/join ", " (repeat (count vs) ph))]
                                        (swap! params into vs)
                                        (str col " IN (" placeholders ")"))

                                      :else
                                      (do (swap! params conj val)
                                          (str col " " op " " ph)))))
                                filters))
         group-parts    (cond-> []
                          date-trunc (conj "period")
                          (seq group-by) (into (map #(column-sql % table) group-by)))
         order-parts    (cond-> []
                          (seq order-by)
                          (into (map (fn [o]
                                       (let [c (get o :column (get o "column"))
                                             order-col (if (and date-trunc (= c date-col)) "period" c)
                                             d (or (get o :direction) (get o "direction") "ASC")]
                                         (str (if (= order-col "period") "period" (column-sql order-col table))
                                              " " d)))
                                     order-by))
                          (and date-trunc (empty? order-by))
                          (conj "period ASC"))
         sql            (str "SELECT " select-str
                             " FROM " (table-sql table)
                             (apply str join-clauses)
                             (when (seq where-clauses)
                               (str " WHERE " (string/join " AND " where-clauses)))
                             (when (seq group-parts)
                               (str " GROUP BY " (string/join ", " group-parts)))
                             (when (seq order-parts)
                               (str " ORDER BY " (string/join ", " order-parts)))
                             " LIMIT " limit)]
     {:sql sql :params @params})))

(defn- reporting-schema-response
  [conn-id schema registry joins]
  (let [detail (when conn-id (db/get-connection-detail conn-id))
        tables (vec (sort (keys registry)))
        columns (into {}
                      (map (fn [[tbl cols]]
                             [tbl (mapv #(select-keys % [:col :type :quoted?]) cols)])
                           registry))
        column-count (reduce + 0 (map count (vals registry)))
        context-count (when conn-id
                        (count (db/get-schema-context conn-id :schema schema)))
        detected      (fingerprints/detect-sources tables)]
    {:mode (if conn-id "connection" "demo")
     :connection conn-id
     :connection_label (or (:connection_name detail) "Default demo")
     :dbtype (or (:dbtype detail) "postgresql")
     :schema schema
     :tables tables
     :columns columns
     :joins joins
     :table_count (count tables)
     :column_count column-count
     :trained (some-> context-count pos?)
     :context_count (or context-count 0)
     :detected_sources (mapv (fn [{:keys [source key confidence]}]
                               {:source source :key key :confidence confidence})
                             detected)}))

(defn- parse-isl-param
  [value]
  (cond
    (map? value) value
    (string? value)
    (try
      (json/parse-string value true)
      (catch Exception _
        nil))
    :else nil))

(defn- saved-report-response
  [row & {:keys [include-isl]}]
  (cond-> {:report_id (:report_id row)
           :conn_id (:conn_id row)
           :schema_name (:schema_name row)
           :name (:name row)
           :description (:description row)
           :scope_tables (:scope_tables row)
           :created_by (:created_by row)
           :created_at_utc (:created_at_utc row)
           :updated_at_utc (:updated_at_utc row)}
    include-isl (assoc :isl (:isl row))))

(defn- execute-reporting-isl
  ([conn-id detail schema table-filter isl-doc]
   (let [registry (reporting-registry conn-id schema :table-filter table-filter)
         joins    (reporting-joins conn-id schema registry)]
     (execute-reporting-isl conn-id detail schema table-filter isl-doc registry joins nil)))
  ([conn-id detail schema table-filter isl-doc registry joins]
   (execute-reporting-isl conn-id detail schema table-filter isl-doc registry joins nil))
  ([conn-id detail schema table-filter isl-doc registry joins sem-model]
   (execute-reporting-isl conn-id detail schema table-filter isl-doc registry joins sem-model nil))
  ([conn-id detail schema table-filter isl-doc registry joins sem-model session]
   ;; Phase 3: inject lazy joins BEFORE validation so that cross-entity
   ;; semantic refs (e.g. drivers.region) have their JOINs present when
   ;; validate-isl checks column refs against joined tables.
   (let [isl-doc (if sem-model
                   (try (sem-assoc/inject-lazy-joins isl-doc sem-model)
                        (catch clojure.lang.ExceptionInfo e
                          (throw (ex-info (str "Lazy join resolution failed: " (ex-message e))
                                          (merge {:status 400 :isl isl-doc}
                                                 (ex-data e))))))
                   isl-doc)
         ;; Phase 4: inject RLS dimension filters when sem-model + session present
         isl-doc (if (and sem-model session)
                   (let [model-row  (try (first (semantic-model/list-semantic-models
                                                  conn-id :schema schema :status "published"))
                                         (catch Exception _ nil))
                         model-id   (:model_id model-row)
                         policies   (when model-id
                                      (try (sem-gov/list-rls-policies model-id)
                                           (catch Exception _ [])))
                         rls-result (when (seq policies)
                                      (sem-gov/apply-rls-filters sem-model policies session))]
                     (cond
                       (nil? rls-result)         isl-doc
                       (:blocked rls-result)     (throw (ex-info "Access denied by row-level security policy"
                                                                 {:status 403
                                                                  :error "rls_blocked"
                                                                  :reasons (:reasons rls-result)}))
                       (seq (:filters rls-result)) (update isl-doc :filters
                                                           (fn [existing]
                                                             (vec (concat (or existing [])
                                                                          (:filters rls-result)))))
                       :else isl-doc))
                   isl-doc)
         validation (validate-isl isl-doc registry joins sem-model)]
     (when-not (:valid validation)
       (throw (ex-info "ISL validation failed"
                       {:status 400
                        :details (:errors validation)
                        :isl isl-doc})))
     (let [dbtype  (some-> (:dbtype detail) string/lower-case)
           {:keys [sql params]} (compile-isl isl-doc registry schema joins dbtype sem-model)
           ds      (if conn-id (db/get-ds conn-id false) db/ds)
           rows    (jdbc/execute! ds
                                  (into [sql] params)
                                  {:builder-fn rs/as-unqualified-lower-maps
                                   :timeout 30})
           columns (if (seq rows) (mapv name (keys (first rows))) [])]
       {:sql sql
        :params params
        :rows rows
        :columns columns
        :count (count rows)
        :isl isl-doc
        :joins joins
        :registry registry
        :connection conn-id
        :connection_label (or (:connection_name detail) "Default demo")
        :schema schema
        :scope_tables (some->> table-filter sort vec)}))))

(defn reporting-schema [request]
  (try
    (let [params        (:params request)
          conn-id       (parse-optional-int (or (:conn_id params) (:conn-id params)) :conn_id)
          detail        (when conn-id (db/get-connection-detail conn-id))
          dbtype        (some-> (:dbtype detail) string/lower-case)
          schema        (or (:schema params) (:schema detail))
          force-refresh (parse-optional-bool (or (:force_refresh params) (:force-refresh params)))
          table-filter  (parse-reporting-table-filter params)]
      (when (and conn-id (not (contains? reporting-dbtypes dbtype)))
        (throw (ex-info "Connection type is not yet supported for generic AI reporting"
                        {:status 400 :dbtype dbtype :conn_id conn-id})))
      (let [registry (reporting-registry conn-id schema
                                         :force-refresh force-refresh
                                         :table-filter table-filter)
            joins    (reporting-joins conn-id schema registry :force-refresh force-refresh)]
        (http-response/ok
         (reporting-schema-response conn-id schema registry joins))))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn reporting-ask [request]
  (let [params   (:params request)
        question (or (:question params) (:message params))]
    (if (clojure.string/blank? question)
      {:status 400
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string {:error "question is required"})}
      (try
        (let [conn-id       (parse-optional-int (or (:conn_id params) (:conn-id params)) :conn_id)
              detail        (when conn-id (db/get-connection-detail conn-id))
              dbtype        (some-> (:dbtype detail) string/lower-case)
              schema        (or (:schema params) (:schema detail))
              table-filter  (parse-reporting-table-filter params)
              session       (:session request)
              _             (when (and conn-id (not (contains? reporting-dbtypes dbtype)))
                              (throw (ex-info "Connection type is not yet supported for generic AI reporting"
                                              {:status 400 :dbtype dbtype :conn_id conn-id})))
              registry      (reporting-registry conn-id schema :table-filter table-filter)
              joins         (reporting-joins conn-id schema registry)
              schema-ctx    (when conn-id
                              (db/get-schema-context conn-id :schema schema :table-filter table-filter))
              ;; Use semantic model when a published model exists
              sem-models    (try (semantic-model/list-semantic-models
                                  conn-id :schema schema :status "published")
                                (catch Exception _ []))
              sem-model-raw (some-> (first sem-models)
                                    :model_id
                                    semantic-model/get-semantic-model
                                    :model)
              ;; Phase 3: apply perspective if specified
              perspective-name (:perspective params)
              sem-model     (if (and sem-model-raw (not (string/blank? perspective-name)))
                              (let [model-id (:model_id (first sem-models))
                                    persp    (sem-persp/get-perspective-by-name model-id perspective-name)]
                                (if persp
                                  (sem-persp/apply-perspective sem-model-raw (:spec persp))
                                  (throw (ex-info (str "Perspective not found: " perspective-name)
                                                  {:status 400 :perspective perspective-name}))))
                              sem-model-raw)
              tool          (build-isl-tool registry joins sem-model)
              base-prompt   (if sem-model
                              (build-isl-prompt-from-model sem-model registry joins schema-ctx)
                              (build-isl-prompt registry joins schema-ctx))
              prompt        (if (follow-up? question)
                              (append-history-context base-prompt session conn-id table-filter)
                              base-prompt)
              isl-doc-raw   (nl->isl question prompt tool)
              ;; Inject lazy joins before validation so cross-entity refs pass
              isl-doc       (if sem-model
                              (try (sem-assoc/inject-lazy-joins isl-doc-raw sem-model)
                                   (catch clojure.lang.ExceptionInfo e
                                     (throw (ex-info (str "Lazy join resolution failed: " (ex-message e))
                                                     (merge {:status 400 :isl isl-doc-raw}
                                                            (ex-data e))))))
                              isl-doc-raw)
              ;; Phase 4: inject RLS dimension filters
              isl-doc       (if (and sem-model session)
                              (let [model-id   (:model_id (first sem-models))
                                    policies   (when model-id
                                                 (try (sem-gov/list-rls-policies model-id)
                                                      (catch Exception _ [])))
                                    rls-result (when (seq policies)
                                                 (sem-gov/apply-rls-filters sem-model policies session))]
                                (cond
                                  (nil? rls-result)         isl-doc
                                  (:blocked rls-result)     (throw (ex-info "Access denied by row-level security policy"
                                                                            {:status 403
                                                                             :error "rls_blocked"
                                                                             :reasons (:reasons rls-result)}))
                                  (seq (:filters rls-result)) (update isl-doc :filters
                                                                      (fn [existing]
                                                                        (vec (concat (or existing [])
                                                                                     (:filters rls-result)))))
                                  :else isl-doc))
                              isl-doc)
              validation    (validate-isl isl-doc registry joins sem-model)]
          (if-not (:valid validation)
            {:status 400
             :headers {"Content-Type" "application/json"}
             :body (json/generate-string {:error   "ISL validation failed"
                                          :details (:errors validation)
                                          :isl     isl-doc})}
            (let [{:keys [sql rows columns count scope_tables]}
                  (execute-reporting-isl conn-id detail schema table-filter isl-doc registry joins sem-model session)
                  response-session (update-reporting-history session conn-id table-filter isl-doc sql)]
              {:status 200
               :headers {"Content-Type" "application/json"}
               :session response-session
               :body (json/generate-string
                      {:sql      sql
                       :columns  columns
                       :rows     rows
                       :count    count
                       :isl      isl-doc
                       :connection conn-id
                       :connection_label (or (:connection_name detail) "Default demo")
                       :schema schema
                       :scope_tables scope_tables
                       :follow_up_used (boolean (follow-up? question))
                       :pipeline "NL → ISL → Validate → SQL → Execute"})})))
        (catch clojure.lang.ExceptionInfo e
          (let [status (:status (ex-data e))]
            {:status (or status 400)
             :headers {"Content-Type" "application/json"}
             :body (json/generate-string
                    (merge {:error (ex-message e)}
                           (select-keys (ex-data e) [:details :isl :data])))}))
        (catch Exception e
          {:status 500
           :headers {"Content-Type" "application/json"}
           :body (json/generate-string {:error (.getMessage e)})})))))

(defn reporting-save [request]
  (try
    (let [params       (:params request)
          conn-id      (parse-optional-int (or (:conn_id params) (:conn-id params)) :conn_id)
          detail       (when conn-id (db/get-connection-detail conn-id))
          dbtype       (some-> (:dbtype detail) string/lower-case)
          schema       (or (:schema params) (:schema_name params) (:schema detail))
          table-filter (parse-reporting-table-filter params)
          name         (some-> (or (:name params) (:title params)) str string/trim)
          description  (some-> (or (:description params) (:summary params)) str string/trim)
          isl-doc      (parse-isl-param (or (:isl params) (:query params)))
          ;; Phase 4: persist semantic context so saved reports stay bound to the model
          model-id-param     (parse-optional-int (:semantic_model_id params) :semantic_model_id)
          perspective-param  (some-> (:perspective params) str string/trim not-empty)]
      (when (string/blank? name)
        (throw (ex-info "Report name is required" {:status 400})))
      (when-not (map? isl-doc)
        (throw (ex-info "A valid ISL document is required to save a report" {:status 400})))
      (when (and conn-id (not (contains? reporting-dbtypes dbtype)))
        (throw (ex-info "Connection type is not yet supported for generic AI reporting"
                        {:status 400 :dbtype dbtype :conn_id conn-id})))
      ;; Resolve semantic model for validation (same path as reporting-ask)
      (let [sem-models    (when conn-id
                            (try (semantic-model/list-semantic-models
                                   conn-id :schema schema :status "published")
                                 (catch Exception _ [])))
            sem-model-raw (some-> (first sem-models)
                                  :model_id
                                  semantic-model/get-semantic-model
                                  :model)
            resolved-model-id (or model-id-param (:model_id (first sem-models)))
            sem-model     (if (and sem-model-raw (not (string/blank? perspective-param)))
                            (let [persp (sem-persp/get-perspective-by-name resolved-model-id perspective-param)]
                              (if persp
                                (sem-persp/apply-perspective sem-model-raw (:spec persp))
                                sem-model-raw))
                            sem-model-raw)
            ;; Inject lazy joins so cross-entity refs validate
            isl-doc       (if sem-model
                            (try (sem-assoc/inject-lazy-joins isl-doc sem-model)
                                 (catch Exception _ isl-doc))
                            isl-doc)
            registry      (reporting-registry conn-id schema :table-filter table-filter)
            joins         (reporting-joins conn-id schema registry)
            validation    (validate-isl isl-doc registry joins sem-model)]
        (when-not (:valid validation)
          (throw (ex-info "ISL validation failed"
                          {:status 400
                           :details (:errors validation)
                           :isl isl-doc})))
        (let [saved (-> (db/save-report! {:conn_id conn-id
                                          :schema_name schema
                                          :name name
                                          :description description
                                          :scope_tables table-filter
                                          :isl isl-doc
                                          :created_by (get-in request [:session :user])
                                          :semantic_model_id resolved-model-id
                                          :perspective_name perspective-param})
                        :report_id
                        db/get-saved-report)]
          (http-response/ok {:report (saved-report-response saved :include-isl true)}))))
    (catch clojure.lang.ExceptionInfo e
      {:status (or (:status (ex-data e)) 400)
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string
              (merge {:error (ex-message e)}
                     (select-keys (ex-data e) [:details :isl :data])))} )
    (catch Exception e
      {:status 500
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string {:error (.getMessage e)})})))

(defn reporting-saved [request]
  (try
    (let [params  (:params request)
          conn-id (parse-optional-int (or (:conn_id params) (:conn-id params)) :conn_id)
          reports (db/list-saved-reports conn-id)]
      (http-response/ok {:reports (mapv saved-report-response reports)}))
    (catch clojure.lang.ExceptionInfo e
      {:status (or (:status (ex-data e)) 400)
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string {:error (ex-message e) :data (ex-data e)})})
    (catch Exception e
      {:status 500
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string {:error (.getMessage e)})})))

(defn reporting-run-saved [request]
  (try
    (let [params    (:params request)
          report-id (parse-required-int (or (:report_id params)
                                            (get-in request [:path-params :report_id]))
                                        :report_id)
          saved     (db/get-saved-report report-id)]
      (when-not saved
        (throw (ex-info "Saved report not found" {:status 404 :report_id report-id})))
      (let [conn-id      (:conn_id saved)
            detail       (when conn-id (db/get-connection-detail conn-id))
            schema       (or (:schema_name saved) (:schema detail))
            table-filter (some->> (:scope_tables saved) set not-empty)
            session      (:session request)
            ;; Phase 4: restore semantic context from saved report
            saved-model-id     (:semantic_model_id saved)
            saved-perspective  (:perspective_name saved)
            sem-model-raw (when saved-model-id
                            (some-> (semantic-model/get-semantic-model saved-model-id) :model))
            sem-model     (if (and sem-model-raw (not (string/blank? saved-perspective)))
                            (let [persp (sem-persp/get-perspective-by-name saved-model-id saved-perspective)]
                              (if persp
                                (sem-persp/apply-perspective sem-model-raw (:spec persp))
                                sem-model-raw))
                            sem-model-raw)
            registry     (reporting-registry conn-id schema :table-filter table-filter)
            joins        (reporting-joins conn-id schema registry)
            result       (execute-reporting-isl conn-id detail schema table-filter
                                                (:isl saved) registry joins sem-model session)]
        {:status 200
         :headers {"Content-Type" "application/json"}
         :body (json/generate-string
                (merge (select-keys result [:sql :columns :rows :count :isl :connection :connection_label :schema :scope_tables])
                       {:saved_report (saved-report-response saved)
                        :semantic_model_id saved-model-id
                        :perspective saved-perspective
                        :pipeline "Saved Report → Semantic Model → Validate → SQL → Execute"}))}))
    (catch clojure.lang.ExceptionInfo e
      (let [status (:status (ex-data e))]
        {:status (or status 400)
         :headers {"Content-Type" "application/json"}
         :body (json/generate-string
                (merge {:error (ex-message e)}
                       (select-keys (ex-data e) [:details :isl :data :report_id])))}))
    (catch Exception e
      {:status 500
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string {:error (.getMessage e)})})))

(defn reporting-delete-saved [request]
  (try
    (let [report-id (parse-required-int (or (get-in request [:path-params :report_id])
                                            (get-in request [:params :report_id]))
                                        :report_id)
          saved     (db/get-saved-report report-id)]
      (when-not saved
        (throw (ex-info "Saved report not found" {:status 404 :report_id report-id})))
      (db/delete-saved-report! report-id)
      (http-response/ok {:deleted true :report_id report-id}))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))


;; ── Schema Context / Training Endpoints ─────────────────────────────────

(defn reporting-context [request]
  (try
    (let [params   (:params request)
          conn-id  (parse-required-int (or (:conn_id params) (:conn-id params)) :conn_id)
          detail   (db/get-connection-detail conn-id)
          schema   (or (:schema params) (:schema detail))
          table-filter (parse-reporting-table-filter params)
          entries  (db/get-schema-context conn-id :schema schema :table-filter table-filter)
          ;; Group by table for easier consumption
          by-table (reduce (fn [acc e]
                             (let [tbl (:table_name e)]
                               (update acc tbl (fnil conj [])
                                       (select-keys e [:context_id :column_name :description :sample_values :source]))))
                           {}
                           entries)]
      (http-response/ok {:conn_id conn-id
                         :schema schema
                         :tables by-table
                         :entry_count (count entries)
                         :trained (pos? (count entries))}))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn reporting-context-save [request]
  (try
    (let [params      (:params request)
          conn-id     (parse-required-int (or (:conn_id params) (:conn-id params)) :conn_id)
          detail      (db/get-connection-detail conn-id)
          schema      (or (:schema params) (:schema_name params) (:schema detail))
          table-name  (or (:table_name params) (:table params))
          column-name (or (:column_name params) (:column params))
          description (or (:description params) (:desc params))
          sample-vals (or (:sample_values params) (:samples params))
          source      (or (:source params) "manual")]
      (when (string/blank? table-name)
        (throw (ex-info "table_name is required" {:status 400})))
      (when (string/blank? description)
        (throw (ex-info "description is required" {:status 400})))
      (db/upsert-schema-context!
       {:conn_id     conn-id
        :schema_name schema
        :table_name  table-name
        :column_name (when-not (string/blank? column-name) column-name)
        :description description
        :sample_values (cond
                         (sequential? sample-vals) sample-vals
                         (string? sample-vals)     (try (json/parse-string sample-vals) (catch Exception _ nil))
                         :else nil)
        :source source})
      (http-response/ok {:saved true :table_name table-name :column_name column-name}))
    (catch clojure.lang.ExceptionInfo e
      {:status (or (:status (ex-data e)) 400)
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string {:error (ex-message e) :data (ex-data e)})})
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn reporting-context-delete [request]
  (try
    (let [context-id (parse-required-int (or (get-in request [:path-params :context_id])
                                              (get-in request [:params :context_id]))
                                          :context_id)]
      (db/delete-schema-context! context-id)
      (http-response/ok {:deleted true :context_id context-id}))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn reporting-auto-train [request]
  (try
    (let [params   (:params request)
          conn-id  (parse-required-int (or (:conn_id params) (:conn-id params)) :conn_id)
          detail   (db/get-connection-detail conn-id)
          schema   (or (:schema params) (:schema detail))
          table-filter (parse-reporting-table-filter params)
          registry (reporting-registry conn-id schema :table-filter table-filter)
          call-llm (requiring-resolve 'bitool.ai.llm/call-llm)
          trained  (atom [])
          errors   (atom [])
          ;; Detect known SaaS schemas first — curated beats LLM-guessed
          detected (fingerprints/detect-sources (keys registry))
          curated  (fingerprints/curated-descriptions-for-registry detected registry)]
      ;; For each table in scope, use curated description if available, else LLM
      (doseq [[tbl cols] (take 30 (sort-by key registry))]
        (try
          (if-let [cur (get curated tbl)]
            ;; ── Curated path: known SaaS table, no LLM call needed ──
            (do
              (db/upsert-schema-context!
               {:conn_id conn-id :schema_name schema :table_name tbl
                :column_name nil :description (:table-desc cur)
                :source "fingerprint"})
              (swap! trained conj {:table tbl :description (:table-desc cur) :source "fingerprint"})
              ;; Save curated column descriptions
              (doseq [[col-name desc] (:columns cur)]
                (when (some #(= col-name (:col %)) cols)
                  (db/upsert-schema-context!
                   {:conn_id conn-id :schema_name schema :table_name tbl
                    :column_name col-name :description desc
                    :source "fingerprint"}))))
            ;; ── LLM path: unknown table, sample + ask ──
            (let [sample-rows (db/sample-table-data conn-id tbl :schema schema :n 3)
                  col-names   (mapv :col cols)
                  sample-text (if (seq sample-rows)
                                (string/join "\n"
                                             (map (fn [row]
                                                    (string/join ", "
                                                                 (map (fn [c]
                                                                        (str c "=" (pr-str (get row (keyword c)))))
                                                                      col-names)))
                                                  (take 3 sample-rows)))
                                "(no sample data available)")
                  prompt      (str "You are a data documentation assistant.\n"
                                   "Given this database table and sample data, write a concise one-sentence description "
                                   "of what this table contains. Then for each column, write a brief description.\n\n"
                                   "Table: " tbl "\n"
                                   "Columns: " (string/join ", " col-names) "\n"
                                   "Sample rows:\n" sample-text "\n\n"
                                   "Respond ONLY as JSON with this exact structure:\n"
                                   "{\"table_description\": \"...\", \"columns\": {\"col_name\": \"description\", ...}}\n"
                                   "No markdown fences. Just raw JSON.")
                  result      (call-llm nil prompt
                                        :anthropic-model "claude-sonnet-4-6"
                                        :openai-model "gpt-4.1-mini"
                                        :max-tokens 512
                                        :temperature 0)
                  parsed      (try
                                (json/parse-string
                                 (if (string? result)
                                   result
                                   (json/generate-string result))
                                 true)
                                (catch Exception _ nil))]
              (when parsed
                ;; Save table description
                (when-let [td (:table_description parsed)]
                  (db/upsert-schema-context!
                   {:conn_id conn-id :schema_name schema :table_name tbl
                    :column_name nil :description td
                    :sample_values (when (seq sample-rows)
                                     (mapv #(into {} %) (take 3 sample-rows)))
                    :source "auto-train"})
                  (swap! trained conj {:table tbl :description td :source "auto-train"}))
                ;; Save column descriptions
                (doseq [[col-key desc] (:columns parsed)]
                  (let [col-name (name col-key)]
                    (when (and (some #(= col-name (:col %)) cols) (string? desc))
                      (db/upsert-schema-context!
                       {:conn_id conn-id :schema_name schema :table_name tbl
                        :column_name col-name :description desc
                        :source "auto-train"})))))))
          (catch Exception e
            (swap! errors conj {:table tbl :error (.getMessage e)}))))
      (http-response/ok {:trained @trained
                         :errors  @errors
                         :tables_processed (count registry)
                         :curated_count (count curated)
                         :detected_sources (mapv :source detected)
                         :conn_id conn-id
                         :schema  schema}))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn reporting-table-stats [request]
  (try
    (let [params     (:params request)
          conn-id    (parse-required-int (or (:conn_id params) (:conn-id params)) :conn_id)
          detail     (db/get-connection-detail conn-id)
          schema     (or (:schema params) (:schema detail))
          table-name (or (:table_name params) (:table params))]
      (when (string/blank? table-name)
        (throw (ex-info "table_name is required" {:status 400})))
      (http-response/ok (db/table-stats conn-id table-name :schema schema)))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn reporting-sample [request]
  (try
    (let [params     (:params request)
          conn-id    (parse-required-int (or (:conn_id params) (:conn-id params)) :conn_id)
          detail     (db/get-connection-detail conn-id)
          schema     (or (:schema params) (:schema detail))
          table-name (or (:table_name params) (:table params))
          n          (min 10 (max 1 (or (parse-optional-int (:n params) :n) 5)))]
      (when (string/blank? table-name)
        (throw (ex-info "table_name is required" {:status 400})))
      (let [rows (db/sample-table-data conn-id table-name :schema schema :n n)
            columns (if (seq rows) (mapv name (keys (first rows))) [])]
        (http-response/ok {:table_name table-name
                           :columns columns
                           :rows rows
                           :count (count rows)})))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn execution-demand [request]
  (try
    (http-response/ok (ingest-execution/execution-demand-snapshot))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/internal-server-error {:error (.getMessage e)}))))

(defn save-endpoint [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          gid     (:gid (:session request))
          g       (db/getGraph gid)
          updated (g2/save-endpoint g id params)
          cp      (persist-graph! request updated)
          ep-cfg  (g2/getData cp id)
          _       (endpoint/register-endpoint! gid id ep-cfg)
          rp      (g2/get-endpoint-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn save-response-builder [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-response-builder g id params)
          cp      (persist-graph! request updated)
          rp      (g2/get-response-builder-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn save-validator [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-validator g id params)
          cp      (persist-graph! request updated)
          rp      (g2/get-validator-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      ((if (= 409 (:status (ex-data e)))
         http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :field (:field (ex-data e)) :data (ex-data e)}))))

(defn save-sc [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-sc g id params)
          cp      (persist-graph! request updated)
          rp      (g2/get-sc-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      ((if (= 409 (:status (ex-data e)))
         http-response/conflict
         http-response/bad-request)
       {:error (ex-message e) :field (:field (ex-data e)) :data (ex-data e)}))))

(defn save-wh [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-wh g id params)
          cp      (persist-graph! request updated)
          rp      (g2/get-wh-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn save-conditional [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-conditional g id params)
          cp      (persist-graph! request updated)
          rp      (g2/get-conditional-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn save-logic [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-logic g id params)
          cp      (persist-graph! request updated)
          rp      (g2/get-logic-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))
    (catch Exception e
      (http-response/bad-request {:error (.getMessage e)}))))

(defn save-auth [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-auth g id params)
          cp      (persist-graph! request updated)
          rp      (g2/get-auth-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn save-dx [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-dx g id params)
          cp      (persist-graph! request updated)
          rp      (g2/get-dx-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn save-rl [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-rl g id params)
          cp      (persist-graph! request updated)
          rp      (g2/get-rl-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn save-cr [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-cr g id params)
          cp      (persist-graph! request updated)
          rp      (g2/get-cr-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn save-lg [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-lg g id params)
          cp      (persist-graph! request updated)
          rp      (g2/get-lg-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn save-cq [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-cq g id params)
          cp      (persist-graph! request updated)
          rp      (g2/get-cq-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn save-ev [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-ev g id params)
          cp      (persist-graph! request updated)
          rp      (g2/get-ev-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn save-ci [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-ci g id params)
          cp      (persist-graph! request updated)
          rp      (g2/get-ci-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn test-endpoint [request]
  (let [params (:params request)
        gid    (:gid (:session request))
        id     (Integer. (:id params))
        g      (db/getGraph gid)
        ep-cfg (g2/getData g id)]
    (endpoint/register-endpoint! gid id ep-cfg)
    (endpoint/execute-graph gid id
      {:path  (:test_path_params params)
       :query (:test_query_params params)
       :body  (:test_body params)}
      request)))

(defn deploy-endpoints [request]
  (try
    (let [gid (:gid (:session request))]
      (if (or (nil? gid) (= "" (str gid)))
        (http-response/bad-request {:error "No graph selected. Please create or open a graph first."})
        (http-response/ok (endpoint/deploy-graph-endpoints! gid))))
    (catch Exception e
      (http-response/internal-server-error {:error (str "Failed to deploy endpoints: " (.getMessage e))}))))

(defn add-function [request]
  (let [item (:item (:params request)) ]
  (graph/addFunction item)
  (layout/render request (getHtml item) (getItemData item) )))

(defn add-grid [request] 
  (let [item (:item (:params request)) ]
  (layout/render request (getHtml item) (getItemData item) )))

(defn add-table [request]
  (http-response/ok (mapCoordinates (addTable request))))

(defn add-single [request]
  (let [gid (:gid (:session request))]
    (if (or (nil? gid) (= "" (str gid)))
      (http-response/bad-request {:error "No graph selected. Please create or open a graph first."})
      (try
        (http-response/ok (mapCoordinates (addSingle request)))
        (catch Exception e
          (when (backend-debug-logging-enabled?)
            (println "add-single error:" (.getMessage e))
            (.printStackTrace e))
          (http-response/internal-server-error {:error (str "Failed to add node: " (.getMessage e))}))))))

(defn move-single [request]
  (try
    (let [params (:params request)
          gid    (:gid (:session request))
          rect   (:rect params)
          id     (Integer. (:id rect))
          x      (:x rect)
          y      (:y rect)
          g      (db/getGraph gid)
          updated (-> g
                      (assoc-in [:n id :na :x] x)
                      (assoc-in [:n id :na :y] y))
          g2     (persist-graph! request updated)]
      (http-response/ok (mapCoordinates g2)))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))
    (catch Exception e
      (when (backend-debug-logging-enabled?)
        (println "move-single error:" (.getMessage e)))
      (http-response/internal-server-error {:error (str "Failed to move node: " (.getMessage e))}))))

(defn connect-single [request]
  (http-response/ok (mapCoordinates (connectSingle request))))

(defn delete-table [request]
  (http-response/ok (mapCoordinates (deleteTable request))))

(defn remove-node [request]
  (try
    (let [params (walk/keywordize-keys (:params request))
          id     (Integer. (:id params))
          gid    (:gid (:session request))
          g      (db/getGraph gid)
          btype  (:btype (g2/getData g id))
          _      (when (some #{btype} ["Ep" "Wh"])
                   (endpoint/unregister-endpoint! gid id))
          cp     (persist-graph! request (g2/remove-node g id))]
      (http-response/ok (mapCoordinates cp)))
    (catch clojure.lang.ExceptionInfo e
      (graph-save-error-response e))))

(defn- rebase-nodes
  "Shift all node IDs in imported-g upward by offset, remapping edge targets too.
   Node 1 (Output) in imported-g is dropped — it merges into the base graph's O node."
  [nodes offset]
  (into {}
        (for [[id nd] (dissoc nodes 1)
              :let [new-id (+ id offset)
                    new-edges (into {} (for [[eid ev] (:e nd)
                                             :let [new-eid (if (= eid 1) 1 (+ eid offset))]]
                                         [new-eid ev]))]]
          [new-id (assoc nd :e new-edges)])))

(defn import-open-api [request]
  (try
    (let [params     (walk/keywordize-keys (:params request))
          gid        (:gid (:session request))
          spec-json  (or (:spec params) "{}")
          graph-name (or (:graph_name params) "Imported API")
          spec       (json/parse-string spec-json true)
          imported-g (openapi/spec->graph spec graph-name)
          base-g     (try (db/getGraph gid) (catch Exception _ nil))
          ;; Use max existing ID (not count) to avoid collisions with sparse ID sets
          offset     (if base-g (apply max (keys (:n base-g))) 0)
          merged-g   (if base-g
                       (update base-g :n merge (rebase-nodes (:n imported-g) offset))
                       imported-g)
          cp         (persist-graph! request merged-g)
          new-gid    (get-in cp [:a :id])
          new-ver    (get-in cp [:a :v])
          session    (:session request)
          ;; Register all Ep/Wh nodes that came in from the import
          _          (doseq [[id nd] (:n cp)
                             :let [btype (get-in nd [:na :btype])]
                             :when (some #{btype} ["Ep" "Wh"])]
                       (endpoint/register-endpoint! new-gid id (get-in nd [:na])))]
      (->
        (http-response/ok (mapCoordinates cp))
        (assoc :session (assoc session :gid new-gid :ver new-ver))))
    (catch Exception e
      (http-response/bad-request {:error (.getMessage e)}))))

(defn save-rect-join [request]
  (http-response/ok (mapCoordinates (saveRectJoin request))))

(defn html-handler [request]
  (try
    (let [template-name (safe-template-name (get-in request [:path-params :file]))
          template-path (str template-name ".html")]
      (when-not (io/resource (str "html/" template-path))
        (throw (ex-info "Template not found"
                        {:status 404
                         :template template-name})))
      (layout/render request template-path))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))))

(defn handle-post-request [request]
  (let [form-data (:form-params request)] ;; Extract form params as a map
    (response/response
      (json/generate-string form-data)))) ;; Convert to JSON string

;; (defn save-conn [request]
;;     (handle-post-request request))
   

(defn call-function [func-name & args]
  (if-let [func (get allowed-legacy-save-functions func-name)]
    (apply func args)
    (throw (ex-info "Unsupported save function"
                    {:status 404
                     :function func-name}))))

(defn get-item2 [request ]
     (let [ ;; _ (dbg-pp request)
            _ (dbg (:query-params request))
            params (walk/keywordize-keys (:query-params request))
            _ (dbg (str "id: " (:id params)))
            item (g2/get-item (:conn-id params 108) (:id params))
            _ (dbg-pp item)
            ] 
         (http-response/ok item)))

(def iv (nonce/random-bytes 16))   ;; 16 bytes random iv

(def encryption-key (hash/sha256 "mysecret")) ;; 32 bytes key

(defn encrypt [text] (crypto/encrypt text encryption-key iv
                               {:alg :aes128-cbc-hmac-sha256}))

(defn decrypt [text] (-> (crypto/decrypt text encryption-key iv {:alg :aes128-cbc-hmac-sha256})
    (codecs/bytes->str)))

(defn post-json
  [request]
  (let [json-body (json/generate-string (:form-params request)) ;; Convert Clojure map to JSON string
        _ (dbg (:form-params request))
        name (get-in request [:path-params :fn])
        _ (dbg (str "Name: " name))
        url (str "http://localhost:8081/" name)
        response  (client/post url
                               {:headers {"Content-Type" "application/json"} ;; Set headers
                                :form-params (:form-params request)})] ;; Set the request body
    response)) ;; Return the response body

(defn fn-handler [request]
  (try
    (let [name (get-in request [:path-params :fn])
          response (call-function name request)]
      (http-response/ok response))
    (catch clojure.lang.ExceptionInfo e
      ((case (:status (ex-data e))
         404 http-response/not-found
         http-response/bad-request)
       {:error (ex-message e) :data (ex-data e)}))))

(defn home-routes []
  [ "" 
   {:middleware [with-bad-request-number-format
                 middleware/wrap-formats]}
   ["/" {:get home-page}]
   ["/html/:file" {:get html-handler}]
   ["/save/:fn" {:post fn-handler}]
   ["/customWebComponent" {:get custom}]
   ["/graph" {:post graph-page}]
   ["/listModels" {:get list-models}]
   ["/listConnections" {:get list-connections}]
   ["/getConnectionDetail" {:get get-connection-detail}]
   ["/getConnectionTree" {:get get-connection-tree}]
   ["/getConnectionTreeChildren" {:post get-connection-tree-children}]
   ["/createApiConnection" {:post create-api-connection}]
   ["/saveDbConnection" {:post save-db-connection}]
   ["/saveConnectorConnection" {:post save-connector-connection}]
   ["/testDbConnection" {:post test-db-connection}]
   ["/connectionHealth" {:get get-connection-health}]
   ["/updateDbConnection" {:post update-db-connection}]
   ["/deleteConnection" {:post delete-connection}]
   ["/testConnectionById" {:post test-connection-by-id}]
   ["/newgraph" {:post new-graph}]
   ["/getItem" {:get get-item}]
   ["/getEndpoints" {:get get-endpoints}]
   ["/getEndpointSchema" {:get get-endpoint-schema}]
   ["/getItem2" {:post get-item2}]
   ["/getFunction" {:get get-function}]
   ["/addFilter" {:post add-filter}]
   ["/saveFilter" {:post save-filter}]
   ["/addSingle" {:post add-single}]
   ["/moveSingle" {:post move-single}]
   ["/connectSingle" {:post connect-single}]
   ["/saveApi" {:post save-api}]
   ["/saveKafkaSource" {:post save-kafka-source}]
   ["/saveFileSource" {:post save-file-source}]
   ["/previewApiSchemaInference" {:post preview-api-schema-inference}]
   ["/aiExplainPreviewSchema" {:post ai-explain-preview-schema}]
   ["/aiSuggestBronzeKeys" {:post ai-suggest-bronze-keys}]
   ["/aiExplainModelProposal" {:post ai-explain-model-proposal}]
   ["/aiExplainProposalValidation" {:post ai-explain-proposal-validation}]
   ["/aiSuggestSilverTransforms" {:post ai-suggest-silver-transforms}]
   ["/aiGenerateSilverFromBRD" {:post ai-generate-silver-from-brd}]
   ["/aiGenerateGoldFromBRD" {:post ai-generate-gold-from-brd}]
   ["/aiSuggestGoldMartDesign" {:post ai-suggest-gold-mart-design}]
   ["/aiExplainSchemaDrift" {:post ai-explain-schema-drift}]
   ["/aiSuggestDriftRemediation" {:post ai-suggest-drift-remediation}]
   ["/aiExplainEndpointBusinessShape" {:post ai-explain-endpoint-business-shape}]
   ["/aiExplainTargetStrategy" {:post ai-explain-target-strategy}]
   ["/aiGenerateMetricGlossary" {:post ai-generate-metric-glossary}]
   ["/aiExplainRunOrKpiAnomaly" {:post ai-explain-run-or-kpi-anomaly}]
   ["/proposeSilverSchema" {:post propose-silver-schema}]
   ["/silverProposals" {:get list-silver-proposals}]
   ["/silverProposals/:proposal_id" {:get get-silver-proposal}]
   ["/updateSilverProposal" {:post update-silver-proposal}]
   ["/compileSilverProposal" {:post compile-silver-proposal}]
   ["/previewTargetData" {:get preview-target-data}]
   ["/synthesizeSilverGraph" {:post synthesize-silver-graph}]
   ["/validateSilverProposal" {:post validate-silver-proposal}]
   ["/validateSilverProposalWarehouse" {:post validate-silver-proposal-warehouse}]
   ["/reviewSilverProposal" {:post review-silver-proposal}]
   ["/publishSilverProposal" {:post publish-silver-proposal}]
   ["/executeSilverRelease" {:post execute-silver-release}]
   ["/pollSilverModelRun" {:post poll-silver-model-run}]
   ["/proposeGoldSchema" {:post propose-gold-schema}]
   ["/goldProposals" {:get list-gold-proposals}]
   ["/goldProposals/:proposal_id" {:get get-gold-proposal}]
   ["/updateGoldProposal" {:post update-gold-proposal}]
   ["/compileGoldProposal" {:post compile-gold-proposal}]
   ["/synthesizeGoldGraph" {:post synthesize-gold-graph}]
   ["/validateGoldProposal" {:post validate-gold-proposal}]
   ["/validateGoldProposalWarehouse" {:post validate-gold-proposal-warehouse}]
   ["/reviewGoldProposal" {:post review-gold-proposal}]
   ["/publishGoldProposal" {:post publish-gold-proposal}]
   ["/executeGoldRelease" {:post execute-gold-release}]
   ["/pollGoldModelRun" {:post poll-gold-model-run}]
   ["/runApiIngestion" {:post run-api-ingestion}]
   ["/runKafkaIngestion" {:post run-kafka-ingestion}]
   ["/runFileIngestion" {:post run-file-ingestion}]
   ["/bronzeSourceBatches" {:get bronze-source-batches-route}]
   ["/bronzeSourceObservabilitySummary" {:get bronze-source-observability-summary-route}]
   ["/bronzeSourceObservabilityAlerts" {:get bronze-source-observability-alerts-route}]
   ["/previewCopybookSchema" {:post preview-copybook-schema-route}]
   ["/apiBatches" {:get list-api-batches}]
   ["/apiBadRecords" {:get list-api-bad-records-route}]
   ["/apiSchemaApprovals" {:get list-api-schema-approvals-route}]
   ["/reviewApiSchema" {:post review-api-schema-route}]
   ["/promoteApiSchema" {:post promote-api-schema-route}]
   ["/rollbackApiBatch" {:post rollback-api-batch}]
   ["/archiveApiBatch" {:post archive-api-batch}]
   ["/applyApiRetention" {:post apply-api-retention}]
   ["/replayApiBadRecords" {:post replay-api-bad-records}]
   ["/verifyApiCommitClosure" {:get verify-api-commit-closure-route}]
   ["/resetApiCheckpoint" {:post reset-api-checkpoint-route}]
   ["/apiObservabilitySummary" {:get api-observability-summary-route}]
   ["/apiObservabilityAlerts" {:get api-observability-alerts-route}]
   ["/apiBronzeProofSignoff" {:post record-api-bronze-proof-signoff :get list-api-bronze-proof-signoffs}]
   ["/runSchedulerIngestion" {:post run-scheduler-ingestion}]
   ["/executionRuns" {:get list-execution-runs}]
   ["/executionDemand" {:get execution-demand}]
   ["/executionRuns/:run_id" {:get get-execution-run}]
   ["/executionRuns/:run_id/replay" {:post replay-execution-run}]
   ["/controlPlane/tenants" {:get list-tenants :post save-tenant}]
   ["/controlPlane/workspaces" {:get list-workspaces :post save-workspace}]
   ["/controlPlane/users" {:get list-users :post save-user}]
   ["/controlPlane/workspaceMembers" {:get list-workspace-members :post save-workspace-member}]
   ["/controlPlane/session" {:get control-plane-session}]
   ["/controlPlane/login" {:post control-plane-login}]
   ["/controlPlane/logout" {:post control-plane-logout}]
   ["/controlPlane/secrets" {:post save-managed-secret}]
   ["/controlPlane/auditEvents" {:get list-audit-events}]
   ["/controlPlane/graphAssignment" {:post assign-graph-workspace}]
   ["/controlPlane/graphDependencies" {:post save-graph-dependencies}]
   ["/graphLineage" {:get graph-lineage}]
   ["/reporting" {:get reporting-page}]
   ["/api/reporting/data" {:get reporting-data}]
   ["/api/reporting/schema" {:get reporting-schema}]
   ["/api/reporting/ask" {:post reporting-ask}]
   ["/api/reporting/save" {:post reporting-save}]
   ["/api/reporting/saved" {:get reporting-saved}]
   ["/api/reporting/run-saved" {:post reporting-run-saved}]
   ["/api/reporting/saved/:report_id" {:delete reporting-delete-saved}]
   ["/api/reporting/context" {:get reporting-context}]
   ["/api/reporting/context/saveEntry" {:post reporting-context-save}]
   ["/api/reporting/context/id/:context_id" {:delete reporting-context-delete}]
   ["/api/reporting/auto-train" {:post reporting-auto-train}]
   ["/api/reporting/table-stats" {:get reporting-table-stats}]
   ["/api/reporting/sample" {:get reporting-sample}]
   ["/freshnessDashboard" {:get freshness-dashboard}]
   ["/freshnessAlerts" {:get freshness-alerts}]
   ["/usageDashboard" {:get usage-dashboard}]
   ["/saveJoin" {:post save-join}]
   ["/saveRectJoin" {:post save-rect-join}]
   ["/saveColumn" {:post save-column}]
   ["/saveAggregation" {:post save-aggregation}]
   ["/saveFunction" {:post save-function}]
   ["/saveMapping" {:post save-mapping}]
   ["/saveTarget" {:post save-target}]
   ["/runTarget" {:post run-target}]
   ["/saveUnion" {:post save-join}]
   ["/saveSorter" {:post save-sorter}]
   ["/saveEndpoint" {:post save-endpoint}]
   ["/saveResponseBuilder" {:post save-response-builder}]
   ["/saveValidator" {:post save-validator}]
   ["/saveAuth" {:post save-auth}]
   ["/saveDx" {:post save-dx}]
   ["/saveRl" {:post save-rl}]
   ["/saveCr" {:post save-cr}]
   ["/saveLg" {:post save-lg}]
   ["/saveCq" {:post save-cq}]
   ["/saveEv" {:post save-ev}]
   ["/saveCi" {:post save-ci}]
   ["/saveSc" {:post save-sc}]
   ["/saveWh" {:post save-wh}]
   ["/saveConditional" {:post save-conditional}]
   ["/saveLogic" {:post save-logic}]
   ["/testEndpoint" {:post test-endpoint}]
   ["/deployEndpoints" {:post deploy-endpoints}]
   ["/addFunction" {:get add-function}]
   ["/addGrid" {:get add-grid}]
   ["/addtable" {:post add-table }]
   ["/getConn" {:get get-conn }]
   ["/getFunctionTypes" {:get (fn [_] (http-response/ok []))}]
   ["/deleteTable" {:post delete-table }]
   ["/removeNode" {:post remove-node }]
   ["/importOpenApi" {:post import-open-api}]
   ["/gil/validate" {:post gil-api/validate-handler}]
   ["/gil/compile" {:post gil-api/compile-handler}]
   ["/gil/apply" {:post gil-api/apply-handler}]
   ["/gil/from-nl" {:post gil-api/from-nl-handler}]
   ["/about" {:get about-page}]])
