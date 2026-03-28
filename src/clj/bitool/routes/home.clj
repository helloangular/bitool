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
   )
  (:import [java.net URI InetAddress]))

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


(defn testReq [request] (let [x (:params request)]
					(dbg (:table1 x))
					(dbg (:table2 x))
					(+ 1 1)))
(defn get-conn[request]
  (let [conn-id-str (:conn-id (:params request))]
    (if (or (nil? conn-id-str) (= "" conn-id-str))
      (http-response/ok [])
      (http-response/ok (g2/getDBTree (Integer. conn-id-str))))))

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
    ("postgresql" "oracle" "sqlserver" "mysql" "db2") "RDBMS"
    nil))

(defn- connection->tree-item
  [conn]
  (let [dbtype (:dbtype conn)]
    {:conn_id (:id conn)
     :label (or (:connection_name conn) (str "Connection " (:id conn)))
     :dbtype dbtype
     :treeParent (connection-tree-parent dbtype)
     :nodetype (when (= "api" (some-> dbtype string/lower-case))
                 "api-connection")}))

(defn list-connections [_request]
  (let [connections (->> (db/list-all-connections-summary)
                         (map connection->tree-item)
                         (remove #(nil? (:treeParent %)))
                         vec)]
    (http-response/ok {:connections connections})))

(defn get-connection-detail [request]
  (try
    (let [params (:params request)
          conn-id (parse-required-int (or (:conn_id params) (:conn-id params)) :conn_id)
          detail (db/get-connection-detail conn-id)]
      (if detail
        (http-response/ok detail)
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
      (http-response/ok {:status "ok" :conn_id conn-id}))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :data (ex-data e)}))
    (catch Exception e
      (http-response/ok {:status "error"
                         :conn_id (or (some-> request :params :conn_id)
                                      (some-> request :params :conn-id))
                         :error (.getMessage e)}))))

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
      (http-response/ok {:status "ok"}))
    (catch NumberFormatException e
      (http-response/bad-request {:error (.getMessage e)}))
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
     :workspace-key (:workspace_key params)
     :updated-by (or (:updated_by params)
                     (get-in request [:session :user])
                     "system")}))

(defn- persist-graph!
  [request graph]
  (control-plane/persist-graph! graph (request-graph-save-context request)))

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
          result         (ingest-execution/enqueue-api-request! gid node-id {:endpoint-name endpoint-name
                                                                             :trigger-type "manual"})]
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

(def key (hash/sha256 "mysecret")) ;; 32 bytes key

(defn encrypt [text] (crypto/encrypt text key iv
                               {:alg :aes128-cbc-hmac-sha256}))

(defn decrypt [text] (-> (crypto/decrypt text key iv {:alg :aes128-cbc-hmac-sha256})
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
   ["/createApiConnection" {:post create-api-connection}]
   ["/testDbConnection" {:post test-db-connection}]
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
   ["/controlPlane/secrets" {:post save-managed-secret}]
   ["/controlPlane/auditEvents" {:get list-audit-events}]
   ["/controlPlane/graphAssignment" {:post assign-graph-workspace}]
   ["/controlPlane/graphDependencies" {:post save-graph-dependencies}]
   ["/graphLineage" {:get graph-lineage}]
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
