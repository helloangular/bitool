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
   [bitool.middleware :as middleware]
   [selmer.parser :as selmer]
   [selmer.filters :as filters]
   [ring.util.response :as response ]
   [ring.middleware.session :as session]
   [ring.util.http-response :as http-response]
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
   [bitool.endpoint :as endpoint]
   [bitool.api.openapi :as openapi]
   [bitool.gil.api :as gil-api]
   ))

(filters/add-filter! :second second)

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

(defn mapCoordinates[g]
      (let [
		retmap (splitCpSp g)
                _ (tel/log! {:level :info, :msg (str "retmap : " retmap)})
                _ (println "++++++++++ CP retmap ++++++++++++")
                _ (println (getCoordinates (:cp retmap)))
                _ (println "++++++++++ SP retmap ++++++++++++")
                _ (println (:sp retmap))
                ret  (assoc {} :cp (getCoordinates (:cp retmap)) :sp (g2/getOrphanAttrs (:sp retmap)))
                _ (println "++++++++++ RETMAP ++++++++++++")
                _ (pp/pprint ret)

           ]
             (into (getCoordinates (:cp retmap)) (g2/getOrphanAttrs (:sp retmap)))))
      ;;     (into (getCoordinates (:cp retmap)) (:sp retmap))))
  ;;         (assoc {} :cp (getCoordinates (:cp retmap)) :sp (:sp retmap))))

(defn about-page [{session :session}]
  (-> (http-response/ok "Home Page Setting session")
      (assoc :session (assoc session :user "Harish"))
      (assoc :headers {"Content-Type" "text/plain"})))

(defn home-page [request]
  (let [_ (println (str "Session :" (:session request)))]
  (layout/render request "index.html" {:name "Harish"})))

(defn custom [request]
  (let [_ (println (str "Session :" (:session request)))]
  (layout/render request "customWebComponent.html" {:name "Harish"})))


(defn testReq [request] (let [x (:params request)]
					(println (:table1 x))
					(println (:table2 x))
					(+ 1 1)))
(defn get-conn[request]
  (let [conn-id-str (:conn-id (:params request))]
    (if (or (nil? conn-id-str) (= "" conn-id-str))
      (http-response/ok [])
      (http-response/ok (g2/getDBTree (Integer. conn-id-str))))))

(defn get-btype[alias]
      (if (some #{"join" "union" "projection" "aggregation" "sorter" "filter" "function" "target" "output" "api-connection" "graphql-builder" "mapping" "conditionals" "endpoint" "response-builder" "validator" "auth" "db-execute" "rate-limiter" "cors" "logger" "cache" "event-emitter" "circuit-breaker" "scheduler" "webhook"} [alias])
          alias
          "T"))

(defn addSingle[request] (let [ _ (pp/pprint request)
                                 params (:params request)
                              _ (println (str "BTYPE : " (:alias params)))
                              _ (println (str "ALIAS class: " (class (:alias params))))
                              _ (println (str "ALIAS count: " (count (:alias params))))
                              gid (:gid (:session request))
                              alias (:alias params)
 			      _ (println (str "gid : " gid))
			      g (g2/add-single-node gid (Integer. (anil? (:conn_id params) "123")) alias (get-btype alias) (:x params) (:y params))
                               ]
                               g))

(defn connectSingle[request] (let [ _ (pp/pprint request)
                                 params (:params request)
                              gid (:gid (:session request))
 			      _ (println (str "gid : " gid))
			      g (g2/connect-single-node gid (Integer. (:conn_id params)) (Integer. (:src params)) (Integer. (:dest params)))
                               ]
			       g))

(defn saveRectJoin[request] (let [
                              ;;  _ (pp/pprint request)
                                 params (:params request)
                              gid (:gid (:session request))
 			      _ (println (str "gid : " gid))
                              _ (println "Inside save-rect-join")
                              g (g2/rect-join gid (Integer. (:src params)) (Integer. (:dest params)))
                              _ (println "FINAL GRAPH")
                                _ (pp/pprint g)
                               ]
                               g))

(defn addTable [request] (let [ _ (pp/pprint request)
                              params (:params request)
			      t1 (:table1 params) 
			      t2 (Integer. (:table2 params))
			      c1 (Integer. (:conn_id params))
                              gid (:gid (:session request))
                              jtype (:action params)
 			      _ (println (str "gid : " gid))
                              _ (println (str "t1 -home: " t1))
                              _ (println (str "t2 -home: " t2))
                              _ (println (str "c2 -home: " c1))
                              t1-map (db/get-table c1 t1)
			      g (g2/processAddTable gid t2 c1 t1-map (g2/btype-codes jtype))
                              _ (println "AFTER processAddTable")
                        ;;      _ (pp/pprint g)
                              session (:session request)
                              _ (println "Session " session)
                              _ (println "Printing Graph")
			      _ (print (class g))
			      ]
                              g))
                              ;; (g2/createCoordinates g)))

(defn deleteTable [request] (let [params (:params request)
			      t1 (:item params)
                              gid (:gid (:session request))
 			      _ (println (str "gid : " gid))
                              _ (println (str "t1 -home: " t1))
			      g (graph/deleteTable gid t1)
                              _ (println g)
			      ]
                              (println "Printing Graph")
			      (print (class g))
                              g))
                              ;; (g2/createCoordinates g)))


(defn addFilter [request]
  (let [params (:params request)
        item (:item params)
        gid (:gid (:session request))
        ftype (:label params)
        _ (println (str "ITEM " item))
        _ (println (str "ftype " ftype))
        retmap (g2/addRightClickNode gid (Integer. item) (g2/btype-codes ftype))]
                              (assoc retmap :cp (getCoordinates (:cp retmap)))))

(defn graph-page [request]
  (let [
         params (:params request)
         gid (:gid params)
         _ (println (str "home gid : " gid))
         graph (db/getGraph (Integer. gid))
       ]
     ;; (http-response/ok { "alias" "Output" "btype" "O" "x" "250" "y" "250" "id" 1 "parent" 0 })))
     (http-response/ok (mapCoordinates graph))))

     ;; (layout/render request "graphbody.html" {:items  (g2/displayGraph graph) :gid gid  })))
      ;; (http-response/ok (mapCoordinates (db/getGraph gid)))))

(defn new-graph [request]
  (let [
	graphname (:graphname (:params request))
        _ (println (str "Params : " (:params request))) 
        _ (println (str "Graph name : " graphname)) 
	graph (g2/createGraph (:graphname (:params request)))
        gid (get-in graph [:a :id])
        ver (get-in graph [:a :v])
        _ (println (str "home gid : " gid))
        _ (println (str "home ver : " ver))
        _ (println (str "home graph : " graph))
        session (:session request)
        _ (println (str "Session Contents :" session ))
        _ (println "DISPLAYGRAPH")
        _ (println (g2/displayGraph graph))
        _ (println "DISPLAYGRAPH")
        ]
  (->
    ;; (layout/render request "graphbody.html" {:items  (g2/displayGraph graph) :gid gid  })
    ;; (http-response/ok [{"alias" "Output","y" 250,"x" 250,"btype" "O","parent" 0,"id" 1},{"alias" "sqldi","y" 250,"x" 150,"btype" "T","parent" 1,"id" 2}])
     (http-response/ok { "alias" "Output" "btype" "O" "x" "250" "y" "250" "id" 1 "parent" 0 })
     (assoc :session (assoc session :gid gid :ver ver)))))

(defn getHtml [item] (let [cat (first (str item))
                           _ (println (str "cat " cat))
                           _ (println (str "class " (class cat)))
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
                              _ (println (str "btype : " btype)) 
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
                "Ap" g2/get-target-item
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
           _ (println (str "id: " id))
           _ (println (str "gid: " gid))
           g (db/getGraph gid)
           _ (pp/pprint g)
           btype (g2/getBtype g id)
           fx (get-fx-from-btype btype)
           _ (println "ITEM------")
           _ (println fx)
           _ (println (class btype))
           _ (println id)
           _ (println (str "btype " btype))]
     (http-response/ok (apply fx [id g])))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e)}))
    (catch Exception e
      (println "get-item error:" (.getMessage e))
      (.printStackTrace e)
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
             _ (prn-v url)
           ] 
      (http-response/ok (endpoint-json (sc/list-endpoints-from-url url)))))

(defn get-endpoint-schema[request]
      (let [
             url (:url (:params request))
             _ (prn-v url)
             specurl (:spec (:params request))
             _ (prn-v specurl)
             method (:method (:params request))
             _ (prn-v method)
           ] 
           (http-response/ok (sc/endpoint-nodes-from-url specurl url (keyword (string/lower-case method))))))

(defn get-function [request] 
  (let [function (:function (:params request))
        _ (println "function------")
        _ (println function) ]
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
  (let [params (:params request)
        id (:id params)
         _ (println params)
       ]
       (http-response/ok (g2/get-item id (db/insertGraph (g2/update-agg (db/getGraph (:gid (:session request)))  (Integer. (:id params)) params))))))


(defn save-union [request]
  (let [params (:params request)
        id (:id params)
         _ (println params)
       ]
       (http-response/ok (g2/get-item id (db/insertGraph (g2/update-agg (db/getGraph (:gid (:session request)))  (Integer. (:id params)) params))))))

(defn save-function [request]
  (let [params (:params request)
        id (:id params)
         _ (println params)
       ]
       (http-response/ok (g2/get-item id (db/insertGraph (g2/update-projections (db/getGraph (:gid (:session request)))  (Integer. (:id params)) params))))))

(defn save-mapping [request]
  (let [params (:params request)
        g (db/getGraph (:gid (:session request)))
        id (Integer. (anil? (:id params) (first (first (filter (fn [[k,v]] (= "mapping" (get-in v [:na :btype]))) (:n g))))))
         _ (println params)
       ]
       (http-response/ok (g2/get-mapping-item id (db/insertGraph (g2/update-mapping g id (:mapping params)))))))


(defn save-target [request]
  (let [params (:params request)
        g (db/getGraph (:gid (:session request)))
        id (:id params)
         _ (println params)
       ]
       (http-response/ok (g2/get-target-item id (db/insertGraph (g2/update-target g id params))))))

(defn run-target [request]
  (let [params (:params request)
        g (db/getGraph (:gid params))
        tid (:tid params)
         _ (println params)
       ]
       ;; (http-response/ok (g2/run-target g tid))))
        (http-response/ok (g2/get-target-item g tid))))

(defn save-column [request]
  (let [params (:params request)
        id (:id params)
        _ (println "==================================== PARAMS ========================================")
                                                     _ (println params)
                                                     ]
                                                     (http-response/ok (g2/get-item id (db/insertGraph (g2/append-nodes-tcols (db/getGraph (:gid (:session request)))  (conj [] (Integer. (:id params))) (conj '() (g2/create-column params))))))))


(defn save-join [request]
  (let [
					     params (:params request)
                                                     _ (println "PRINTING PARAMS")
                                                     _ (println params)
                                                     id (Integer. (:id params))
 						     g (db/getGraph (:gid (:session request)))
                                                     cp (db/insertGraph (g2/save-join g (Integer. id) params))
						     rp (g2/get-item id g) 
						    ]
							(http-response/ok rp)))

(defn save-filter [request]
  (let [
					     params (:params request)
                                                     _ (println "PRINTING PARAMS")
                                                     _ (println params)
                                                     id (Integer. (:id params))
 						     g (db/getGraph (:gid (:session request)))
                                                     cp (db/insertGraph (g2/save-filter g (Integer. id) (set/rename-keys params {:having :where})))
						     rp (g2/get-item id g) 
						    ]
							(http-response/ok rp)))


(defn save-sorter [request]
  (let [
                                             params (:params request)
                                                     _ (println params)
                                                     _ (println "PRINTING PARAMS")
                                                     sorters (:sorters params)
                                                     t (:name params)
                                                     id (Integer. (:id params))
                                                     g (db/getGraph (:gid (:session request)))
                                                     cp (db/insertGraph (g2/save-sorter g (Integer. id) params))
                                                     rp (g2/get-item id g)
                                                    ]
                                                        (http-response/ok rp)))

(defn save-api [request]
  (let [
                                             params (:params request)
                                                     _ (println "PRINTING PARAMS")
                                                     _ (println params)
                                                     id (Integer. (:id params))
                				     _ (prn-v id)
                                                     g (db/getGraph (:gid (:session request)))
                                                     cp (db/insertGraph (g2/save-api g (Integer. id) params))
                                                     rp (g2/get-item id g)
                                                    ]
                                                        (http-response/ok (mapCoordinates cp))))

(defn save-endpoint [request]
  (let [params  (:params request)
        id      (Integer. (:id params))
        gid     (:gid (:session request))
        g       (db/getGraph gid)
        updated (g2/save-endpoint g id params)
        cp      (db/insertGraph updated)
        ep-cfg  (g2/getData cp id)
        _       (endpoint/register-endpoint! gid id ep-cfg)
        rp      (g2/get-endpoint-item id cp)]
    (http-response/ok rp)))

(defn save-response-builder [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-response-builder g id params)
          cp      (db/insertGraph updated)
          rp      (g2/get-response-builder-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :field (:field (ex-data e))}))))

(defn save-validator [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-validator g id params)
          cp      (db/insertGraph updated)
          rp      (g2/get-validator-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :field (:field (ex-data e))}))))

(defn save-sc [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-sc g id params)
          cp      (db/insertGraph updated)
          rp      (g2/get-sc-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :field (:field (ex-data e))}))))

(defn save-wh [request]
  (let [params  (:params request)
        id      (Integer. (:id params))
        g       (db/getGraph (:gid (:session request)))
        updated (g2/save-wh g id params)
        cp      (db/insertGraph updated)
        rp      (g2/get-wh-item id cp)]
    (http-response/ok rp)))

(defn save-conditional [request]
  (let [params  (:params request)
        id      (Integer. (:id params))
        g       (db/getGraph (:gid (:session request)))
        updated (g2/save-conditional g id params)
        cp      (db/insertGraph updated)
        rp      (g2/get-conditional-item id cp)]
    (http-response/ok rp)))

(defn save-logic [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-logic g id params)
          cp      (db/insertGraph updated)
          rp      (g2/get-logic-item id cp)]
      (http-response/ok rp))
    (catch Exception e
      (http-response/bad-request {:error (.getMessage e)}))))

(defn save-auth [request]
  (let [params  (:params request)
        id      (Integer. (:id params))
        g       (db/getGraph (:gid (:session request)))
        updated (g2/save-auth g id params)
        cp      (db/insertGraph updated)
        rp      (g2/get-auth-item id cp)]
    (http-response/ok rp)))

(defn save-dx [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-dx g id params)
          cp      (db/insertGraph updated)
          rp      (g2/get-dx-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :field (:field (ex-data e))}))))

(defn save-rl [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-rl g id params)
          cp      (db/insertGraph updated)
          rp      (g2/get-rl-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :field (:field (ex-data e))}))))

(defn save-cr [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-cr g id params)
          cp      (db/insertGraph updated)
          rp      (g2/get-cr-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :field (:field (ex-data e))}))))

(defn save-lg [request]
  (let [params  (:params request)
        id      (Integer. (:id params))
        g       (db/getGraph (:gid (:session request)))
        updated (g2/save-lg g id params)
        cp      (db/insertGraph updated)
        rp      (g2/get-lg-item id cp)]
    (http-response/ok rp)))

(defn save-cq [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-cq g id params)
          cp      (db/insertGraph updated)
          rp      (g2/get-cq-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :field (:field (ex-data e))}))))

(defn save-ev [request]
  (let [params  (:params request)
        id      (Integer. (:id params))
        g       (db/getGraph (:gid (:session request)))
        updated (g2/save-ev g id params)
        cp      (db/insertGraph updated)
        rp      (g2/get-ev-item id cp)]
    (http-response/ok rp)))

(defn save-ci [request]
  (try
    (let [params  (:params request)
          id      (Integer. (:id params))
          g       (db/getGraph (:gid (:session request)))
          updated (g2/save-ci g id params)
          cp      (db/insertGraph updated)
          rp      (g2/get-ci-item id cp)]
      (http-response/ok rp))
    (catch clojure.lang.ExceptionInfo e
      (http-response/bad-request {:error (ex-message e) :field (:field (ex-data e))}))))

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
          (println "add-single error:" (.getMessage e))
          (.printStackTrace e)
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
          g2     (-> g
                     (assoc-in [:n id :na :x] x)
                     (assoc-in [:n id :na :y] y)
                     (db/insertGraph))]
      (http-response/ok (mapCoordinates g2)))
    (catch Exception e
      (println "move-single error:" (.getMessage e))
      (http-response/internal-server-error {:error (str "Failed to move node: " (.getMessage e))}))))

(defn connect-single [request]
  (http-response/ok (mapCoordinates (connectSingle request))))

(defn delete-table [request]
  (http-response/ok (mapCoordinates (deleteTable request))))

(defn remove-node [request]
  (let [params (walk/keywordize-keys (:params request))
        id     (Integer. (:id params))
        gid    (:gid (:session request))
        g      (db/getGraph gid)
        btype  (:btype (g2/getData g id))
        _      (when (some #{btype} ["Ep" "Wh"])
                 (endpoint/unregister-endpoint! gid id))
        cp     (db/insertGraph (g2/remove-node g id))]
    (http-response/ok (mapCoordinates cp))))

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
          cp         (db/insertGraph merged-g)
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
  (let [path (get-in request [:path-params :file])]
    (layout/render request (str  path ".html"))))

(defn handle-post-request [request]
  (let [form-data (:form-params request)] ;; Extract form params as a map
    (response/response
      (json/generate-string form-data)))) ;; Convert to JSON string

;; (defn save-conn [request]
;;     (handle-post-request request))
   

(defn call-function [func-name & args]
  (let [func (resolve (symbol (str "bitool.graph2/" func-name )))
       _ (println func) 
       _ (println *ns*)
       _ (println "------------------------------ARGS------------------------------")
       _ (println args)
       _ (println "------------------------------BODY------------------------------")
       _ (println (class (:json-params (first args))))

       ]
    (if (and func (fn? @func))
      (apply @func args)
      (throw (Exception. (str "Function " func-name " not found!"))))))

(defn get-item2 [request ]
     (let [ ;; _ (pp/pprint request)
            _ (println (:query-params request))
            params (walk/keywordize-keys (:query-params request))
            _ (println (str "id: " (:id params)))
            item (g2/get-item (:conn-id params 108) (:id params))
            _ (pp/pprint item)
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
        _ (println (:form-params request))
        name (get-in request [:path-params :fn])
        _ (println (str "Name: " name))
        url (str "http://localhost:8081/" name)
        response  (client/post url
                               {:headers {"Content-Type" "application/json"} ;; Set headers
                                :form-params (:form-params request)})] ;; Set the request body
    response)) ;; Return the response body

(defn fn-handler [request]
  (let [name (get-in request [:path-params :fn])
        response (call-function name request)]
   (http-response/ok response)))

(defn home-routes []
  [ "" 
   {:middleware [
;;middleware/wrap-csrf
                 middleware/wrap-formats
                 ]}
   ["/" {:get home-page}]
   ["/html/:file" {:get html-handler}]
   ["/save/:fn" {:post fn-handler}]
   ["/customWebComponent" {:get custom}]
   ["/graph" {:post graph-page}]
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
