  (ns bitool.routes.home
  (:require
   [bitool.layout :as layout]
   [bitool.db :as db]
   [ubergraph.core :as uber]
   [bitool.graph2 :as g2]
   [bitool.graph :as graph]
   [clojure.string :as string]
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
(defn get-conn[request] (g2/getDBTree (Integer. (:conn-id (:params request)))))

(defn get-btype[alias]
      (if (some #{"join" "union" "projection" "aggregation" "sorter" "filter" "function" "target" "output" "api-connection" "mapping" "conditionals" } [alias])
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
			      g (g2/add-single-node gid (Integer. (:conn_id params)) alias (get-btype alias) (:x params) (:y params))
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
                              _ (pp/pprint g)
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
        retmap (g2/addRightClickNode gid (Integer. item) (g2/btype-codes ftype))
        _ (pp/pprint retmap) ]
                              (assoc retmap :cp (getCoordinates (:cp retmap)))))

(defn graph-page [request]
  (let [
         gid (Integer. (:gid (:params request)))
         session (:session request)
         _ (println (str "home gid : " gid))
       ]
       (-> 
          (layout/render request "graph.html" {:items  (g2/displayGraph (db/getGraph gid)) })
          (assoc :session (assoc session :gid gid)))))

(defn new-graph [request]
  (let [
	graphname (:graphname (:params request))
        _ (println (str "Params : " (:params request))) 
        _ (println (str "Graph name : " graphname)) 
	graph (g2/createGraph (:graphname (:params request)))
        gid (get-in graph [:a :id])
        _ (println (str "home gid : " gid))
        _ (println (str "home graph : " graph))
        session (:session request)
        _ (println (str "Session Contents :" session ))
        _ (println "DISPLAYGRAPH")
        _ (println (g2/displayGraph graph))
        _ (println "DISPLAYGRAPH")
        ]
  (->
    ;; (layout/render request "graphbody.html" {:items  (g2/displayGraph graph) :gid gid  })
     (http-response/ok { "alias" "Output" , "x" "250" "y" "250" "id" 1 "parent" 0 })
     (assoc :session (assoc session :gid gid)))))

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
                "M" g2/get-mapping-item
                "Tg" g2/get-target-item
                g2/get-item))

(defn get-item [request] 
  (let [ params (:params request)
         id (Integer. (:id params))
         gid (:gid (:session request))
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
         ;;_ (println (getItemData id btype)) 
         ;;_ (println (getHtml btype)) ]
 (http-response/ok (apply fx [id g]))))
 ;;(http-response/ok (apply fx [id (getItemData gid id btype) g]))))
 ;; (layout/render request (getHtml btype) (assoc (getItemData gid id btype) :gid gid) )))

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
       (http-response/ok (g2/get-item id (db/insertGraph (g2/update-projection (db/getGraph (:gid (:session request)))  (Integer. (:id params)) params))))))

(defn save-mapping [request]
  (let [params (:params request)
        g (db/getGraph (:gid (:session request)))
        id (Integer. (g2/anil? (:id params) (first (first (filter (fn [[k,v]] (= "mapping" (get-in v [:na :btype]))) (:n g))))))
         _ (println params)
       ]
       (http-response/ok (g2/get-mapping-item id (db/insertGraph (g2/update-mapping g id (:mapping params)))))))

(defn save-column [request]
  (let [params (:params request)
        id (:id params)
        _ (println "==================================== PARAMS ========================================")
                                                     _ (println params)
                                                     ]
                                                     (http-response/ok (g2/get-item id (db/insertGraph (g2/update-nodes-tcols (db/getGraph (:gid (:session request)))  (conj [] (Integer. (:id params))) (conj '() (g2/create-column params))))))))

(defn save-join [request]
  (layout/render request "graph.html"  {:items (let [params (:params request)
                                                     _ (println "Inside SaveJoin")
                                                     _ (println params)
 						     joinCount (getJoinCount params)
						     _ (println  joinCount)
        				             where (createWhere joinCount params) 
						     _ (println (str "WHERE " where))
                                                     tid (Integer/parseInt (:tid params)) 
						     _ (println tid)]
                                                     (graph/updateTable (assoc (graph/getTableDef tid "out") :jcondition where) tid ))}))

(defn save-filter [request]
  (let [
					     params (:params request)
                                                     _ (println params)
                                                     _ (println "PRINTING PARAMS")
        				             where (:exp params) 
                                                     t (:name params) 
                                                     id (Integer. (:id params))
 						     g (db/getGraph (:gid (:session request)))
                                                     cp (db/insertGraph (g2/update-node g (Integer. id) {:sql where :name t}))
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
                                                     cp (db/insertGraph (g2/update-node g (Integer. id) {:sorters sorters :name t}))
                                                     rp (g2/get-item id g)
                                                    ]
                                                        (http-response/ok rp)))



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
  (http-response/ok (mapCoordinates (addSingle request))))

(defn connect-single [request]
  (http-response/ok (mapCoordinates (connectSingle request))))

(defn delete-table [request]
  (http-response/ok (mapCoordinates (deleteTable request))))

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
   ["/graph" {:get graph-page}]
   ["/newgraph" {:post new-graph}]
   ["/getItem" {:get get-item}]
   ["/getItem2" {:post get-item2}]
   ["/getFunction" {:get get-function}]
   ["/addFilter" {:post add-filter}]
   ["/saveFilter" {:post save-filter}]
   ["/addSingle" {:post add-single}]
   ["/connectSingle" {:post connect-single}]
   ["/saveJoin" {:post save-join}]
   ["/saveRectJoin" {:post save-rect-join}]
   ["/saveColumn" {:post save-column}]
   ["/saveAggregation" {:post save-aggregation}]
   ["/saveFunction" {:post save-function}]
   ["/saveMapping" {:post save-mapping}]
   ["/saveUnion" {:post save-union}]
   ["/saveSorter" {:post save-sorter}]
   ["/addFunction" {:get add-function}]
   ["/addGrid" {:get add-grid}]
   ["/addtable" {:post add-table }]
   ["/getConn" {:get get-conn }]
   ["/deleteTable" {:post delete-table }]
   ["/about" {:get about-page}]])
