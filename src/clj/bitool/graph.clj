(ns bitool.graph
	(:require [ubergraph.core :as uber])
	(:require [ubergraph.alg :as ualg])
        (:require [loom.graph :as graph])
        (:require [loom.alg :as alg])
        (:require [com.rpl.specter :as sp])
        (:require [next.jdbc :as jdbc])
        (:require [next.jdbc.sql :as sql])
        (:require [methodical.core :as m])
	(:require [clojure.string :as string])
        (:require [clojure.pprint :as pp]))

;; (refer-closure :exclude '[filter for group-by into partition-by set update])

(defmacro while-let
    "Repeatedly executes body while test expression is true, evaluating the body with binding-form bound to the value of test."
    [bindings & body]
    (let [form (first bindings) test (second bindings)]
        `(loop [~form ~test]
             (when ~form
                 ~@body
                 (recur ~test)))))

(defn call [name & args]
       (let [ _ (println (str "FUNCTION NAME " name)) 
              fun (ns-resolve *ns* (symbol name))]
       (apply fun args)))

(comment 

(def db-config
  {:dbtype "postgresql"
   :dbname "bitool"
   :host "localhost"
   :user "postgres"
   :password "postgres"})

(def db (jdbc/get-datasource db-config))

(def ds (jdbc/get-datasource db-spec))

(defn runsql[sql-map] (jdbc/execute! db (sql/format sql-map)))

(defn insertSqlfn [name tablelist columnlist description sql datadef]
   (let [ insert-sqlfn-map {:insert-into [:sqlfn]
              :columns [:name :tablelist :columnlist :description :sql :datadef]
              :values [[name tablelist columnlist description sql datadef]]} ]
        (runsql insert-sqlfn-map)))   
)

(def db-spec
  {:dbtype "postgresql"
   :dbname "bitool"
   :host "localhost"
   :user "postgres"
   :password "postgres"})

(def ds (jdbc/get-datasource db-spec))

(defn parse-int [s]
   (Integer. (re-find  #"\d+" s )))

(defn getCalculatedColumn[tid] (first (jdbc/execute! ds ["select * from calc_column where id = ?" tid])))

(defn node-with-attr-val [g k v] (first (filter #(= v (k (uber/attrs g %))) (uber/nodes g))))

(defn find-output-node [g] (node-with-attr-val g :name "op"))

(defn getFilterData[tid] (first (jdbc/execute! ds ["select * from sqlfilter  where id = ?" tid])))

(defn addNodes[g nodes] (uber/add-nodes* g nodes))

(defn addEdge[g edge] (uber/add-edges g [(first (uber/src edge)) (first (uber/dest edge))]))

(defn addEdges[g edges] 
   (reduce addEdge g edges))

(defn reverseSiblingLati [g sibling parent] (case (uber/attr g sibling :lati)
                                  		:nil  (- (uber/attr g parent :lati) 1)
             					true  (+ (uber/attr g sibling :lati) 2) ))

(defn getLevelFromSibling [g sibling] ( if sibling (case (uber/attr g sibling :level)
                                  			:U  :U
             						:L  :L 
            						nil :L) 
					   :S ))

(defn getLevel [g sibling parent] (case (uber/attr g parent :level)
                                  		:U  :U
             					:L  :L 
            					:S (getLevelFromSibling g sibling)))


(defn getLati [g sibling parent] (let [plati (uber/attr g parent :lati)]
					( if sibling ( if (uber/attr g sibling :lati) (+ plati 1) (- plati 1)) 
					     plati)))


(defn getLevelLatiFromParent [g parent] (let [level (uber/attr g parent :level) 
					      lati (uber/attr g parent :lati)]
					 [level lati]))


(defn getSibling [g node] (if node (let [parent (first (uber/successors g node))
				cnodes (uber/predecessors g parent)
				sibling (first (remove #(= node %) cnodes))]
			  sibling)))


(defn getBtype[nodeName] (let [ _ (println (str "nodeName " nodeName))
                                subcat (subs nodeName 0 1) 
                                _ (println subcat) ] 
			(case subcat
                         	"j" "join"
                         	"p" "projection"
                         	"t" "table"
                         	"f" (if (= subcat "u" ) "function" "where")
                         	"o" "output" )))


(defn getUL[g node] (if node (if (= node (find-output-node g)) [:S 0 0 "output"] (let [parent  (first (uber/successors g node))
                          _ (println (str " node : " node))
                          _ (println (str "parent " parent))
                          sibling (getSibling g node)
                       ;;   _ (println (str "sibling " sibling))
                       ;;   _ (println (str "parent longi " (uber/attr g parent :longi) ))
          		  longi (- (uber/attr g parent :longi) 1)
			  level (getLevel g sibling parent)
			  lati (getLati g sibling parent)
 			  btype (getBtype (uber/attr g node :name))
                       ;;   _ (println (str "BTYPE " btype))
			  ]
			  [level longi lati btype]))))

           


(defn setUL[g node] (if node (let [attrs (getUL g node)
                                _ (println (str "START setUL node " node))
                                  _ (println "Attrs : " attrs) ]
				(-> g
			  	 (uber/add-attr node :level (first attrs))
			   	(uber/add-attr node :longi (second attrs))
			   	(uber/add-attr node :lati (nth attrs 2))
			   	(uber/add-attr node :btype (nth attrs 3))
;;				(#(do (println "END setUL node ") %))
                                ))))





(defn setAttr [g node] (if node (let [ pred (uber/predecessors g node)
                                    leaf1 (first pred)
                                    leaf2 (second pred)]
;;                                (println (str "START setAttr node " node))
;;                                  (println (str "leaf1 " leaf1))
;;                                  (println (str "leaf2 " leaf2))
				  (-> g
;;				      (#(do (println "->g") %))
                                      (setUL leaf1)
;;				      (#(do (println "setUL leaf1") %))
                                      (setUL leaf2)
;;				      (#(do (println "setUL leaf2") %))
                                      (setAttr leaf1)
;;				      (#(do (println "setAttr leaf1") %))
                                      (setAttr leaf2)
;;				      (#(do (println "setAttr leaf2") %))
;;				      (#(do (println "END setAttr leaf2") %))
				      ))))


(defn updateStartAttrs [g strt] (let [
                                      _ (println "---- updateStartAttrs -------")
                                      _ (println strt)
                                      _ (println (uber/has-node? g strt))
                                      _ (println (count (str strt)))
                                     ]
				    (-> g
				    (uber/add-attr (if (uber/has-node? g strt) strt (str strt)) :longi 0)
         			    (uber/add-attr (if (uber/has-node? g strt) strt (str strt)) :level :S)
         			    (uber/add-attr (if (uber/has-node? g strt) strt (str strt)) :lati 0))))


(defn updateAttrs [g start] (map #(setUL g %) (-> g
                     (updateStartAttrs start)
		     ualg/topsort
		     reverse)))


(defn updateNode [g node] (-> g
			      (setUL node)))


(defn  topsort [g start] (-> g
			     ualg/topsort
		       	     reverse))


(defn updateGraphAttrs[g start] (reduce updateNode (updateStartAttrs g start) (topsort g start)))


(defn getAttrs [g start] (let [updatedG (updateGraphAttrs g start)]
				(map #(uber/node-with-attrs updatedG  %) (topsort g start))))


(defn getAttrsByLevel[g start lvl] (filter #(= lvl (:level (second %))) (getAttrs g start)))

 
(defn getLatiByLevel[g start lvl] (map #(:lati (second %)) (getAttrsByLevel g start lvl)))


(defn getAttrByLevel[g start lvl attr] (let
                                          [
      			;;			_ (println (str "ggetAttrsByLevel: " (getAttrsByLevel g start lvl)))
      			;;			_ (println (str "g: " g))
      			;;			_ (println (str "start: " start))
      			;;			_ (println (str "lvl: " lvl))
      			;;			_ (println (str "attr: " attr))
					  ] (map #(attr (second %)) (getAttrsByLevel g start lvl))))

(defn nicep [seq] (doseq [x seq]
                     (println (str "size : " (count seq)))
                     (println (str "pseq : " x))))

(defn getLatiHeightByLevel[g start lvl] (let [lati (getAttrByLevel g start lvl :lati)
                                          ;;    _ (println (str "size : " (count lati)))
				              min (if (empty? lati) 0 (reduce min lati))
					      max (if (empty? lati) 0 (reduce max lati))]
						(if (= min max) max (abs (- max min)) )
                                        ))


(defn getLongiHeightByLevel[g start lvl] (let [longi (getAttrByLevel g start lvl :longi)]
				              (if (empty? longi) 0 (abs (reduce min longi)))))


(defn getLatiHeight[g start] (+ (getLatiHeightByLevel g start :U) (getLatiHeightByLevel g start :L)))

(defn getLongiHeight[g start] (+ 1 (max (getLongiHeightByLevel g start :U) (getLongiHeightByLevel g start :L))))

;; (def latiEnd (+ 2 (getLatiHeightByLevel g3 :op :L)))

;; (def longiEnd (getLongiHeight g3 :op)) 

(defn getTableName [name] (if (string/starts-with? name ":") (subs name 1) name))


(defn createCoordinates[g start] (let [
					attrs (getAttrs g start)
                                       latiEnd (+ 2 (getLatiHeightByLevel g start :L))
				       longiEnd (getLongiHeight g start)
				       coord {}]
					(map #(assoc coord  :name (:name (second %)) 
							    :y (+ (:lati (second %)) latiEnd)
							    :x (+ (:longi (second %)) longiEnd)
                                                            :btype (:btype (second %))
                                                            :tid (name (first %))
					 		   ) attrs)))
 
(defn escape-double-quotes [input-str]
  (clojure.string/replace input-str #"\"" "\\\""))

(defn collToString [coll] (escape-double-quotes (binding [*print-dup* true] (pr-str coll))))

(defn getGraphFromDB ([tx gid] (let [_ (println (str "GID : " gid))] (read-string (:sqlgraph/definition (jdbc/execute-one! tx ["select definition from sqlgraph where id = ?" (Integer. gid)])))))
                     ([gid] (jdbc/with-transaction [tx ds]
                              (getGraphFromDB tx gid))))

(defn getTableFile [t direc] (let [_ (println "Table : " t)] (str "db" direc "/" (string/replace (str t) #":" "") ".txt")))

;; (defn getTableFromDB[t] (let [ gtable (:gtable/definition (first (sql/find-by-keys ds :gtable {:name (name t)} {:columns [[:definition :definition]]})))
;;(defn getTableFromDB[tid] (let [ gtable (:gtable/definition (first (sql/query ds ["select definition from gtable where id = ?" (if (string/starts-with? t ":") (subs t 1) t)])))
(defn getTableFromDB ([tx tid] (let [ 
                               _ (println (str "tid1 " tid))
                               _ (println (str "tid1 class" (class tid)))
				 gtable (:gtable/definition (first (sql/query tx ["select definition from gtable where id = ?" tid])))
                               _ (println gtable)
                               _ (println (str "GTABLE " gtable))
				]
                             (read-string (str gtable))))
                     ([tid] (jdbc/with-transaction [tx ds] 
                              (getTableFromDB tx tid))))

(defn getTableDef [t] (read-string (slurp (getTableFile t "in"))))

(defn createTable [name cols] {:tname name, :tcols cols})

(defn blankTable [name] (createTable name ()))

(defn getTableName [t] (let [_ (println (str "TableName is " t))] 
                          (:name (second t))))

(defn getTableId [t] (let [_ (println (str "TableName is " t))] 
                          (Integer/parseInt (first t))))

(defn getTableColumns [tname] (:tcols (getTableDef tname)))

(defn getDBTableColumns [tx tid] (:tcols (getTableFromDB tx tid)))

(defn tableExists[gid tname] (let [tid (:gtable/id (first (sql/find-by-keys ds :gtable {:name (name tname) :graph_id gid} )))]
                              (if tid tid -1)))

(defn columnExists[gid tname cname] (:sqlcolumn/name (first (sql/query ds ["select c.name from sqltable t , sqlcolumn c where t.graph_id = ? and t.name = ? and c.name = ? and t.id = c.table_id" gid tname cname]))))

( comment

(defn saveTable [tx t gid] (let [tname (name (:tname t))
                              tid (tableExists gid tname ) ] 
                        (do (println "inside saveTable")
                        (println t)
                        (doseq [column (:tcols t)]
			  (println column)
			  (println (class column)))
                        (println (class t))
                                    (if-not tid  
					(let [tid (:sqltable/id (sql/insert!
                                     	        tx	
                                     		:sqltable
                                      		{:name tname :dbname "1" :graph_id gid}))] 
                                    	(doseq [column (:tcols t)]
                                        	(if-not (columnExists 3 tname  column ) (sql/insert!
                                                tx
                                                :sqlcolumn
                                                {:name (str column) :table_id tid}))))))))

 )


(defn insertTable ([t gid] (jdbc/with-transaction [tx ds]
                            (insertTable tx t gid)))
		([tx t gid] (let [tname (name (:tname t))
                              tabledef (collToString t) ] 
					(:gtable/id (sql/insert!
                                     		tx
                                     		:gtable
                                      		{:name tname :dbname "1" :graph_id gid :definition tabledef}))
                                            )))

(defn updateTable ([t tid] (jdbc/with-transaction [tx ds]
                            (updateTable tx t tid)))
		([tx t tid] (let [
_ (println "-------------------------------UPDATE TABLE ----------------------")
				tname (:tname t)
                              ;;tid (tableExists gid tname ) 
                              tabledef (collToString t) 
                                         _ (println (str "--------------------------------DEBUG updateTable tabledef " tabledef)) 
 					 _ (println (str "--------------------------------DEBUG table id " tid)) 
] 
                               (do (sql/update! tx :gtable {:definition tabledef} {:id tid} )
                                            tid))))


(defn getNodeByName [name] (if (string/starts-with? name ":") name (str ":" name)))

( comment 
(defn insertTable [graph tx t gid] (let [
			      _ (println (str "t " t))			      
			      tname (name (:tname t))
                              _ (println graph)
                              tabledef (collToString t) ] 
					(-> graph
                                                (uber/add-attr (getNodeByName (:tname t)) :tid (:gtable/id (sql/insert!
                                     					tx
                                     					:gtable
                                      					{:name tname :dbname "1" :graph_id gid :definition tabledef}))))))

(defn updateTable ([t tid] (jdbc/with-transaction [tx ds] 
                            (updateTable tx t tid )))
                  ([tx t tid] (let [ _ (println "-------------------------------UPDATE TABLE ----------------------")
                                         tname (name (:tname t))
                                         tabledef (collToString t) 
                                         _ (println (str "--------------------------------DEBUG updateTable tabledef " tabledef)) 
 					 _ (println (str "--------------------------------DEBUG table id " tid)) ] 
                                         (do (sql/update! tx :gtable {:definition tabledef} {:id tid} )
                                            				  (str tid)))))
)

(defn saveGraph [tx gid definition] (sql/update! tx :sqlgraph {:definition definition} {:id gid} )) 

(defn addFunction [name gid] (let [_ (println "Calling saveFunction")]
                                     (:calc_column/id (sql/insert! ds :calc_column {:name name :graph_id gid}))))

(defn addFilter [name gid] (let [_ (println "Calling saveFilter")] 
                   (:sqlfilter/id (sql/insert! ds :sqlfilter {:name name :graph_id gid}))))

(defn updateFilter [name gid where] (let [_ (println "Calling updateFilter")] 
                   (sql/update! ds :sqlfilter {:filter_condition where} {:name name :graph_id gid} )))

(defn updateTableColumns [tx gid tid cols] (let [t (getTableFromDB tx tid)]
                                               (updateTable tx (assoc t :tcols (concat (:tcols t) cols)) tid)))

(defn createNewNode ([tx tORname gid] (let [ g (getGraphFromDB tx gid)
                                          nclass (class tORname)
                                      [table nodeName]  (cond 
                                              (= nclass java.lang.String) [{:tname tORname :tcols (getTableColumns tORname) } tORname] 
                                              (= nclass clojure.lang.PersistentArrayMap) [tORname (:tname tORname)] ) ]
                                      (vec [(str (insertTable tx table gid)) { :name nodeName }] )))
                    ([tORname gid] (jdbc/with-transaction [tx ds]
                                  (createNewNode tx tORname gid))))

(defn saveJoin [tx gid joinNode t1 t2] (let [
                                        _ (println "Inside SaveJoin ")
					_ (println (str " joinNode : " joinNode))
					_ (println (str " t1 : " t1))
					_ (println (str " t2 : " t2))
				   ] (updateTable tx {:tname (getTableName joinNode) :j1Tables [t1] :j2Tables [t2] :j1Columns (getDBTableColumns tx (Integer/parseInt t1)) :j2Columns (getDBTableColumns tx (Integer/parseInt t2)) } (getTableId joinNode))))

(defn saveProjection [tx gid projNode t1 t2] (updateTable tx {:tname (getTableName projNode) :tcols (concat (getDBTableColumns tx (Integer/parseInt t1)) (getDBTableColumns tx (Integer/parseInt t2))) } (getTableId projNode)))

(defn saveNewSubGraph [gid tx t1 t2 subj] (do 
					    (saveJoin tx gid t1 t2)
				            ;; (insertTable tx (getTableDef t2) gid)
				            (saveProjection tx gid t1 t2)))

(defn getParent [g child] (let [ _ (println "getParent g")
				 _ (uber/pprint g)
				 _ (println "child ")
                                 - (println child)
                                 _ (println "parent ")
 				 _ (println (first (uber/successors g child))) ]
                           (first (uber/successors g child))))

(defn updateColumns [tx gid g gnode cols] (loop [x gnode]
					(when (and x (getParent g x) (not (string/starts-with? (str (getParent g x)) ":f")))
                                            (println (str "----------------------------------------------------------updating " (getParent g x)))
                                            (updateTableColumns tx gid (Integer/parseInt (getParent g x)) cols)
                                            (recur (getParent g x)))))

(defn updateGraphWithNewTableColumns [g tx gid  t2 proj] (let [cols (getTableColumns t2)]
                                                       (updateColumns tx gid g proj cols)
						       g ))

(defn getNewTable [tx g gnode gid t1Node t1Edge]  (let [name (:name (second gnode))
                                     n (first gnode) 
                                     joinNode (createNewNode (blankTable (str "join-" name)) gid)
                                     projNode (createNewNode (blankTable (str "proj-" name)) gid)
                                     subg (uber/digraph [gnode joinNode] [joinNode projNode])                                   
                                     _ (println (str "nth into " [(nth (into [] (uber/nodes subg)) 2) (uber/dest t1Edge)])) 
                                     _ (println (str "(second (uber/nodes subg))" (second (uber/nodes subg))))
                                     _ (println (str "t1Node " t1Node))
                                     _ (println (str "gnode " gnode))
 				    ]
                                    (do 
                                        (saveJoin tx gid joinNode t1Node (first gnode))
				        (saveProjection tx gid projNode t1Node (first gnode))
                 		      ;;  (saveNewSubGraph gid tx t1Node gnode subg) 
                                        (-> g
                                            (uber/add-nodes-with-attrs gnode)
                                            (uber/add-nodes-with-attrs joinNode)
                                            (uber/add-nodes-with-attrs projNode)
                        		    (addEdges (uber/edges subg))
                        		    (uber/add-edges [(first projNode) (uber/dest t1Edge)])
                        		    (uber/remove-edges t1Edge)
                        		    (uber/add-edges [t1Node (first joinNode)])
                                            (updateGraphWithNewTableColumns tx gid (getTableName gnode) (first projNode)) 
                                            ))))


(defn saveColumn[params] (sql/update! ds :calc_column {:returntype (:datatype params) :length (Integer/parseInt (:length params)) :sql (:sql params) } {:name (:fname params) }))



(defn updateGraph ([g gid] (jdbc/with-transaction [tx ds]
                            (updateGraph g tx gid ))) 
		  ([g tx gid] (do 
                        (println (str "Inside updateGraph " ))
                        (println (str "GRAPH " (pr-str g)))
                        (println (str "Graph gid :" gid))
                        (println (collToString g))
                        (jdbc/execute! tx ["update sqlgraph set definition = ? where id = ?" (collToString g) gid ] ) g)))   

(defn getNodeName [node] (:name (second node)))

;;(defn addFirstTable [tx gid t1] (let [g (uber/digraph [t1 :op])]
(defn addFirstTable [tx gid t1] (let [ _ (println "Inside AddFirstTable")
                                      ginitial (getGraphFromDB gid) 
                                      op (first (uber/nodes ginitial))
                                      t1Node (createNewNode tx t1 gid) 
                                      t1N (first t1Node) 
                                      g (uber/add-nodes-with-attrs ginitial t1Node) ]
                           (do (println "Inside Do")
                               (println t1N)
                               (println t1Node)
                               (println op)
                               (println (str "graphfrom db " g))
                               (println "TableDef")
                               (println "---------BREAKPOINT----------------")
                               (updateGraph g tx gid)
                               (updateTableColumns tx gid (Integer/parseInt (name op)) (getTableColumns (getNodeName t1Node)))
			       (-> g
                                   (uber/add-edges [t1N op] )
                                   (updateGraph tx gid)))))

(defn listNodes [] (ualg/topsort (getGraphFromDB "testgraph")))

(defn fromClause [] (clojure.string/join ","  (map #(subs % 2 ) (filter #(clojure.string/starts-with? %  ":t" ) (listNodes)))))

(defn selectClause [] (clojure.string/join ","  (:tcols (getTableDef :op))))

(defn string-starts-with-any?
  [s prefixes]
  (some #(clojure.string/starts-with? s %) prefixes))

(defn filterClause [] (clojure.string/join " AND " (filter identity (map :where (map #(getTableFromDB %) (filter #(clojure.string/starts-with? %  ":filter" ) (listNodes)))))))

(defn prepareJoin[params] (map #(str (first %) " = " (second %)) params))

(defn joinClause [] (clojure.string/join " AND " (map #(clojure.string/join " AND " %) (map prepareJoin (map :jcondition (map #(getTableFromDB %) (filter #(clojure.string/starts-with? %  ":join" ) (listNodes))))))))
 

(defn whereClause [] (filter #(string-starts-with-any? %  '(":join", ":filter")) (listNodes)))

(defn getQuery [] (assoc {} :query (str "select " (selectClause) " from " (fromClause) " where " (clojure.string/join " AND " (list (joinClause) (filterClause))))))


(defn addTable [tx gid graph t1 t2]
	(let [ 
                t2Node (createNewNode tx t2 gid)
                t2N (Integer/parseInt (first t2Node))
		_ (println (str " t1 : " t1))
		_ (println (str " t2 : " t2N))
                _ (uber/pprint graph)
                t1Edge (uber/find-edge graph {:src (str t1)})
                t1Node (uber/src t1Edge)
		updGraph (getNewTable tx graph t2Node gid t1Node t1Edge)
                _ (println (str "t1Edge " t1Edge))
                _ (println (str "t1Node " t1Node))
		_ (println "MAIN GRAPH updated")
                _ (uber/pprint updGraph)
		_ (println "NOW creating new graph")
             ]
             updGraph))
           

(defn removeNodesFromDB [graph tx nodesToRemovedIds] (let [] (doseq [tid nodesToRemovedIds]
								(sql/delete! tx :gtable {:id tid})) graph))

(defn getNodesToRemove [t graph] (let [suc1 (first (uber/successors graph t))
  				       _ (println (str "suc1 " suc1))
                                       suc2 (first (uber/successors graph suc1))
 				       ]
                                (conj  [t] suc1 suc2)))

(defn getNodesToRemoveId [graph nodesToRemove] (let [result (atom [])]
                                                  (doseq [n nodesToRemove]
									(swap! result conj (uber/attr graph n :tid))) @result))

(defn deleteTable [gid tid]
	(let [ t (:tname (getTableFromDB (Integer/parseInt tid)))
	       _ (println (str " t : " t))
               graph (getGraphFromDB gid)
               src (getSibling graph t)
               nodesToRemove (getNodesToRemove t graph)
               nodesToRemoveIds (getNodesToRemoveId graph nodesToRemove)
               dest (first (uber/successors graph (last nodesToRemove)))
             ]
		 (jdbc/with-transaction [tx ds]  
                        (-> graph
			(uber/remove-nodes* nodesToRemove)
			(uber/add-edges [src dest])
                        (updateGraph tx gid) 
                        (removeNodesFromDB tx nodesToRemoveIds)))))

( comment
(defn isOpNode [n] (let [n1 (read-string n)] 
                            (if (and (not= (class n1)  clojure.lang.Keyword)
                                     (= :op (first n1))) true false)))
)

(defn integer-or-string-integer? [x]
  (or (integer? x)
      (and (string? x)
           (try
             (Integer/parseInt x)
             true
             (catch NumberFormatException e false)))))


(defn isOpNode [n] (let [_ (println (str "integer? " n " : "  (integer-or-string-integer? n))) 
                         table (if (integer? n) (getTableFromDB (Integer/parseInt n)) {:tname "dummy"})
                         _ (println (str "table " table))
                         _ (println (:tname table))
                        ] (if (and (integer-or-string-integer? n) (= "op" (:tname (getTableFromDB (Integer/parseInt n))))) true false)))

(defn isFirstTable [t1 t2] (let [t1op (isOpNode t1)
                                 t2op (isOpNode t2)
                                 _ (println (str "t1op" t1op))
                                 _ (println (str "t2op" t2op))
                                 _ (println (count t1))
                                 _ (println (count t2))
                                 _ (println (class t1))
                                 _ (println (class t2))
                                ]

                                (or (isOpNode t1) 
                                    (isOpNode t2))))


(defn processAddTable [gid t1 t2] (jdbc/with-transaction [tx ds] 
					(updateGraph (if (isFirstTable t1 t2) (addFirstTable tx gid t1) 
                                                                              (addTable tx gid (getGraphFromDB gid) t2 t1)) tx gid)))

(defn createGraph [name]  
 (let [ g 
{
  :a { :name ""  :v 0  :id 0 } 
  :n {
       1 {
           :na {
                 :tcols { } 
                 :name "op" 
                 :btype "output" 
               }
         }
     }
}
 ]
 (assoc-in g [ :a :name ] name )))

(defn nodecount [ g ] (count (:n g)))

(defn add-node [ g table-map ] 
   (assoc-in g [ :n (+ 1 (nodecount g)) ] { :na table-map } ))

(defn add-edge [ g edge ] 
   (assoc-in g [ :n (first edge) :e (second edge) ] {} ))

(defn add-edges [ g edges ] 
   (reduce add-edge g edges))

(defn top-sort [ g ] (alg/topsort (graph/digraph (into {} (map (fn [[k v]] [k (vec (keys (:e v)))]) (:n g))))))

(defn find-edge [ g node nodeType ] (let [ km  (get-in g [ :n node :e ])]
                                         (if km [ node (first (keys km))] km )))
(defn parent [ g node ] (-> g
                            (find-edge node "src")
                            second))

(defn tail [ seq e ] (drop (.indexOf seq e) seq))

(defn successors [ g node ] (tail  (top-sort g) (parent g node)))

(defn nodes-to-update [ g node ] ((successors g node)))

(defn add-first-table [ g table-map ] 
   (-> g 
       (add-node table-map)
       (add-edge [ 2 1 ])))


(defn x [hm] (into {} (map (fn [[k v]] [k (keys (:e v))]) hm )))

(defn createGraph [name] (jdbc/with-transaction [tx ds]                       
                        	(let [ ginit (uber/add-attr (uber/digraph :op) :op :name "op")
                                       sqlgraph (sql/insert!
                                                tx 
                                                :sqlgraph
                                                {:name name :dbname "1" :definition (collToString ginit)})
                                      _ (println (str "------- sqlgraph ------ : " sqlgraph))
                                      gid (:sqlgraph/id sqlgraph)
                                      _ (println (str "SQLGRAPH ID " gid))  
                                      op (uber/nodes ginit)
                                      op_final (createNewNode tx {:tname "op" :tcols []} gid)
                                      g  (uber/add-nodes-with-attrs (uber/remove-nodes* ginit op)  op_final)
                                      _ (println "------This is the final graph --------")
                                      _ (uber/pprint g) 
                                     ]
                                     (do (updateGraph g tx gid)
            				 (println "-----AFTER UPDATE --------")
                                         (uber/pprint g) 
                                         (assoc sqlgraph :sqlgraph/definition (collToString g))))))
                                 

;; (defn newGraph [name] (createGraph name (uber/digraph :op)))

(defn saveSpecialNode [nodeType name gid] ())

(defn addSpecialNode [gid graph tid nodeType] (let [ 
                                                _ (println (str "tid : " tid))
						t (:tname (getTableFromDB (Integer/parseInt tid)))
                                                _ (println (str "t : " t))
                                                fnode (str nodeType "-"  t)
					      _ (println (str "SAVING FILTER" fnode))
                                              fnode_id  (call (str "bitool.graph/add" (string/capitalize nodeType)) (str fnode) gid) 
                                                _ (println (str "fnode_id : " fnode_id))
                                          ;;    _ (uber/pprint graph)  
                                          ]
			      (-> graph
			      	(uber/add-edges [fnode (uber/dest (uber/find-edge graph {:src t }))])
                              	(uber/remove-edges (uber/find-edge graph {:src t}))
			      	(uber/add-edges [t fnode])
                                (uber/add-attr fnode :tid fnode_id)
				(updateGraph gid))))


 

;;([:proj-:t2 {:level :S, :longi -1, :lati 0}] [:join-:t2 {:level :S, :longi -2, :lati 0}] [:t1 {:level :L, :longi -3, :lati -1}] [:proj-:t3 
;; {:level :L, :longi -3, :lati 1}] [:join-:t3 {:level :L, :longi -4, :lati 1}] [:t2 {:level :L, :longi -5, :lati 0}] [:t3 {:level :L, :longi -5, :lati 2}])
;;
;;                        
;;			
;;
;;
;;				(-5 2)	
;;				      t3	(-4 1)		(-3 1)			
;;						join-t3		proj-t3			      
;;				      t2				 	join-t2		proj-t2 	op
;;				   (-5 0) 					(-2 0)		(-1 0)
;;				        			     t1
;;								(-3 -1)
;; [:t3 :join-t3] [:t2 :join-t3] [:join-t3 :proj-t3] [:proj-t3 :join-t2] [:t1 :join-t2] [:join-t2 :proj-t2] [:proj-t2 :op]
;;
;;
;;

