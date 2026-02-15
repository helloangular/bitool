(ns bitool.graph2
	(:require [bitool.db :as db]
                  [bitool.exceptions :as ex]
                  [bitool.utils :refer :all]
                  [bitool.macros :refer :all]
        	  [loom.graph :as graph]
        	[loom.alg :as alg]
        	[com.rpl.specter :as sp]
        	[next.jdbc :as jdbc]
        	[next.jdbc.sql :as sql]
        	[methodical.core :as m]
        	[clojure.string :as string]
                [clojure.set :as set]
        	[cheshire.core :as json]
        	[cljs.core :as cljs]
        	[clojure.walk :as walk]
        	[clojure.pprint :as pp]
                [taoensso.telemere :as tel]))

(def rectangles
	{
	    "T" ["J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "O"]
	    "V" ["J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "O"]
	    "P" ["J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "O"]
	    "Ap" ["T" "J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "O"]
	    "J" ["T" "V" "P" "A" "S" "Fi" "Fu" "J" "U" "O"]
	    "U" ["T" "V" "P" "A" "S" "Fi" "Fu" "J" "U" "O"]
        })
;; Tg -> Target , TgT Target Table

(def btype-codes
         {
  "function" "Fu",
  "aggregation" "A",
  "sorter" "S",
  "union" "U",
  "mapping" "Mp",
  "filter" "Fi",
  "table" "T",
  "join" "J",
  "projection" "P",
  "target" "Tg",
  "api-connection" "Ap",
  "graphql-builder" "Gq",
  "conditionals" "C",
  "output" "O"
        })

(comment example does not work
(defn ex-update-projection [g id params]
  (->args [g id params]
      update-rename-tcols
      update-excluded-tcols))
;; calls e.g. (update-rename-tcols g id params)
)

(defn assoc-in-multi [m path & kvs]
  (apply update-in m path assoc kvs))

(defn not-in? [elem coll]
  (not (some #(= elem %) coll)))

(defn add-to-map [m k v]
  (update m k #(if (nil? %) [v] (conj % v))))

(def gtest
  {:a {:name "" :v 0 :id 0}
   :n {1 {:na {:tcols [{:x "a1" :y "b1" :z ["r1" "r2"]}
                       {:x "a1" :y "b2" :z ["d1" "d2"]}]
               :name "O"
               :btype "O"}}}})

(defn update-or-add-tcol [g id endpoint_url pmap]
  (let [path [:n id :na :endpoints]
        endpoints (get-in g path)
        found? (seq (sp/select [sp/ALL #(= (:endpoint_url %) endpoint_url)] endpoints))]
    (if found?
      ;; Update existing element
      (sp/setval [:n id :na :endpoints sp/ALL #(= (:endpoint_url %) endpoint_url)] 
                 pmap 
                 g)
      ;; Add new element
      (sp/setval [:n id :na :endpoints sp/AFTER-ELEM] 
                 pmap 
                 g))))

;; Example 1: Update existing :y "b1"
;; (update-or-add-tcol gtest 1 "b1" ["new1" "new2"])
;; Updates the :z value for the element with :y "b1"

;; Example 2: Add new element for :y "b3"
;; (update-or-add-tcol gtest 1 "b3" ["x1" "x2"])

(defn remove-elements
  ([coll pred] (remove-elements coll pred nil))  ;; Default case when no extra args are given
  ([coll pred & rest]
   (cond
     (map? coll)  (into {} (remove (fn [[k v]] (apply pred k v rest)) coll))  ;; Remove key-value pairs that satisfy pred
     (set? coll)  (into #{} (remove #(apply pred % rest) coll))              ;; Remove elements from set
     (vector? coll) (vec (remove #(apply pred % rest) coll))                 ;; Remove elements from vector
     (sequential? coll) (doall (remove #(apply pred % rest) coll))            ;; Remove elements from list (realize seq)
     :else (throw (IllegalArgumentException. "Unsupported collection type")))))

;;(remove-elements {:a 1 :b 2 :c 3} (fn [_ v threshold] (> v threshold)) 2)  
;; => {:a 1, :b 2}

;;(remove-elements [1 2 3 4 5] (fn [n threshold] (> n threshold)) 3)  
;; => [1 2 3]

(defn createGraph [name] 
 (let [ g
{
  :a { :name ""  :v 0  :id 0 }
  :n {
       1 {
           :na {
                 :tcols {}
                 :name "O"
                 :btype "O"
               }
         }
     }
}
 ]
 (-> g
     (assoc-in [ :a :name ] name )
     (db/insertGraph))))

;; (defn version+[g] (update-in g [:a :v] inc))

(defn add-attr [g node attr val] (assoc-in g [:n node :na attr] val))

(defn add-attrs [g node attrs-map]
  (update-in g [:n node :na] merge attrs-map))

(defn attr [g node k] (get-in g [:n node :na k]))

(defn nodecount [ g ] (count (:n g)))

(defn node[id g] (get-in g [:n id :na]))

(defn node-name[id g] (:name (node id g)))

(defn btype[id g] (:btype (node id g)))

(defn tcols[g id] (let [node (node id g)]
      (if (some #{(btype id g)} ["J" "association"]) (concat (:t1C node) (:t2C node)) (:tcols node))))

(defn tmap[g node] (get-in g [:n node :na]))

(def node-keys {"filter" [:sql], "calculated" [:returntype :length :description :sql :datadef] , "aggregation" [:groupby :having]}) 

(defn add-keys-with-empty-values [m keys]
  (reduce (fn [acc key]
            (assoc acc key ""))
          m
          keys))

(defn create-node [ name btype ] (add-keys-with-empty-values { :name name :btype btype :tcols [] } (get node-keys btype)))

(defn prefix-table-columns[g table-map]
      (update table-map :tcols (fn [v] {(+ 1 (nodecount g)) (vec v)})))
  ;;    (update table-map :tcols #(map (fn [s] [(+ 1 (nodecount g)) s]) %)))

(defn add-node 
        ([ g table-map ]
         (assoc-in g [:n (+ 1 (nodecount g))] {:na table-map}))
        ([g c2 table-map ]
         (let [
                _ (println (str "c2 : " c2))
                _ (println (pp/pprint  table-map))
              ]
              (add-node g (assoc table-map :c c2))))
        ([g c2 table-map x y]
         (add-node g c2 (assoc table-map :x x :y y))))  

(defn update-node [g id params]
      (update-in g [:n id :na] merge params))

(defn add-unijoin [ g name type] (add-node g (create-node (str type "-" name) type)))

(defn add-target [ g ] (add-node g (create-node "target" "Tg")))

(defn add-mapping [ g ] (add-node g (create-node "mapping" "Mp")))

(defn add-projection [ g name ] (add-node g (create-node (str "projection-" name) "P")))

(defn add-edge [ g edge ]
   (assoc-in g [ :n (first edge) :e (second edge) ] {} ))

(defn delete-edge [ g edge ]
      (update-in g [:n (first edge) :e] dissoc (second edge)))

(defn add-edges [ g edges ]
   (reduce add-edge g edges))

(defmulti getData (fn [g item] (class g)))
      
(defmethod getData java.lang.String [g item]
        (let [ 
               _ ( println "CAlling M1")      
             ](get-in (db/getGraph (Integer. g)) [:n item :na])))
                                         
(defmethod getData clojure.lang.PersistentArrayMap [g item]
             (get-in g [:n item :na]))

(defn find-nodes-by-attr [g attr value]
  (let [nodes (:n g)]
    (into {}
          (filter (fn [[node-id node-data]]
                    (= (get-in node-data [:na attr]) value))
                  nodes))))

(defn find-node-by-attr [g attr value]
        (first (find-nodes-by-attr g attr value)))

(defn find-node-ids-by-attr [g attr value]
  (let [nodes (:n g)]
    (keep (fn [[node-id node-data]]
            (when (= (get-in node-data [:na attr]) value)
              node-id))
          nodes)))

(defn find-node-id-by-attr [g attr value]
	(first (find-node-ids-by-attr g attr value)))

(defn find-node-by-name [ g name ]
  (find-node-by-attr g :name name))

(defn find-node-id-by-btype [ g btype ]
  (find-node-id-by-attr g :btype btype))

(defn find-edge [ g node nodeType ] (let [ km  (get-in g [ :n node :e ])
                                         ;;  _ (println km) 
                                         ]
                                         (if km [ node (first (keys km))] km )))

(defn parent [ g node ] (-> g
                            (find-edge node "src")
                            second))

(defn contains-node? [node value]
  ;; Check if the nested structure contains the specific node as a key
  (and (map? value)
       (some #(= node (key %)) value)))

(defn filter-map-by-node [m node]
  (into {}
        (filter (fn [[_ v]] (contains-node? node (:e v))) m)))

(defn find-edges 
      ([g node] (let [ km  (get-in g [ :n node :e ])
                                         ;;  _ (println km) 
                                         ]
                                         (if km (map #(conj [node] %) (keys km)) km )))
      ([ g node nodeType ] 
             (keys (filter-map-by-node (:n g) node))))

(defn parents [ g node ] (map second (find-edges g node)))

(defn top-sort-all [ g ] (remove #(= (btype % g) "TgT") (alg/topsort (graph/digraph (into {} (map (fn [[k v]] [k (vec (keys (:e v)))]) (:n g)))))))





(defn getParentCols_tmp[g id]
      (let [
            parent (parent g id)
            _ (println (str "parent : " parent))
            parent-btype (btype parent g)
           ]
           (if (some #{parent-btype} ["filter" "sorter" "lookup" "association"])
               (tcols (parent g parent) g)
               (tcols parent g))))

;; (defn getFinalNode[g]
;;       (last (top-sort-all g)))

;; (comment
(defn getFinalNode[g]
      (let [
		op (find-node-id-by-btype g "O")
                mp_tg (parent g op)
                _ (prn-v mp_tg)
                btype (if mp_tg (btype mp_tg g) "none")
                _ (prn-v btype)
                tg (case btype
                         "Mp" (parent g mp_tg)
                         "Tg" mp_tg
                         "none" op)
           ]
           tg))
;;)

(defn getFinalNodeType[g]
      (btype (getFinalNode g) g))

(defn children [ g node ] (-> g
                              (find-edges node "dest")))
(comment
(defn find-child-by-attr[g node attr val]
      (let [
 		children (children g node)
           ]
           (filter (fn [g attr val] (= val (attr g ( )))))))
)

(defn tail [ seq e ] (drop (.indexOf seq e) seq))

(defn head [ seq e ] (take (.indexOf seq e) seq))


(defn predecessors [g node]
  "Performs BFS to get all nested children of a given node, returning a flat list."
  (loop [queue (children g node) ; Start with direct children
         result []]
    (if (empty? queue)
      result
      (let [current (first queue)
            children (children g current)]
        (recur (concat (rest queue) children) ;; Enqueue children
               (conj result current)))))) ;; Collect nodes

(defn connected-main-nodes[g]
      (let [
		finalNode (getFinalNode g)
                restNodes (predecessors g finalNode)
           ]
           (conj restNodes finalNode)))

(defn unconnected-nodes[g]
      (vec (clojure.set/difference (set (keys (:n g))) (set (connected-main-nodes g)))))
	  
(defn top-sort [ g ] (filter (complement (set (unconnected-nodes g))) 
                             (alg/topsort (graph/digraph (into {} (map (fn [[k v]] [k (vec (keys (:e v)))]) (:n g)))))))

(defn connected-graph[g]
      (sp/transform [:n]
                #(apply dissoc % (unconnected-nodes g))
                g))

(defn unconnected-graph[g]
      (sp/transform [:n]
                #(apply dissoc % (connected-main-nodes g))
                g))

(defn successors [ g node ] (if (= node (getFinalNode g)) [] (tail  (top-sort g) (parent g node))))

(defn getTcols[g id]
        (let [tid (first (filter #(not (some #{(btype % g)} ["filter" "sorter" "lookup" "association"] )) (predecessors g id)))
              _ (println (str "------TID : " tid))
             ]
             (tcols g tid)))

(defn remove-by-value [v elem]
  (vec (filter #(not= % elem) v)))

(defn getSibling[g node] (if node (let [parent (parent g node)
                                        _ (println (str "parent :" parent))
                                        children (children g parent)]
                                        _ (println children)
                                  (first (remove-by-value children node))) node))



(defn create-column [params]
  (conj (conj [] (:id params)) (mapv params [:technical_name :data_type :nullable :length :precision :scale :key :expression :transform :af :exclude :alias])))

(defn split_dot_second[str] 
      (let [
             - (println "split_dot_second")
             _ (println  str)
           ] 
      (second (string/split str  #"\."))))


(defn get-mapping-node[g t2]
      (first (filter #(= (btype % g) "Mp") (children g t2))))

(defn setMappingTarget[g mnode t2-table]
	(sp/multi-transform [:n mnode :na (sp/multi-path [:target (sp/terminal-val (:tcols t2-table))]
 							 [:mapping (sp/terminal-val [])])] g))

(defn map-tcols[t2 t2-table g]
           (aif (get-mapping-node g t2) (setMappingTarget g it t2-table) g))

(defn isOrphan[g node]
      (and (nil? (parent g node)) (nil? (children g node))))

(defn getOrphanNodes [g]
  (sp/transform
    [:n sp/ALL]
    (fn [[k v]]
      (if (isOrphan g k)
        [k v]
        sp/NONE))
    g)) 

(defn getOrphanNodeAttrs[g acc node]
      ;; (conj acc {"alias" (node-name node g), "y" (attr g node :y) , "x" (attr g node :x) ,"btype" (attr g node :btype) ,"parent"   (if (nil? (parent g node)) 0 (parent g node)) ,"id" node}))
       (conj acc {"alias" (node-name node g), 
                  "y" (attr g node :y) , 
		  "x" (attr g node :x) ,
		  "btype" (attr g node :btype) ,
		  "parent" (case (count (parents g node))
                                        0 0
                                        1 (first (parents g node))
                                        (parents g node))   ,
		  "id" node}))

(defn getOrphanAttrs[g]
      (reduce (partial getOrphanNodeAttrs g) [] (keys (:n g))))

(comment
(defn append-nodes-tcols
  "Update the data graph by appending the specified columns to the :tcols value of the given nodes.
  Parameters:
  - data: The nested data map.
  - node-keys: Sequence of nodes to update.
  - columns: Sequence of columns to apprintend to :tcols."
  [g node-keys columns]
  (let [
         _ (println "=============COLUMNS============")
         _ (println columns)
       ]
  (update g :n
          (fn [nodes]
            (reduce
             (fn [acc node-key]
               (if-let [node (get acc node-key)]
                 (assoc acc node-key
                        (update-in node [:na :tcols]
                                   #(concat (or % []) columns))) ; Apprintend columns to :tcols
                 acc))
             nodes
             node-keys)))))
)

(defn- tcols->map [tcols]
  (cond
    (nil? tcols) {}
    (map? tcols) tcols
    (sequential? tcols) (into {} tcols)   ; handles ([2 [...]]) or [[2 [...]] ...]
    :else (throw (ex-info "Unsupported :tcols shape"
                          {:type (type tcols) :value tcols}))))

(defn- columns->map [columns]
  (cond
    (nil? columns) {}
    (map? columns) columns
    ;; if it's a single entry pair like [2 [...]]
    (and (sequential? columns)
         (= 2 (count columns))
         (not (sequential? (first columns)))) ; first is key, not another pair
    {(first columns) (second columns)}
    ;; if it's a seq of entries like ([2 [...]] [3 [...]])
    (sequential? columns) (into {} columns)
    :else (throw (ex-info "Unsupported columns shape"
                          {:type (type columns) :value columns}))))

(defn append-nodes-tcols
  "Append columns into [:na :tcols], ensuring :tcols is stored as a map keyed by table-id (or similar)."
  [g node-keys columns]
  (let [columns-map (columns->map columns)]
    (update g :n
            (fn [nodes]
              (reduce
                (fn [acc node-key]
                  (if-let [node (get acc node-key)]
                    (assoc acc node-key
                           (update-in node [:na :tcols]
                                      (fn [tcols]
                                        (let [m (tcols->map tcols)]
                                          (reduce-kv
                                            (fn [m k cols]
                                              ;; ensure we append vectors/seqs of cols
                                              (update m k (fnil into []) (or cols [])))
                                            m
                                            columns-map)))))
                    acc))
                nodes
                node-keys)))))


(defn replace-cols [src cols]
  (reduce
    (fn [acc [tid [name & _ :as col-data]]]
      (sp/setval
        [sp/ALL #(let [[t [n & _]] %] (and (= t tid) (= n name)))]
        [tid col-data]
        acc))
    cols
    src))

(defn update-nodes-tcols
  "Update the data graph by appending the specified columns to the :tcols value of the given nodes.
  Parameters:
  - data: The nested data map.
  - node-keys: Sequence of nodes to update.
  - columns: Sequence of columns to apprintend to :tcols."
  [g node-keys columns]
  (update g :n
          (fn [nodes]
            (reduce
             (fn [acc node-key]
               (if-let [node (get acc node-key)]
                 (assoc acc node-key
                        (update-in node [:na :tcols]
                                   #(replace-cols columns %))) ; Apprintend columns to :tcols
                 acc))
             nodes
             node-keys))))

(defn find-nodes-btype
  "Find nodes in the data map under :n where :btype matches one of the target-values.
  Optionally restrict search to specific node keys provided in node-keys."
  [g target-values node-keys]
  (->> (get g :n)
       (filter (fn [[k v]]
                 (and (some #{k} node-keys)                  ; Restrict to specific nodes
                      (some #{(get-in v [:na :btype])}       ; Match :btype to target values
                            target-values))))
       (map first)))  ; Extract the matching keys

(defn nodes-to-update[g start-node]
      (find-nodes-btype g ["O" "Tg" "P" "Fi" "S" "A" "Fu"] (successors g start-node)))

(defn find-output[g start-node]
      (first (find-nodes-btype g ["O"] (successors g start-node))))

(defn update-output[g id k v]
      (assoc-in g [:n (find-output g id) :na k id] v))

(defn get-agg-columns [columns]
  (mapv #(select-keys % [:tid :technical_name :af]) 
     (first (sp/select [(sp/filterer #(some? (:af %)))] columns))))

(defn update-agg-column[cols col]
    (sp/setval  [sp/ALL #(and (= (first %) (:tid col)) (= (first (second %)) (split_dot_second (:technical_name col)))) (sp/nthpath 1) (sp/nthpath 9) ] (:af col) cols))

(defn update-agg-columns[g id columns]
      (reduce #(update-agg-column  %1 %2) (tcols g id) (get-agg-columns columns)))

(defn update-agg-tcols[g id columns]
      (assoc-in g [:n id :na :tcols] (update-agg-columns g id columns)))

(defn update-having[g id having]
      (-> g
          (aassoc-in [:n id :na :having] having)
          (update-output id :having having)))

(defn get-groupby-columns [columns]
      (mapv first (map vals (mapv #(select-keys % [:technical_name]) (filter #(not (contains? % :function)) columns)))))

(defn get-groupby[columns]
      (clojure.string/join "," (get-groupby-columns columns)))

(defn update-groupby[g id group-by]
      (let [groupby (get-groupby group-by)]
      		(-> g
                    (aassoc-in [:n id :na :groupby] groupby)
                    (update-output id :groupby groupby))))

(defn update-agg[g id params]
      (let [
             columns (:items params)
             group-by (:group params)
           ]
      	   (update-agg-tcols (update-groupby (update-having g id (:having params)) id group-by) id columns)))

(defn col-matcher [col]
  (sp/pred #(and (= (first %) (:tid col))
                 (= (first (second %)) (split_dot_second (:technical_name col))))))

(defn save-filter[g id params]
      	(-> g
            (update-node id params)
            (update-output id :where (:where params))))

(defn save-sorter[g id params]
      	(-> g
            (update-node id params)
            (update-output id :sorters (:sorters params))))

(defn save-join[g id params]
      	(-> g
            (update-node id params)))

(defn link-mapping[g id params]
        (let [
		mid (nodecount g)
                tcols (nodes->columns id (:selected_nodes params))
                g1 (sp/setval [:n id :e mid] (assoc params :endpoint_url (:endpoint_url params)) g)
             ]
	        (add-attrs g1 mid {:source tcols :target tcols :mapping []})))

(defn link-target[g id params]
      (let [
             tid (nodecount g)
             mid (- tid 1)
             g1 (add-edge g [mid tid])
           ]
           (add-attrs g1 tid (select-keys params [:table_name :create_table :truncate]))))

(defn create-target[g id endpoint_url params]
      (-> g
          (add-mapping)
          (link-mapping id params)
          (add-target)
          (link-target id params)))

(defn endpoint-target-exists [g id endpoint_url]
     (let [edge-path [:n id :e]  ;; Changed from [:n id :na :e]
           edges (get-in g edge-path {})]
       (sp/select-one
         [sp/ALL
          (sp/if-path [sp/LAST :endpoint_url (sp/pred= endpoint_url)]
            sp/FIRST)]
         edges)))

(defn upsert-edge [g id endpoint_url params]
      (let [
		edge-id (endpoint-target-exists g id endpoint_url)
                _ (prn-v edge-id)
                _ (prn-v endpoint_url)
                _ (prn-v params)
           ]
           (if edge-id
           	(sp/transform [:n id :e edge-id] #(merge % params) g)
                (create-target g id endpoint_url params))))

(defn save-api[g id params]
        (let [
		_ (println "==========save-api============")
                _ (prn-v params)
             ]
      	(-> g
            (update-or-add-tcol id (:endpoint_url params) (select-keys params [:endpoint_url :selected_nodes :table_name :create_table :truncate]))
            (update-node id (select-keys params [:specification_url :api_name :truncate :create_table]))
            (upsert-edge id (:endpoint_url params) params))))


(defn convert-col[col]
      [(:tid col) (mapv col [:technical_name :data_type :is_nullable :character_maximum_length :numeric_precision :numeric_scale
         :key :exp :transform :af :excluded :alias ])])  

(defn update-column[cols col]
    (let
         [
		_ (println "--------COLS---------")
		_ (println cols)
		_ (println "--------COL---------")
		_ (println col)
         ]
      (sp/setval  [sp/ALL (col-matcher col)] (convert-col col) cols)))

(defn update-columns[g id columns]
      (reduce #(update-column  %1 %2) (tcols g id) columns))

(defn update-projection[g id params]
      (let [
             _ (println "update-tcols")
             _ (pp/pprint g)
             - (println (str "id : " id)) 
             - (pp/pprint (str "params : " params)) 
           ]
      (assoc-in g [:n id :na :tcols] (update-columns g id (:items params)))))

(defn update-projections[g id params]
      (reduce #(update-projection %1 %2 params) g (cons id (nodes-to-update g id)))) 

(defn update-mapping[g id mapping]
      (assoc-in g [:n id :na :mapping] mapping))

(defn update-target[g id params]
      (update-in g [:n id :na] merge params))

(defn update-table-cols [ g 
                          start-node 
                          table 
                        ]
      (let [
                          _ (println "******INSIDE******* update-table-cols")
                          _ (println (str "start-node : " start-node)) 
                          _ (println (str "table : " table)) 
           ]
      (append-nodes-tcols g (find-nodes-btype g ["O" "Tg" "P" "Fi" "S" "A" "Fu"] (successors g start-node)) (:tcols table))))

(defn populate-join-cols [g join t1 t2]
      (assoc-in g [:n join :na] (-> (node join g) 
          (assoc :t1cols (:tcols t1) :t2cols (:tcols t2) :join-cols [])
          )))
          ;; (dissoc :tcols))))

(defn update-btype[t-map]
      (let [
             _ (println (str "t-map : " t-map))
             alias (:name t-map)
             _ (println (str "alias : " alias)) 
             btype (get btype-codes (anil? (:btype t-map) alias)) 
             _ (println (str "btype : " btype)) 
           ]
           (assoc t-map :btype (if btype btype "T"))))

(defn add-single-node[gid c t-name btype x y]
	(let [
                _ (println (str "BTYPE :: " btype))
                _ (println "BTYPE :::::" )
                g (db/getGraph gid)
;;		t-map (update-btype (assoc (db/get-table c t-name) :btype btype))
		t-map (update-btype {:name t-name :btype btype})
;;		t-map (db/get-table c t-name)
            ;;    _ (println (str "t-map : " t-map)) 
              ;;  prefix-t-map (prefix-table-columns g t-map)
               ;; _ (println (str "prefix-t-map : " prefix-t-map)) 
             ]
             (-> g 
              ;;   (add-node c prefix-t-map x y)
                 (add-node c t-map x y)
                 (db/insertGraph))))

(defn add-first-table [ g c2 table-map ]
   (let [
		prefix-table-map (prefix-table-columns g table-map) 
                g1 (add-node g c2 prefix-table-map)
                t2 (nodecount g1)
	] 
   (-> g1
       (add-edge [t2 1])
       (update-table-cols t2 prefix-table-map)
       (db/insertGraph))))


;; {:tname :join-:tAddress, :j1Tables [":tCatalog"], :j2Tables [":tAddress"], :j1Columns ["cat_id" "item_id" "item_name" "price"], :j2Columns ["cust_id" "first_name" "city" "zipcode"]
;;  :join-columns [ [ 1 2 ] [ 3 4 ] ] :join-type "inner" }

;; Storage:  {:name :join-:tAddress,  :join-columns [ [ “id” “id” ] [ “dept” “dept_id” ] ] :join-type "inner" :distinct true }
;; Json :  {:name :join-:tAddress, :j1T  “customer” , :j2T  “address” ,  :j1C [ “id” “name” ] :j2C [ “dept_id” “dept_name” ] :join-columns [ [ “id” “id” ] [ “dept” “dept_id” ] ] :join-type "inner" :distinct true }

(defn get-join [g j]
   (let [ join (get-in g [:n j :na])
          children (children g j)
          c1-node (node (first children) g)
          c2-node (node (second children) g) ]
        (-> join
            (assoc :j1T (:name c1-node))
            (assoc :j2T (:name c2-node))
            (assoc :j1C (:tcols c1-node))
            (assoc :j2C (:tcols c2-node))
            (assoc :join_columns []))))

(defn unijoin-add-table[g uid t1 type]
	(update-node g uid (-> (node uid g) 
                               (update :tables #(conj % t1))
                   	       (update (keyword type)  #(assoc % t1 (mapv second (tcols g t1)))))))

(defn unijoin-add-first-table[g uid t1 type]
	(unijoin-add-table (update-node g uid (assoc (node uid g) :tables [] (keyword type) {})) uid t1 type))

(defn set-join[g j]
      (assoc-in g [:n j :na] (get-join g j)))

(defn join-table [ g t1 c2 t2-table jtype] (let [ t2-name (:name t2-table)
                                          _ (println (str "t2-name" t2-name))
					  _ (println (str "t2-table" t2-table))
 					  tmp (find-edge g t1 "src")
                                          _ (println (str "tmp" tmp))
                                          dest (second tmp) 
                                          _ (println (str "dest" dest))
                                          prefix-table-map (prefix-table-columns g t2-table)
                                          _ (println "PREFIX_TABLE_MAP")
                                          _ (pp/pprint prefix-table-map)
                                          g2 (add-node g c2 prefix-table-map)
                                          _ (println  "g2" )
 					  _ (pp/pprint g2)
                                          t2 (nodecount g2)
                                          _ (println (str "t2" t2))
                                          g2-join (add-unijoin g2 t2-name jtype)
                                          _ (println  "g2-join" )
 					  _ (pp/pprint g2-join)
                                          join-t2 (nodecount g2-join)
                                          _ (println  (str "join-t2 " join-t2))
                                          g2-proj (add-projection g2-join t2-name)
                                          _ (println  "g2-proj" )
 					  _ (pp/pprint g2-proj)
    					  proj-t2 (nodecount g2-proj) 
                                          _ (println  (str "proj-t2 " proj-t2))
                                        ]
                                        (-> g2-proj
                                            (delete-edge tmp)
    					    (add-edges [[t2 join-t2] [t1 join-t2] [join-t2 proj-t2] [proj-t2 dest]])
                                            ;; (set-join join-t2)
 					    (unijoin-add-first-table join-t2 t1 jtype)
 					    (unijoin-add-table join-t2 t2 jtype)
                                            (append-nodes-tcols [ proj-t2 ] (get-in g2-proj [:n t1 :na :tcols]))
					    (update-table-cols t2 prefix-table-map)
					    (db/insertGraph))))


(defn validate-connect[g t1 t2]
    (let
	[
	  btype1 (btype t1 g)
                _ (println (str "btype1 : " btype1 )) 
	  btype2 (btype t2 g)
                _ (println (str "btype1 : " btype2 )) 
	]
        (if (some (set (get rectangles btype1)) [btype2]) true false)))

   
(defn connect-single-node [gid t1 t2]
   (let [
                _ (println (str "t1 class : " (class t1) " t2 class : " (class t2)))
		g (db/getGraph gid)
                _ (println (str "VALID : " (validate-connect g t1 t2)))
	] 
       (if (validate-connect g t1 t2)
           (-> g
               (add-edge [t1 t2])
             ;;  (update-table-cols t2 (tcols g t1))
               (db/insertGraph)) g)))

(defn isFirstTable[t1 t2] (or (= 1 t1) (= 1 t2)))

(defn is-union[g t1]
     (= "U" (btype t1 g)))

(defn not-empty-string? [s]
  (and (string? s) (not (clojure.string/blank? s))))

(defn add-assoc[colmap column]
      (let [ af (nth column 9)
             _ (println "--------------ADD-ASSOC-----------------")
	     _ (pp/pprint colmap)
             _ (pp/pprint column)
           ]
	   (if (and (not-empty-string? af) (not= af "None")) (assoc colmap "af" af) colmap)))

(defn column_row_details [g table_id table_prefix row]
    (let
         [
                _ (println "-------------------My ROW ------------")
		_ (pp/pprint row)
                _ (println "-------------------End ROW ------------")
                _ (prn-v table_id)
                _ (prn-v table_prefix)
         ]
    {
     "tid" table_id
     "business_name"  (str table_prefix "." (:column_name row))
     "technical_name" (str table_prefix "." (:column_name row))
     "semantic_type"  (:semantic_type row)
     "data_type"      (:data_type row)
     "is_nullable"    (:is_nullable row)
     "character_maximum_length"    (:character_maximum_length row)
     "numeric_precision"    (:numeric_precision row)
     "numeric_scale"    (:numeric_scale row)
     "key"            (:key row)
     "column_type"    "column"
     "exp"            (:exp row)
     "transform"      (:transform row)
     "af"             (:af row)
     "excluded"       (:excluded row)
     "alias"          (:alias row)
               }))

(defn column_details[g row]
    (let [
          _ (prn-v row)
	  newrow (second row)
          table_id (first row)
          _ (println (str "table_prefix" table_id))
          _ (println (str "node-name" (node-name table_id g)))          
          table_prefix (node-name table_id g) 
          _ (println (str "table_prefix" table_prefix))
         ]
    (column_row_details g table_id table_prefix newrow)))

(defn item_master[id alias btype tmap]
      (let [
           	item { 	"id" id,                          
        		"alias" alias,                    
        		"parent_object" "Table_view",     
        		"business_name" alias,            
        		"technical_name" alias,           
        		"btype" btype	}
           ]
           (case btype 
  		 "A" (assoc item "having" (:having tmap))
  		 "Fi" (assoc item "expression" (:sql tmap))
  		 "Tg" (merge item (select-keys tmap [:truncate :create_table :c :connection :table_name]))
 		 item)))

(defn item_columns
  ([g columns] into [] (map #(column_details g %) (vec (for [[k v] columns, item v] [k item])) ))
  ([g columns table_id table_prefix] (map #(column_row_details g table_id table_prefix %) )(vec (for [[k v] columns, item v] [k item])) ))

(defn get-by-table-column [cols tid column_name]
        (let [
		_ (println " get-by-table-column")
		_ (println " cols")
		_ (pp/pprint cols)
		_ (println " tid")
		_ (println  tid)
		_ (println " column_name")
		_ (println  column_name)
             ]
       	(first (sp/select [sp/ALL #(and (= (first %) tid) (= (first (second %)) column_name))] cols))))

(comment 
test get-by-table-column

(def txmap
{
  :sourceTid 2
, :targetTid 7
, :sourceColumn "sqldi.id"
, :targetColumn "employee.id"
, :transformations ["TRIM (No parameters)" "TO_DATE (No parameters)"]
})

source
(def cols 
'([2 ["id" "integer" "NO" nil 32 0 "YES" "" "" "" ""]]
     [2 ["name" "text" "YES" nil nil nil "NO" "" "" "" ""]]
     [3 ["id" "integer" "NO" nil 32 0 "YES" "" "" "" ""]]
     [3 ["version" "integer" "NO" nil 32 0 "YES" "" "" "" ""]]
     [3 ["name" "text" "YES" nil nil nil "NO" "" "" "" ""]]
     [3 ["definition" "text" "YES" nil nil nil "NO" "" "" "" ""]])
)

target
(def cols 
'(["id" "integer" "NO" nil 32 0 "YES" "" "" "" ""]
     ["name" "text" "YES" nil nil nil "NO" "" "" "" ""]
     ["score" "integer" "YES" nil 32 0 "NO" "" "" "" ""])
)
(def cols 
'([7 ["id" "integer" "NO" nil 32 0 "YES" "" "" "" ""]]
     [7 ["name" "text" "YES" nil nil nil "NO" "" "" "" ""]]
     [7 ["score" "integer" "YES" nil 32 0 "NO" "" "" "" ""]])
)
(def g (db/getGraph 835))

mapping node 7

)

(defn get-map-column[g cols txmap tid name]
      (column_details g (get-by-table-column cols (tid txmap) (split_dot_second (name txmap)))))

(defmulti getData (fn [g item] (class g)))

(defmethod getData java.lang.String [g item]
	(let [
               _ ( println "CAlling M1")
             ](get-in (db/getGraph (Integer. g)) [:n item :na])))

(defmethod getData clojure.lang.PersistentArrayMap [g item]
	(let [
               _ ( println "CAlling M2")
             ]
	(get-in g [:n item :na])))

(defn get-item [id g]
     (let [
            _ (println (str "id " (class id) " g " (class g)))
            _ (println (pp/pprint g))
            tmap (getData g id) 
            _ (println "TMAP")
            _ (pp/pprint tmap)
            columns (:tcols tmap)
            _ (println "COLUMNS")
            _ (pp/pprint columns)
            name (:name tmap)
            alias (if (= name "O") "Output" name)
            btype (:btype tmap)
            _ (println columns) 
          ]
        (assoc (item_master id alias btype tmap) "items" (item_columns g columns))))


(defn get-target-item [id g]
     (let [
            _ (println (str "id " (class id) " g " (class g)))
            _ (println (pp/pprint g))
            tmap (getData g id) 
            _ (println "TMAP")
            _ (pp/pprint tmap)
            columns (:tcols tmap)
            _ (println "COLUMNS")
            _ (pp/pprint columns)
            name (:name tmap)
            alias name
            btype (:btype tmap)
            _ (println columns) 
          ]
        (assoc (item_master id alias btype  tmap) "items" (item_columns g columns))))
        ;; (assoc (item_master id alias btype  tmap) "items" (item_columns g columns id (:table tmap)))))

(defn mapping_items[g tmap]
      (let [
              mapping (:mapping tmap)
              scols (:source tmap)
              tcols (:target tmap)
              _ (println "mapping")
              _ (pp/pprint mapping)
              _ (println "scols")
              _ (pp/pprint scols)
              _ (println "tcols")
              _ (pp/pprint tcols)
           ]
      (mapv #(assoc {} 	
                "source" (:source %)
                "target" (:target %)
          	"transform" (:transform %)) mapping)))


(defn make-map[k v]
      {k v}) 

(defn make-vec[k v]
      [k v]) 

(defn add-key [k vec]
    (let [
 	   _ (prn-v k)
           - (prn-v vec)
           temp  (mapv #(make-vec k %) vec)
           _ (prn-v temp)
         ]
    	 (mapv #(make-map k %) vec)))

(defn setMappingTcols
  ([g mid sid tid]
   (let [source-tcols (tcols g sid)
         target-tcols (tcols g tid)]
     (tel/log! {:level :info 
                :msg "About to assoc-in"
                :data {:mid mid
                       :sid sid
                       :tid tid
                       :source-tcols source-tcols
                       :source-type (type source-tcols)
                       :source-first (first source-tcols)
                       :target-tcols target-tcols
                       :target-type (type target-tcols)}})
     (-> g   
         (assoc-in-when sid [:n mid :na :source] source-tcols)   
         (assoc-in-when sid [:n mid :na :sourceid] sid)   
         (assoc-in [:n mid :na :target] target-tcols)
         (assoc-in [:n mid :na :targetid] tid)
         (assoc-in [:n mid :na :mapping] []))))
   ([g mid sid tid src_attrs]
    (-> g
        (setMappingTcols mid sid tid)
        (assoc-in [:n mid :na :src_attrs] src_attrs))))

(defn getTarget[id g target]
      (let [
                _ (println "Target -----------")
		_ (println target)
		row (first target)
		_ (println row)
                _ (println (count row))
           ]
      	   (if (= (count row) 2)
           	(item_columns g target)
          	(item_columns g target id (:table (node (first (successors g id)) g))))))

(defn get-mapping-item[id g]
        (let [
               tmap (getData g id)
               name (:name tmap)
               table_prefix (:table (node (first (successors g id)) g))
            alias name
            btype (:btype tmap)
               _ (println "TMAP")
               _ (println tmap)
             ]
        (assoc (item_master id alias btype tmap) "source" (item_columns g (:source tmap)) "target" (getTarget id g (:target tmap)) "mapping" (mapping_items g tmap))))

(defn addMappingGraph[g t1]    ;; returns graph g
	(let [
                _ (println "===================ADD MAPPING ==================")
		g1 (add-mapping g)
		mapping_id (nodecount g1)
 		edge (find-edge g1 t1 "src")
                target_id (second edge)
                g (-> g1
                      (delete-edge edge)
                      (add-edges [[t1 mapping_id] [mapping_id target_id]])
                      (setMappingTcols mapping_id t1 target_id false))
             ]
             g ))

(defn addMapping[g t1]
	(let [
                g (addMappingGraph g t1)
                mapping_id (nodecount g)
                cp (db/insertGraph g)
 		sp (getOrphanAttrs cp)
                rp (get-mapping-item mapping_id g)
             ]
             {:cp cp :rp rp :sp sp}))

;; (defn api-target[g api_id endpoint_url t2-name]

(defn target-table[g tid c2 t2-table]
	(let [ t2-name (:name t2-table)
               - (println "-----------------TARGET-TABLE------------------")
               _ (println (str "t2-name" t2-name))
               _ (println (str "t2-table" t2-table))
	       prefix-table-map (assoc (prefix-table-columns g t2-table) :btype "TgT")
   	       _ (println (str "prefix-table-map" prefix-table-map))
               g1 (if (nil? c2) (add-node g prefix-table-map) (add-node g c2 prefix-table-map))
               mid (first (children g1 tid)) 
               _ (println (str "mid" mid))
               mapping_exists (= "Mp" (btype mid g1))
               _ (println (str "mapping_exists" mapping_exists))
               t2 (nodecount g1)
               _ (prn-v t2)
               gmap (if (not mapping_exists) (addMappingGraph g1 mid) g1)
               _ (prn-v tid)
               mid (first (children gmap tid))
               _ (prn-v mid)
               oid (first (children gmap mid)) 
               _ (prn-v oid)
               _ (println "==========MY GMAP ==============")
               _ (prn-v  gmap )
             ;;  _ (pp/pprint gmap)
               _ (println (str "t2" t2))
               gmap1 (setMappingTcols gmap mid (if mapping_exists nil oid) t2)
		_ (tel/log! :info (str "DEBUG mid=" mid " type=" (type mid)))
		_ (tel/log! :info (str "DEBUG g[:n]=" (:n g)))
		_ (tel/log! :info (str "DEBUG g[:n mid]=" (get-in g [:n mid])))
              ]
              (->> gmap1
                  (sp/multi-transform [:n tid :na (sp/multi-path [:c (sp/terminal-val c2)]
						      	      [:tcols (sp/terminal-val (:tcols prefix-table-map))])])
		  (map-tcols tid prefix-table-map)
                  (db/insertGraph))))

(defn txunion[key-fn val-fn m]
  (let [
         _ (println "---------------txunion--------------")
         _ (prn-v key-fn)
         _ (prn-v val-fn)
         _ (prn-v m)
       ]
  	(into {} (map (fn [[k v]] [(key-fn k) (mapv val-fn (add-key k v))]) m))))       

(defn get-unijoin-item[btype id g]
        (let [
               _ (println "INSIDE GET UNION/JOIN ITEM")
               tmap (getData g id)
               name (:name tmap)
            alias name
            btype (:btype tmap)
               _ (println "TMAP")
               _ (println tmap)
               _ (prn-v btype)
            items (txunion #(node-name % g) #(column_row_details g (first (keys %)) (node-name (first (keys %)) g) (first (vals %))) ((keyword btype) tmap))
               _ (prn-v items)
             ]
        (assoc (item_master id alias btype  tmap) "tables" (mapv #(node-name % g) (:tables tmap)) "join_items" (anil? (:join_items tmap) []) "items" items )))



(defn create-unijoin[g t1 type]
	(let [
		g1 (add-unijoin g (node-name t1 g) type)
		id (nodecount g1)
		edge (find-edge g1 t1 "src")
                g (-> g1
 		      (delete-edge edge)
                      (add-edges [[t1 id] [id (second edge)]])
                      (unijoin-add-first-table id t1 type))
                      cp (db/insertGraph g)
 		      sp (getOrphanAttrs cp)
                      rp (get-item id g)
             ]
             {:cp cp :rp rp :sp sp}))

(defn setTcols
      ([g id]
       (assoc-in g [:n id :na :tcols] (getTcols g id)))
      ([g sid tid]
       (assoc-in g [:n tid :na :tcols] (get-in g [:n sid :na :tcols]))))

(defn addSpecialNode [g tid nodeType] (let [
						_ (pp/pprint g)
						_ (println (str "tid:" tid))
						_ (println (str "Node Name " (node-name tid g)))
                                                fnode (str nodeType "-"  (node-name tid g))
 						graph (add-node g (create-node fnode nodeType))
						_ (pp/pprint graph)
 						id (nodecount graph)
						_ (println (str "Node id:" id))
 						edge (find-edge graph tid "src")
						_ (println "edge:")
						_ (print edge)
                                     		g (-> graph
                                		  (delete-edge edge)
                                		  (add-edges [[tid id] [id (second edge)]])
  						  (setTcols id))
                                		cp (db/insertGraph g)
 						sp (getOrphanAttrs cp)
  						rp (get-item id g) 
                                   	      ] 
                                   	  {:cp cp :rp rp :sp sp}))




(defn addTarget[g t1]
	(let [
		g1 (add-target g)
		id (nodecount g1)
                g (-> g1
                      (add-edge [t1 id])
                      (setTcols id))
                cp (db/insertGraph g)
 		sp (getOrphanAttrs cp)
                rp (get-item id g)
             ]
             {:cp cp :rp rp :sp sp}))

(defn addRightClickNode[gid tid nodeType]
	(let [
		g (db/getGraph gid)
	     ]	
		(case nodeType
 			("U" "J") (create-unijoin g tid nodeType)
                        "Tg" (addTarget g tid)
                        "Mp" (addMapping g tid)
			(addSpecialNode g tid nodeType)))) 
 
(defn unijoin-table[g uid c2 t2 t2-table type]
	(let [ t2-name (:name t2-table)
               - (println "-----------------UNION-TABLE------------------")
               _ (println (str "t2-name" t2-name))
               _ (println (str "t2-table" t2-table))
	       prefix-table-map (prefix-table-columns g t2-table)
               g2 (if (nil? c2) g (add-node g c2 prefix-table-map)) ; if c2 is nil the node is already added ( comes from rect-join)
               _ (println  "g2" )
               _ (pp/pprint g2)
               src (anil? t2 (nodecount g2))
               _ (println (str "src" src))
              ]
              (-> g2
                  (add-edge [src uid])
		  (unijoin-add-table uid src type)
                  (db/insertGraph))))


(defn rect-join[gid src dest]
	(let [
                _ (println "Inside rect-join")
                _ (println (str "src : " src))
                _ (println (str "dest : " dest))
		g (db/getGraph gid)
                _ (println (str "validate : " (validate-connect g src dest)))
                c (attr g src :c)
	     ]
             (if (validate-connect g src dest)
		 (let [
			g1 (case (btype dest g)
                       		"T" (target-table g dest c (tmap g src))
                                "U" (unijoin-table g dest nil src (tmap g src) "U")
                                "J"  (unijoin-table g dest nil src (tmap g src) "J")
                     		(add-edge g [src dest]))
                      ]
                      (-> g1 
                          (update-table-cols src (tmap g src))
                          (db/insertGraph)))
                  (throw (ex/valid-err 
            			"Email is required" 
            			{:field :email})))))    ;; else

;; assign :layer attribute to a node starting with output/target node as layer 0 and children assigned 0 + 1	
(defn layer_node[g n]
	(if-let [parent (parent g n)
                ]
                (add-attr g n :layer (+ (attr g parent :layer) 1))
                (add-attr g n :layer 0)))
              
;;;; assign :layer attribute to all nodes in graph using reduce 
(defn layer_graph[g]
	(reduce layer_node g (reverse (top-sort g)))) 

;; get layer index of parent
(defn parent-layer-idx[g n]
      (or (attr g (parent g n) :layer_idx) 0))

;; find nodes in graph which dont have :layer_idx assigned ( nil )
(defn no-layer-idx[g nodes]
      (filter #(= nil (attr g % :layer_idx)) nodes))

;; get :layer attribute of node 
(defn layer[g n]
      (attr g n :layer))

;; build a map of key: :layer value: vector of nodes.Assumes layer is assigned when a node is added on UI
(defn build-layer-map[g]
(reduce (fn [acc x]
          (add-to-map acc (layer g x) x))
        {}
        (reverse (top-sort g))))

;; assign :layer_idx to nodes in layer l 
(defn build-layer-id-l[g layer_map l]
      (let [
		unassigned_nodes (sort-by #(parent-layer-idx g %) (no-layer-idx g (get layer_map l)))
		max_idx (apply max (mapv #(or (attr g % :layer_idx) 0) unassigned_nodes))
                _ (println (str "unassigned_nodes" unassigned_nodes))
                _ (println (str "max_idx" max_idx))
                _ (println (str "max_idx coll" (mapv #(or (attr g % :layer_idx) 0) unassigned_nodes)))
	   ]
	   (reduce (fn [acc [k v]] 
		       	(add-attr acc k :layer_idx v)) 
                       	g 
 			(map-indexed (fn[idx node] [node (+ max_idx 1 idx)]) unassigned_nodes))))

;;;; add :layer_id to al layers
(defn build-layer-idx[g]
        (let [
		layer-map (build-layer-map g)
             ]
		(reduce (fn[acc x]
				(build-layer-id-l acc layer-map x)) g (sort (keys layer-map)))))


;; ---------------------- End Graph Builder -------------------------


;; ---------------------- Start Tree --------------------------------

(defn build-tree
  [current-tree levels conn-id path]
  (if (empty? levels)
    current-tree
    (let [current-level (first levels)
          _ (println (str "level : " current-level))
          _ (println (str "path : " path))
          ;; Fetch node names dynamically based on the current level and path
          node-names (case current-level
                       :databases (db/get-database conn-id)
                       :schemas (db/get-schemas conn-id (last path))
                       :tables (db/get-tables conn-id (nth path 0) (last path))
                       :columns (map #(db/join-column %) (db/get-columns conn-id (nth path 0) (nth path 1) (last path)))
                       [])]
      ;; Debugging - Log fetched node names at each level
      (println "Level:" current-level "Path:" path "Node Names:" node-names)
      (reduce (fn [tree node]
                (assoc tree node (build-tree (get tree node {})
                                             (rest levels)
                                             conn-id
                                             (conj path node))))
              current-tree
              node-names))))

;; Define levels for the tree
(def levels [:databases :schemas :tables :columns])

;; Example conn-id
;; (def conn-id 6)

;; Build the tree
;; (def tree
;;  (build-tree {} levels conn-id []))

;; Print the resulting tree
;; (println tree)

(defn transform-tree [tree]
  (map (fn [[key value]]
         {:label key
          :items (when (seq value)
                   (transform-tree value))})
       tree))

(defn getDBTree[conn-id]
   (let [ tree (first (transform-tree (build-tree {} levels conn-id [])))
          _ (println "CLASS TREE")
          _ (println (class tree))
          _ (pp/pprint tree)
        ]
        tree))

 ;; (json/generate-string (transform-tree (build-tree {} levels conn-id [])) {:pretty true}))


;; ---------------------- End Tree ----------------------------------
                     
 
;; ---------------------- UI Service Calls --------------------------

(defn is-target[g t1]
      (= "Tg" (btype t1 g)))

(defn processAddTable [gid t1 c2 t2-map jtype] (let [g (db/getGraph gid) 
       					   _ (println "---------------------------------------------------------------------------")
                                           _ (println (str "t1 : " t1))
                                           _ (println (str "c2 : " c2))
                                           _ (println (str "t2-map : " t2-map))
                                           type (if (is-union g t1) "U" "J")
                                         ] 
                                        (if (isFirstTable t1 t2-map) 
   						(add-first-table g c2 t2-map)
                                                (case (btype t1 g)
                         			 	"Tg" (target-table g t1 c2 t2-map)
                                       			"U" (unijoin-table g t1 c2 nil t2-map "U")
                                                        "J"  (unijoin-table g t1 c2 nil t2-map "J")
 							(join-table g t1 c2 t2-map jtype)))))

(defn save-conn [ args ]
    (let [ params (:json-params args)
           _ (println "-------------- BODY PARAMS -------------------")
           _ (pp/pprint args) ] 
    (def conn-id (:connection/id (db/insert-data :connection (sp/transform :port #(Integer/parseInt %) (walk/keywordize-keys (:params args))))))
    {:conn-id conn-id :tree-data (getDBTree conn-id)}))
     ;; {(getDBTree conn-id)}))


(defn save-conn1 [ args ]

{
  "conn-id" 73,
  "tree-data" {
    "label" "postgres",
    "items" [
      {
        "label" "public",
        "items" [
          {
            "label" "accounts",
            "items" [
              {
                "label" "user_id" ,
              },
              {
                "label" "username" ,
              },
              {
                "label" "password"
              },
              {
                "label" "email"
              },
              {
                "label" 
                  "created_at"
              },
              {
                "label" 
                  "last_login"
                
              }
            ]
          },
          {
            "label" "users",
            "items" [
              {
                "label" "id"
              },
              {
                "label" "username"
              },
              {
                "label" "password"
              },
              {
                "label" "email"
              },
              {
                "label" 
                  "created_at"
              }
            ]
          }
        ]
      }
    ]
  }
}

  )


;; ---------------------- End UI Service Calls ----------------------
;; -------------------  Graph Display ----------------------------


(defn getLevelFromSibling [g sibling] ( if sibling (case (attr g sibling :level)
                                                        :U  :U
                                                        :L  :L
                                                        nil :L)
                                           :S ))

(defn getLevel [g sibling parent] (let [
		;;			  _ (println (str "sibling : " sibling)) 
		;;			  _ (println (str "parent : " parent)) 
                  ;;                        _ (pp/pprint g)
                                       ]
					(case (attr g parent :level)
                                                :U  :U
                                                :L  :L
                                                :S (getLevelFromSibling g sibling))))

(defn getLati [g sibling parent] (let [plati (attr g parent :lati)

                                       ]
                                        ( if sibling ( if (attr g sibling :lati) (+ plati 1) (- plati 1))
                                             plati)))

(defn getBtype[g node] (attr g node :btype))


          
(defn getUL[g node] (if node (if (= node (getFinalNode g)) [:S 0 0 (getFinalNodeType g)] (let [
                    ;;      _ (println (str " node : " node))
                          parent  (first (successors g node))
                       ;;   _ (println "After successors")
                      ;;    _ (println (str "parent " parent))
                          sibling (getSibling g node)
                     ;;     _ (println (str "node " node " sibling " sibling))
                          level (getLevel g sibling parent)
                          lati (getLati g sibling parent)
                    ;;      _ (println (str "lati :" lati))
                    ;;      _ (println (str "level :" level))
                          longi (- (attr g parent :longi) 1)
                    ;;      _ (println (str "parent longi " (attr g parent :longi) ))
                          btype (getBtype  g node)
                   ;;       _ (println (str "BTYPE " btype))
                          ]
                          [level longi lati btype]))))

(defn setUL[g node] (if node (let [
                      ;;          _ (println (str "START setUL node " node))
                                   attrs (getUL g node)
                        ;;          _ (println "Attrs : " attrs) 
                                  ]
                                (-> g
                                 (add-attr node :level (first attrs))
                                (add-attr node :longi (second attrs))
                                (add-attr node :lati (nth attrs 2))
                                (add-attr node :btype (nth attrs 3))
                            ;;  (#(do (println "END setUL node ") %))
                                ))))

(defn updateNode [g node] (-> g
                              (setUL node)))

(defn updateAttrs[g] (reduce updateNode g (reverse (top-sort g))))

;;(defn getAttrs[g] (remove-elements (into {} (map (fn [[k v]] {k (:na v)}) (:n (updateAttrs g)))) (fn [k _ g] (and (nil? (parent g k)) (= "T" (btype k g)))) g))
(defn getAttrs[g] (let [
			gconn (connected-graph g)
                       ]
                       (into {} (map (fn [[k v]] {k (:na v)}) (:n (updateAttrs gconn))))))


(defn getAttrsByLevel[g lvl] (filter #(= lvl (:level (second %))) (getAttrs g)))

(defn getAttrByLevel[g lvl attr] (let
                                          [
                        ;;                      _ (println (str "ggetAttrsByLevel: " (getAttrsByLevel g start lvl)))
                        ;;                      _ (println (str "g: " g))
                        ;;                      _ (println (str "start: " start))
                        ;;                      _ (println (str "lvl: " lvl))
                        ;;                      _ (println (str "attr: " attr))
                                          ] (map #(attr (second %)) (getAttrsByLevel g lvl))))

(defn getLatiByLevel[g lvl] (map #(:lati (second %)) (getAttrsByLevel g lvl)))

(defn getLatiHeightByLevel[g lvl] (let [lati (getAttrByLevel g lvl :lati)
                                          ;;    _ (println (str "size : " (count lati)))
                                              min (if (empty? lati) 0 (reduce min lati))
                                              max (if (empty? lati) 0 (reduce max lati))]
                                                (if (= min max) max (abs (- max min)) )
                                        ))

(defn getLongiHeightByLevel[g lvl] (let [longi (getAttrByLevel g lvl :longi)]
                                              (if (empty? longi) 0 (abs (reduce min longi)))))


(defn getLongiHeight[g] (+ 1 (max (getLongiHeightByLevel g :U) (getLongiHeightByLevel g :L))))

(defn createCoordinates[gdef] (let [
                                       _ (println "==============GDEF BELOW==============")
                                       _ (pp/pprint gdef)
                                        attrs (getAttrs gdef)
                                       _ (println "Prinitng Attrs --------------------------------")
                                       _ (pp/pprint attrs)
                                       _ (println "END Prinitng Attrs --------------------------------")
                                       latiEnd (+ 2 (getLatiHeightByLevel gdef :L))
                                       _ (println (str "latiEnd : " latiEnd))
                                       longiEnd (getLongiHeight gdef)
                                       coord {}]
                                        (map #(assoc coord  :alias (:name (second %))
                                                            :y (+ (:lati (second %)) latiEnd)
                                                            :x (+ (:longi (second %)) longiEnd)
                                                            :btype (:btype (second %))
                                                            :parent (case (count (parents gdef (first %)))
                                                                          0 0
                                                                          1 (first (parents gdef (first %))) 
                                                                          (parents gdef (first %)))
                                                            :id (first %)
                                                           ) attrs)))

(defn displayGraph [g] (let [ _ (println "Inside DISAPLAYGRAPH ")
                              coord (createCoordinates g)
                              _ (println "PRINTING COORD")
                               _ (println coord) ]
                             (map #(update % :y (fn[x] (+ (* x 50) 150)) ) (map #(update % :x (fn[x] (+ (* x 300) 150)) ) coord))))

(defn listNodes [g] (top-sort g))

(defn fromClause [g] (clojure.string/join ","  (map #(attr g % :name) (filter #(and (= "T" (btype % g)) (not (isOrphan g %))) (listNodes g)))))

(defn uniquify-column-names [cols]
     (->> cols
          (remove nil?)
          (reduce
           (fn [[seen acc] col]
             (let [k (or (:column_name col) "")
                   n (get seen k 0)
                   new-name (if (zero? n) k (str k "_" n))]
               [(assoc seen k (inc n))  ; <— changed from update
                (conj acc (assoc col :column_name new-name))]))
           [{} []])
          second))

(defn mapping-target-name[g mid]
      (node-name (first (keys (attr g mid :target))) g))

(defn mapping-target-columns[g mid]
      (string/join "," (map :column_name (first (vals (attr g mid :target))))))


(defn target-name[g tid]
      (node-name (first (keys (attr g tid :tcols))) g))

(defn insert-table-name[g id attr-name]
      (if= attr-name :tcols
           (attr g id :table_name)
           (node-name (first (keys (attr g id attr-name))) g)))

(defn insert-columns[g id attr-name]
      (->> (if= attr-name :tcols
           	(->> (vals (tcols g id))
                     (apply concat)
                     uniquify-column-names)
           	(first (vals (attr g id attr-name))))
           (map :column_name)
           (string/join ",")))

(defn insert-stmt[g id attr]
      (str "insert into " (insert-table-name g id attr) " ( " (insert-columns g id attr) " ) " )) 

(defn gen-insert[g tid]
     (aif (get-mapping-node g tid)
     	 (insert-stmt g it :target) 
         (insert-stmt g tid :tcols)))

(defn select-columns[g id attr-name]
      (->> (if= attr-name :tcols
                (->> (vals (tcols g id))
                     (apply concat)
                     uniquify-column-names)
                (mapv :technical_name (map :source (get-in g [:n id :na :mapping]))))
           (string/join ",")))

(defn select-stmt[g id attr]
      (str "select "  (select-columns g id attr) " " )) 

(defn gen-select[g tid]
     (aif (get-mapping-node g tid)
     	 (select-stmt g it :source) 
         (select-stmt g tid :tcols)))


(comment

(defn selectClause [g] (clojure.string/join ","  (:tcols (getTableDef :op))))

(defn string-starts-with-any?
  [s prefixes]
  (some #(clojure.string/starts-with? s %) prefixes))

(defn filterClause [] (clojure.string/join " AND " (filter identity (map :where (map #(getTableFromDB %) (filter #(clojure.string/starts-with? %  ":filter" ) (listNodes)))))))

(defn prepareJoin[params] (map #(str (first %) " = " (second %)) params))

(defn joinClause [] (clojure.string/join " AND " (map #(clojure.string/join " AND " %) (map prepareJoin (map :jcondition (map #(getTableFromDB %) (filter #(clojure.string/starts-with? %  ":join" ) (listNodes))))))))


(defn whereClause [] (filter #(string-starts-with-any? %  '(":join", ":filter")) (listNodes)))

(defn getQuery [] (assoc {} :query (str "select " (selectClause) " from " (fromClause) " where " (clojure.string/join " AND " (list (joinClause) (filterClause))))))

)

(defn list-connections []
  (vec
    (map (fn [{:keys [id connection_name dbtype dbname schema]}]
           {:id   id
            :name (format "%s ( %s:%s:%s )"
                          connection_name dbtype dbname (or schema "nil"))})
         (db/select-columns :connection
                         [:id :connection_name :dbtype :dbname :schema]))))
(comment
(defn run-target[g tid values]
	(let [
		tmap (node tid g)
                table (:table_name tmap)
                create_table (:create_table table) 
                truncate (:truncate table)
                cid (:cid table)
                columns (:target (node (first (children g tid)) g))
                insert (db/make-insert-sql cid table columns)
             ]
             (do
		(if create_table 
                    (db/create-table cid table))
		(if truncate 
                    (db/truncate-table cid table))
                (db/insert-row! cid table columns values))))
)         
