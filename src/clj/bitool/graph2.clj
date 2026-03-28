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
	    "T"  ["J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "C" "O"]
	    "V"  ["J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "C" "O"]
	    "P"  ["J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "C" "O"]
	    "Ap" ["T" "J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "C" "O"]
	    "Kf" ["T" "J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "C" "O"]
	    "Fs" ["T" "J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "C" "O"]
	    "J"  ["T" "V" "P" "A" "S" "Fi" "Fu" "J" "U" "C" "O"]
	    "U"  ["T" "V" "P" "A" "S" "Fi" "Fu" "J" "U" "C" "O"]
	    "Ep" ["Au" "Vd" "Rl" "Cr" "Lg" "Ci" "Cq" "Dx"
	          "J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "C" "Rb" "O"]
	    "Rb" ["O"]
	    "Vd" ["Au" "Dx" "Rl" "Cr" "Lg" "Ci" "Cq" "Ev"
	          "J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "Rb" "O"]
	    "Au" ["Vd" "Dx" "Rl" "Cr" "Lg" "Ci" "Cq" "Ev"
	          "J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "Rb" "O"]
	    "Rl" ["Vd" "Au" "Dx" "Cr" "Lg" "Ci" "Cq" "Ev"
	          "J" "U" "P" "A" "S" "Fi" "Fu" "Rb" "O"]
	    "Cr" ["Vd" "Au" "Dx" "Rl" "Lg" "Ci" "Cq" "Ev"
	          "J" "U" "P" "A" "S" "Fi" "Fu" "Rb" "O"]
	    "Lg" ["Vd" "Au" "Dx" "Rl" "Cr" "Ci" "Cq" "Ev"
	          "J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "Rb" "O"]
	    "Ci" ["Vd" "Au" "Dx" "Rl" "Cr" "Lg" "Cq" "Ev"
	          "J" "U" "P" "A" "S" "Fi" "Fu" "Rb" "O"]
	    "Cq" ["Vd" "Au" "Dx" "Rl" "Cr" "Lg" "Ci" "Ev"
	          "J" "U" "P" "A" "S" "Fi" "Fu" "Rb" "O"]
	    "Dx" ["J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "Rb" "Lg" "O"]
	    "Ev" ["Rb" "O"]
	    "Sc" ["Au" "Vd" "Rl" "Lg" "Dx"
	          "J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "O"]
	    "Wh" ["Au" "Vd" "Rl" "Lg" "Dx"
	          "J" "U" "P" "A" "S" "Fi" "Fu" "Rb" "O"]
	    "Fu" ["J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "C" "Rb" "O"]
    "C"  ["J" "U" "P" "A" "S" "Fi" "Fu" "Tg" "Rb" "O"]
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
  "api" "Ap",
  "kafka-source" "Kf",
  "file-source" "Fs",
  "graphql-builder" "Gq",
  "conditionals" "C",
  "endpoint" "Ep",
  "response-builder" "Rb",
  "validator" "Vd",
  "auth" "Au",
  "db-execute" "Dx",
  "rate-limiter" "Rl",
  "cors" "Cr",
  "logger" "Lg",
  "cache" "Cq",
  "event-emitter" "Ev",
  "circuit-breaker" "Ci",
  "scheduler" "Sc",
  "webhook" "Wh",
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

(def node-keys {"filter" [:sql], "calculated" [:returntype :length :description :sql :datadef] , "aggregation" [:groupby :having],
                 "api-connection" [:source_system :specification_url :base_url :auth_ref :endpoint_configs]
                 "kafka-source" [:source_system :connection_id :bootstrap_servers :security_protocol :consumer_group_id :topic_configs]
                 "file-source" [:source_system :connection_id :base_path :transport :file_configs]
                 "endpoint" [:http_method :route_path :path_params :query_params :body_schema :response_format :description]
                 "response-builder" [:status_code :response_type :headers :template]
                 "validator" [:rules]
                 "auth" [:auth_type :token_header :secret :claims_to_cols]
                 "db-execute" [:connection_id :operation :sql_template :result_mode]
                 "rate-limiter" [:max_requests :window_seconds :key_type :burst]
                 "cors" [:allowed_origins :allowed_methods :allowed_headers :allow_credentials :max_age]
                 "logger" [:log_level :log_fields]
                 "cache" [:cache_key :ttl_seconds :strategy]
                 "event-emitter" [:event_name :payload_template]
                 "circuit-breaker" [:failure_threshold :reset_timeout :fallback_response]
                 "scheduler" [:cron_expression :timezone :params :enabled]
                 "target" [:target_kind :connection_id :catalog :schema :write_mode :table_format :partition_columns :merge_keys :cluster_by :options
                           :silver_job_id :gold_job_id :silver_job_params :gold_job_params :trigger_gold_on_success
                           :sf_load_method :sf_stage_name :sf_warehouse :sf_file_format :sf_on_error :sf_purge]
                 "webhook"   [:webhook_path :secret_header :secret_value :payload_format]
                 "conditionals" [:cond_type :branches :default_branch]
                 "function" [:fn_name :fn_params :fn_lets :fn_return :fn_outputs]})

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

(defn remove-node
  "Remove node id from the graph and drop all edges pointing to it."
  [g id]
  (let [g' (update g :n dissoc id)]
    (update g' :n
            (fn [nodes]
              (into {}
                    (map (fn [[nid nd]]
                           [nid (update nd :e dissoc id)])
                         nodes))))))

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
	  
(defn top-sort [ g ]
      (let [existing-nodes   (set (keys (:n g)))
            connected-nodes  (complement (set (unconnected-nodes g)))
            sorted-nodes     (alg/topsort (graph/digraph (into {} (map (fn [[k v]] [k (vec (keys (:e v)))]) (:n g)))))]
        (filter #(and (contains? existing-nodes %)
                      (connected-nodes %))
                sorted-nodes)))

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
        (let [tid (first (filter #(not (some #{(btype % g)} ["filter" "sorter" "lookup" "association" "conditionals"] )) (predecessors g id)))]
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

(defn- endpoint-node-label
  "Label shown on endpoint-like nodes in the canvas."
  [na]
  (case (:btype na)
    "Ep" (let [method (-> (or (:http_method na) "") str string/upper-case)
               route  (or (:route_path na) "")]
           (string/trim (str (when (seq method) (str method " ")) route)))
    "Wh" (or (:webhook_path na) "")
    ""))

(defn getOrphanNodeAttrs[g acc node-id]
      (let [na (node node-id g)]
       (conj acc {"alias" (:name na)
                  "endpoint_label" (endpoint-node-label na)
                  "y" (attr g node-id :y) ,
		  "x" (attr g node-id :x) ,
		  "btype" (:btype na) ,
		  "parent" (case (count (parents g node-id))
                                        0 0
                                        1 (first (parents g node-id))
                                        (parents g node-id))   ,
		  "id" node-id})))

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

(def ^:private api-load-types #{"full" "incremental" "snapshot"})
(def ^:private api-pagination-strategies #{"none" "offset" "page" "cursor" "token" "time" "link-header"})
(def ^:private api-schema-modes #{"manual" "infer" "hybrid"})
(def ^:private api-schema-evolution-modes #{"none" "advisory" "additive"})
(def ^:private api-schema-enforcement-modes #{"strict" "additive" "permissive"})
(def ^:private api-inferred-types #{"STRING" "INT" "BIGINT" "BOOLEAN" "DATE" "TIMESTAMP" "DOUBLE"})
(def ^:private api-column-name-pattern #"^[A-Za-z_][A-Za-z0-9_]*$")

(declare api-endpoint-configs-from-params)

(defn- trim-str [v]
  (some-> v str string/trim))

(defn- non-blank-str [v default]
  (let [s (trim-str v)]
    (if (seq s) s default)))

(defn- parse-bool [v]
  (cond
    (boolean? v) v
    (string? v) (#{"true" "1" "yes" "on"} (string/lower-case (string/trim v)))
    :else (boolean v)))

(defn- safe-int-default [v default]
  (try
    (Integer/parseInt (str v))
    (catch Exception _ default)))

(defn- kw-map [m]
  (walk/keywordize-keys (or m {})))

(defn- kw-maps [coll]
  (mapv kw-map (or coll [])))

(defn- split-csv [v]
  (cond
    (nil? v) []
    (sequential? v) (->> v
                         (map trim-str)
                         (remove string/blank?)
                         vec)
    :else (->> (string/split (str v) #",")
               (map trim-str)
               (remove string/blank?)
               vec)))

(defn- parse-jsonish-map [v]
  (cond
    (map? v) (kw-map v)
    (string/blank? (str v)) {}
    :else (try
            (-> v cheshire.core/parse-string walk/keywordize-keys)
            (catch Exception _
              {}))))

(defn- parse-jsonish-coll [v field-name]
  (cond
    (sequential? v) (kw-maps v)
    (string/blank? (str v)) []
    :else (try
            (let [parsed (cheshire.core/parse-string v true)]
              (when-not (sequential? parsed)
                (throw (ex-info (str (name field-name) " must be a JSON array")
                                {:field field-name})))
              (kw-maps parsed))
            (catch clojure.lang.ExceptionInfo e
              (throw e))
            (catch Exception _
              (throw (ex-info (str "Invalid JSON array for " (name field-name))
                              {:field field-name}))))))

(defn- normalize-field-type [v]
  (some-> (non-blank-str v nil) str string/upper-case))

(defn- normalize-inferred-field [field]
  (let [field (kw-map field)]
    {:path (non-blank-str (:path field) "")
     :column_name (non-blank-str (:column_name field)
                                 (some-> (:path field) (non-blank-str nil) path->name))
     :source_kind (non-blank-str (:source_kind field) "inferred")
     :enabled (if (contains? field :enabled) (parse-bool (:enabled field)) true)
     :type (or (normalize-field-type (:type field)) "STRING")
     :observed_types (vec (or (:observed_types field) []))
     :nullable (if (contains? field :nullable) (parse-bool (:nullable field)) true)
     :confidence (double (or (:confidence field) 0.0))
     :sample_coverage (double (or (:sample_coverage field) 0.0))
     :depth (safe-int-default (:depth field) 0)
     :array_mode (non-blank-str (:array_mode field) "scalar")
     :override_type (or (normalize-field-type (:override_type field)) "")
     :notes (non-blank-str (:notes field) "")}))

(defn- default-schema-enforcement-mode
  [schema-evolution-mode explicit-mode]
  (or (non-blank-str explicit-mode nil)
      (case (non-blank-str schema-evolution-mode "advisory")
        "additive" "additive"
        "strict" "strict"
        "permissive" "permissive"
        "advisory" "permissive"
        "none" "permissive"
        "permissive")))

(defn normalize-api-endpoint-config [cfg]
  (let [cfg                    (kw-map cfg)
        retry-policy           (kw-map (:retry_policy cfg))
        json-explode-rules     (kw-maps (:json_explode_rules cfg))
        primary-key-fields     (split-csv (:primary_key_fields cfg))
        selected-nodes         (split-csv (:selected_nodes cfg))
        inferred-fields        (mapv normalize-inferred-field (parse-jsonish-coll (:inferred_fields cfg) :inferred_fields))
        load-type              (non-blank-str (:load_type cfg) "full")
        pagination-strategy    (non-blank-str (:pagination_strategy cfg) "none")
        pagination-location    (non-blank-str (:pagination_location cfg) "query")
        schema-mode            (non-blank-str (:schema_mode cfg) "manual")
        schema-evolution-mode  (non-blank-str (:schema_evolution_mode cfg) "advisory")
        schema-enforcement-mode (default-schema-enforcement-mode (:schema_evolution_mode cfg)
                                                                 (:schema_enforcement_mode cfg))
        schema-review-state     (non-blank-str (:schema_review_state cfg) "optional")
        endpoint-url           (non-blank-str (:endpoint_url cfg) "")
        endpoint-name          (non-blank-str (:endpoint_name cfg) endpoint-url)
        bronze-table-name      (non-blank-str (:bronze_table_name cfg) (or (:table_name cfg) ""))
        silver-table-name      (non-blank-str (:silver_table_name cfg) "")
        watermark-column       (non-blank-str (:watermark_column cfg) "")
        cursor-field           (non-blank-str (:cursor_field cfg) "")
        cursor-param           (non-blank-str (:cursor_param cfg) "cursor")
        page-size              (safe-int-default (:page_size cfg) 100)
        overlap-minutes        (safe-int-default (:watermark_overlap_minutes cfg) 0)
        sample-records         (safe-int-default (:sample_records cfg) 100)
        max-inferred-columns   (safe-int-default (:max_inferred_columns cfg) 100)
        rate-limit-per-page-ms (safe-int-default (:rate_limit_per_page_ms cfg) 0)
        source-max-concurrency (safe-int-default (:source_max_concurrency cfg) nil)
        credential-max-concurrency (safe-int-default (:credential_max_concurrency cfg) nil)
        circuit-breaker-failure-threshold (safe-int-default (:circuit_breaker_failure_threshold cfg) nil)
        circuit-breaker-window-seconds (safe-int-default (:circuit_breaker_window_seconds cfg) nil)
        circuit-breaker-reset-timeout-seconds (safe-int-default (:circuit_breaker_reset_timeout_seconds cfg) nil)
        circuit-breaker-half-open-max-requests (safe-int-default (:circuit_breaker_half_open_max_requests cfg) nil)
        checkpoint-alert-seconds (safe-int-default (:checkpoint_alert_seconds cfg) nil)
        retry-volume-alert-24h (safe-int-default (:retry_volume_alert_24h cfg) nil)
        replay-failure-alert-7d (safe-int-default (:replay_failure_alert_7d cfg) nil)
        bad-record-alert-ratio (when (contains? cfg :bad_record_alert_ratio)
                                 (try
                                   (Double/parseDouble (str (:bad_record_alert_ratio cfg)))
                                   (catch Exception _
                                     ::invalid-double)))]
    {:endpoint_name endpoint-name
     :endpoint_url endpoint-url
     :http_method (-> (non-blank-str (:http_method cfg) "GET") string/upper-case)
     :selected_nodes selected-nodes
     :schema_mode schema-mode
     :sample_records sample-records
     :max_inferred_columns max-inferred-columns
     :type_inference_enabled (if (contains? cfg :type_inference_enabled) (parse-bool (:type_inference_enabled cfg)) true)
     :schema_evolution_mode schema-evolution-mode
     :schema_enforcement_mode schema-enforcement-mode
     :schema_review_state schema-review-state
     :require_schema_approval (parse-bool (:require_schema_approval cfg))
     :inferred_fields inferred-fields
     :load_type load-type
     :pagination_strategy pagination-strategy
     :pagination_location pagination-location
     :cursor_field cursor-field
     :cursor_param cursor-param
     :offset_param (non-blank-str (:offset_param cfg) "offset")
     :limit_param (non-blank-str (:limit_param cfg) "limit")
     :offset_field (non-blank-str (:offset_field cfg) "offset")
     :limit_field (non-blank-str (:limit_field cfg) "limit")
     :total_field (non-blank-str (:total_field cfg) "total")
     :is_last_field (non-blank-str (:is_last_field cfg) "isLast")
     :page_param (non-blank-str (:page_param cfg) "page")
     :size_param (non-blank-str (:size_param cfg) "page_size")
     :page_field (non-blank-str (:page_field cfg) "page")
     :total_pages_field (non-blank-str (:total_pages_field cfg) "total_pages")
     :token_param (non-blank-str (:token_param cfg) "page_token")
     :token_field (non-blank-str (:token_field cfg) "nextPageToken")
     :time_param (non-blank-str (:time_param cfg) "updated_since")
     :time_field (non-blank-str (:time_field cfg) watermark-column)
     :window_size_param (non-blank-str (:window_size_param cfg) "window_size")
     :time_window_minutes (safe-int-default (:time_window_minutes cfg) 60)
     :link_header_name (non-blank-str (:link_header_name cfg) "link")
     :watermark_column watermark-column
     :watermark_overlap_minutes overlap-minutes
     :rate_limit_per_page_ms (max 0 (long (or rate-limit-per-page-ms 0)))
     :source_max_concurrency (when (some? source-max-concurrency) (max 1 (long source-max-concurrency)))
     :credential_max_concurrency (when (some? credential-max-concurrency) (max 1 (long credential-max-concurrency)))
     :circuit_breaker_enabled (if (contains? cfg :circuit_breaker_enabled)
                                (parse-bool (:circuit_breaker_enabled cfg))
                                nil)
     :circuit_breaker_failure_threshold (when (some? circuit-breaker-failure-threshold)
                                          (max 1 (long circuit-breaker-failure-threshold)))
     :circuit_breaker_window_seconds (when (some? circuit-breaker-window-seconds)
                                       (max 1 (long circuit-breaker-window-seconds)))
     :circuit_breaker_reset_timeout_seconds (when (some? circuit-breaker-reset-timeout-seconds)
                                              (max 1 (long circuit-breaker-reset-timeout-seconds)))
     :circuit_breaker_half_open_max_requests (when (some? circuit-breaker-half-open-max-requests)
                                               (max 1 (long circuit-breaker-half-open-max-requests)))
     :checkpoint_alert_seconds (when (some? checkpoint-alert-seconds)
                                 (max 1 (long checkpoint-alert-seconds)))
     :bad_record_alert_ratio bad-record-alert-ratio
     :retry_volume_alert_24h (when (some? retry-volume-alert-24h)
                               (max 1 (long retry-volume-alert-24h)))
     :replay_failure_alert_7d (when (some? replay-failure-alert-7d)
                                (max 0 (long replay-failure-alert-7d)))
     :primary_key_fields primary-key-fields
     :bronze_table_name bronze-table-name
     :silver_table_name silver-table-name
     :retry_policy {:max_retries (safe-int-default (:max_retries retry-policy) 3)
                    :base_backoff_ms (safe-int-default (:base_backoff_ms retry-policy) 1000)}
     :json_explode_rules json-explode-rules
     :query_params (parse-jsonish-map (:query_params cfg))
     :request_headers (parse-jsonish-map (:request_headers cfg))
     :body_params (parse-jsonish-map (:body_params cfg))
     :page_size page-size
     :enabled (if (contains? cfg :enabled) (parse-bool (:enabled cfg)) true)
     ;; backward-compatible fields used by current UI
     :table_name bronze-table-name
     :create_table (parse-bool (:create_table cfg))
     :truncate (parse-bool (:truncate cfg))}))

(defn validate-api-endpoint-config [cfg]
  (let [endpoint-url         (:endpoint_url cfg)
        load-type            (:load_type cfg)
        pagination-strategy  (:pagination_strategy cfg)
        inferred-fields      (vec (or (:inferred_fields cfg) []))]
    (when (string/blank? endpoint-url)
      (throw (ex-info "API endpoint_url must not be blank" {:field :endpoint_url})))
    (when-not (contains? api-load-types load-type)
      (throw (ex-info (str "Unsupported load_type '" load-type "'")
                      {:field :load_type :value load-type})))
    (when-not (contains? api-pagination-strategies pagination-strategy)
      (throw (ex-info (str "Unsupported pagination_strategy '" pagination-strategy "'")
                      {:field :pagination_strategy :value pagination-strategy})))
    (when-not (#{"query" "body"} (:pagination_location cfg))
      (throw (ex-info "pagination_location must be 'query' or 'body'"
                      {:field :pagination_location :value (:pagination_location cfg)})))
    (when-not (contains? api-schema-modes (:schema_mode cfg))
      (throw (ex-info (str "Unsupported schema_mode '" (:schema_mode cfg) "'")
                      {:field :schema_mode :value (:schema_mode cfg)})))
    (when-not (contains? api-schema-evolution-modes (:schema_evolution_mode cfg))
      (throw (ex-info (str "Unsupported schema_evolution_mode '" (:schema_evolution_mode cfg) "'")
                      {:field :schema_evolution_mode :value (:schema_evolution_mode cfg)})))
    (when-not (contains? api-schema-enforcement-modes (:schema_enforcement_mode cfg))
      (throw (ex-info (str "Unsupported schema_enforcement_mode '" (:schema_enforcement_mode cfg) "'")
                      {:field :schema_enforcement_mode :value (:schema_enforcement_mode cfg)})))
    (when-not (#{"optional" "required"} (:schema_review_state cfg))
      (throw (ex-info "schema_review_state must be optional or required"
                      {:field :schema_review_state
                       :value (:schema_review_state cfg)})))
    (when (and (not= load-type "full")
               (string/blank? (:watermark_column cfg)))
      (throw (ex-info "watermark_column is required for incremental and snapshot loads"
                      {:field :watermark_column})))
    (when (and (= pagination-strategy "cursor")
               (string/blank? (:cursor_field cfg)))
      (throw (ex-info "cursor_field is required for cursor pagination"
                      {:field :cursor_field})))
    (when (and (= pagination-strategy "token")
               (string/blank? (:token_field cfg)))
      (throw (ex-info "token_field is required for token pagination"
                      {:field :token_field})))
    (when (and (= pagination-strategy "time")
               (string/blank? (:time_field cfg)))
      (throw (ex-info "time_field is required for time pagination"
                      {:field :time_field})))
    (when (empty? (:primary_key_fields cfg))
      (throw (ex-info "primary_key_fields must contain at least one field"
                      {:field :primary_key_fields})))
    (when (<= (long (or (:sample_records cfg) 0)) 0)
      (throw (ex-info "sample_records must be greater than 0"
                      {:field :sample_records :value (:sample_records cfg)})))
    (when (<= (long (or (:max_inferred_columns cfg) 0)) 0)
      (throw (ex-info "max_inferred_columns must be greater than 0"
                      {:field :max_inferred_columns :value (:max_inferred_columns cfg)})))
    (when (and (some? (:source_max_concurrency cfg))
               (<= (long (:source_max_concurrency cfg)) 0))
      (throw (ex-info "source_max_concurrency must be greater than 0"
                      {:field :source_max_concurrency :value (:source_max_concurrency cfg)})))
    (when (and (some? (:credential_max_concurrency cfg))
               (<= (long (:credential_max_concurrency cfg)) 0))
      (throw (ex-info "credential_max_concurrency must be greater than 0"
                      {:field :credential_max_concurrency :value (:credential_max_concurrency cfg)})))
    (doseq [[field value]
            [[:circuit_breaker_failure_threshold (:circuit_breaker_failure_threshold cfg)]
             [:circuit_breaker_window_seconds (:circuit_breaker_window_seconds cfg)]
             [:circuit_breaker_reset_timeout_seconds (:circuit_breaker_reset_timeout_seconds cfg)]
             [:circuit_breaker_half_open_max_requests (:circuit_breaker_half_open_max_requests cfg)]
             [:checkpoint_alert_seconds (:checkpoint_alert_seconds cfg)]
             [:retry_volume_alert_24h (:retry_volume_alert_24h cfg)]]]
      (when (and (some? value) (<= (long value) 0))
        (throw (ex-info (str (name field) " must be greater than 0")
                        {:field field :value value}))))
    (when (and (some? (:replay_failure_alert_7d cfg))
               (neg? (long (:replay_failure_alert_7d cfg))))
      (throw (ex-info "replay_failure_alert_7d must be greater than or equal to 0"
                      {:field :replay_failure_alert_7d
                       :value (:replay_failure_alert_7d cfg)})))
    (when (= ::invalid-double (:bad_record_alert_ratio cfg))
      (throw (ex-info "bad_record_alert_ratio must be a valid decimal"
                      {:field :bad_record_alert_ratio
                       :value (:bad_record_alert_ratio cfg)})))
    (when (and (some? (:bad_record_alert_ratio cfg))
               (or (neg? (double (:bad_record_alert_ratio cfg)))
                   (> (double (:bad_record_alert_ratio cfg)) 1.0)))
      (throw (ex-info "bad_record_alert_ratio must be between 0.0 and 1.0"
                      {:field :bad_record_alert_ratio
                       :value (:bad_record_alert_ratio cfg)})))
    (doseq [field inferred-fields]
      (when (string/blank? (:path field))
        (throw (ex-info "inferred_fields.path must not be blank"
                        {:field :inferred_fields :value field})))
      (when (or (string/blank? (:column_name field))
                (not (re-matches api-column-name-pattern (:column_name field))))
        (throw (ex-info "inferred_fields.column_name must be a valid identifier"
                        {:field :inferred_fields :value field})))
      (when-not (contains? api-inferred-types (:type field))
        (throw (ex-info (str "Unsupported inferred field type '" (:type field) "'")
                        {:field :inferred_fields :value field})))
      (when (and (seq (:override_type field))
                 (not (contains? api-inferred-types (:override_type field))))
        (throw (ex-info (str "Unsupported inferred field override_type '" (:override_type field) "'")
                        {:field :inferred_fields :value field}))))
    (let [enabled-columns (->> inferred-fields
                               (filter #(not= false (:enabled %)))
                               (map :column_name)
                               frequencies
                               (keep (fn [[column-name n]]
                                       (when (> n 1) column-name)))
                               vec)]
      (when (seq enabled-columns)
        (throw (ex-info "inferred_fields contains duplicate enabled column_name values"
                        {:field :inferred_fields :columns enabled-columns}))))
    cfg))

(defn normalize-api-node-params [params]
  (let [params            (kw-map params)
        endpoint-configs  (api-endpoint-configs-from-params params)]
    {:api_name          (:api_name params)
     :source_system     (non-blank-str (:source_system params) "samara")
     :specification_url (:specification_url params)
     :base_url          (:base_url params)
     :auth_ref          (kw-map (:auth_ref params))
     :endpoint_configs  endpoint-configs
     :endpoints         endpoint-configs}))

(def ^:private kafka-deserializers
  #{"string" "json" "avro" "protobuf" "bytes"})

(def ^:private file-formats
  #{"csv" "jsonl" "parquet" "fixed_width"})

(def ^:private file-transports
  #{"local" "s3" "sftp" "azure_blob"})

(defn- infer-file-format
  [path]
  (let [path (some-> path str string/lower-case)]
    (cond
      (string/ends-with? path ".csv") "csv"
      (or (string/ends-with? path ".jsonl")
          (string/ends-with? path ".ndjson")) "jsonl"
      (string/ends-with? path ".parquet") "parquet"
      :else "fixed_width")))

(defn- normalize-kafka-topic-config
  [cfg]
  (let [cfg         (kw-map cfg)
        topic-name  (non-blank-str (:topic_name cfg) "")
        endpoint    (non-blank-str (:endpoint_name cfg) topic-name)]
    {:endpoint_name endpoint
     :topic_name topic-name
     :enabled (if (contains? cfg :enabled) (parse-bool (:enabled cfg)) true)
     :key_deserializer (non-blank-str (:key_deserializer cfg) "string")
     :value_deserializer (non-blank-str (:value_deserializer cfg) "json")
     :schema_registry_url (non-blank-str (:schema_registry_url cfg) "")
     :schema_registry_auth_ref (kw-map (:schema_registry_auth_ref cfg))
     :json_explode_rules (vec (or (:json_explode_rules cfg) []))
     :primary_key_fields (cond
                           (sequential? (:primary_key_fields cfg)) (vec (map str (:primary_key_fields cfg)))
                           :else (split-csv (:primary_key_fields cfg)))
     :watermark_column (non-blank-str (:watermark_column cfg) "")
     :bronze_table_name (non-blank-str (:bronze_table_name cfg) "")
     :schema_mode (non-blank-str (:schema_mode cfg) "manual")
     :inferred_fields (vec (or (:inferred_fields cfg) []))
     :max_poll_records (safe-int-default (:max_poll_records cfg) 500)
     :max_poll_interval_ms (safe-int-default (:max_poll_interval_ms cfg) 300000)
     :auto_offset_reset (non-blank-str (:auto_offset_reset cfg) "earliest")
     :batch_flush_rows (safe-int-default (:batch_flush_rows cfg) 1000)
     :batch_flush_bytes (safe-int-default (:batch_flush_bytes cfg) 52428800)
     :rate_limit_per_poll_ms (safe-int-default (:rate_limit_per_poll_ms cfg) 0)
     :options (parse-jsonish-map (:options cfg))}))

(defn- validate-kafka-topic-config
  [cfg]
  (when (string/blank? (:topic_name cfg))
    (throw (ex-info "topic_name is required"
                    {:field :topic_name :value (:topic_name cfg)})))
  (when-not (contains? kafka-deserializers (:key_deserializer cfg))
    (throw (ex-info "Unsupported key_deserializer"
                    {:field :key_deserializer :value (:key_deserializer cfg)})))
  (when-not (contains? kafka-deserializers (:value_deserializer cfg))
    (throw (ex-info "Unsupported value_deserializer"
                    {:field :value_deserializer :value (:value_deserializer cfg)})))
  (when-not (#{"earliest" "latest"} (:auto_offset_reset cfg))
    (throw (ex-info "auto_offset_reset must be earliest or latest"
                    {:field :auto_offset_reset :value (:auto_offset_reset cfg)})))
  cfg)

(defn- normalize-kafka-node-params
  [params]
  (let [params        (kw-map params)
        topic-configs (->> (:topic_configs params)
                           (map normalize-kafka-topic-config)
                           (map validate-kafka-topic-config)
                           vec)]
    {:source_system (non-blank-str (:source_system params) "kafka")
     :connection_id (when-let [conn-id (some-> (:connection_id params) str trim-str not-empty)]
                      (safe-int-default conn-id nil))
     :bootstrap_servers (non-blank-str (:bootstrap_servers params) "")
     :security_protocol (non-blank-str (:security_protocol params) "PLAINTEXT")
     :consumer_group_id (non-blank-str (:consumer_group_id params) "")
     :topic_configs topic-configs}))

(defn save-kafka-source
  [g id params]
  (update-node g id (normalize-kafka-node-params params)))

(defn- normalize-field-spec
  [spec]
  (let [spec (kw-map spec)]
    {:name (non-blank-str (:name spec) "")
     :start (safe-int-default (:start spec) 1)
     :length (safe-int-default (:length spec) 0)
     :type (non-blank-str (:type spec) "string")}))

(defn- normalize-file-config
  [cfg]
  (let [cfg        (kw-map cfg)
        paths      (cond
                     (sequential? (:paths cfg)) (vec (map str (:paths cfg)))
                     (seq (non-blank-str (:path cfg) "")) [(str (:path cfg))]
                     :else [])
        first-path (first paths)
        endpoint   (non-blank-str (:endpoint_name cfg)
                                  (some-> first-path java.io.File. .getName))]
    {:endpoint_name endpoint
     :enabled (if (contains? cfg :enabled) (parse-bool (:enabled cfg)) true)
     :transport (non-blank-str (:transport cfg) "local")
     :path first-path
     :paths paths
     :format (non-blank-str (:format cfg) (infer-file-format first-path))
     :delimiter (non-blank-str (:delimiter cfg) ",")
     :has_header (if (contains? cfg :has_header) (parse-bool (:has_header cfg)) true)
     :encoding (non-blank-str (:encoding cfg) "UTF-8")
     :primary_key_fields (cond
                           (sequential? (:primary_key_fields cfg)) (vec (map str (:primary_key_fields cfg)))
                           :else (split-csv (:primary_key_fields cfg)))
     :watermark_column (non-blank-str (:watermark_column cfg) "")
     :bronze_table_name (non-blank-str (:bronze_table_name cfg) "")
     :json_explode_rules (vec (or (:json_explode_rules cfg) []))
     :schema_mode (non-blank-str (:schema_mode cfg) "manual")
     :inferred_fields (vec (or (:inferred_fields cfg) []))
     :field_specs (->> (:field_specs cfg) (map normalize-field-spec) vec)
     :copybook (non-blank-str (:copybook cfg) "")
     :batch_flush_rows (safe-int-default (:batch_flush_rows cfg) 1000)
     :batch_flush_bytes (safe-int-default (:batch_flush_bytes cfg) 52428800)
     :options (parse-jsonish-map (:options cfg))}))

(defn- validate-file-config
  [cfg]
  (when-not (seq (:paths cfg))
    (throw (ex-info "file source config requires path or paths"
                    {:field :path :value cfg})))
  (when-not (= 1 (count (str (:delimiter cfg))))
    (throw (ex-info "file source delimiter must be a single character"
                    {:field :delimiter
                     :value (:delimiter cfg)})))
  (when-not (contains? file-transports (:transport cfg))
    (throw (ex-info "Unsupported file transport"
                    {:field :transport :value (:transport cfg)})))
  (when-not (contains? file-formats (:format cfg))
    (throw (ex-info "Unsupported file format"
                    {:field :format :value (:format cfg)})))
  cfg)

(defn- normalize-file-node-params
  [params]
  (let [params       (kw-map params)
        file-configs (->> (:file_configs params)
                          (map normalize-file-config)
                          (map validate-file-config)
                          vec)]
    {:source_system (non-blank-str (:source_system params) "file")
     :connection_id (when-let [conn-id (some-> (:connection_id params) str trim-str not-empty)]
                      (safe-int-default conn-id nil))
     :base_path (non-blank-str (:base_path params) "")
     :transport (non-blank-str (:transport params) "local")
     :file_configs file-configs}))

(defn save-file-source
  [g id params]
  (update-node g id (normalize-file-node-params params)))

(defn- api-endpoint-configs-from-params [params]
  (let [params (kw-map params)
        endpoint-configs (:endpoint_configs params)
        current-endpoint (:endpoint_url params)]
    (cond
      (seq endpoint-configs)
      (mapv #(validate-api-endpoint-config (normalize-api-endpoint-config %)) endpoint-configs)

      (sequential? current-endpoint)
      (mapv #(validate-api-endpoint-config
               (normalize-api-endpoint-config
                 (if (map? %) %
                     {:endpoint_url %})))
            current-endpoint)

      (seq (trim-str current-endpoint))
      [(validate-api-endpoint-config
         (normalize-api-endpoint-config params))]

      :else [])))

(defn- output-node-id [g]
  (->> (:n g)
       (keep (fn [[node-id node]]
               (when (= "O" (get-in node [:na :btype]))
                 node-id)))
       first))

(defn- legacy-api-mapping-ids [g api-id]
  (->> (get-in g [:n api-id :e] {})
       keys
       (filter #(= "Mp" (get-in g [:n % :na :btype])))))

(defn- mapping-target-ids [g mapping-id]
  (->> (get-in g [:n mapping-id :e] {})
       keys
       (filter #(= "Tg" (get-in g [:n % :na :btype])))))

(defn- incoming-node-ids [g target-id]
  (->> (:n g)
       (keep (fn [[node-id node]]
               (when (contains? (:e node) target-id)
                 node-id)))))

(defn- configured-target-node? [g target-id]
  (let [target (get-in g [:n target-id :na])]
    (boolean
      (or (some? (:connection_id target))
          (seq (trim-str (:connection target)))
          (seq (trim-str (:target_kind target)))
          (seq (trim-str (:catalog target)))
          (seq (trim-str (:schema target)))
          (seq (trim-str (:table_name target)))
          (true? (:create_table target))
          (true? (:truncate target))
          (seq (:partition_columns target))
          (seq (:merge_keys target))
          (seq (:cluster_by target))
          (seq (:options target))))))

(defn- ensure-edge [g from-id to-id]
  (if (contains? (get-in g [:n from-id :e] {}) to-id)
    g
    (add-edge g [from-id to-id])))

(defn- cleanup-legacy-api-targets [g api-id]
  (let [output-id (output-node-id g)]
    (reduce
      (fn [graph mapping-id]
        (let [target-ids            (vec (mapping-target-ids graph mapping-id))
              configured-target-ids (filterv #(configured-target-node? graph %) target-ids)]
          (if (and (seq configured-target-ids) (nil? output-id))
            graph
            (let [graph' (reduce
                           (fn [g' target-id]
                             (cond
                               (configured-target-node? g' target-id)
                               (ensure-edge g' output-id target-id)

                               (= [mapping-id] (vec (incoming-node-ids g' target-id)))
                               (remove-node g' target-id)

                               :else g'))
                           graph
                           target-ids)]
              (remove-node graph' mapping-id)))))
      g
      (legacy-api-mapping-ids g api-id))))

(defn save-api[g id params]
        (let [params            (kw-map params)
              endpoint-configs  (api-endpoint-configs-from-params params)
              node-params       (normalize-api-node-params params)
              g1                (update-node g id node-params)]
      	(cleanup-legacy-api-targets g1 id)))


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

(defn- normalize-target-params [params]
  (let [params (kw-map params)]
    {:connection_id (when-let [conn-id (some-> (:connection_id params) str trim-str not-empty)]
                      (safe-int-default conn-id nil))
     :connection (non-blank-str (:connection params) "")
     :target_kind (non-blank-str (:target_kind params) "databricks")
     :catalog (non-blank-str (:catalog params) "")
     :schema (non-blank-str (:schema params) "")
     :table_name (non-blank-str (:table_name params) "")
     :write_mode (let [write-mode (non-blank-str (:write_mode params) "append")]
                   (if (= "overwrite" write-mode) "replace" write-mode))
     :table_format (non-blank-str (:table_format params) "delta")
     :partition_columns (split-csv (:partition_columns params))
     :merge_keys (split-csv (:merge_keys params))
     :cluster_by (split-csv (:cluster_by params))
     :options (parse-jsonish-map (:options params))
     :silver_job_id (non-blank-str (:silver_job_id params) "")
     :gold_job_id (non-blank-str (:gold_job_id params) "")
     :silver_job_params (parse-jsonish-map (:silver_job_params params))
     :gold_job_params (parse-jsonish-map (:gold_job_params params))
     :trigger_gold_on_success (parse-bool (:trigger_gold_on_success params))
     :create_table (parse-bool (:create_table params))
     :truncate (parse-bool (:truncate params))
     :sf_load_method (non-blank-str (:sf_load_method params) "jdbc")
     :sf_stage_name (non-blank-str (:sf_stage_name params) "")
     :sf_warehouse (non-blank-str (:sf_warehouse params) "")
     :sf_file_format (non-blank-str (:sf_file_format params) "")
     :sf_on_error (non-blank-str (:sf_on_error params) "ABORT_STATEMENT")
     :sf_purge (parse-bool (:sf_purge params))
     :c (or (when-let [conn-id (some-> (:connection_id params) str trim-str not-empty)]
              (safe-int-default conn-id nil))
            (:c params))}))

(defn save-target [g id params]
  (let [node-params (normalize-target-params params)]
    (update-node g id node-params)))

(defn update-table-cols [ g 
                          start-node 
                          table 
                        ]
      (let [
                          _ (println "******INSIDE******* update-table-cols")
                          _ (println (str "start-node : " start-node)) 
                          _ (println (str "table : " table)) 
           ]
      (append-nodes-tcols g (find-nodes-btype g ["O" "Tg" "P" "Fi" "S" "A" "Fu" "C"] (successors g start-node)) (:tcols table))))

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
           (let [g1 (add-edge g [t1 t2])]
             (-> g1
                 (update-table-cols t1 (tmap g1 t1))
                 (db/insertGraph))) g)))

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
               })

(defn column_details[g row]
    (let [newrow (second row)
          table_id (first row)
          table_prefix (node-name table_id g)]
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
  		 "Tg" (merge item (select-keys tmap [:truncate :create_table :c :connection :table_name :target_kind :connection_id :catalog :schema :write_mode :table_format :partition_columns :merge_keys :cluster_by :options]))
  		 "Ap" (merge item (select-keys tmap [:api_name :source_system :specification_url :base_url :auth_ref :endpoint_configs]))
       "Kf" (merge item (select-keys tmap [:source_system :connection_id :bootstrap_servers :security_protocol :consumer_group_id :topic_configs]))
       "Fs" (merge item (select-keys tmap [:source_system :connection_id :base_path :transport :file_configs]))
  		 "Ep" (merge item (select-keys tmap [:http_method :route_path :path_params
                                                      :query_params :body_schema :response_format :description]))
  		 "Rb" (merge item (select-keys tmap [:status_code :response_type :headers :template]))
		 "Vd" (assoc item "rules" (:rules tmap))
		 "Au" (merge item (select-keys tmap [:auth_type :token_header :claims_to_cols]))
		 "Dx" (merge item (select-keys tmap [:connection_id :operation :sql_template :result_mode]))
		 "Rl" (merge item (select-keys tmap [:max_requests :window_seconds :key_type :burst]))
		 "Cr" (merge item (select-keys tmap [:allowed_origins :allowed_methods :allowed_headers :allow_credentials :max_age]))
		 "Lg" (merge item (select-keys tmap [:log_level :log_fields]))
		 "Cq" (merge item (select-keys tmap [:cache_key :ttl_seconds :strategy]))
		 "Ev" (merge item (select-keys tmap [:event_name :payload_template]))
		 "Ci" (merge item (select-keys tmap [:failure_threshold :reset_timeout :fallback_response]))
		 "Sc" (merge item (select-keys tmap [:cron_expression :timezone :params]))
		 "Wh" (merge item (select-keys tmap [:webhook_path :secret_header :payload_format]))
		 "C"  (merge item (select-keys tmap [:cond_type :branches :default_branch]))
		 "Fu" (merge item (select-keys tmap [:fn_name :fn_params :fn_lets :fn_return :fn_outputs]))
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
		(get-in (db/getGraph (Integer. g)) [:n item :na]))

(defmethod getData clojure.lang.PersistentArrayMap [g item]
		(get-in g [:n item :na]))

(defn get-item [id g]
     (let [tmap (getData g id)
           columns (:tcols tmap)
           name (:name tmap)
           alias (if (= name "O") "Output" name)
           btype (:btype tmap)]
        (assoc (item_master id alias btype tmap) "items" (item_columns g columns))))


(defn get-target-item [id g]
     (let [tmap    (getData g id)
           columns (:tcols tmap)
           name    (:name tmap)
           btype   (:btype tmap)]
        (assoc (item_master id name btype tmap)
               "connection_id" (:connection_id tmap)
               "target_kind" (:target_kind tmap)
               "catalog" (:catalog tmap)
               "schema" (:schema tmap)
               "table_name" (:table_name tmap)
               "write_mode" (:write_mode tmap)
               "table_format" (:table_format tmap)
               "partition_columns" (:partition_columns tmap)
               "merge_keys" (:merge_keys tmap)
               "cluster_by" (:cluster_by tmap)
               "options" (:options tmap)
               "silver_job_id" (:silver_job_id tmap)
               "gold_job_id" (:gold_job_id tmap)
               "silver_job_params" (:silver_job_params tmap)
               "gold_job_params" (:gold_job_params tmap)
               "trigger_gold_on_success" (:trigger_gold_on_success tmap)
               "sf_load_method" (:sf_load_method tmap)
               "sf_stage_name" (:sf_stage_name tmap)
               "sf_warehouse" (:sf_warehouse tmap)
               "sf_file_format" (:sf_file_format tmap)
               "sf_on_error" (:sf_on_error tmap)
               "sf_purge" (:sf_purge tmap)
               "items" (item_columns g columns))))
        ;; (assoc (item_master id alias btype  tmap) "items" (item_columns g columns id (:table tmap)))))

(defn get-api-item [id g]
     (let [tmap    (getData g id)
           name    (:name tmap)
           btype   (:btype tmap)
           columns (:tcols tmap)]
       (assoc (item_master id name btype tmap)
              "items"            (item_columns g columns)
              "api_name"         (:api_name tmap)
              "source_system"    (:source_system tmap)
              "specification_url" (:specification_url tmap)
              "base_url"         (:base_url tmap)
              "auth_ref"         (:auth_ref tmap)
              "endpoint_configs" (:endpoint_configs tmap))))

(defn get-kafka-source-item [id g]
     (let [tmap    (getData g id)
           name    (:name tmap)
           btype   (:btype tmap)
           columns (:tcols tmap)]
       (assoc (item_master id name btype tmap)
              "items" (item_columns g columns)
              "source_system" (:source_system tmap)
              "connection_id" (:connection_id tmap)
              "bootstrap_servers" (:bootstrap_servers tmap)
              "security_protocol" (:security_protocol tmap)
              "consumer_group_id" (:consumer_group_id tmap)
              "topic_configs" (:topic_configs tmap))))

(defn get-file-source-item [id g]
     (let [tmap    (getData g id)
           name    (:name tmap)
           btype   (:btype tmap)
           columns (:tcols tmap)]
       (assoc (item_master id name btype tmap)
              "items" (item_columns g columns)
              "source_system" (:source_system tmap)
              "connection_id" (:connection_id tmap)
              "base_path" (:base_path tmap)
              "transport" (:transport tmap)
              "file_configs" (:file_configs tmap))))

;; ---- Endpoint (Ep) node functions ----

(defn ep-params->tcols
  "Convert endpoint path_params, query_params, and body_schema into standard tcols format.
   Produces maps matching the format expected by column_row_details."
  [node-id path-params query-params body-schema]
  (let [gk       (fn [m & ks] (some #(get m %) ks))
        make-col (fn [col-name dtype nullable]
                   {:column_name (or col-name "")
                    :data_type   (or dtype "varchar")
                    :is_nullable (if nullable "YES" "NO")})
        path-cols  (mapv #(make-col (gk % :param_name :name "param_name") (gk % :data_type "data_type") false) path-params)
        query-cols (mapv #(make-col (gk % :param_name :name "param_name") (gk % :data_type "data_type") (not (gk % :required "required"))) query-params)
        body-cols  (mapv #(make-col (gk % :field_name :name "field_name") (gk % :data_type "data_type") (not (gk % :required "required"))) body-schema)]
    {node-id (vec (concat path-cols query-cols body-cols))}))

(defn- mget
  "Read a value from map `m` using the first present key from `ks`."
  [m & ks]
  (some (fn [k] (when (contains? m k) (get m k))) ks))

(defn save-endpoint
  "Save endpoint node configuration. Converts params into columns."
  [g id params]
  (let [params       (walk/keywordize-keys (or params {}))
        kw-maps      (fn [coll] (mapv #(into {} (map (fn [[k v]] [(keyword k) v]) %)) coll))
        path-params  (kw-maps (or (:path_params params) []))
        query-params (kw-maps (or (:query_params params) []))
        body-schema  (kw-maps (or (:body_schema params) []))
        method       (-> (or (:http_method params) "GET") str string/upper-case)
        method       (if (contains? #{"GET" "POST" "PUT" "DELETE" "PATCH" "HEAD" "OPTIONS" "TRACE"} method)
                       method
                       "GET")
        tcols        (ep-params->tcols id path-params query-params body-schema)
        node-params  {:http_method method
                      :route_path (or (:route_path params) "")
                      :response_format (or (:response_format params) "json")
                      :description (or (:description params) "")
                      :path_params path-params
                      :query_params query-params
                      :body_schema body-schema}
        g          (update-node g id (assoc node-params :tcols tcols))
        child-ids  (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-endpoint-item
  "Retrieve endpoint node data for UI display."
  [id g]
  (let [tmap        (getData g id)
        name        (or (mget tmap :name "name") "")
        btype       (or (mget tmap :btype "btype") "Ep")
        columns     (or (mget tmap :tcols "tcols") {})
        http-method (-> (or (mget tmap :http_method "http_method") "GET") str string/upper-case)]
    (assoc (item_master id name btype tmap)
           "items" (item_columns g columns)
           "http_method" http-method
           "route_path" (or (mget tmap :route_path "route_path") "")
           "path_params" (or (mget tmap :path_params "path_params") [])
           "query_params" (or (mget tmap :query_params "query_params") [])
           "body_schema" (or (mget tmap :body_schema "body_schema") [])
           "response_format" (or (mget tmap :response_format "response_format") "json")
           "description" (or (mget tmap :description "description") ""))))

(defn parse-rb-headers
  "Validate and parse headers string. Must be empty or a JSON object.
   Returns the string as-is if valid, throws ex-info if not."
  [headers-str]
  (let [s (clojure.string/trim (or headers-str ""))]
    (if (empty? s)
      s
      (let [parsed (try (cheshire.core/parse-string s)
                        (catch Exception _ ::invalid))]
        (if (and (not= parsed ::invalid) (map? parsed))
          s
          (throw (ex-info "headers must be a JSON object or empty"
                          {:field :headers :value s})))))))

(defn parse-rb-template
  "Validate template: must be a vector of maps each with non-blank output_key and source_column.
   Rows where both are blank are silently dropped (user left an empty row).
   Rows where exactly one is blank are rejected."
  [template]
  (let [kw-maps (fn [coll] (mapv #(into {} (map (fn [[k v]] [(keyword k) v]) %)) coll))
        rows    (kw-maps (or template []))]
    (mapv (fn [row]
            (let [ok (clojure.string/trim (str (:output_key row "")))
                  sc (clojure.string/trim (str (:source_column row "")))]
              (when (or (empty? ok) (empty? sc))
                (throw (ex-info "each template row must have a non-blank output_key and source_column"
                                {:field :template :row row})))
              {:output_key ok :source_column sc}))
          (remove (fn [r]
                    (and (empty? (clojure.string/trim (str (:output_key r ""))))
                         (empty? (clojure.string/trim (str (:source_column r ""))))))
                  rows))))

(defn- find-upstream-tcols
  "Find the nearest upstream node with non-empty tcols."
  [g id]
  ;; Use incoming edges directly to avoid parent/child naming confusion in legacy helpers.
  (loop [queue (seq (find-edges g id "src"))
         visited #{}]
    (if-let [nid (first queue)]
      (let [rest-q (rest queue)]
        (if (contains? visited nid)
          (recur rest-q visited)
          (let [cols (tcols g nid)]
            (if (seq cols)
              cols
              (recur (concat rest-q (remove visited (find-edges g nid "src")))
                     (conj visited nid))))))
      {})))

(defn save-response-builder
  "Save response builder node configuration."
  [g id params]
  (let [params      (walk/keywordize-keys (or params {}))
        headers     (parse-rb-headers (:headers params))
        template    (parse-rb-template (:template params))
        parent-tcols (find-upstream-tcols g id)
        node-params {:status_code   (or (:status_code params) "200")
                     :response_type (or (:response_type params) "json")
                     :headers       headers
                     :template      template
                     :tcols         parent-tcols}
        g          (update-node g id node-params)
        child-ids  (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-response-builder-item
  "Retrieve response builder node data for UI display."
  [id g]
  (let [tmap        (getData g id)
        name        (or (mget tmap :name "name") "")
        btype       (or (mget tmap :btype "btype") "Rb")
        parent-cols (or (not-empty (mget tmap :tcols "tcols"))
                        (find-upstream-tcols g id))]
    (assoc (item_master id name btype tmap)
           "status_code"   (or (mget tmap :status_code "status_code") "200")
           "response_type" (or (mget tmap :response_type "response_type") "json")
           "headers"       (or (mget tmap :headers "headers") "")
           "template"      (or (mget tmap :template "template") [])
           "items"         (item_columns g parent-cols))))

;; --- Validator (Vd) ---

(def valid-rule-types
  #{"required" "min" "max" "min-length" "max-length" "regex" "one-of" "type"})

(defn validate-rules [rules]
  (doseq [[i row] (map-indexed vector rules)]
    (when (clojure.string/blank? (str (:field row "")))
      (throw (ex-info (str "Rule " (inc i) ": field name must not be blank")
                      {:field :rules :row i})))
    (when-not (valid-rule-types (:rule row))
      (throw (ex-info (str "Rule " (inc i) ": unknown rule type '" (:rule row)
                           "'. Must be one of: " (clojure.string/join ", " (sort valid-rule-types)))
                      {:field :rules :row i}))))
  rules)

(defn save-validator [g id params]
  (let [kw-maps      (fn [coll] (mapv #(into {} (map (fn [[k v]] [(keyword k) v]) %)) coll))
        rules        (validate-rules (kw-maps (or (:rules params) [])))
        parent-tcols (getTcols g id)
        g            (update-node g id {:rules rules :tcols parent-tcols})
        child-ids    (map second (find-edges g id))]
    (reduce (fn [acc cid]
              (assoc-in acc [:n cid :na :tcols] (getTcols acc cid)))
            g child-ids)))

(defn get-validator-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "rules" (:rules tmap)
           "items" (item_columns g (:tcols tmap)))))

;; --- Scheduler (Sc) ---

(defn sc-params->tcols [id params]
  (let [system-cols [{:column_name "triggered_at" :data_type "varchar" :is_nullable "NO"}
                     {:column_name "job_id"        :data_type "varchar" :is_nullable "NO"}
                     {:column_name "run_number"    :data_type "integer" :is_nullable "NO"}]
        param-cols  (mapv #(hash-map :column_name (:name %)
                                     :data_type   (or (:data_type %) "varchar")
                                     :is_nullable "YES") params)]
    {id (vec (concat system-cols param-cols))}))

(defn validate-sc-cron [expr]
  (let [s (clojure.string/trim (or expr ""))]
    (when (empty? s)
      (throw (ex-info "cron_expression must not be blank" {:field :cron_expression})))
    ;; 5 whitespace-separated fields; each field: digits, *, -, /, ,
    (when-not (re-matches #"[\d*/,\-]+\s+[\d*/,\-]+\s+[\d*/,\-]+\s+[\d*/,\-]+\s+[\w*/,\-]+" s)
      (throw (ex-info (str "Invalid cron expression: '" s "'. Expected 5 fields (min hour day month weekday)")
                      {:field :cron_expression})))
    s))

(defn validate-sc-timezone [tz]
  (let [s (clojure.string/trim (or tz "UTC"))]
    (try
      (java.time.ZoneId/of s)
      s
      (catch java.time.DateTimeException _
        (throw (ex-info (str "Invalid or unknown timezone: '" s "'") {:field :timezone}))))))

(defn save-sc [g id params]
  (let [cron-expr (validate-sc-cron (:cron_expression params))
        timezone  (validate-sc-timezone (:timezone params))
        sc-params (kw-maps (:params params))
        tcols     (sc-params->tcols id sc-params)
        node-params {:cron_expression cron-expr
                     :timezone        timezone
                     :enabled         (if (contains? params :enabled) (parse-bool (:enabled params)) true)
                     :params          sc-params
                     :tcols           tcols}
        g         (update-node g id node-params)
        child-ids (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-sc-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "cron_expression" (:cron_expression tmap)
           "timezone"        (:timezone tmap)
           "enabled"         (:enabled tmap)
           "params"          (:params tmap)
           "items"           (item_columns g (:tcols tmap)))))

;; --- Webhook (Wh) ---

(defn wh-tcols [id]
  {id [{:column_name "event_type"  :data_type "varchar" :is_nullable "YES"}
       {:column_name "payload"     :data_type "json"    :is_nullable "NO"}
       {:column_name "received_at" :data_type "varchar" :is_nullable "NO"}]})

(defn save-wh [g id params]
  (let [tcols          (wh-tcols id)
        existing       (getData g id)
        ;; Preserve existing secret when UI sends empty string (user did not re-enter it)
        secret-value   (let [v (clojure.string/trim (str (:secret_value params "")))]
                         (if (empty? v)
                           (:secret_value existing "")
                           v))
        node-params    {:webhook_path   (:webhook_path params)
                        :secret_header  (:secret_header params)
                        :secret_value   secret-value
                        :payload_format (or (:payload_format params) "json")
                        :tcols          tcols}
        g              (update-node g id node-params)
        child-ids      (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-wh-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    ;; secret_value intentionally omitted — never sent to the UI
    (assoc (item_master id name btype tmap)
           "webhook_path"    (:webhook_path tmap)
           "secret_header"   (:secret_header tmap)
           "secret_set"      (boolean (seq (str (:secret_value tmap ""))))
           "payload_format"  (:payload_format tmap)
           "items"           (item_columns g (:tcols tmap)))))

;; --- Conditional (C) ---

(defn save-conditional
  "Save conditional node configuration.
   :cond_type is one of: if-else, if-elif-else, multi-if, case, cond, pattern-match
   :branches is a vector of branch maps (structure varies by cond_type)
   :default_branch is the default/else group name."
  [g id params]
  (let [node-params {:cond_type      (or (:cond_type params) "if-else")
                     :branches       (or (:branches params) [])
                     :default_branch (or (:default_branch params) "")}]
    (update-node g id node-params)))

(defn get-conditional-item
  "Retrieve conditional node data for UI display.
   Returns parent columns as 'items' so the UI can reference them in conditions."
  [id g]
  (let [tmap    (getData g id)
        name    (:name tmap)
        btype   (:btype tmap)
        columns (or (:tcols tmap) (getTcols g id))]
    (assoc (item_master id name btype tmap)
           "cond_type"      (or (:cond_type tmap) "if-else")
           "branches"       (or (:branches tmap) [])
           "default_branch" (or (:default_branch tmap) "")
           "items"          (item_columns g columns))))

;; --- Logic / Function (Fu) ---

(defn logic-outputs->tcols
  "Convert fn_outputs vector into tcols format.
   Each output is {:output_name \"x\" :data_type \"int\"}.
   Produces {node-id [{:column_name \"x\" :data_type \"int\" :is_nullable \"YES\"} ...]}."
  [node-id outputs]
  {node-id (mapv (fn [o]
                   {:column_name (:output_name o)
                    :data_type   (or (:data_type o) "varchar")
                    :is_nullable "YES"})
                 (or outputs []))})

(defn save-logic
  "Save logic/function node configuration.
   :fn_name    — function name
   :fn_params  — [{:param_name \"a\" :param_type \"int\" :source_column \"table.col\"} ...]
   :fn_lets    — [{:variable \"d\" :expression \"3 * a + b\"} ...]
   :fn_return  — return expression string
   :fn_outputs — [{:output_name \"result\" :data_type \"double\"} ...] defines produced columns"
  [g id params]
  (let [kw-maps    (fn [coll] (mapv #(into {} (map (fn [[k v]] [(keyword k) v]) %)) coll))
        fn-params  (kw-maps (or (:fn_params params) []))
        fn-lets    (kw-maps (or (:fn_lets params) []))
        fn-outputs (kw-maps (or (:fn_outputs params) []))
        tcols      (logic-outputs->tcols id fn-outputs)
        node-params {:fn_name    (or (:fn_name params) "MyFunction")
                     :fn_params  fn-params
                     :fn_lets    fn-lets
                     :fn_return  (or (:fn_return params) "")
                     :fn_outputs fn-outputs
                     :tcols      tcols}
        g          (update-node g id node-params)
        child-ids  (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-logic-item
  "Retrieve logic/function node data for UI display.
   Returns parent columns as 'items' so user can map them to params."
  [id g]
  (let [tmap       (getData g id)
        name       (:name tmap)
        btype      (:btype tmap)
        parent-cols (or (:tcols tmap) (getTcols g id))]
    (assoc (item_master id name btype tmap)
           "fn_name"    (or (:fn_name tmap) "MyFunction")
           "fn_params"  (or (:fn_params tmap) [])
           "fn_lets"    (or (:fn_lets tmap) [])
           "fn_return"  (or (:fn_return tmap) "")
           "fn_outputs" (or (:fn_outputs tmap) [])
           "items"      (item_columns g parent-cols))))

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
                                     		outputId (find-node-id-by-btype graph "O")
                                     		g (if edge
                                     		    ;; Parent has outgoing edge: splice new node in between
                                     		    (-> graph
                                		      (delete-edge edge)
                                		      (add-edges [[tid id] [id (second edge)]])
  						      (setTcols id))
                                     		    ;; Parent is orphan: connect parent → new node → Output
                                     		    (-> graph
                                     		      (add-edges [[tid id] [id outputId]])
                                     		      (setTcols id)))
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
    (let [params  (:json-params args)
          conn-id (:connection/id
                   (db/insert-data :connection
                                   (sp/transform :port #(Integer/parseInt %)
                                                 (walk/keywordize-keys (:params args)))))]
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


(defn- normalize-layout-level
  [value default]
  (case value
    :U :U
    :L :L
    :S :S
    default))

(defn getLevelFromSibling [g sibling]
  (if sibling
    (normalize-layout-level (attr g sibling :level) :L)
    :S))

(defn getLevel [g sibling parent] (let [
		;;			  _ (println (str "sibling : " sibling)) 
		;;			  _ (println (str "parent : " parent)) 
                  ;;                        _ (pp/pprint g)
                                       ]
					(case (normalize-layout-level (attr g parent :level) nil)
                                                :U  :U
                                                :L  :L
                                                :S (getLevelFromSibling g sibling)
                                                (getLevelFromSibling g sibling))))

(defn getLati [g sibling parent] (let [plati (or (attr g parent :lati) 0)

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
                          longi (- (or (attr g parent :longi) 1) 1)
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
                                                            :endpoint_label (endpoint-node-label (second %))
                                                            :y (+ (:lati (second %)) latiEnd)
                                                            :x (+ (:longi (second %)) longiEnd)
                                                            :btype (:btype (second %))
                                                            :parent (case (count (parents gdef (first %)))
                                                                          0 0
                                                                          1 (first (parents gdef (first %))) 
                                                                          (parents gdef (first %)))
                                                            :id (first %)
                                                           ) attrs)))

(defn displayGraph [g] (let [coord (createCoordinates g)]
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
) ;; end comment

;; ---------------------------------------------------------------------------
;; Auth (Au)
;; ---------------------------------------------------------------------------

(defn save-auth [g id params]
  (let [kw-maps    (fn [coll] (mapv #(into {} (map (fn [[k v]] [(keyword k) v]) %)) coll))
        claims     (kw-maps (or (:claims_to_cols params) []))
        claim-cols (mapv (fn [c] {:column_name (:column c)
                                  :data_type   (or (:data_type c) "varchar")
                                  :is_nullable "YES"}) claims)
        parent-cols (vec (for [[_ v] (getTcols g id), item v] item))
        merged-cols {id (vec (concat parent-cols claim-cols))}
        ;; Preserve existing secret when UI sends blank (secret is never echoed back)
        existing-secret (:secret (getData g id))
        new-secret      (let [v (string/trim (str (:secret params "")))]
                          (if (empty? v) existing-secret v))
        node-params {:auth_type      (:auth_type params)
                     :token_header   (:token_header params)
                     :secret         new-secret
                     :claims_to_cols claims
                     :tcols          merged-cols}
        g           (update-node g id node-params)
        child-ids   (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-auth-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "auth_type"      (:auth_type tmap)
           "token_header"   (:token_header tmap)
           "secret_set"     (boolean (seq (str (:secret tmap ""))))
           "claims_to_cols" (:claims_to_cols tmap)
           "items"          (item_columns g (:tcols tmap)))))

;; ---------------------------------------------------------------------------
;; DB Execute (Dx)
;; ---------------------------------------------------------------------------

(defn parse-dx-tcols [id sql-template operation]
  (if (= "SELECT" (string/upper-case (or operation "SELECT")))
    (let [select-body (first (string/split
                               (string/replace sql-template #"(?i)^\s*select\s+" "")
                               #"(?i)\s+from\s+"))
          col-names   (map string/trim (string/split (or select-body "") #","))]
      {id (mapv #(hash-map :column_name % :data_type "varchar" :is_nullable "YES") col-names)})
    {id [{:column_name "affected_rows" :data_type "integer" :is_nullable "NO"}]}))

(defn validate-dx-sql [sql operation]
  (let [allowed #{"SELECT" "INSERT" "UPDATE" "DELETE"}
        op (string/upper-case (or operation "SELECT"))]
    (when-not (allowed op)
      (throw (ex-info "DB Execute: disallowed SQL operation" {:field :operation})))
    (when (re-find #"(?i)(CREATE|DROP|ALTER|TRUNCATE|GRANT|REVOKE)" (or sql ""))
      (throw (ex-info "DB Execute: DDL not permitted in sql_template" {:field :sql_template})))))

(defn save-dx [g id params]
  (let [sql       (:sql_template params)
        operation (or (:operation params) "SELECT")
        _         (validate-dx-sql sql operation)
        tcols     (parse-dx-tcols id sql operation)
        node-params {:connection_id (:connection_id params)
                     :operation     operation
                     :sql_template  sql
                     :result_mode   (or (:result_mode params) "single")
                     :tcols         tcols}
        g         (update-node g id node-params)
        child-ids (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-dx-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "connection_id" (:connection_id tmap)
           "operation"     (:operation tmap)
           "sql_template"  (:sql_template tmap)
           "result_mode"   (:result_mode tmap)
           "items"         (item_columns g (:tcols tmap)))))

;; ---------------------------------------------------------------------------
;; Rate Limiter (Rl) — transparent pass-through
;; ---------------------------------------------------------------------------

(defn- safe-int [v default]
  (try (Integer. (str v)) (catch Exception _ default)))

(defn save-rl [g id params]
  (let [parent-tcols (getTcols g id)
        g (update-node g id {:max_requests   (safe-int (:max_requests params) 100)
                             :window_seconds (safe-int (:window_seconds params) 60)
                             :key_type       (or (:key_type params) "ip")
                             :burst          (safe-int (:burst params) 0)
                             :tcols          parent-tcols})
        child-ids (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-rl-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "max_requests"   (:max_requests tmap)
           "window_seconds" (:window_seconds tmap)
           "key_type"       (:key_type tmap)
           "burst"          (:burst tmap)
           "items"          (item_columns g (:tcols tmap)))))

;; ---------------------------------------------------------------------------
;; CORS (Cr) — transparent pass-through
;; ---------------------------------------------------------------------------

(defn save-cr [g id params]
  (let [str->vec     (fn [v] (if (string? v) (string/split v #",\s*") (or v [])))
        parent-tcols (getTcols g id)
        g (update-node g id {:allowed_origins   (str->vec (:allowed_origins params))
                             :allowed_methods   (str->vec (:allowed_methods params))
                             :allowed_headers   (str->vec (:allowed_headers params))
                             :allow_credentials (= "true" (str (:allow_credentials params)))
                             :max_age           (safe-int (:max_age params) 86400)
                             :tcols             parent-tcols})
        child-ids (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-cr-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "allowed_origins"   (:allowed_origins tmap)
           "allowed_methods"   (:allowed_methods tmap)
           "allowed_headers"   (:allowed_headers tmap)
           "allow_credentials" (:allow_credentials tmap)
           "max_age"           (:max_age tmap)
           "items"             (item_columns g (:tcols tmap)))))

;; ---------------------------------------------------------------------------
;; Logger (Lg) — transparent pass-through
;; ---------------------------------------------------------------------------

(defn save-lg [g id params]
  (let [parent-tcols (getTcols g id)
        g (update-node g id {:log_level     (or (:log_level params) "INFO")
                             :fields_to_log (or (:fields_to_log params) [])
                             :destination   (or (:destination params) "console")
                             :format        (or (:format params) "json")
                             :external_url  (:external_url params)
                             :tcols         parent-tcols})
        child-ids (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-lg-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "log_level"     (:log_level tmap)
           "fields_to_log" (:fields_to_log tmap)
           "destination"   (:destination tmap)
           "format"        (:format tmap)
           "external_url"  (:external_url tmap)
           "items"         (item_columns g (:tcols tmap)))))

;; ---------------------------------------------------------------------------
;; Cache (Cq) — transparent pass-through
;; ---------------------------------------------------------------------------

(defn save-cq [g id params]
  (let [parent-tcols (getTcols g id)
        g (update-node g id {:cache_key   (or (:cache_key params) "")
                             :ttl_seconds (safe-int (:ttl_seconds params) 300)
                             :strategy    (or (:strategy params) "read-through")
                             :tcols       parent-tcols})
        child-ids (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-cq-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "cache_key"   (:cache_key tmap)
           "ttl_seconds" (:ttl_seconds tmap)
           "strategy"    (:strategy tmap)
           "items"       (item_columns g (:tcols tmap)))))

;; ---------------------------------------------------------------------------
;; Event Emitter (Ev) — transparent pass-through, fire-and-forget
;; ---------------------------------------------------------------------------

(defn save-ev [g id params]
  (let [parent-tcols (getTcols g id)
        g (update-node g id {:topic        (or (:topic params) "")
                             :broker_url   (or (:broker_url params) "")
                             :key_template (or (:key_template params) "")
                             :format       (or (:format params) "json")
                             :tcols        parent-tcols})
        child-ids (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-ev-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "topic"        (:topic tmap)
           "broker_url"   (:broker_url tmap)
           "key_template" (:key_template tmap)
           "format"       (:format tmap)
           "items"        (item_columns g (:tcols tmap)))))

;; ---------------------------------------------------------------------------
;; Circuit Breaker (Ci) — transparent pass-through
;; ---------------------------------------------------------------------------

(defn save-ci [g id params]
  (let [parent-tcols (getTcols g id)
        g (update-node g id {:failure_threshold (safe-int (:failure_threshold params) 5)
                             :reset_timeout     (safe-int (:reset_timeout params) 30)
                             :fallback_response (or (:fallback_response params) "{}")
                             :tcols             parent-tcols})
        child-ids (map second (find-edges g id))]
    (reduce (fn [acc cid] (assoc-in acc [:n cid :na :tcols] (getTcols acc cid))) g child-ids)))

(defn get-ci-item [id g]
  (let [tmap  (getData g id)
        name  (:name tmap)
        btype (:btype tmap)]
    (assoc (item_master id name btype tmap)
           "failure_threshold" (:failure_threshold tmap)
           "reset_timeout"     (:reset_timeout tmap)
           "fallback_response" (:fallback_response tmap)
           "items"             (item_columns g (:tcols tmap)))))
