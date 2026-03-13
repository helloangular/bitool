(ns bitool.gil.compiler
  (:require [bitool.db :as db]
            [bitool.graph2 :as g2]
            [bitool.endpoint :as endpoint]
            [bitool.gil.schema :as schema]
            [bitool.gil.validator :as validator]
            [clojure.string :as string]
            [clojure.set :as set]))

(defn- node-by-ref [nodes]
  (into {} (map (fn [n] [(:node-ref n) n]) nodes)))

(defn- incoming-map [edges]
  (reduce (fn [m [_ to]] (update m to (fnil inc 0))) {} edges))

(defn- outgoing-adj [edges]
  (reduce (fn [m [from to]] (update m from (fnil conj []) to)) {} edges))

(defn- topo-sort-refs
  "Deterministic topological sort by node-ref."
  [nodes edges]
  (let [refs (set (map :node-ref nodes))
        indeg0 (merge (zipmap refs (repeat 0)) (incoming-map edges))
        adj (outgoing-adj edges)]
    (loop [remaining refs
           indeg indeg0
           out []]
      (if (empty? remaining)
        out
        (let [zeroes (sort (filter #(zero? (get indeg % 0)) remaining))]
          (if (empty? zeroes)
            ;; Validation blocks cycles, but keep deterministic fallback.
            (vec (sort refs))
            (let [n (first zeroes)
                  indeg' (reduce (fn [m child] (update m child dec))
                                 indeg
                                 (get adj n []))]
              (recur (disj remaining n) indeg' (conj out n)))))))))

(defn- avg [xs]
  (if (seq xs)
    (/ (reduce + xs) (count xs))
    100))

(defn- compute-layout
  "Compute coordinates for refs without explicit positions."
  [nodes edges order]
  (let [by-ref (node-by-ref nodes)
        parent-refs (reduce (fn [m [from to]] (update m to (fnil conj []) from)) {} edges)]
    (loop [coords {}
           source-y 100
           refs order]
      (if (empty? refs)
        coords
        (let [r (first refs)
              node (get by-ref r)
              pos (:position node)
              parents (get parent-refs r)
              c (cond
                  (and (map? pos) (number? (:x pos)) (number? (:y pos)))
                  {:x (:x pos) :y (:y pos)}

                  (= "output" (:type node))
                  (let [xs (map :x (vals coords))
                        ys (map :y (vals coords))]
                    {:x (+ 200 (if (seq xs) (apply max xs) 100))
                     :y (int (Math/round (double (avg ys))))})

                  (empty? parents)
                  {:x 100 :y source-y}

                  :else
                  (let [pcs (map coords parents)
                        xs (map :x pcs)
                        ys (map :y pcs)]
                    {:x (+ 200 (apply max xs))
                     :y (int (Math/round (double (avg ys))))}))]
          (recur (assoc coords r c)
                 (if (empty? parents) (+ source-y 200) source-y)
                 (rest refs)))))))

(defn- save-fn-name [f]
  (if-let [n (some-> f meta :name)]
    (name n)
    "update-node"))

(defn plan-gil
  "Pure planner. No DB access."
  [gil]
  (if (= :patch (:intent gil))
    {:steps (mapv (fn [p] {:action :patch :patch p}) (:patches gil))
     :layout {}
     :node-order []
     :warnings []}
    (let [nodes (:nodes gil)
          edges (:edges gil)
          order (topo-sort-refs nodes edges)
          layout (compute-layout nodes edges order)
          by-ref (node-by-ref nodes)
          create-steps
          (->> order
               (keep (fn [ref]
                       (let [node (get by-ref ref)
                             t (:type node)]
                         (when (not= t "output")
                           {:action :create-node
                            :ref ref
                            :type t
                            :btype (schema/type->btype t)
                            :alias (:alias node)
                            :x (get-in layout [ref :x])
                            :y (get-in layout [ref :y])}))))
               vec)
          edge-steps (mapv (fn [[from to]] {:action :create-edge :from from :to to}) edges)
          save-steps
          (->> order
               (keep (fn [ref]
                       (let [node (get by-ref ref)
                             cfg  (or (:config node) {})
                             sf   (get-in schema/node-types [(:type node) :save-fn])]
                         (when (seq cfg)
                           {:action :save-config
                            :ref ref
                            :save-fn (save-fn-name sf)
                            :config cfg}))))
               vec)]
      {:steps (vec (concat [{:action :create-graph :name (:graph-name gil)}]
                           create-steps
                           edge-steps
                           save-steps))
       :layout layout
       :node-order order
       :warnings []})))

(defn- rename-op [m]
  (into {} (map (fn [[k v]] (if (and (= k :alias) (= v "O")) [k "Output"] [k v])) m)))

(defn- update-coordinates [coord-v]
  (map #(update % :y (fn [x] (+ (* x 50) 150)))
       (map #(update % :x (fn [x] (+ (* x 100) 150))) coord-v)))

(defn- get-coordinates [g]
  (let [n (g2/nodecount g)]
    (if (= n 0)
      []
      (map rename-op (update-coordinates (g2/createCoordinates g))))))

(defn- panel-items [g]
  (let [cp (g2/connected-graph g)
        sp (g2/unconnected-graph g)]
    (into (get-coordinates cp) (g2/getOrphanAttrs sp))))

(defn- find-new-node-id [before after]
  (let [before-ids (set (keys (:n before)))
        after-ids  (set (keys (:n after)))
        new-ids    (set/difference after-ids before-ids)]
    (first (sort > new-ids))))

(defn resolve-node-ref
  "Resolve patch refs in the current graph."
  [g ref]
  (validator/resolve-node-ref g ref))

(defn- maybe-save-config!
  [gid node-id type-name config]
  (when (seq config)
    (let [save-fn (get-in schema/node-types [type-name :save-fn])
          g       (db/getGraph gid)]
      (if save-fn
        (db/insertGraph (save-fn g node-id config))
        (db/insertGraph (g2/update-node g node-id config))))))

(defn- set-gil-refs!
  [gid ref->id]
  (let [g  (db/getGraph gid)
        g' (reduce (fn [acc [ref id]]
                     (update-in acc [:n id :na] assoc :gil_ref ref))
                   g
                   ref->id)]
    (db/insertGraph g')))

(defn- register-ep-wh!
  [gid g]
  (->> (:n g)
       (keep (fn [[id _]]
               (let [na (get-in g [:n id :na])
                     bt (:btype na)]
                 (when (contains? #{"Ep" "Wh"} bt)
                   (when-let [entry (endpoint/register-endpoint! gid id na)]
                     {:node_id id
                      :btype bt
                      :method (-> (:method entry) name string/upper-case)
                      :deployed_path (:path entry)})))))
       vec))

(defn- apply-build
  [gil]
  (let [plan        (plan-gil gil)
        order       (:node-order plan)
        layout      (:layout plan)
        nodes       (:nodes gil)
        by-ref      (node-by-ref nodes)
        output-ref  (:node-ref (first (filter #(= "output" (:type %)) nodes)))
        created-ids (atom {})
        g0          (g2/createGraph (:graph-name gil))
        gid         (get-in g0 [:a :id])]
    ;; create nodes (output already exists as node 1)
    (doseq [ref order
            :let [node (get by-ref ref)]
            :when (not= "output" (:type node))]
      (let [before (db/getGraph gid)
            _      (g2/add-single-node gid nil (:alias node) (schema/type->btype (:type node))
                                        (get-in layout [ref :x]) (get-in layout [ref :y]))
            after  (db/getGraph gid)
            new-id (find-new-node-id before after)]
        (swap! created-ids assoc ref new-id)))

    ;; include output ref and stamp all gil refs in one persisted pass
    (set-gil-refs! gid (merge @created-ids {output-ref 1}))

    ;; create edges in declaration order
    (doseq [[from to] (:edges gil)]
      (let [from-id (get (merge @created-ids {output-ref 1}) from)
            to-id   (get (merge @created-ids {output-ref 1}) to)]
        (when (and from-id to-id)
          (g2/connect-single-node gid from-id to-id))))

    ;; save configs in topo order
    (doseq [ref order
            :let [node (get by-ref ref)
                  cfg  (or (:config node) {})
                  id   (if (= ref output-ref) 1 (get @created-ids ref))]]
      (when id
        (maybe-save-config! gid id (:type node) cfg)))

    (let [g-final (db/getGraph gid)
          deployed (register-ep-wh! gid g-final)]
      {:graph-id gid
       :version (get-in g-final [:a :v])
       :node-map (merge @created-ids {output-ref 1})
       :graph g-final
       :panel (panel-items g-final)
       :registered-endpoints deployed})))

(defn- incoming-ids [g node-id]
  (->> (:n g)
       (keep (fn [[id nd]]
               (when (contains? (set (keys (:e nd))) node-id) id)))
       vec))

(defn- outgoing-ids [g node-id]
  (vec (keys (get-in g [:n node-id :e] {}))))

(defn- remove-edges [g edges]
  (reduce (fn [acc [from to]]
            (if (contains? (set (keys (get-in acc [:n from :e] {}))) to)
              (g2/delete-edge acc [from to])
              acc))
          g
          edges))

(defn- add-edges [g edges]
  (reduce (fn [acc [from to]]
            (if (g2/validate-connect acc from to)
              (g2/add-edge acc [from to])
              (throw (ex-info "Invalid edge during patch graph rewrite."
                              {:from from :to to}))))
          g
          edges))

(defn- apply-add-node-patch!
  [gid patch]
  (let [node      (:node patch)
        node-ref  (:node-ref node)
        type-name (:type node)
        btype     (schema/type->btype type-name)
        x         (or (get-in node [:position :x]) 100)
        y         (or (get-in node [:position :y]) 100)
        after-id  (when-let [r (:after patch)] (resolve-node-ref (db/getGraph gid) r))
        before-id (when-let [r (:before patch)] (resolve-node-ref (db/getGraph gid) r))
        before-g  (db/getGraph gid)
        _         (g2/add-single-node gid nil (:alias node) btype x y)
        after-g   (db/getGraph gid)
        new-id    (find-new-node-id before-g after-g)]
    (set-gil-refs! gid {node-ref new-id})
    (cond
      (and after-id before-id)
      (let [g (db/getGraph gid)
            g (remove-edges g [[after-id before-id]])
            g (add-edges g [[after-id new-id] [new-id before-id]])]
        (db/insertGraph g))

      after-id
      (g2/connect-single-node gid after-id new-id)

      before-id
      (g2/connect-single-node gid new-id before-id)

      :else nil)
    (maybe-save-config! gid new-id type-name (:config node))
    [node-ref new-id]))

(defn- apply-remove-node-patch!
  [gid patch]
  (let [g (db/getGraph gid)
        id (resolve-node-ref g (:ref patch))
        bt (get-in g [:n id :na :btype])]
    (when id
      (when (contains? #{"Ep" "Wh"} bt)
        (endpoint/unregister-endpoint! gid id))
      (db/insertGraph (g2/remove-node g id)))))

(defn- apply-update-config-patch!
  [gid patch]
  (let [g (db/getGraph gid)
        id (resolve-node-ref g (:ref patch))
        t (when id (schema/btype->type (get-in g [:n id :na :btype])))]
    (when (and id t)
      (maybe-save-config! gid id t (:config patch)))))

(defn- apply-add-edge-patch!
  [gid patch]
  (let [g (db/getGraph gid)
        from-id (resolve-node-ref g (:from patch))
        to-id   (resolve-node-ref g (:to patch))]
    (when (and from-id to-id)
      (g2/connect-single-node gid from-id to-id))))

(defn- apply-remove-edge-patch!
  [gid patch]
  (let [g (db/getGraph gid)
        from-id (resolve-node-ref g (:from patch))
        to-id   (resolve-node-ref g (:to patch))]
    (when (and from-id to-id)
      (db/insertGraph (remove-edges g [[from-id to-id]])))))

(defn- apply-move-node-patch!
  [gid patch]
  (let [g0 (db/getGraph gid)
        id (resolve-node-ref g0 (:ref patch))
        after-id (when-let [r (:after patch)] (resolve-node-ref g0 r))
        before-id (when-let [r (:before patch)] (resolve-node-ref g0 r))]
    (when id
      (let [parents (incoming-ids g0 id)
            children (outgoing-ids g0 id)
            old-before (first children)
            old-after (first parents)
            use-after (or after-id old-after)
            use-before (or before-id old-before)
            g1 (remove-edges g0 (concat (map #(vector % id) parents)
                                        (map #(vector id %) children)))
            g2 (if (and use-after use-before)
                 (remove-edges g1 [[use-after use-before]])
                 g1)
            g3 (add-edges g2 (cond-> []
                               use-after (conj [use-after id])
                               use-before (conj [id use-before])))]
        (db/insertGraph g3)))))

(defn- apply-patch
  [gid patch]
  (case (:op patch)
    :add-node (apply-add-node-patch! gid patch)
    :remove-node (apply-remove-node-patch! gid patch)
    :update-config (apply-update-config-patch! gid patch)
    :add-edge (apply-add-edge-patch! gid patch)
    :remove-edge (apply-remove-edge-patch! gid patch)
    :move-node (apply-move-node-patch! gid patch)
    nil))

(defn- apply-patch-intent
  [gil session]
  (let [gid (:gid session)
        created (atom {})]
    (doseq [patch (:patches gil)]
      (when (= :add-node (:op patch))
        (when-let [[ref id] (apply-patch gid patch)]
          (swap! created assoc ref id)))
      (when (not= :add-node (:op patch))
        (apply-patch gid patch)))
    (let [g-final (db/getGraph gid)
          deployed (register-ep-wh! gid g-final)]
      {:graph-id gid
       :version (get-in g-final [:a :v])
       :node-map @created
       :graph g-final
       :panel (panel-items g-final)
       :registered-endpoints deployed})))

(defn apply-gil
  "Execute validated GIL against the DB."
  [gil session]
  (if (= :patch (:intent gil))
    (apply-patch-intent gil session)
    (apply-build gil)))
