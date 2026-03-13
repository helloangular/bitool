(ns bitool.gil.validator
  (:require [bitool.gil.schema :as schema]
            [clojure.string :as string]
            [clojure.set :as set]))

(defn- err
  ([code message] {:code code :message message})
  ([code message m] (merge {:code code :message message} m)))

(defn- warn
  ([code message] {:code code :message message})
  ([code message m] (merge {:code code :message message} m)))

(defn- parse-int [v]
  (try
    (cond
      (integer? v) v
      (string? v)  (Integer/parseInt (string/trim v))
      :else nil)
    (catch Exception _ nil)))

(defn- positive-int? [v]
  (let [n (parse-int v)]
    (and n (pos? n))))

(defn- status-code? [v]
  (let [n (parse-int v)]
    (and n (<= 100 n 599))))

(defn- simple-cron? [v]
  (boolean
    (and (string? v)
         (re-matches #"[\d*/,\-]+\s+[\d*/,\-]+\s+[\d*/,\-]+\s+[\d*/,\-]+\s+[\w*/,\-]+" (string/trim v)))))

(defn resolve-node-ref
  "Resolve a ref against an existing graph.
   Order: :gil_ref, :name, numeric id."
  [g ref]
  (let [ref-str (some-> ref str)
        by-gil (first
                 (for [[id {:keys [na]}] (:n g)
                       :when (= ref-str (str (:gil_ref na)))]
                   id))
        by-name (first
                  (for [[id {:keys [na]}] (:n g)
                        :when (= ref-str (str (:name na)))]
                    id))
        by-id (let [n (parse-int ref-str)]
                (when (contains? (:n g) n) n))]
    (or by-gil by-name by-id)))

(defn- incoming-map [edges]
  (reduce (fn [m [_ to]] (update m to (fnil inc 0))) {} edges))

(defn- outgoing-map [edges]
  (reduce (fn [m [from _]] (update m from (fnil inc 0))) {} edges))

(defn- has-cycle?
  [refs edges]
  (let [refs      (set refs)
        indeg0    (merge (zipmap refs (repeat 0))
                         (incoming-map edges))
        out-adj   (reduce (fn [m [from to]] (update m from (fnil conj []) to)) {} edges)
        queue0    (into clojure.lang.PersistentQueue/EMPTY
                        (filter #(zero? (get indeg0 % 0)) refs))]
    (loop [q queue0
           indeg indeg0
           visited 0]
      (if (empty? q)
        (< visited (count refs))
        (let [n (peek q)
              q (pop q)
              [indeg q]
              (reduce
                (fn [[in qq] child]
                  (let [nxt (dec (get in child 0))
                        in  (assoc in child nxt)]
                    (if (zero? nxt)
                      [in (conj qq child)]
                      [in qq])))
                [indeg q]
                (get out-adj n []))]
          (recur q indeg (inc visited)))))))

(defn- validate-config-keys [node]
  (let [t (:type node)
        cfg (or (:config node) {})
        allowed (set (get-in schema/node-types [t :config-keys] []))
        required (set (get-in schema/node-types [t :required-config] []))
        unknown (remove allowed (keys cfg))
        missing (remove #(contains? cfg %) required)]
    {:unknown unknown
     :missing missing}))

(defn- validate-node [node]
  (let [t (:type node)
        ref (:node-ref node)
        cfg (or (:config node) {})
        {:keys [unknown missing]} (validate-config-keys node)
        errs (atom [])
        warns (atom [])]
    (when (or (nil? ref) (string/blank? (str ref)))
      (swap! errs conj (err :missing-node-ref "Node is missing :node-ref")))
    (when-not (schema/valid-type? t)
      (swap! errs conj (err :unknown-node-type
                       (str "Unknown node type: " t)
                       {:ref ref :type t})))
    (doseq [k unknown]
      (swap! errs conj (err :unknown-config-key
                       (str "Unknown config key " k " for node type " t)
                       {:ref ref :key k :type t})))
    (doseq [k missing]
      (swap! errs conj (err :missing-required-config
                       (str "Missing required config key " k " for node type " t)
                       {:ref ref :key k :type t})))

    ;; type-specific checks
    (when (= t "endpoint")
      (let [m (some-> (:http_method cfg) str string/upper-case)
            p (:route_path cfg)]
        (when (and m (not (contains? schema/http-methods m)))
          (swap! errs conj (err :invalid-http-method
                           (str "Invalid endpoint http_method: " m)
                           {:ref ref})))
        (when (and (string? p) (not (string/starts-with? p "/")))
          (swap! errs conj (err :invalid-route-path
                           "Endpoint route_path must start with '/'."
                           {:ref ref :route_path p})))))

    (when (= t "auth")
      (let [a (some-> (:auth_type cfg) str string/lower-case)]
        (when (and a (not (contains? schema/auth-types a)))
          (swap! errs conj (err :invalid-auth-type
                           (str "Invalid auth_type: " a)
                           {:ref ref})))))

    (when (= t "join")
      (let [j (some-> (:join_type cfg) str string/lower-case)]
        (when (and j (not (contains? schema/join-types j)))
          (swap! errs conj (err :invalid-join-type
                           (str "Invalid join_type: " j)
                           {:ref ref})))))

    (when (= t "conditionals")
      (let [c (some-> (:cond_type cfg) str string/lower-case)]
        (when (and c (not (contains? schema/cond-types c)))
          (swap! errs conj (err :invalid-cond-type
                           (str "Invalid cond_type: " c)
                           {:ref ref})))))

    (when (= t "db-execute")
      (let [op (some-> (:operation cfg) str string/upper-case)]
        (when (and op (not (contains? schema/dx-operations op)))
          (swap! errs conj (err :invalid-dx-operation
                           (str "Invalid db-execute operation: " op)
                           {:ref ref})))))

    (when (= t "validator")
      (doseq [rule (or (:rules cfg) [])]
        (let [r (some-> (:rule rule) str string/lower-case)]
          (when (and r (not (contains? schema/vd-rule-types r)))
            (swap! errs conj (err :invalid-validator-rule
                             (str "Invalid validator rule: " r)
                             {:ref ref :rule r})))))) 

    (when (= t "rate-limiter")
      (when (and (contains? cfg :max_requests) (not (positive-int? (:max_requests cfg))))
        (swap! errs conj (err :invalid-max-requests
                         "rate-limiter :max_requests must be a positive integer."
                         {:ref ref})))
      (when (and (contains? cfg :window_seconds) (not (positive-int? (:window_seconds cfg))))
        (swap! errs conj (err :invalid-window-seconds
                         "rate-limiter :window_seconds must be a positive integer."
                         {:ref ref}))))

    (when (= t "cache")
      (when (and (contains? cfg :ttl_seconds) (not (positive-int? (:ttl_seconds cfg))))
        (swap! errs conj (err :invalid-ttl
                         "cache :ttl_seconds must be a positive integer."
                         {:ref ref}))))

    (when (= t "response-builder")
      (when (and (contains? cfg :status_code) (not (status-code? (:status_code cfg))))
        (swap! errs conj (err :invalid-status-code
                         "response-builder :status_code must be an integer 100..599."
                         {:ref ref}))))

    (when (= t "scheduler")
      (when (and (contains? cfg :cron_expression) (not (simple-cron? (str (:cron_expression cfg)))))
        (swap! errs conj (err :invalid-cron
                         "scheduler :cron_expression must be a valid 5-part cron."
                         {:ref ref :cron_expression (:cron_expression cfg)}))))

    (when (empty? cfg)
      (swap! warns conj (warn :empty-config
                         (str "Node '" ref "' has empty config.")
                         {:ref ref :type t})))

    {:errors @errs
     :warnings @warns}))

(defn- validate-build [gil]
  (let [nodes (or (:nodes gil) [])
        edges (or (:edges gil) [])
        refs  (mapv :node-ref nodes)
        type-by-ref (into {} (map (fn [n] [(:node-ref n) (:type n)]) nodes))
        errs (atom [])
        warns (atom [])]
    (when-not (seq nodes)
      (swap! errs conj (err :missing-nodes "build intent requires non-empty :nodes")))
    (when-not (contains? gil :edges)
      (swap! errs conj (err :missing-edges "build intent requires :edges")))
    (when-not (seq (str (:graph-name gil)))
      (swap! errs conj (err :missing-graph-name "build intent requires :graph-name")))
    (when (seq (:patches gil))
      (swap! errs conj (err :invalid-build-shape "build intent must not include :patches")))

    (let [dups (->> refs frequencies (filter (fn [[_ c]] (> c 1))) (map first))]
      (doseq [dup dups]
        (swap! errs conj (err :duplicate-node-ref
                         (str "Duplicate :node-ref '" dup "'.")
                         {:ref dup}))))

    (doseq [node nodes
            :let [{:keys [errors warnings]} (validate-node node)]]
      (doseq [e errors] (swap! errs conj e))
      (doseq [w warnings] (swap! warns conj w)))

    ;; Edge checks
    (let [seen (atom #{})]
      (doseq [e edges]
        (if-not (and (vector? e) (= 2 (count e)))
          (swap! errs conj (err :invalid-edge-shape
                           "Edge must be a 2-item vector [from-ref to-ref]."
                           {:edge e}))
          (let [[from to] e
                key [from to]]
            (when (contains? @seen key)
              (swap! errs conj (err :duplicate-edge
                               (str "Duplicate edge " from " -> " to)
                               {:from from :to to})))
            (swap! seen conj key)
            (when (= from to)
              (swap! errs conj (err :self-edge
                               (str "Self-edge not allowed for '" from "'.")
                               {:from from :to to})))
            (when-not (contains? (set refs) from)
              (swap! errs conj (err :unknown-edge-ref
                               (str "Edge source '" from "' does not exist.")
                               {:from from})))
            (when-not (contains? (set refs) to)
              (swap! errs conj (err :unknown-edge-ref
                               (str "Edge target '" to "' does not exist.")
                               {:to to})))
            (when (and (contains? type-by-ref from)
                       (contains? type-by-ref to)
                       (not (schema/valid-edge? (type-by-ref from) (type-by-ref to))))
              (swap! errs conj (err :illegal-edge
                               (str "Illegal edge " from " -> " to)
                               {:from from :to to})))))))

    ;; Structural graph rules
    (let [incoming (incoming-map edges)
          outgoing (outgoing-map edges)
          output-refs (filter #(= "output" (type-by-ref %)) refs)]
      (when-not (= 1 (count output-refs))
        (swap! errs conj (err :invalid-output-count
                         "build graphs must contain exactly one output node.")))

      (doseq [r refs
              :let [t (type-by-ref r)]]
        (when (and (schema/source-type? t)
                   (zero? (get outgoing r 0)))
          (swap! errs conj (err :source-without-outgoing
                           (str "Source node '" r "' has no outgoing edge.")
                           {:ref r})))
        (when (and (not (schema/source-type? t))
                   (not (schema/terminal-type? t))
                   (zero? (get incoming r 0)))
          (swap! errs conj (err :node-without-incoming
                           (str "Node '" r "' has no incoming edge.")
                           {:ref r}))))

      (doseq [r refs
              :when (= "join" (type-by-ref r))]
        (when-not (= 2 (get incoming r 0))
          (swap! errs conj (err :join-needs-two-parents
                           (str "Join node '" r "' needs exactly two incoming edges.")
                           {:ref r :incoming (get incoming r 0)})))))

      (when (and (seq refs) (has-cycle? refs edges))
        (swap! errs conj (err :cycle-detected "Graph contains a cycle.")))

    {:errors @errs
     :warnings @warns}))

(defn- patch-op-valid? [op]
  (contains? #{:add-node :remove-node :update-config :add-edge :remove-edge :move-node} op))

(defn- btype-at [g id]
  (get-in g [:n id :na :btype]))

(defn- type-at [g id]
  (schema/btype->type (btype-at g id)))

(defn- validate-patch [patch existing-graph]
  (let [errs (atom [])
        warns (atom [])
        op (:op patch)]
    (when-not (patch-op-valid? op)
      (swap! errs conj (err :invalid-patch-op
                       (str "Invalid patch op '" op "'.")
                       {:op op})))

    (when (and (= op :add-node) (nil? (:node patch)))
      (swap! errs conj (err :missing-patch-node "add-node patch requires :node")))

    (when-let [node (:node patch)]
      (let [{:keys [errors warnings]} (validate-node node)]
        (doseq [e errors] (swap! errs conj e))
        (doseq [w warnings] (swap! warns conj w))))

    (when (contains? #{:remove-node :update-config :move-node} op)
      (when-not (:ref patch)
        (swap! errs conj (err :missing-patch-ref (str (name op) " requires :ref")))))

    (when (contains? #{:add-edge :remove-edge} op)
      (when-not (:from patch)
        (swap! errs conj (err :missing-patch-from (str (name op) " requires :from"))))
      (when-not (:to patch)
        (swap! errs conj (err :missing-patch-to (str (name op) " requires :to")))))

    ;; Existing graph dependent checks
    (if-not existing-graph
      (swap! warns conj (warn :no-existing-graph
                         "Patch validation skipped ref resolution because no current graph was provided."))
      (do
        (when-let [r (:ref patch)]
          (when-not (resolve-node-ref existing-graph r)
            (swap! errs conj (err :unknown-patch-ref
                             (str "Patch ref '" r "' does not resolve.")
                             {:ref r}))))
        (when-let [a (:after patch)]
          (when-not (resolve-node-ref existing-graph a)
            (swap! errs conj (err :unknown-patch-after
                             (str "Patch :after ref '" a "' does not resolve.")
                             {:after a}))))
        (when-let [b (:before patch)]
          (when-not (resolve-node-ref existing-graph b)
            (swap! errs conj (err :unknown-patch-before
                             (str "Patch :before ref '" b "' does not resolve.")
                             {:before b}))))
        (when (contains? #{:add-edge :remove-edge} op)
          (let [from-id (resolve-node-ref existing-graph (:from patch))
                to-id   (resolve-node-ref existing-graph (:to patch))]
            (when-not from-id
              (swap! errs conj (err :unknown-patch-from
                               (str "Patch :from ref '" (:from patch) "' does not resolve.")
                               {:from (:from patch)})))
            (when-not to-id
              (swap! errs conj (err :unknown-patch-to
                               (str "Patch :to ref '" (:to patch) "' does not resolve.")
                               {:to (:to patch)})))
            (when (and from-id to-id (= op :add-edge))
              (let [from-type (type-at existing-graph from-id)
                    to-type   (type-at existing-graph to-id)]
                (when-not (schema/valid-edge? from-type to-type)
                  (swap! errs conj (err :illegal-patch-edge
                                   (str "Illegal edge " (:from patch) " -> " (:to patch))
                                   {:from (:from patch) :to (:to patch)})))))))
        (when (= op :update-config)
          (let [rid (:ref patch)
                node-id (resolve-node-ref existing-graph rid)
                node-type (when node-id (type-at existing-graph node-id))
                allowed (set (get-in schema/node-types [node-type :config-keys] []))]
            (doseq [k (keys (or (:config patch) {}))]
              (when-not (contains? allowed k)
                (swap! errs conj (err :unknown-config-key
                                 (str "Unknown config key " k " for node type " node-type)
                                 {:ref rid :key k :type node-type}))))))))

    {:errors @errs
     :warnings @warns}))

(defn validate
  "Validate normalized GIL. Pass existing graph for patch validation."
  ([gil] (validate gil nil))
  ([gil existing-graph]
   (let [errs (atom [])
         warns (atom [])
         intent (:intent gil)
         gv (:gil-version gil)]
     (when-not (= schema/gil-version gv)
       (swap! errs conj (err :invalid-gil-version
                        (str "gil-version must be " schema/gil-version ", got " gv))))
     (when-not (contains? #{:build :patch} intent)
       (swap! errs conj (err :invalid-intent "intent must be :build or :patch.")))

     (if (= intent :build)
       (let [{:keys [errors warnings]} (validate-build gil)]
         (doseq [e errors] (swap! errs conj e))
         (doseq [w warnings] (swap! warns conj w)))
       (do
         (when (or (nil? (:patches gil)) (empty? (:patches gil)))
           (swap! errs conj (err :missing-patches "patch intent requires non-empty :patches")))
         (when (or (seq (:nodes gil)) (seq (:edges gil)))
           (swap! errs conj (err :invalid-patch-shape "patch intent must not include :nodes or :edges")))
         (when-not existing-graph
           (swap! errs conj (err :missing-target-graph "patch intent requires an existing graph in session.")))
         (doseq [patch (:patches gil)
                 :let [{:keys [errors warnings]} (validate-patch patch existing-graph)]]
           (doseq [e errors] (swap! errs conj e))
           (doseq [w warnings] (swap! warns conj w)))))

     (let [errors @errs
           warnings @warns]
       {:valid (empty? errors)
        :errors errors
        :warnings warnings}))))
