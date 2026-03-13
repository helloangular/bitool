(ns bitool.gil.normalize
  (:require [bitool.gil.schema :as schema]
            [clojure.string :as string]
            [clojure.walk :as walk]))

(defn- normalize-key
  "Convert string/keyword keys to kebab-case keywords."
  [k]
  (cond
    (keyword? k) (-> k name (string/replace "_" "-") keyword)
    (string? k)  (-> k (string/replace "_" "-") keyword)
    :else k))

(defn- normalize-config-key
  "Convert config keys to snake_case keywords expected by graph2 save fns."
  [k]
  (let [s (cond
            (keyword? k) (name k)
            (string? k) k
            :else (str k))]
    (-> s
        (string/replace "-" "_")
        (string/replace #"([a-z0-9])([A-Z])" "$1_$2")
        string/lower-case
        keyword)))

(defn- keywordize-kebab
  "Recursively keywordize keys and convert snake_case to kebab-case."
  [x]
  (walk/postwalk
    (fn [node]
      (if (map? node)
        (into {} (map (fn [[k v]] [(normalize-key k) v]) node))
        node))
    x))

(defn- normalize-intent [intent]
  (cond
    (keyword? intent) intent
    (string? intent)  (-> intent string/lower-case keyword)
    :else nil))

(defn- normalize-ref [r]
  (when-not (nil? r)
    (str r)))

(defn- normalize-position [p]
  (when (map? p)
    {:x (:x p) :y (:y p)}))

(defn- normalize-node [node idx]
  (let [raw-type    (:type node)
        t0          (schema/canonical-type (if (keyword? raw-type) (name raw-type) raw-type))
        alias       (or (:alias node) (when t0 (str t0 "-" (inc idx))))
        config      (if (map? (:config node))
                      (into {}
                            (map (fn [[k v]] [(normalize-config-key k) v]))
                            (:config node))
                      {})
        node-ref    (or (:node-ref node) (:ref node))]
    {:node-ref (normalize-ref node-ref)
     :type     t0
     :alias    (when alias (str alias))
     :config   config
     :position (normalize-position (:position node))}))

(defn- normalize-edge [edge]
  (cond
    (and (vector? edge) (= 2 (count edge)))
    [(normalize-ref (first edge)) (normalize-ref (second edge))]

    (map? edge)
    [(normalize-ref (:from edge)) (normalize-ref (:to edge))]

    :else nil))

(defn- normalize-patch [patch idx]
  (let [op (cond
             (keyword? (:op patch)) (:op patch)
             (string? (:op patch))  (-> (:op patch) string/lower-case keyword)
             :else nil)]
    (cond-> {:op op}
      (:ref patch)    (assoc :ref (normalize-ref (:ref patch)))
      (:after patch)  (assoc :after (normalize-ref (:after patch)))
      (:before patch) (assoc :before (normalize-ref (:before patch)))
      (:from patch)   (assoc :from (normalize-ref (:from patch)))
      (:to patch)     (assoc :to (normalize-ref (:to patch)))
      (:config patch) (assoc :config (if (map? (:config patch)) (:config patch) {}))
      (:node patch)   (assoc :node (normalize-node (:node patch) idx)))))

(defn normalize
  "Normalize raw GIL input into canonical EDN."
  [raw-gil]
  (let [m0      (keywordize-kebab (or raw-gil {}))
        nodes   (mapv normalize-node (or (:nodes m0) []) (range))
        edges   (->> (or (:edges m0) [])
                     (map normalize-edge)
                     (remove nil?)
                     vec)
        patches (->> (or (:patches m0) [])
                     (map-indexed (fn [idx p] (normalize-patch p idx)))
                     vec)
        intent0 (normalize-intent (:intent m0))
        intent  (or intent0
                    (cond
                      (and (seq patches) (empty? nodes)) :patch
                      :else :build))]
    (cond-> {:gil-version (or (:gil-version m0) schema/gil-version)
             :intent intent
             :graph-name (:graph-name m0)
             :description (:description m0)}
      (= intent :build) (assoc :nodes nodes :edges edges)
      (= intent :patch) (assoc :patches patches))))
