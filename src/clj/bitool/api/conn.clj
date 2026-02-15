(ns bitool.api.conn
  (:require [cheshire.core :as json]
            [clj-http.client :as http]))


;; ---------- helpers ----------------------------------------------------------

(defn- number-type [n]
  (cond
    (integer? n) :int
    (float? n)   :float
    :else        :number))

;; Replace classify with one that recognizes general seqs:
(defn- classify [v]
  (cond
    (nil? v)            :nil
    (string? v)         :string
    (boolean? v)        :boolean
    (map? v)            :map
    (vector? v)         :vector
    (sequential? v)     :seq     ;; <— NEW: covers LazySeq, list, etc.
    (number? v)         (number-type v)
    (keyword? v)        :keyword
    :else               :any))

(defn- scalar? [schema]
  (and (map? schema)
       (not (#{:map :vector :one-of} (:type schema)))))

(defn- make-scalar [t nullable?]
  {:type t :nullable? (boolean nullable?)})

(defn- normalize-one-of [schemas]
  (let [flat (mapcat #(if (= (:type %) :one-of) (:of %) [%]) schemas)
        grouped (group-by :type flat)
        promote-number (fn [gs]
                         (let [ts (set (map :type gs))]
                           (cond
                             (and (ts :int) (ts :float))
                             [(make-scalar :number (some true? (map :nullable? gs)))]
                             :else gs)))]
    (let [collapsed (->> grouped
                         (mapcat (fn [[t gs]]
                                   (if (#{:int :float :number} t)
                                     (promote-number gs)
                                     gs))))]
      (if (= 1 (count collapsed))
        (first collapsed)
        {:type :one-of :of (vec collapsed)}))))

;; ---------- forward declarations to avoid circular refs ----------------------
(declare merge-schemas merge-maps merge-vectors)

(defn- merge-maps [a b]
  (let [ks (into (set (keys (:keys a))) (keys (:keys b)))]
    {:type :map
     :nullable? (or (:nullable? a) (:nullable? b))
     :keys (into {}
                 (for [k ks]
                   [k (cond
                        (and (get-in a [:keys k]) (get-in b [:keys k]))
                        (merge-schemas (get-in a [:keys k]) (get-in b [:keys k]))

                        (get-in a [:keys k]) (get-in a [:keys k])
                        :else                (get-in b [:keys k]))]))}))

(defn- merge-vectors [a b]
  (let [item (merge-schemas (:items a) (:items b))]
    {:type :vector
     :nullable? (or (:nullable? a) (:nullable? b))
     :items item}))

(defn merge-schemas [a b]
  (cond
    (nil? a) b
    (nil? b) a
    (or (= (:type a) :one-of) (= (:type b) :one-of))
    (normalize-one-of [a b])
    (= (:type a) :map (:type b) :map)       (merge-maps a b)
    (= (:type a) :vector (:type b) :vector) (merge-vectors a b)
    (and (scalar? a) (scalar? b) (= (:type a) (:type b)))
    (make-scalar (:type a) (or (:nullable? a) (:nullable? b)))
    (and (scalar? a) (scalar? b)
         (#{:int :float :number} (:type a))
         (#{:int :float :number} (:type b)))
    (make-scalar :number (or (:nullable? a) (:nullable? b)))
    :else (normalize-one-of [a b])))

;; ---------- inference --------------------------------------------------------

;; In infer-schema, coerce seq to vector
(defn infer-schema [v]
  (let [t (classify v)]
    (case t
      :seq    (recur (vec v))     ;; <— NEW: treat seqs as vectors
      :map    {:type :map
               :nullable? false
               :keys (into {} (for [[k vv] v] [k (infer-schema vv)]))}
      :vector (let [items (map infer-schema v)
                    base  (reduce merge-schemas nil items)]
                {:type :vector
                 :nullable? false
                 :items (or base (make-scalar :any false))})
      :nil    (make-scalar :any true)
      (make-scalar t false))))

;; ---------- flattening for UI nodes -----------------------------------------

(defn- seg->s [seg]
  (cond
    (= seg "[]") "[]"
    (keyword? seg) (name seg)
    (string? seg) seg
    :else (str seg)))

(defn- path->string [path]
  (->> path
       (map seg->s)
       (reduce (fn [acc seg]
                 (cond
                   (empty? acc) seg
                   (= seg "[]") (str acc seg)
                   :else (str acc "." seg)))
               "")))

(defn schema->nodes
  "Emit leaf-like nodes for UI mapping."
  ([schema] (schema->nodes schema ["$"]))
  ([schema path]
   (let [emit (fn [sch]
                [{:path      (path->string path)
                  :type      (or (:type sch) :unknown)
                  :nullable? (boolean (:nullable? sch))
                  :raw       sch}])]
     (case (:type schema)
       :map
       (mapcat (fn [[k sch]] (schema->nodes sch (conj path (name k))))
               (:keys schema))

       :vector
       (schema->nodes (:items schema) (conj path "[]"))

       :one-of (emit schema)
       (emit schema)))))

;; ---------- fetch + end-to-end ----------------------------------------------

(defn fetch-json
  "GET + parse JSON. Optional {:headers .. :query-params ..}."
  ([url] (fetch-json url {}))
  ([url {:keys [headers query-params] :as _opts}]
   (let [resp (http/get url {:headers headers
                             :query-params query-params
                             :throw-exceptions true
                             :as :text})
         body (:body resp)]
     (json/parse-string body true))))

(defn nodes-from-url
  "Fetch URL -> infer schema -> flatten to nodes."
  ([url] (nodes-from-url url {}))
  ([url opts]
   (let [data   (fetch-json url opts)
         schema (infer-schema data)
         nodes  (vec (schema->nodes schema))]
     {:schema schema
      :nodes  nodes})))
