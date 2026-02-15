(ns bitool.connector.schema-tree
  (:require [clojure.string :as str]
            [cheshire.core :as json]
            [bitool.connector.schema :as schema]))

(defn node
  ([label] {:label label})
  ([label value] {:label label :value value})
  ([label value items] {:label label :value value :items items}))

(defn type-tag [m]
  (cond
    (= "object" (:type m)) "object"
    (= "array"  (:type m)) "array"
    (:enum m)               "enum"
    (:oneOf m)              "oneOf"
    (:allOf m)              "allOf"
    (:anyOf m)              "anyOf"
    :else (or (:type m) "unknown")))

(defn prop->label [k spec]
  (str (name k) " : " (type-tag spec)))

(declare schema->tree*)

(defn resolve-if-ref [x]
  (if (and (map? x) (or (:$ref x) (get x "$ref")))
    (or (schema/resolve-ref (or (:$ref x) (get x "$ref"))) x)
    x))

(defn properties->items [props required-set]
  (->> props
       (map (fn [[k v]]
              (let [v*  (resolve-if-ref v)
                    lbl (str (prop->label k v*)
                             (when (contains? required-set (name k)) " (required)"))]
                (schema->tree* lbl v*))))
       (sort-by :label)
       vec))

(defn array-items->children [items-spec]
  (let [it* (resolve-if-ref items-spec)]
    [(schema->tree* (str "items : " (type-tag it*)) it*)]))

(defn schema->tree*
  "Resolve $ref at entry, then build nodes."
  [label schema-node]
  (let [s (resolve-if-ref schema-node)]
    (cond
      (= "object" (:type s))
      (let [req (set (:required s))
            props (:properties s)]
        (node label {:kind "object"} (properties->items props req)))

      (= "array" (:type s))
      (node label {:kind "array"} (array-items->children (:items s)))

      (:oneOf s)
      (node (str label " : oneOf") {:kind "oneOf"}
            (mapv #(schema->tree* "option" (resolve-if-ref %)) (:oneOf s)))

      (:anyOf s)
      (node (str label " : anyOf") {:kind "anyOf"}
            (mapv #(schema->tree* "option" (resolve-if-ref %)) (:anyOf s)))

      (:allOf s)
      (node (str label " : allOf") {:kind "allOf"}
            (mapv #(schema->tree* "part" (resolve-if-ref %)) (:allOf s)))

      (:enum s)
      (node (str label " : enum") {:kind "enum" :values (:enum s)})

      :else
      (node label {:kind (or (:type s) "unknown")}))))

;; your original name, now works even if items is a $ref
(defn issue-tree []
  (let [schema {:type "array" :items {:$ref "#/components/schemas/issue"}}]
    [(schema->tree* "issues : array" schema)]))

