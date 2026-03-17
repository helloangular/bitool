(ns bitool.utils
    (:require [clojure.string :as string]
              [selmer.parser :as parser]))

(defn assoc-in-when [m condition path value]
  (if condition
    (assoc-in m path value)
    m))


(defn descendant? [superclass subclass]
  (.isAssignableFrom superclass subclass))

(defn path->name [s]
  (let [base (-> (or s "")
                 (string/replace #"^\$\.?" "")
                 (string/replace #"\[\]" "_items")
                 (string/replace #"\." "_")
                 (string/replace #"[^A-Za-z0-9_]" "_")
                 (string/replace #"_+" "_")
                 (string/replace #"^_+" "")
                 (string/replace #"_+$" ""))]
    (cond
      (string/blank? base) "col"
      (re-matches #"^[0-9].*" base) (str "col_" base)
      :else base)))

(defn nodes->columns[id nodes]                              
       {id (mapv (fn[v] {:column_name (path->name v)}) nodes)})
