(ns bitool.utils
    (:require [selmer.parser :as parser]))

(defn assoc-in-when [m condition path value]
  (if condition
    (assoc-in m path value)
    m))


(defn descendant? [superclass subclass]
  (.isAssignableFrom superclass subclass))

(defn path->name [s]
  (-> s
      (clojure.string/replace #"^\$\.?" "")   ;; remove leading $ or $.  
      (clojure.string/replace #"\." "_")))     ;; replace remaining dots with _

(defn nodes->columns[id nodes]                              
       {id (mapv (fn[v] {:column_name (path->name v)}) nodes)})
