(ns bitool.config
  (:require
    [clojure.string :as string]
    [cprop.core :refer [load-config]]
    [cprop.source :as source]
    [mount.core :refer [args defstate]]))

(defstate env
  :start
  (load-config
    :merge
    [(args)
     (source/from-system-props)
     (source/from-env)]))

(defn current-role
  []
  (some-> (or (:bitool-role env)
              (:role env)
              "all")
          str
          string/trim
          string/lower-case))

(defn enabled-role?
  [role]
  (let [configured (current-role)
        requested  (some-> role name string/lower-case)]
    (or (= configured "all")
        (= configured requested))))
