(ns bitool.connector.extract
  (:require 
            [bitool.connector.config :as config]
            [cheshire.core :as json]
            [clojure.string :as str]
            [taoensso.timbre :as log]))

(defn get-in-path [m path-str]
  (reduce
    (fn [acc key]
      (cond
        (map? acc) (get acc (keyword key))
        (vector? acc) (let [idx (try (Integer/parseInt key) (catch Exception _ nil))]
                        (when (and idx (< idx (count acc)))
                          (nth acc idx)))
        :else nil))
    m
    (str/split path-str #"\.")))

(defn extract-fields-by-table [json mapping]
  (reduce-kv
    (fn [acc api-path table-col]
      (let [[table col] (str/split table-col #"\.")]
        (assoc-in acc [table col] (get-in-path json api-path))))
    {}
    mapping))

