(ns bitool.connector.config
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]))

(def default-path "config.edn")

(defn read-config-file [path]
  (try
    (-> path io/reader java.io.PushbackReader. edn/read)
    (catch Exception e
      (log/error e (str "Failed to read config file at " path))
      (System/exit 1))))

(defn parse-args [args]
  (when-let [config-arg (some #(re-find #"^--config=(.+)$" %) args)]
    (second config-arg)))

(defn validate-config
  [{:keys [base-url endpoint auth]}]
  (when-not (and base-url endpoint)
    (log/error "Missing required fields `base-url` or `endpoint` in config.edn")
    (System/exit 1))
  true)

(defn load-config
  ([] (load-config nil)) ; ← allows calling with 0 args
  ([args]
   (let [path (or (parse-args args)
                  (System/getenv "CONNECTOR_CONFIG")
                  default-path)
         _    (log/infof "Loading config from %s" path)
         conf (read-config-file path)]
     (validate-config conf)
     conf)))

