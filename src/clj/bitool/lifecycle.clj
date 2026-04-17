(ns bitool.lifecycle
  "Process lifecycle and graceful-drain state."
  (:require [bitool.config :refer [env]]
            [clojure.string :as string]))

(defonce ^:private draining?* (atom false))
(defonce ^:private draining-since* (atom nil))

(defn- parse-int-safe
  [value default]
  (try
    (if (some? value)
      (Integer/parseInt (str value))
      default)
    (catch Exception _
      default)))

(defn shutdown-drain-ms
  []
  (max 0 (parse-int-safe (get env :bitool-shutdown-drain-ms) 5000)))

(defn mark-draining!
  []
  (reset! draining?* true)
  (reset! draining-since* (java.time.Instant/now))
  true)

(defn clear-draining!
  []
  (reset! draining?* false)
  (reset! draining-since* nil)
  false)

(defn draining?
  []
  @draining?*)

(defn draining-since
  []
  @draining-since*)

(defn drain-exempt-path?
  [uri]
  (contains? #{"/health" "/ready"} (some-> uri str string/trim)))

(defn health-payload
  []
  {:ok true
   :status "ok"
   :draining (draining?)
   :draining_since (some-> (draining-since) str)})

(defn readiness-payload
  []
  (if (draining?)
    {:ok false
     :ready false
     :status "draining"
     :draining_since (some-> (draining-since) str)}
    {:ok true
     :ready true
     :status "ready"}))
