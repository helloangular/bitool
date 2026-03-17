(ns bitool.platform.plugins
  (:require [clojure.string :as string]
            [clojure.tools.logging :as log]))

(defonce ^:private execution-handler-registry (atom {}))

(defn- normalize-request-kind [request-kind]
  (some-> request-kind name string/lower-case keyword))

(defn register-execution-handler!
  [request-kind {:keys [execute workload-classifier description]}]
  (let [request-kind (normalize-request-kind request-kind)]
    (when-not request-kind
      (throw (ex-info "request-kind is required for execution handler registration" {})))
    (when-not (fn? execute)
      (throw (ex-info "execution handler must provide an :execute function"
                      {:request_kind request-kind})))
    (when (contains? @execution-handler-registry request-kind)
      (log/warn "Replacing registered execution handler"
                {:request_kind request-kind}))
    (swap! execution-handler-registry
           assoc
           request-kind
           (cond-> {:request_kind request-kind
                    :execute execute}
             (fn? workload-classifier) (assoc :workload-classifier workload-classifier)
             (string? description) (assoc :description description)))
    (get @execution-handler-registry request-kind)))

(defn resolve-execution-handler
  [request-kind]
  (get @execution-handler-registry (normalize-request-kind request-kind)))

(defn registered-execution-handlers
  []
  (->> @execution-handler-registry
       vals
       (sort-by :request_kind)
       vec))
