(ns bitool.pipeline.registry
  "Shared registry loaders for connector knowledge, metric packages, and entity registry.
   No dependencies on other pipeline modules — breaks circular dependency."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as string]))

;; ---------------------------------------------------------------------------
;; Connector knowledge
;; ---------------------------------------------------------------------------

(defn load-connector-knowledge
  "Load connector knowledge EDN for a given system name."
  [system-name]
  (when-let [res (io/resource (str "connector-knowledge/" system-name ".edn"))]
    (edn/read-string (slurp res))))

(defn list-connectors
  "List all available connector knowledge files."
  []
  (->> ["samsara"]
       (map (fn [sys] (when-let [ck (load-connector-knowledge sys)]
                        {:system (:system ck) :display-name (:display-name ck)})))
       (remove nil?)
       vec))

;; ---------------------------------------------------------------------------
;; Metric packages
;; ---------------------------------------------------------------------------

(defn load-metric-package
  "Load a metric package EDN by name."
  [package-name]
  (when-let [res (io/resource (str "metric-packages/" package-name ".edn"))]
    (edn/read-string (slurp res))))

(defn list-metric-packages
  "List all available metric packages."
  []
  (->> ["fleet_analytics"]
       (map (fn [pkg] (when-let [mp (load-metric-package pkg)]
                        {:name (:name mp) :display-name (:display-name mp)
                         :description (:description mp)})))
       (remove nil?)
       vec))

;; ---------------------------------------------------------------------------
;; Entity registry
;; ---------------------------------------------------------------------------

(defn load-entity-registry
  "Load the canonical entity registry."
  []
  (when-let [res (io/resource "entity-registry/canonical_entities.edn")]
    (edn/read-string (slurp res))))

;; ---------------------------------------------------------------------------
;; Metric registry
;; ---------------------------------------------------------------------------

(defn load-metric-registry
  "Load the metric registry."
  []
  (when-let [res (io/resource "metric-packages/metric_registry.edn")]
    (edn/read-string (slurp res))))

;; ---------------------------------------------------------------------------
;; Naming conventions
;; ---------------------------------------------------------------------------

(defn sanitize-table-name [s]
  (-> (str s)
      (string/replace #"[^a-zA-Z0-9_]" "_")
      (string/replace #"_+" "_")
      (string/replace #"^_|_$" "")
      string/lower-case))

(defn bronze-table-name [source-system endpoint-path]
  (sanitize-table-name (str source-system "_" (string/replace endpoint-path #"/" "_") "_raw")))

(defn silver-table-name [entity-kind entity-name]
  (let [prefix (if (= entity-kind "fact") "fct" "dim")]
    (sanitize-table-name (str prefix "_" entity-name))))

(defn gold-table-name [model-name]
  (sanitize-table-name model-name))

(defn pipeline-graph-name [source-system pipeline-name]
  (sanitize-table-name (str source-system "_" (or pipeline-name "pipeline"))))
