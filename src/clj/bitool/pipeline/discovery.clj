(ns bitool.pipeline.discovery
  "Stage 2: Source Discovery + Semantic Planning.
   Determines what semantic entities the user needs (from Gold/BRD intent)
   and what sources are available (from connector knowledge, OpenAPI, existing graphs).
   Pure functions — no side effects."
  (:require [bitool.pipeline.registry :as reg]
            [clojure.string :as string]))

;; ---------------------------------------------------------------------------
;; Required entities (top-down: from Gold/BRD intent)
;; ---------------------------------------------------------------------------

(defn required-entities-for-metrics
  "Given a set of metric names, return the union of required Silver entities."
  [metric-names]
  (let [registry (reg/load-metric-registry)]
    (->> metric-names
         (mapcat (fn [m]
                   (let [k (keyword m)]
                     (get-in registry [k :requires-entities] []))))
         distinct
         vec)))

(defn required-entities-for-gold
  "Given Gold model specs (from PipelineSpec or intent), extract all required entities."
  [gold-models metric-packages-used]
  (let [;; From explicit gold model depends-on
        explicit-deps (->> gold-models
                           (mapcat :depends-on)
                           (map #(string/replace % #"^(dim_|fct_)" ""))
                           distinct
                           vec)
        ;; From metric packages
        metric-deps (when (seq metric-packages-used)
                      (let [packages (remove nil? (map reg/load-metric-package metric-packages-used))]
                        (->> packages
                             (mapcat :gold-models)
                             (mapcat :depends-on)
                             (map #(string/replace % #"^(dim_|fct_)" ""))
                             distinct
                             vec)))
        ;; From metric registry (if business metrics named)
        business-metrics (->> gold-models
                              (mapcat :measures)
                              (remove nil?))]
    (->> (concat explicit-deps (or metric-deps [])
                 (when (seq business-metrics)
                   (required-entities-for-metrics business-metrics)))
         distinct
         vec)))

;; ---------------------------------------------------------------------------
;; Available entities (bottom-up: from connector knowledge)
;; ---------------------------------------------------------------------------

(defn available-entities-from-connector
  "Given a connector knowledge map, return entities available from its endpoints."
  [connector]
  (->> (:endpoints connector)
       (map (fn [ep]
              {:entity      (:entity ep)
               :endpoint    (:path ep)
               :key         (:key ep)
               :watermark   (:watermark ep)
               :grain       (:grain ep)
               :field-count (count (:fields ep))}))
       vec))

(defn available-entities-for-system
  "Discover available entities for a source system."
  [system-name]
  (when-let [connector (reg/load-connector-knowledge system-name)]
    {:system    system-name
     :entities  (available-entities-from-connector connector)}))

;; ---------------------------------------------------------------------------
;; Entity-to-endpoint mapping
;; ---------------------------------------------------------------------------

(defn entity-to-endpoint-map
  "Build a map from entity name to endpoint path for a connector."
  [connector]
  (->> (:endpoints connector)
       (map (fn [ep] [(:entity ep) (:path ep)]))
       (into {})))

(defn endpoints-for-entities
  "Given required entity names and a connector, return the minimal set of endpoints needed."
  [required-entity-names connector]
  (let [entity->ep (entity-to-endpoint-map connector)]
    (->> required-entity-names
         (map (fn [entity]
                {:entity   entity
                 :endpoint (get entity->ep entity)
                 :status   (if (get entity->ep entity) :available :missing)}))
         vec)))
