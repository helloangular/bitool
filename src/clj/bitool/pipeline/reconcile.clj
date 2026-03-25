(ns bitool.pipeline.reconcile
  "Stage 3: Coverage Reconciliation Engine.
   Compares required entities (from Gold/BRD intent) with available entities
   (from source discovery) and produces a coverage report.
   Pure functions — no side effects."
  (:require [bitool.pipeline.discovery :as discovery]
            [bitool.pipeline.registry :as reg]
            [clojure.set :as set]
            [clojure.string :as string]))

;; ---------------------------------------------------------------------------
;; Coverage status computation
;; ---------------------------------------------------------------------------

(defn reconcile-coverage
  "Compare required entities against available entities.
   Returns a coverage report with status, missing entities, and recommendations.

   required-entities: [\"vehicle\" \"driver\" \"trip\"]
   available-entities: [{:entity \"vehicle\" :endpoint \"fleet/vehicles\" :status :available}
                        {:entity \"driver\"  :endpoint \"fleet/drivers\"  :status :available}
                        {:entity \"trip\"    :endpoint nil               :status :missing}]"
  [required-entities available-entity-results]
  (let [available-set (set (map :entity (filter #(= :available (:status %)) available-entity-results)))
        required-set  (set required-entities)
        missing       (set/difference required-set available-set)
        covered       (set/intersection required-set available-set)
        status        (cond
                        (empty? missing) "ready"
                        (empty? covered) "blocked"
                        :else "partial")]
    {:status              status
     :required-entities   (vec (sort required-set))
     :available-entities  (vec (sort covered))
     :missing-entities    (vec (sort missing))
     :entity-details      available-entity-results
     :recommendations     (mapv (fn [entity]
                                  {:entity entity
                                   :action "add_endpoint"
                                   :message (str "Add a Bronze endpoint that provides the '" entity "' entity")})
                                missing)}))

;; ---------------------------------------------------------------------------
;; Full reconciliation from intent
;; ---------------------------------------------------------------------------

(defn reconcile-from-intent
  "Run full reconciliation for a PipelineIntent.
   1. Determine required entities from Gold/BRD intent
   2. Discover available entities from source system
   3. Compute coverage status

   Returns: {:coverage {...} :required [...] :available [...] :connector-knowledge {...}}"
  [{:keys [source gold] :as intent}]
  (let [system-name  (:system source)
        connector    (reg/load-connector-knowledge system-name)

        ;; Determine required entities
        metric-pkgs  (->> gold (map :metric-package) (remove nil?) vec)
        business-domain (get-in intent [:business :domain])
        required     (cond
                       ;; Explicit gold models with depends-on
                       (some :depends-on gold)
                       (discovery/required-entities-for-gold gold metric-pkgs)

                       ;; Metric package reference
                       (seq metric-pkgs)
                       (discovery/required-entities-for-gold [] metric-pkgs)

                       ;; Business domain metrics
                       (get-in intent [:business :metrics])
                       (discovery/required-entities-for-metrics
                        (get-in intent [:business :metrics]))

                       ;; Source-first: no gold specified, suggest from available
                       :else [])

        ;; Discover available entities
        available    (if connector
                       (discovery/endpoints-for-entities
                        (if (seq required)
                          required
                          ;; Source-first: all entities from connector
                          (map :entity (:endpoints connector)))
                        connector)
                       [])

        ;; Compute coverage
        coverage     (if (seq required)
                       (reconcile-coverage required available)
                       {:status "source_first"
                        :required-entities []
                        :available-entities (vec (sort (map :entity (filter #(= :available (:status %)) available))))
                        :missing-entities []
                        :entity-details available
                        :recommendations [{:action "suggest_gold"
                                           :message "No Gold requirements specified. Available entities can support Silver/Gold models."}]})]
    {:coverage            coverage
     :required-entities   required
     :available-entities  available
     :connector-knowledge connector
     :metric-packages-used metric-pkgs}))

;; ---------------------------------------------------------------------------
;; Coverage-aware endpoint selection
;; ---------------------------------------------------------------------------

(defn select-bronze-endpoints
  "Given reconciliation results, return the list of endpoints to include in Bronze.
   Includes all available endpoints that map to required entities,
   plus any explicitly requested objects from the intent."
  [reconciliation {:keys [bronze] :as intent}]
  (let [;; Explicitly requested objects
        explicit-objects (set (map :object bronze))
        ;; Entities available from connector
        entity-endpoints (->> (:available-entities reconciliation)
                              (filter #(= :available (:status %)))
                              (map (fn [ae] {:object (:endpoint ae) :entity (:entity ae)}))
                              vec)
        ;; Merge: explicit requests + entity-driven
        all-objects (set (concat explicit-objects (map :object entity-endpoints)))]
    (vec (sort all-objects))))
