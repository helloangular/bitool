(ns bitool.pipeline.planner
  "Stage 4: PipelineIntent -> PipelineSpec (deterministic, no LLM).
   Resolves connector knowledge, computes table names, builds dependency graph.
   Incorporates coverage reconciliation from discovery + reconcile."
  (:require [clojure.string :as string]
            [bitool.pipeline.registry :as reg]
            [bitool.pipeline.reconcile :as reconcile]))

;; ---------------------------------------------------------------------------
;; Delegate to registry for all loader and naming functions
;; ---------------------------------------------------------------------------

(def load-connector-knowledge reg/load-connector-knowledge)
(def load-metric-package      reg/load-metric-package)
(def list-connectors          reg/list-connectors)
(def list-metric-packages     reg/list-metric-packages)
(def sanitize-table-name      reg/sanitize-table-name)
(def bronze-table-name        reg/bronze-table-name)
(def silver-table-name        reg/silver-table-name)
(def gold-table-name          reg/gold-table-name)
(def pipeline-graph-name      reg/pipeline-graph-name)

;; ---------------------------------------------------------------------------
;; Inferred field generation from connector knowledge
;; ---------------------------------------------------------------------------

(defn- endpoint-field->inferred-field
  "Convert a connector knowledge field to an inferred_fields entry."
  [explode-path field]
  (let [col-prefix (sanitize-table-name (str (string/replace explode-path #"/" "_") "_items"))
        col-name   (str col-prefix "_" (:name field))]
    {:path          (str explode-path "[]." (:name field))
     :column_name   col-name
     :source_kind   "spec"
     :enabled       true
     :type          (or (:type field) "STRING")
     :observed_types [(or (:type field) "STRING")]
     :nullable      true
     :confidence    1.0
     :sample_coverage 1.0
     :depth         2
     :array_mode    "scalar"
     :override_type ""
     :notes         ""
     :is_watermark  (= "watermark" (:role field))}))

;; ---------------------------------------------------------------------------
;; Core planner: PipelineIntent -> PipelineSpec
;; ---------------------------------------------------------------------------

(defn plan-pipeline
  "Convert a PipelineIntent into a PipelineSpec.
   Deterministic — no LLM calls, no side effects."
  [{:keys [source target bronze silver gold ops] :as intent}]
  (let [system-name  (:system source)
        connector    (reg/load-connector-knowledge system-name)
        _            (when-not connector
                       (throw (ex-info (str "Unknown connector: " system-name)
                                       {:system system-name})))
        catalog      (or (:catalog target) "main")
        bronze-schema (or (:bronze-schema target) "bronze")
        silver-schema (or (:silver-schema target) "silver")
        gold-schema   (or (:gold-schema target) "gold")
        platform     (or (:platform target) "databricks")

        ;; Resolve Bronze endpoints from intent objects
        requested-objects (set (map :object bronze))
        endpoint-map  (into {} (map (fn [ep] [(:path ep) ep]) (:endpoints connector)))

        ;; Build Bronze endpoint configs
        bronze-configs
        (mapv (fn [b-intent]
                (let [ep      (get endpoint-map (:object b-intent))
                      explode (or (:explode-path b-intent) (:explode ep) "data")
                      wm      (or (:watermark b-intent) (:watermark ep))
                      pk      (or (:primary-key b-intent) (:key ep) "id")
                      fields  (or (:fields ep) [])]
                  {:endpoint_name    (:object b-intent)
                   :endpoint_url     (:object b-intent)
                   :http_method      (or (:method ep) "GET")
                   :load_type        (or (:load-type source) "incremental")
                   :pagination_strategy (get-in connector [:pagination :strategy] "cursor")
                   :cursor_field     (get-in connector [:pagination :cursor-field] "")
                   :cursor_param     (get-in connector [:pagination :cursor-param] "")
                   :json_explode_rules [{:path explode}]
                   :watermark_column (when wm
                                      (str (sanitize-table-name
                                            (str (string/replace explode #"/" "_") "_items"))
                                           "_" wm))
                   :primary_key_fields [pk]
                   :schema_mode      "infer"
                   :inferred_fields  (mapv #(endpoint-field->inferred-field explode %)
                                           fields)
                   :sample_records   100
                   :page_size        100
                   :enabled          true
                   :bronze_table_name (bronze-table-name system-name (:object b-intent))}))
              bronze)

        ;; Build Bronze nodes
        bronze-nodes
        [{:node-ref  "api1"
          :node-type "api-connection"
          :config    {:api_name        (pipeline-graph-name system-name nil)
                      :source_system   system-name
                      :base_url        (or (:base-url source) (:base-url connector))
                      :auth_ref        {:type       (or (:auth-method source) (:auth-method connector) "bearer")
                                        :secret_ref (str (string/upper-case system-name) "_API_TOKEN")}
                      :endpoint_configs bronze-configs}}
         {:node-ref  "tg1"
          :node-type "target"
          :config    {:target_kind      platform
                      :catalog          catalog
                      :schema           bronze-schema
                      :write_mode       "append"
                      :table_format     "delta"
                      :partition_columns ["partition_date"]}}]

        ;; Resolve Silver entities
        ;; First try intent, then metric package, then infer from Bronze
        metric-pkg    (when-let [pkg-name (or (some (fn [g] (:metric-package g)) gold)
                                              ;; Also try matching a metric package by system name
                                              (let [candidates (list-metric-packages)]
                                                (first (filter #(string/includes? (str %) system-name) candidates))))]
                        (load-metric-package pkg-name))
        silver-entities
        (if (seq silver)
          ;; User specified Silver entities in intent
          (mapv (fn [s]
                  (let [ep (get endpoint-map (:source s))
                        entity-kind (or (:entity-kind s) "dimension")]
                    {:target-model   (silver-table-name entity-kind (:entity s))
                     :layer          "silver"
                     :source-bronze  (bronze-table-name system-name (:source s))
                     :source-endpoint (:source s)
                     :entity-kind    entity-kind
                     :business-keys  [(or (:dedup-key s) "id")]
                     :columns        (or (:columns s) [])
                     :processing-policy {:ordering-strategy "latest_event_time_wins"
                                         :event-time-column "updated_at"
                                         :late-data-mode "merge"}}))
                silver)
          (if metric-pkg
            ;; Fall back to metric package silver entities
            (mapv (fn [se]
                    {:target-model   (:table se)
                     :layer          "silver"
                     :source-bronze  (bronze-table-name system-name (:source-endpoint se))
                     :source-endpoint (:source-endpoint se)
                     :entity-kind    (:entity-kind se)
                     :business-keys  (:business-keys se)
                     :columns        (:columns se)
                     :processing-policy (:processing-policy se)})
                  (:silver-entities metric-pkg))
            ;; Infer Silver from Bronze endpoints when no explicit Silver or metric package
            (mapv (fn [b]
                    (let [ep-url (or (:object b) (:endpoint b) "unknown")
                          entity-name (-> ep-url
                                          (string/replace #"^fleet/" "")
                                          (string/replace "/" "_")
                                          (string/replace "-" "_"))
                          entity-kind (if (string/includes? ep-url "event") "fact" "dimension")]
                      {:target-model   (silver-table-name entity-kind entity-name)
                       :layer          "silver"
                       :source-bronze  (bronze-table-name system-name ep-url)
                       :source-endpoint ep-url
                       :entity-kind    entity-kind
                       :business-keys  ["id"]
                       :columns        []
                       :processing-policy {:ordering-strategy "latest_event_time_wins"
                                           :event-time-column "updated_at"
                                           :late-data-mode "merge"}}))
                  bronze)))

        ;; Resolve Gold models
        ;; If any gold item references a metric-package, expand from the package
        has-metric-pkg-ref? (some :metric-package gold)
        gold-models
        (if (and (seq gold) (not has-metric-pkg-ref?))
          ;; User specified concrete Gold models (no metric package reference)
          (mapv (fn [g]
                  {:target-model (gold-table-name (:model g))
                   :layer        "gold"
                   :grain        (or (:grain g) "day")
                   :depends-on   (or (:depends-on g) [])
                   :measures     (or (:measures g) [])
                   :dimensions   (or (:dimensions g) [])
                   :sql-template nil})
                gold)
          (if metric-pkg
            ;; Expand from metric package
            (mapv (fn [gm]
                    {:target-model (gold-table-name (:table gm))
                     :layer        "gold"
                     :grain        (or (:grain gm) "day")
                     :depends-on   (or (:depends-on gm) [])
                     :measures     (or (:measures gm) [])
                     :dimensions   (or (:dimensions gm) [])
                     :sql-template (when (:sql-template gm)
                                    (-> (:sql-template gm)
                                        (string/replace "{{silver_schema}}"
                                                        (str catalog "." silver-schema))))})
                  (:gold-models metric-pkg))
            ;; Infer a basic Gold aggregation from Silver entities when nothing specified
            (when (seq silver-entities)
              (let [fact-entities (filter #(= "fact" (:entity-kind %)) silver-entities)
                    dim-entities  (filter #(= "dimension" (:entity-kind %)) silver-entities)
                    primary      (or (first fact-entities) (first silver-entities))
                    ep-name      (-> (or (:source-endpoint primary) "summary")
                                     (string/replace #"^fleet/" "")
                                     (string/replace "/" "_")
                                     (string/replace "-" "_"))]
                [{:target-model (gold-table-name (str ep-name "_daily"))
                  :layer        "gold"
                  :grain        "day"
                  :depends-on   (mapv :target-model silver-entities)
                  :measures     ["count" "distinct_count"]
                  :dimensions   (mapv :target-model dim-entities)
                  :sql-template nil}]))))

        ;; Build assumptions
        assumptions
        (cond-> []
          true (conj (str "Source system: " system-name))
          true (conj (str "Target platform: " platform ", catalog: " catalog))
          true (conj (str "Bronze schema: " bronze-schema ", Silver: " silver-schema ", Gold: " gold-schema))
          (seq bronze-configs)
          (conj (str "Bronze endpoints: " (string/join ", " (map :endpoint_name bronze-configs))))
          true (conj "Load type: incremental with watermark-based dedup")
          true (conj "Bronze tables partitioned by partition_date")
          (seq silver-entities)
          (conj (str "Silver entities: " (string/join ", " (map :target-model silver-entities))))
          (seq gold-models)
          (conj (str "Gold models: " (string/join ", " (map :target-model gold-models))))
          true (conj "Silver uses latest-event-time-wins dedup strategy"))]

    {:pipeline-id      (pipeline-graph-name system-name
                                            (or (:pipeline-name intent) "pipeline"))
     :pipeline-name    (or (:pipeline-name intent)
                           (str (:display-name connector) " Pipeline"))
     :target-platform  platform
     :catalog          catalog
     :bronze-schema    bronze-schema
     :silver-schema    silver-schema
     :gold-schema      gold-schema
     :bronze-nodes     bronze-nodes
     :bronze-edges     [["api1" "tg1"] ["tg1" "o1"]]
     :silver-proposals (vec (or silver-entities []))
     :gold-models      (vec (or gold-models []))
     :assumptions      assumptions
     :coverage         (let [recon (reconcile/reconcile-from-intent intent)]
                         (:coverage recon))
     :ops              (or ops {:schedule nil :retries 3})}))
