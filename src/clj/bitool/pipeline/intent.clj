(ns bitool.pipeline.intent
  "Stage 1: Natural language -> PipelineIntent (LLM call).
   Uses Anthropic tool_use with constrained output schema."
  (:require [bitool.pipeline.registry :as reg]
            [clj-http.client :as http]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [bitool.config :refer [env]]))

;; ---------------------------------------------------------------------------
;; Tool schema for LLM
;; ---------------------------------------------------------------------------

(def pipeline-intent-tool
  {:name "create_pipeline_intent"
   :description "Create a structured pipeline intent from a natural language description. The intent describes what data to ingest, how to transform it, and what analytics to produce."
   :input_schema
   {:type "object"
    :required ["source" "bronze"]
    :properties
    {:pipeline_name {:type "string" :description "Short name for the pipeline"}
     :source
     {:type "object"
      :required ["system" "objects"]
      :properties
      {:system      {:type "string" :description "Source system name (e.g. samsara, stripe)"}
       :objects     {:type "array" :items {:type "string"} :description "API endpoint paths to ingest"}
       :auth_method {:type "string" :enum ["bearer" "api-key" "basic" "oauth2"]}
       :load_type   {:type "string" :enum ["incremental" "full"] :default "incremental"}
       :cadence     {:type "string" :description "Schedule cadence (e.g. hourly, daily)"}}}
     :target
     {:type "object"
      :properties
      {:platform      {:type "string" :enum ["databricks" "snowflake" "postgres"]}
       :catalog       {:type "string"}
       :bronze_schema {:type "string" :default "bronze"}
       :silver_schema {:type "string" :default "silver"}
       :gold_schema   {:type "string" :default "gold"}}}
     :bronze
     {:type "array"
      :items {:type "object"
              :required ["object"]
              :properties
              {:object       {:type "string" :description "API endpoint path"}
               :table        {:type "string" :description "Bronze table name override"}
               :watermark    {:type "string" :description "Field name for incremental watermark"}
               :primary_key  {:type "string" :description "Primary key field"}
               :explode_path {:type "string" :description "JSON path to records array"}}}}
     :silver
     {:type "array"
      :items {:type "object"
              :properties
              {:entity    {:type "string" :description "Business entity name"}
               :table     {:type "string" :description "Silver table name"}
               :source    {:type "string" :description "Source Bronze endpoint path"}
               :dedup_key {:type "string"}
               :entity_kind {:type "string" :enum ["dimension" "fact"]}
               :columns   {:type "array"
                           :items {:type "object"
                                   :properties
                                   {:source {:type "string"}
                                    :target {:type "string"}
                                    :type   {:type "string"}}}}}}}
     :gold
     {:type "array"
      :items {:type "object"
              :properties
              {:model          {:type "string" :description "Gold model name"}
               :grain          {:type "string" :enum ["hour" "day" "week" "month"]}
               :metric_package {:type "string" :description "Named metric package to use"}
               :depends_on     {:type "array" :items {:type "string"}}
               :measures       {:type "array" :items {:type "string"}}
               :dimensions     {:type "array" :items {:type "string"}}}}}
     :ops
     {:type "object"
      :properties
      {:schedule    {:type "string" :description "Cron expression"}
       :retries     {:type "integer"}
       :timeout_min {:type "integer"}
       :alerts      {:type "array" :items {:type "string"}}}}}}})

;; ---------------------------------------------------------------------------
;; System prompt builder
;; ---------------------------------------------------------------------------

(declare select-examples format-examples)

(defn- build-system-prompt
  "Build the full system prompt with connector knowledge, rules, and few-shot examples.
   Follows the medtronic ISL pattern: critical directive + domain docs + rules + examples."
  [user-text]
  (let [connectors (reg/list-connectors)
        packages   (reg/list-metric-packages)
        examples   (select-examples user-text 3)]
    (str
     ;; Critical directive (ISL pattern)
     "CRITICAL: You MUST respond ONLY by calling the create_pipeline_intent tool. "
     "Do NOT respond with text, explanations, or questions. "
     "Your ONLY output must be a tool call with valid PipelineIntent JSON.\n\n"

     "You are a data pipeline architect for Bitool. "
     "Generate structured PipelineIntent specs from natural language.\n\n"

     "## Supported Source Systems\n"
     (string/join ", " (map :display-name connectors))
     "\n\n"

     "## Connector Knowledge\n"
     "For Samsara: endpoints include fleet/vehicles, fleet/drivers, fleet/trips, "
     "fleet/vehicles/stats, fleet/vehicles/locations, fleet/vehicles/fuel-energy, "
     "fleet/safety/events, tags, fleet/hos/logs. "
     "Pagination: cursor-based. Auth: bearer token. "
     "Common watermark: updatedAtTime. Records wrapped in {\"data\": [...]}.\n\n"

     "## Metric Packages\n"
     (string/join "\n" (map (fn [p] (str "- " (:name p) ": " (:description p))) packages))
     "\n\n"

     "## Rules\n"
     "1. Always set load_type to 'incremental' unless user says 'full'\n"
     "2. Default watermark from connector knowledge (updatedAtTime for Samsara entity endpoints)\n"
     "3. Default primary_key to 'id'\n"
     "4. Default explode_path to 'data' for Samsara\n"
     "5. Silver entities should be dim_ (dimensions) or fct_ (facts), not raw mirrors\n"
     "6. If user mentions analytics, use the matching metric_package name in gold\n"
     "7. If ambiguous, make reasonable assumptions\n"
     "8. Default target platform to 'databricks' unless specified\n"
     "9. Default cadence to null (manual) unless user specifies a schedule\n"
     "10. bronze table names: {system}_{endpoint_path_with_underscores}_raw\n\n"

     ;; Few-shot examples (ISL pattern: scored by similarity)
     (when (seq examples)
       (str "## Examples\n"
            (format-examples examples)
            "\n\n")))))

;; ---------------------------------------------------------------------------
;; Few-shot examples (scored by similarity)
;; ---------------------------------------------------------------------------

(def ^:private few-shot-examples
  [{:nl "Pull Samsara fleet/vehicles and fleet/drivers into Bronze on Databricks, create vehicle and driver Silver tables, and build daily fleet utilization Gold"
    :intent {:source {:system "samsara" :objects ["fleet/vehicles" "fleet/drivers"]}
             :target {:platform "databricks" :catalog "main"}
             :bronze [{:object "fleet/vehicles"} {:object "fleet/drivers"}]
             :silver [{:entity "vehicle" :source "fleet/vehicles" :entity_kind "dimension"}
                      {:entity "driver" :source "fleet/drivers" :entity_kind "dimension"}]
             :gold [{:metric_package "fleet_analytics"}]}}
   {:nl "Ingest Samsara fleet/vehicles into Bronze"
    :intent {:source {:system "samsara" :objects ["fleet/vehicles"]}
             :bronze [{:object "fleet/vehicles"}]}}
   {:nl "Build driver safety analytics from Samsara"
    :intent {:source {:system "samsara" :objects ["fleet/drivers" "fleet/safety/events"]}
             :bronze [{:object "fleet/drivers"} {:object "fleet/safety/events"}]
             :gold [{:metric_package "safety_analytics"}]}}
   {:nl "Connect Stripe and build revenue analytics"
    :intent {:source {:system "stripe" :objects ["charges" "refunds" "customers"]}
             :bronze [{:object "charges"} {:object "refunds"} {:object "customers"}]
             :gold [{:metric_package "revenue_analytics"}]}}
   {:nl "Pull Samsara fleet/vehicles/stats every hour into Bronze, create vehicle stats Silver fact table"
    :intent {:source {:system "samsara" :objects ["fleet/vehicles/stats"] :cadence "hourly"}
             :bronze [{:object "fleet/vehicles/stats"}]
             :silver [{:entity "vehicle_stat" :source "fleet/vehicles/stats" :entity_kind "fact"}]}}])

(defn- tokenize [text]
  (set (string/split (string/lower-case (str text)) #"[\s/\-_,.]+")))

(defn- similarity-score [query-tokens example-tokens]
  (let [intersection (count (set/intersection query-tokens example-tokens))
        union        (count (set/union query-tokens example-tokens))]
    (if (zero? union) 0.0 (double (/ intersection union)))))

(defn- select-examples [user-text n]
  (let [query-tokens (tokenize user-text)]
    (->> few-shot-examples
         (map (fn [ex] (assoc ex :score (similarity-score query-tokens (tokenize (:nl ex))))))
         (sort-by :score >)
         (take n)
         vec)))

(defn- format-examples [examples]
  (string/join "\n\n"
    (map (fn [ex]
           (str "User: " (:nl ex) "\n"
                "Tool call: " (json/generate-string (:intent ex))))
         examples)))

;; ---------------------------------------------------------------------------
;; LLM call (Anthropic tool_use with retry + temperature escalation)
;; ---------------------------------------------------------------------------

(def ^:private temperatures [0 0.3 0.7])
(def ^:private max-retries 2)

(defn- call-anthropic
  "Call Anthropic API with tool_use. Supports temperature override."
  [system-prompt user-message & {:keys [temperature] :or {temperature 0}}]
  (let [api-key (or (get env :anthropic-api-key)
                    (System/getenv "ANTHROPIC_API_KEY"))
        _       (when-not api-key
                  (throw (ex-info "ANTHROPIC_API_KEY not set" {:error "missing_api_key"})))
        body    {:model "claude-sonnet-4-5-20250514"
                 :max_tokens 4096
                 :temperature temperature
                 :system system-prompt
                 :tools [pipeline-intent-tool]
                 :tool_choice {:type "tool" :name "create_pipeline_intent"}
                 :messages [{:role "user" :content user-message}]}
        resp    (http/post "https://api.anthropic.com/v1/messages"
                           {:headers {"x-api-key" api-key
                                      "anthropic-version" "2023-06-01"
                                      "content-type" "application/json"}
                            :body (json/generate-string body)
                            :as :json})]
    (let [content (get-in resp [:body :content])
          tool-use (first (filter #(= "tool_use" (:type %)) content))]
      (when-not tool-use
        (throw (ex-info "LLM did not return tool_use"
                        {:content content})))
      (:input tool-use))))

(defn- validate-intent-output
  "Basic validation of LLM output. Returns {:valid true} or {:valid false :errors [...]}."
  [raw]
  (let [errors (cond-> []
                 (nil? (:source raw))
                 (conj {:path ":source" :code :missing :message "source is required"})
                 (nil? (:bronze raw))
                 (conj {:path ":bronze" :code :missing :message "bronze is required"})
                 (and (:source raw) (nil? (:system (:source raw))))
                 (conj {:path ":source.system" :code :missing :message "source.system is required"})
                 (and (:bronze raw) (empty? (:bronze raw)))
                 (conj {:path ":bronze" :code :empty :message "bronze must have at least one endpoint"}))]
    (if (empty? errors)
      {:valid true}
      {:valid false :errors errors})))

(defn- translate-with-retry
  "ISL-style retry loop: try LLM call up to 3 times with temperature escalation
   and validation error feedback."
  [system-prompt user-text]
  (loop [attempt 0
         correction-errors nil]
    (let [temperature (nth temperatures (min attempt (dec (count temperatures))))
          user-message (if correction-errors
                         (str user-text
                              "\n\nPrevious attempt failed validation:\n"
                              (json/generate-string correction-errors {:pretty true})
                              "\n\nFix these errors and try again using the tool.")
                         user-text)
          raw (call-anthropic system-prompt user-message :temperature temperature)
          validation (validate-intent-output raw)]
      (if (:valid validation)
        raw
        (if (>= attempt max-retries)
          (do (log/warn "Intent translation failed after" (inc attempt) "attempts"
                        {:errors (:errors validation)})
              raw) ;; return best effort
          (do (log/info "Intent attempt" attempt "failed, retrying with temp" temperature
                        {:errors (:errors validation)})
              (recur (inc attempt) (:errors validation))))))))

;; ---------------------------------------------------------------------------
;; Public API
;; ---------------------------------------------------------------------------

(defn- normalize-intent-keys
  "Convert JSON-style keys from LLM output to Clojure-style."
  [raw]
  (let [src (:source raw)
        tgt (:target raw)]
    {:intent-type    :pipeline
     :pipeline-name  (:pipeline_name raw)
     :source         {:system      (:system src)
                      :objects     (:objects src)
                      :auth-method (:auth_method src)
                      :load-type   (or (:load_type src) "incremental")
                      :cadence     (:cadence src)}
     :target         {:platform     (or (:platform tgt) "databricks")
                      :catalog      (or (:catalog tgt) "main")
                      :bronze-schema (or (:bronze_schema tgt) "bronze")
                      :silver-schema (or (:silver_schema tgt) "silver")
                      :gold-schema   (or (:gold_schema tgt) "gold")}
     :bronze         (mapv (fn [b]
                             {:object       (:object b)
                              :table        (:table b)
                              :watermark    (:watermark b)
                              :primary-key  (:primary_key b)
                              :explode-path (:explode_path b)})
                           (:bronze raw))
     :silver         (mapv (fn [s]
                             {:entity      (:entity s)
                              :table       (:table s)
                              :source      (:source s)
                              :dedup-key   (:dedup_key s)
                              :entity-kind (:entity_kind s)
                              :columns     (:columns s)})
                           (or (:silver raw) []))
     :gold           (mapv (fn [g]
                             {:model          (:model g)
                              :grain          (:grain g)
                              :metric-package (:metric_package g)
                              :depends-on     (:depends_on g)
                              :measures       (:measures g)
                              :dimensions     (:dimensions g)})
                           (or (:gold raw) []))
     :ops            (when-let [o (:ops raw)]
                       {:schedule   (:schedule o)
                        :retries    (or (:retries o) 3)
                        :timeout-min (:timeout_min o)
                        :alerts     (:alerts o)})}))

(defn parse-intent
  "Parse natural language text into a PipelineIntent using an LLM.
   Uses ISL-style retry loop with temperature escalation and error feedback."
  [text]
  (let [system-prompt (build-system-prompt text)
        raw           (translate-with-retry system-prompt text)]
    (normalize-intent-keys raw)))

(defn parse-intent-mock
  "Parse intent without LLM — for testing.
   Extracts basic info from text heuristically."
  [text]
  (let [lower (string/lower-case text)
        system (cond
                 (string/includes? lower "samsara") "samsara"
                 (string/includes? lower "stripe") "stripe"
                 :else "samsara")
        objects (cond-> []
                  (string/includes? lower "vehicle") (conj "fleet/vehicles")
                  (string/includes? lower "driver") (conj "fleet/drivers")
                  (string/includes? lower "trip") (conj "fleet/trips")
                  (string/includes? lower "stat") (conj "fleet/vehicles/stats")
                  (string/includes? lower "location") (conj "fleet/vehicles/locations")
                  (string/includes? lower "fuel") (conj "fleet/vehicles/fuel-energy")
                  (string/includes? lower "safety") (conj "fleet/safety/events"))
        objects (if (empty? objects) ["fleet/vehicles"] objects)]
    {:intent-type :pipeline
     :pipeline-name (str system "_pipeline")
     :source {:system system
              :objects objects
              :auth-method "bearer"
              :load-type "incremental"
              :cadence (when (string/includes? lower "hourly") "hourly")}
     :target {:platform "databricks"
              :catalog "main"
              :bronze-schema "bronze"
              :silver-schema "silver"
              :gold-schema "gold"}
     :bronze (mapv (fn [obj] {:object obj}) objects)
     :silver []
     :gold   (when (or (string/includes? lower "analytics")
                       (string/includes? lower "gold")
                       (string/includes? lower "utilization"))
               [{:model "fleet_utilization_daily"
                 :grain "day"
                 :metric-package "fleet_analytics"}])
     :ops    {:retries 3}}))

;; ---------------------------------------------------------------------------
;; Edit existing spec via LLM
;; ---------------------------------------------------------------------------

(def edit-spec-tool
  {:name "edit_pipeline_spec"
   :description "Apply an edit to an existing PipelineSpec. Return the full updated spec."
   :input_schema (:input_schema pipeline-intent-tool)})

(defn- build-edit-prompt [current-spec edit-text]
  (str
   (build-system-prompt edit-text)
   "\n\n## Current Pipeline Spec\n"
   "The user has an existing pipeline plan. They want to modify it.\n"
   "Here is the current spec as JSON:\n\n"
   "```json\n"
   (json/generate-string current-spec {:pretty true})
   "\n```\n\n"
   "## Instructions\n"
   "The user will describe a change. Apply it to the current spec and return the FULL updated spec "
   "using the create_pipeline_intent tool. Preserve everything from the current spec that the user "
   "did not ask to change. Common edits:\n"
   "- Changing grain (daily -> weekly)\n"
   "- Adding/removing endpoints\n"
   "- Adding/removing Silver entities or Gold models\n"
   "- Changing table names, watermarks, keys\n"
   "- Changing schedule or operational settings\n"))

(defn edit-intent
  "Edit an existing PipelineIntent/spec based on natural language instruction.
   Sends the current spec + edit text to the LLM."
  [current-spec edit-text]
  (let [system-prompt (build-edit-prompt current-spec edit-text)
        raw           (call-anthropic system-prompt edit-text)]
    (normalize-intent-keys raw)))
