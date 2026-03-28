(ns bitool.ai.assistant
  "Embedded AI assistant — explain, suggest, and refine actions for
   Bronze, Silver, and Gold workflows.

   Each public function:
   1. Assembles deterministic context from graph/proposal/schema data.
   2. Runs deterministic heuristics first (grain planner, validation checks, etc.).
   3. Calls the LLM for explanation/ranking/refinement via ai.llm.
   4. Returns a normalized response envelope.

   All functions degrade gracefully if the LLM is unavailable —
   they return deterministic-only output labeled as such."
  (:require [bitool.ai.llm :as llm]
            [bitool.ingest.grain-planner :as grain]
            [bitool.ops.schema-drift :as drift]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [clojure.string :as string]))

;; ---------------------------------------------------------------------------
;; Response envelope
;; ---------------------------------------------------------------------------

(def ^:private envelope-standard-keys
  #{:summary :confidence :recommendations :edits :open_questions :warnings :debug})

(defn normalize-ai-envelope
  "Normalize an AI response into the standard envelope shape.
   Ensures all expected keys are present even if the LLM omitted them.
   Task-specific keys (e.g. record_grain, pk_reasoning, likely_causes)
   are preserved as-is — only standard keys get defaults."
  [raw task source]
  (let [conf-raw (or (:confidence raw) 0.0)
        ;; Normalize confidence to 0..1 — prompts may return 0-100 scale
        confidence (if (and (number? conf-raw) (> conf-raw 1.0))
                     (/ (double conf-raw) 100.0)
                     (double conf-raw))
        ;; Task-specific keys that aren't part of the standard envelope
        extra-keys (apply dissoc raw (conj envelope-standard-keys :debug))]
    (merge
     extra-keys
     {:summary         (or (:summary raw) "")
      :confidence      confidence
      :recommendations (or (:recommendations raw) [])
      :edits           (or (:edits raw) {})
      :open_questions  (or (:open_questions raw) [])
      :warnings        (or (:warnings raw) [])
      :debug           {:task   task
                        :source source}})))

(defn- deterministic-envelope
  "Build a fallback envelope from deterministic data when LLM is unavailable.
   Extra keys in deterministic-data are preserved alongside the standard envelope."
  [summary task deterministic-data]
  (let [extra (apply dissoc deterministic-data envelope-standard-keys)]
    (merge
     extra
     {:summary         summary
      :confidence      0.0
      :recommendations (or (:recommendations deterministic-data) [])
      :edits           (or (:edits deterministic-data) {})
      :open_questions  (or (:open_questions deterministic-data) [])
      :warnings        (into ["AI explanation was unavailable. Showing deterministic analysis only."]
                             (or (:warnings deterministic-data) []))
      :debug           {:task task :source "deterministic_only"}})))

;; ---------------------------------------------------------------------------
;; Caching (short-lived, in-memory)
;; ---------------------------------------------------------------------------

(def ^:private cache (atom {}))
(def ^:private cache-ttl-ms (* 5 60 1000)) ;; 5 minutes

(defn clear-cache!
  "Clear the in-memory AI response cache. Useful for testing."
  []
  (reset! cache {}))

(defn- cache-key [task input-hash]
  (str task ":" input-hash))

(defn- cache-get [task input-hash]
  (let [k (cache-key task input-hash)
        entry (get @cache k)]
    (when (and entry
               (< (- (System/currentTimeMillis) (:ts entry)) cache-ttl-ms))
      (:result entry))))

(defn- cache-put! [task input-hash result]
  (let [k (cache-key task input-hash)]
    (swap! cache assoc k {:result result :ts (System/currentTimeMillis)})
    result))

(defn- stable-hash [data]
  (str (hash (json/generate-string data {:pretty false}))))

;; ---------------------------------------------------------------------------
;; P1-A: Explain Preview Schema
;; ---------------------------------------------------------------------------

(defn- preview-schema-context
  "Build the context payload for an Explain Preview prompt."
  [{:keys [endpoint_name endpoint_url load_type pagination inferred_fields
           schema_recommendations detected_records_path explode_rules sample_count]}]
  {:endpoint_name        endpoint_name
   :endpoint_url         endpoint_url
   :load_type            load_type
   :pagination           pagination
   :field_count          (count inferred_fields)
   :timestamp_fields     (->> inferred_fields
                               (filter #(re-find #"(?i)TIMESTAMP|DATE" (str (:type %))))
                               (mapv #(select-keys % [:column_name :type :sample_coverage])))
   :id_fields            (->> inferred_fields
                               (filter #(re-find #"(?i)(^id$|_id$|_key$)" (str (:column_name %))))
                               (mapv #(select-keys % [:column_name :type :sample_coverage])))
   :records_path         detected_records_path
   :explode_rules        explode_rules
   :sample_count         sample_count
   :schema_recommendations schema_recommendations})

(def ^:private explain-preview-system-prompt
  "You are a data engineering assistant for BiTool, a medallion architecture platform.
The user has run Preview Schema on an API endpoint. You are given the system's
deterministic analysis: inferred fields, detected record path, timestamp candidates,
identifier candidates, and explode rules.

Your job:
1. Explain WHY the system chose this record path.
2. Explain WHY certain fields look like primary keys or watermarks.
3. Explain any explode rules and what they mean for Bronze table shape.
4. Note any ambiguity or uncertainty.
5. Do NOT invent fields that are not in the context.

Respond as JSON with these keys:
  summary, record_grain, pk_reasoning, watermark_reasoning, explode_reasoning,
  field_notes (array of {field, note}), open_questions (array of strings)")

(defn explain-preview-schema!
  "P1-A: Explain the Preview Schema result for a Bronze endpoint.
   Runs grain planner deterministically, then enriches with LLM explanation."
  [{:keys [endpoint_config inferred_fields] :as params}]
  (let [task       "explain_preview"
        input-hash (stable-hash (select-keys params [:endpoint_config :inferred_fields]))
        cached     (cache-get task input-hash)]
    (if cached
      (assoc-in cached [:debug :source] "cached")
      (let [;; Deterministic analysis first
            grain-result (try
                           (grain/recommend-endpoint-config
                            endpoint_config inferred_fields
                            {:detected-records-path (:detected_records_path endpoint_config)
                             :configured-records-path (get-in endpoint_config [:json_explode_rules 0 :path])})
                           (catch Exception e
                             (log/warn "Grain planner failed in explain-preview" {:error (.getMessage e)})
                             nil))
            context      (preview-schema-context (merge endpoint_config
                                                        {:inferred_fields inferred_fields}))
            context-with-grain (assoc context :grain_planner_result grain-result)]
        (try
          (let [raw  (llm/call-llm-text
                      explain-preview-system-prompt
                      (json/generate-string context-with-grain {:pretty true})
                      :max-tokens 700)
                parsed (try (json/parse-string raw true)
                            (catch Exception _
                              {:summary raw}))
                result (normalize-ai-envelope parsed task "deterministic_plus_ai")]
            (cache-put! task input-hash result))
          (catch Exception e
            (log/warn "LLM unavailable for explain-preview, using deterministic fallback"
                      {:error (.getMessage e)})
            (let [summary (str "Preview detected "
                               (count inferred_fields) " fields"
                               (when-let [path (:records_path context)]
                                 (str " at record path " path))
                               "."
                               (when grain-result
                                 (str " Grain planner recommends "
                                      (get-in grain-result [:grain :path])
                                      " with " (get-in grain-result [:grain :confidence]) "% confidence.")))]
              (cache-put! task input-hash
                          (deterministic-envelope summary task
                                                  {:recommendations (or (:reasons grain-result) [])})))))))))

;; ---------------------------------------------------------------------------
;; P1-B: Suggest Bronze Keys
;; ---------------------------------------------------------------------------

(def ^:private suggest-keys-system-prompt
  "You are a data engineering assistant for BiTool.
The system's deterministic grain planner has already analyzed an API endpoint's
inferred fields and produced primary key, watermark, and grain recommendations
with confidence scores.

Your job:
1. Explain WHY these specific fields were chosen as PK and watermark.
2. If confidence is below 80, suggest alternatives and explain the ambiguity.
3. Provide a grain_label (e.g. 'one row per vehicle per API call').
4. Do NOT recommend fields not present in the inferred fields list.

Respond as JSON with these keys:
  primary_key_fields (array), watermark_column (string or null),
  grain_label (string), confidence (number 0.0-1.0),
  alternatives (array of {fields, watermark, reason}),
  summary (string)")

(defn suggest-bronze-keys!
  "P1-B: Suggest PK/watermark/grain for a Bronze endpoint.
   Runs the deterministic grain planner first, then uses LLM for explanation/ranking."
  [{:keys [endpoint_config inferred_fields] :as params}]
  (let [task       "suggest_bronze_keys"
        input-hash (stable-hash (select-keys params [:endpoint_config :inferred_fields]))
        cached     (cache-get task input-hash)]
    (if cached
      (assoc-in cached [:debug :source] "cached")
      (let [;; Always run deterministic grain planner first
            grain-result (try
                           (grain/recommend-endpoint-config
                            endpoint_config inferred_fields
                            {:detected-records-path (:detected_records_path endpoint_config)
                             :configured-records-path (get-in endpoint_config [:json_explode_rules 0 :path])})
                           (catch Exception e
                             (log/warn "Grain planner failed in suggest-bronze-keys" {:error (.getMessage e)})
                             nil))
            context {:endpoint_name      (:endpoint_name endpoint_config)
                     :inferred_fields    (mapv #(select-keys % [:column_name :path :type :sample_coverage :nullable])
                                               inferred_fields)
                     :grain_planner_result grain-result}]
        (try
          (let [raw    (llm/call-llm-text
                        suggest-keys-system-prompt
                        (json/generate-string context {:pretty true})
                        :max-tokens 500)
                parsed (try (json/parse-string raw true)
                            (catch Exception _
                              {:summary raw}))
                result (normalize-ai-envelope
                        (merge parsed
                               ;; Carry grain planner deterministic output alongside AI explanation
                               {:grain_planner_result grain-result})
                        task "deterministic_plus_ai")]
            (cache-put! task input-hash result))
          (catch Exception e
            (log/warn "LLM unavailable for suggest-bronze-keys, using deterministic fallback"
                      {:error (.getMessage e)})
            (let [summary (cond-> "Deterministic grain planner analysis:"
                            (get-in grain-result [:pk :fields])
                            (str " PK=" (string/join "," (get-in grain-result [:pk :fields])))
                            (get-in grain-result [:watermark :field])
                            (str " WM=" (get-in grain-result [:watermark :field]))
                            (get-in grain-result [:grain :path])
                            (str " Grain=" (get-in grain-result [:grain :path])))]
              (cache-put! task input-hash
                          (deterministic-envelope summary task
                                                  {:recommendations (or (:reasons grain-result) [])
                                                   :grain_planner_result grain-result})))))))))

;; ---------------------------------------------------------------------------
;; P1-C: Explain Model Proposal
;; ---------------------------------------------------------------------------

(def ^:private explain-proposal-system-prompt
  "You are a data engineering assistant for BiTool.
The user has a Silver or Gold model proposal. You are given the proposal metadata:
columns, materialization mode, merge keys, source endpoint, and compiled SQL if available.

Your job:
1. Explain what business entity this model represents.
2. Explain why these columns were selected and what they map from.
3. Explain the materialization and merge key choices.
4. Surface any unresolved assumptions or missing data.

Respond as JSON with these keys:
  summary, business_shape, materialization_reasoning, key_reasoning,
  column_reasoning (array of {column, reasoning}), open_questions (array of strings)")

(defn- proposal-context
  "Build the context payload for an Explain Proposal prompt."
  [{:keys [proposal_id proposal_json compile_result source_endpoint layer]}]
  (let [proposal (if (string? proposal_json)
                   (try (json/parse-string proposal_json true) (catch Exception _ {}))
                   (or proposal_json {}))]
    {:proposal_id      proposal_id
     :layer            (or layer "silver")
     :columns          (or (:columns proposal) (:target_columns proposal) [])
     :materialization  (or (:materialization proposal) (:materialization_mode proposal))
     :merge_keys       (or (:merge_keys proposal) [])
     :processing_policy (:processing_policy proposal)
     :source_endpoint  source_endpoint
     :has_compiled_sql (boolean compile_result)}))

(defn explain-model-proposal!
  "P1-C: Explain a Silver or Gold model proposal."
  [{:keys [proposal_id proposal_json compile_result] :as params}]
  (let [task       "explain_proposal"
        input-hash (stable-hash (select-keys params [:proposal_id :proposal_json]))
        cached     (cache-get task input-hash)]
    (if cached
      (assoc-in cached [:debug :source] "cached")
      (let [context (proposal-context params)]
        (try
          (let [raw    (llm/call-llm-text
                        explain-proposal-system-prompt
                        (json/generate-string context {:pretty true})
                        :max-tokens 900)
                parsed (try (json/parse-string raw true)
                            (catch Exception _ {:summary raw}))
                result (normalize-ai-envelope parsed task "deterministic_plus_ai")]
            (cache-put! task input-hash result))
          (catch Exception e
            (log/warn "LLM unavailable for explain-proposal, using deterministic fallback"
                      {:error (.getMessage e)})
            (let [cols (count (or (:columns context) []))
                  summary (str "Proposal " proposal_id
                               " has " cols " columns"
                               (when (:materialization context)
                                 (str ", materialization=" (:materialization context)))
                               (when (seq (:merge_keys context))
                                 (str ", merge_keys=" (string/join "," (:merge_keys context))))
                               ".")]
              (cache-put! task input-hash
                          (deterministic-envelope summary task {})))))))))

;; ---------------------------------------------------------------------------
;; P1-D: Explain Validation Failure
;; ---------------------------------------------------------------------------

(def ^:private explain-validation-system-prompt
  "You are a data engineering assistant for BiTool.
The user has a model proposal that failed or produced warnings during validation.
You are given the validation result with individual check outcomes.

Your job:
1. Translate each failed check into a plain-language explanation.
2. Identify the most likely root cause.
3. Suggest concrete next steps the user can take to fix the issue.
4. Do NOT pretend to be certain about causes you are guessing at.

Respond as JSON with these keys:
  summary, likely_causes (array of strings),
  suggested_actions (array of strings), warnings (array of strings)")

(defn- validation-context
  "Build the context payload for an Explain Validation Failure prompt."
  [{:keys [proposal_id proposal_json validation_result layer]}]
  (let [proposal (if (string? proposal_json)
                   (try (json/parse-string proposal_json true) (catch Exception _ {}))
                   (or proposal_json {}))]
    {:proposal_id       proposal_id
     :layer             (or layer "silver")
     :column_count      (count (or (:columns proposal) (:target_columns proposal) []))
     :materialization   (or (:materialization proposal) (:materialization_mode proposal))
     :validation_result validation_result}))

(defn explain-proposal-validation!
  "P1-D: Explain why a proposal validation failed or warned."
  [{:keys [proposal_id validation_result] :as params}]
  (let [task       "explain_validation"
        input-hash (stable-hash (select-keys params [:proposal_id :validation_result]))
        cached     (cache-get task input-hash)]
    (if cached
      (assoc-in cached [:debug :source] "cached")
      (let [context (validation-context params)]
        (try
          (let [raw    (llm/call-llm-text
                        explain-validation-system-prompt
                        (json/generate-string context {:pretty true})
                        :max-tokens 700)
                parsed (try (json/parse-string raw true)
                            (catch Exception _ {:summary raw}))
                result (normalize-ai-envelope parsed task "deterministic_plus_ai")]
            (cache-put! task input-hash result))
          (catch Exception e
            (log/warn "LLM unavailable for explain-validation, using deterministic fallback"
                      {:error (.getMessage e)})
            (let [checks  (or (:checks validation_result) [])
                  failed  (filter #(= "FAIL" (:status %)) checks)
                  warned  (filter #(= "WARN" (:status %)) checks)
                  summary (str (count failed) " check(s) failed, "
                               (count warned) " warning(s)"
                               (when (seq failed)
                                 (str ". Failed: " (string/join "; " (map :message failed)))))]
              (cache-put! task input-hash
                          (deterministic-envelope summary task
                                                  {:likely_causes  (mapv :message failed)
                                                   :warnings       (mapv :message warned)})))))))))

;; ---------------------------------------------------------------------------
;; P2-A: Suggest Silver Transforms
;; ---------------------------------------------------------------------------

(def ^:private suggest-transforms-system-prompt
  "You are a data engineering assistant for BiTool, a medallion architecture platform.
The user has an existing Silver model proposal with columns, mappings, and materialization
settings. They want suggestions for improving the transformation logic.

Your job:
1. Identify columns that could benefit from type casting, renaming, or enrichment.
2. Suggest new derived columns (e.g. date parts, concatenations, case-when logic).
3. Suggest filter conditions or deduplication strategies if applicable.
4. Each edit should be a concrete, apply-ready change.

Respond as JSON with these keys:
  summary (string), confidence (number 0.0-1.0),
  edits (object with categories: column_renames, type_casts, derived_columns, filters —
         each an array of {description, target_column, expression}),
  recommendations (array of strings), open_questions (array of strings)")

(defn suggest-silver-transforms!
  "P2-A: Suggest transformation improvements for a Silver proposal."
  [{:keys [proposal_id proposal] :as params}]
  (let [task       "suggest_silver_transforms"
        input-hash (stable-hash (select-keys params [:proposal_id :proposal]))
        cached     (cache-get task input-hash)]
    (if cached
      (assoc-in cached [:debug :source] "cached")
      (let [columns  (or (:columns proposal) (:target_columns proposal) [])
            mappings (or (:column_mappings proposal) [])
            context  {:proposal_id    proposal_id
                      :columns        columns
                      :mappings       mappings
                      :materialization (or (:materialization proposal)
                                          (:materialization_mode proposal))
                      :merge_keys     (or (:merge_keys proposal) [])
                      :column_count   (count columns)}]
        (try
          (let [raw    (llm/call-llm-text
                        suggest-transforms-system-prompt
                        (json/generate-string context {:pretty true})
                        :max-tokens 800)
                parsed (try (json/parse-string raw true)
                            (catch Exception _ {:summary raw}))
                result (normalize-ai-envelope parsed task "deterministic_plus_ai")]
            (cache-put! task input-hash result))
          (catch Exception e
            (log/warn "LLM unavailable for suggest-silver-transforms, using deterministic fallback"
                      {:error (.getMessage e)})
            (let [summary (str "Silver proposal " proposal_id
                               " has " (count columns) " columns"
                               (when (seq mappings)
                                 (str " with " (count mappings) " mappings"))
                               ".")]
              (cache-put! task input-hash
                          (deterministic-envelope summary task {})))))))))

;; ---------------------------------------------------------------------------
;; P2-B: Generate Silver Proposal from BRD
;; ---------------------------------------------------------------------------

(def ^:private silver-brd-system-prompt
  "You are a data engineering assistant for BiTool, a medallion architecture platform.
The user provides a Business Requirements Document (BRD) describing what they want from
a Silver model: business entities, required fields, grain, SLAs, etc. You also receive
the available Bronze source columns from the schema profile.

Your job:
1. Propose a Silver model: target columns, source mappings, materialization, merge keys.
2. Map BRD requirements to available Bronze columns where possible.
3. Flag any BRD requirements that cannot be satisfied by available columns.
4. Provide confidence based on how well Bronze columns cover BRD needs.

Respond as JSON with these keys:
  summary (string), confidence (number 0.0-1.0),
  edits (object with: target_columns — array of {target_column, source_column, data_type, expression},
         materialization — string, merge_keys — array of strings),
  unmapped_requirements (array of strings),
  recommendations (array of strings), open_questions (array of strings)")

(defn generate-silver-proposal-from-brd!
  "P2-B: Generate a Silver proposal from a BRD description and Bronze schema profile."
  [{:keys [brd_text source_columns endpoint_config] :as params}]
  (let [task       "silver_from_brd"
        input-hash (stable-hash (select-keys params [:brd_text :source_columns]))
        cached     (cache-get task input-hash)]
    (if cached
      (assoc-in cached [:debug :source] "cached")
      (let [context {:brd_text       brd_text
                     :source_columns (or source_columns [])
                     :endpoint_name  (:endpoint_name endpoint_config)
                     :column_count   (count (or source_columns []))}]
        (try
          (let [raw    (llm/call-llm-text
                        silver-brd-system-prompt
                        (json/generate-string context {:pretty true})
                        :max-tokens 1000)
                parsed (try (json/parse-string raw true)
                            (catch Exception _ {:summary raw}))
                result (normalize-ai-envelope parsed task "deterministic_plus_ai")]
            (cache-put! task input-hash result))
          (catch Exception e
            (log/warn "LLM unavailable for silver-from-brd, using deterministic fallback"
                      {:error (.getMessage e)})
            (cache-put! task input-hash
                        (deterministic-envelope
                         (str "BRD received (" (count (string/split-lines (or brd_text "")))
                              " lines). " (count (or source_columns [])) " Bronze columns available.")
                         task {}))))))))

;; ---------------------------------------------------------------------------
;; P2-C: Generate Gold Proposal from BRD
;; ---------------------------------------------------------------------------

(def ^:private gold-brd-system-prompt
  "You are a data engineering assistant for BiTool, a medallion architecture platform.
The user provides a Business Requirements Document (BRD) describing what they want from
a Gold mart/aggregate model. You also receive the Silver source columns.

Your job:
1. Propose a Gold model: target columns, aggregations, grain, materialization, merge keys.
2. Map BRD requirements to available Silver columns.
3. Suggest appropriate aggregation functions (SUM, COUNT, AVG, MAX, MIN, etc.).
4. Flag requirements that need Silver columns not yet available.

Respond as JSON with these keys:
  summary (string), confidence (number 0.0-1.0),
  edits (object with: target_columns — array of {target_column, source_column, data_type, expression, aggregation},
         materialization — string, merge_keys — array of strings, grain — string),
  unmapped_requirements (array of strings),
  recommendations (array of strings), open_questions (array of strings)")

(defn generate-gold-proposal-from-brd!
  "P2-C: Generate a Gold proposal from a BRD description and Silver columns."
  [{:keys [brd_text source_columns silver_proposal_id] :as params}]
  (let [task       "gold_from_brd"
        input-hash (stable-hash (select-keys params [:brd_text :source_columns]))
        cached     (cache-get task input-hash)]
    (if cached
      (assoc-in cached [:debug :source] "cached")
      (let [context {:brd_text           brd_text
                     :source_columns     (or source_columns [])
                     :silver_proposal_id silver_proposal_id
                     :column_count       (count (or source_columns []))}]
        (try
          (let [raw    (llm/call-llm-text
                        gold-brd-system-prompt
                        (json/generate-string context {:pretty true})
                        :max-tokens 1000)
                parsed (try (json/parse-string raw true)
                            (catch Exception _ {:summary raw}))
                result (normalize-ai-envelope parsed task "deterministic_plus_ai")]
            (cache-put! task input-hash result))
          (catch Exception e
            (log/warn "LLM unavailable for gold-from-brd, using deterministic fallback"
                      {:error (.getMessage e)})
            (cache-put! task input-hash
                        (deterministic-envelope
                         (str "BRD received (" (count (string/split-lines (or brd_text "")))
                              " lines). " (count (or source_columns [])) " Silver columns available.")
                         task {}))))))))

;; ---------------------------------------------------------------------------
;; P2-D: Suggest Gold Mart Design
;; ---------------------------------------------------------------------------

(def ^:private suggest-gold-mart-system-prompt
  "You are a data engineering assistant for BiTool, a medallion architecture platform.
The user has an existing Gold model proposal. You receive the proposal columns,
materialization, merge keys, and the Silver source information.

Your job:
1. Evaluate the mart design: is the grain appropriate for analytics?
2. Suggest additional aggregation columns or dimension columns.
3. Suggest partition/cluster key optimizations.
4. Identify missing metrics or KPIs common for this entity type.

Respond as JSON with these keys:
  summary (string), confidence (number 0.0-1.0),
  edits (object with categories: add_columns, modify_columns, partition_keys —
         each an array of {description, target_column, expression}),
  recommendations (array of strings), open_questions (array of strings)")

(defn suggest-gold-mart-design!
  "P2-D: Suggest improvements to a Gold mart proposal."
  [{:keys [proposal_id proposal source_table] :as params}]
  (let [task       "suggest_gold_mart"
        input-hash (stable-hash (select-keys params [:proposal_id :proposal]))
        cached     (cache-get task input-hash)]
    (if cached
      (assoc-in cached [:debug :source] "cached")
      (let [columns (or (:columns proposal) (:target_columns proposal) [])
            context {:proposal_id     proposal_id
                     :columns         columns
                     :materialization (or (:materialization proposal)
                                         (:materialization_mode proposal))
                     :merge_keys      (or (:merge_keys proposal) [])
                     :source_table    source_table
                     :column_count    (count columns)}]
        (try
          (let [raw    (llm/call-llm-text
                        suggest-gold-mart-system-prompt
                        (json/generate-string context {:pretty true})
                        :max-tokens 800)
                parsed (try (json/parse-string raw true)
                            (catch Exception _ {:summary raw}))
                result (normalize-ai-envelope parsed task "deterministic_plus_ai")]
            (cache-put! task input-hash result))
          (catch Exception e
            (log/warn "LLM unavailable for suggest-gold-mart, using deterministic fallback"
                      {:error (.getMessage e)})
            (let [summary (str "Gold proposal " proposal_id
                               " has " (count columns) " columns"
                               (when (:materialization context)
                                 (str ", materialization=" (:materialization context)))
                               ".")]
              (cache-put! task input-hash
                          (deterministic-envelope summary task {})))))))))

;; ---------------------------------------------------------------------------
;; P2-E: Explain Schema Drift
;; ---------------------------------------------------------------------------

(def ^:private explain-drift-system-prompt
  "You are a data engineering assistant for BiTool, a medallion architecture platform.
A schema drift event has been detected — the inferred schema from a recent API call
differs from the previously known schema. You are given the drift details: new fields,
missing fields, and type changes, along with the severity classification.

Your job:
1. Explain what changed in plain language.
2. Explain the likely cause (API version change, new feature, deprecation, etc.).
3. Assess the impact on downstream Silver/Gold models.
4. Suggest whether the user should approve, investigate, or block the change.

Respond as JSON with these keys:
  summary (string), confidence (number 0.0-1.0),
  likely_causes (array of strings), impact_assessment (string),
  suggested_action (string — one of: approve, investigate, block),
  recommendations (array of strings), open_questions (array of strings)")

(defn explain-schema-drift!
  "P2-E: Explain a schema drift event with AI-enhanced analysis."
  [{:keys [event_id workspace_key] :as params}]
  (let [task       "explain_drift"
        input-hash (stable-hash (select-keys params [:event_id]))
        cached     (cache-get task input-hash)]
    (if cached
      (assoc-in cached [:debug :source] "cached")
      (let [event (drift/get-drift-event event_id (or workspace_key ""))
            _     (when-not event
                    (throw (ex-info "Drift event not found"
                                    {:status 404 :event_id event_id})))
            drift-json (try (json/parse-string (or (:drift_json event) "{}") true)
                            (catch Exception _ {}))
            severity (drift/classify-drift-severity drift-json)
            context {:event_id      event_id
                     :endpoint_name (:endpoint_name event)
                     :severity      severity
                     :new_fields    (:new_fields drift-json)
                     :missing_fields (:missing_fields drift-json)
                     :type_changes  (:type_changes drift-json)
                     :detected_at   (:detected_at_utc event)}]
        (try
          (let [raw    (llm/call-llm-text
                        explain-drift-system-prompt
                        (json/generate-string context {:pretty true})
                        :max-tokens 700)
                parsed (try (json/parse-string raw true)
                            (catch Exception _ {:summary raw}))
                result (normalize-ai-envelope
                        (merge parsed {:severity severity})
                        task "deterministic_plus_ai")]
            (cache-put! task input-hash result))
          (catch Exception e
            (log/warn "LLM unavailable for explain-drift, using deterministic fallback"
                      {:error (.getMessage e)})
            (let [new-ct     (count (or (:new_fields drift-json) []))
                  missing-ct (count (or (:missing_fields drift-json) []))
                  change-ct  (count (or (:type_changes drift-json) []))
                  summary    (str "Schema drift detected"
                                  (when severity (str " (severity: " severity ")"))
                                  ": " new-ct " new field(s), "
                                  missing-ct " missing field(s), "
                                  change-ct " type change(s).")]
              (cache-put! task input-hash
                          (deterministic-envelope summary task
                                                  {:severity severity})))))))))

;; ---------------------------------------------------------------------------
;; P2-F: Suggest Drift Remediation
;; ---------------------------------------------------------------------------

(def ^:private suggest-remediation-system-prompt
  "You are a data engineering assistant for BiTool, a medallion architecture platform.
A schema drift event has occurred and the user wants to know how to fix or adapt.
You are given the drift details and any DDL preview showing what schema changes
would be needed.

Your job:
1. For new fields: suggest whether to add them to Bronze and which Silver models need updating.
2. For missing fields: suggest whether to mark nullable, remove from Silver, or investigate.
3. For type changes: suggest whether to widen the type or create a migration.
4. Provide concrete edits that the user can apply.

Respond as JSON with these keys:
  summary (string), confidence (number 0.0-1.0),
  edits (object with categories: bronze_changes, silver_changes, gold_changes —
         each an array of {description, action, target}),
  recommendations (array of strings), open_questions (array of strings)")

(defn suggest-drift-remediation!
  "P2-F: Suggest remediation steps for a schema drift event."
  [{:keys [event_id workspace_key] :as params}]
  (let [task       "suggest_drift_remediation"
        input-hash (stable-hash (select-keys params [:event_id]))
        cached     (cache-get task input-hash)]
    (if cached
      (assoc-in cached [:debug :source] "cached")
      (let [event (drift/get-drift-event event_id (or workspace_key ""))
            _     (when-not event
                    (throw (ex-info "Drift event not found"
                                    {:status 404 :event_id event_id})))
            drift-json (try (json/parse-string (or (:drift_json event) "{}") true)
                            (catch Exception _ {}))
            severity (drift/classify-drift-severity drift-json)
            ddl-preview (try (drift/preview-schema-ddl
                                {:workspace-key (or workspace_key "")
                                 :graph-id      (:graph_id event)
                                 :api-node-id   (:api_node_id event)
                                 :endpoint-name (:endpoint_name event)
                                 :event-id      event_id})
                               (catch Exception e
                                 (log/debug "DDL preview unavailable for remediation context"
                                            {:error (.getMessage e)})
                                 nil))
            context {:event_id       event_id
                     :endpoint_name  (:endpoint_name event)
                     :severity       severity
                     :new_fields     (:new_fields drift-json)
                     :missing_fields (:missing_fields drift-json)
                     :type_changes   (:type_changes drift-json)
                     :ddl_preview    (when ddl-preview
                                       {:add_columns  (count (get-in ddl-preview [:ddl_plan :add-columns]))
                                        :widen_columns (count (get-in ddl-preview [:ddl_plan :widen-columns]))})}]
        (try
          (let [raw    (llm/call-llm-text
                        suggest-remediation-system-prompt
                        (json/generate-string context {:pretty true})
                        :max-tokens 800)
                parsed (try (json/parse-string raw true)
                            (catch Exception _ {:summary raw}))
                result (normalize-ai-envelope
                        (merge parsed {:severity severity
                                       :ddl_available (boolean ddl-preview)})
                        task "deterministic_plus_ai")]
            (cache-put! task input-hash result))
          (catch Exception e
            (log/warn "LLM unavailable for suggest-drift-remediation, using deterministic fallback"
                      {:error (.getMessage e)})
            (let [new-ct     (count (or (:new_fields drift-json) []))
                  missing-ct (count (or (:missing_fields drift-json) []))
                  change-ct  (count (or (:type_changes drift-json) []))
                  recs       (cond-> []
                               (pos? new-ct)
                               (conj (str "Review " new-ct " new field(s) for Bronze promotion"))
                               (pos? missing-ct)
                               (conj (str "Investigate " missing-ct " missing field(s) — check API version"))
                               (pos? change-ct)
                               (conj (str "Evaluate " change-ct " type change(s) for widening compatibility")))
                  summary    (str "Drift remediation needed"
                                  (when severity (str " (" severity ")"))
                                  ": " (string/join "; " recs) ".")]
              (cache-put! task input-hash
                          (deterministic-envelope summary task
                                                  {:severity severity
                                                   :ddl_available (boolean ddl-preview)
                                                   :recommendations recs})))))))))

;; ---------------------------------------------------------------------------
;; P3-A: Explain Endpoint Business Shape
;; ---------------------------------------------------------------------------

(def ^:private explain-business-shape-system-prompt
  "You are a data engineering assistant for BiTool, a medallion architecture platform.
The user has an API endpoint with inferred fields. You need to classify the business
shape of this data source.

Your job:
1. Classify the entity type: event, fact, reference, or snapshot.
2. Describe the business entity this endpoint represents.
3. Suggest what downstream Silver model shapes would make sense.
4. Suggest what Gold use cases (dashboards, KPIs, aggregations) this data enables.

Respond as JSON with these keys:
  summary (string), confidence (number 0.0-1.0),
  business_shape (string — plain-language description),
  likely_entity_type (string — one of: event, fact, reference, snapshot),
  downstream_silver_shapes (array of strings),
  downstream_gold_use_cases (array of strings),
  recommendations (array of strings), open_questions (array of strings)")

(defn explain-endpoint-business-shape!
  "P3-A: Explain what business entity an API endpoint represents."
  [{:keys [endpoint_config inferred_fields] :as params}]
  (let [task       "explain_business_shape"
        input-hash (stable-hash (select-keys params [:endpoint_config :inferred_fields]))
        cached     (cache-get task input-hash)]
    (if cached
      (assoc-in cached [:debug :source] "cached")
      (let [context {:endpoint_name  (:endpoint_name endpoint_config)
                     :endpoint_url   (:endpoint_url endpoint_config)
                     :load_type      (:load_type endpoint_config)
                     :field_count    (count inferred_fields)
                     :fields         (mapv #(select-keys % [:column_name :type :sample_coverage :nullable])
                                           inferred_fields)
                     :timestamp_fields (->> inferred_fields
                                            (filter #(re-find #"(?i)TIMESTAMP|DATE" (str (:type %))))
                                            (mapv :column_name))
                     :id_fields       (->> inferred_fields
                                           (filter #(re-find #"(?i)(^id$|_id$|_key$)" (str (:column_name %))))
                                           (mapv :column_name))}]
        (try
          (let [raw    (llm/call-llm-text
                         explain-business-shape-system-prompt
                         (json/generate-string context {:pretty true})
                         :max-tokens 700)
                parsed (try (json/parse-string raw true)
                            (catch Exception _ {:summary raw}))
                result (normalize-ai-envelope parsed task "deterministic_plus_ai")]
            (cache-put! task input-hash result))
          (catch Exception e
            (log/warn "LLM unavailable for explain-business-shape, using deterministic fallback"
                      {:error (.getMessage e)})
            (let [has-ts (seq (:timestamp_fields context))
                  has-id (seq (:id_fields context))
                  entity-type (cond
                                (and has-ts has-id) "event"
                                has-id "reference"
                                has-ts "snapshot"
                                :else "fact")
                  summary (str "Endpoint " (:endpoint_name endpoint_config)
                               " has " (count inferred_fields) " fields. "
                               "Likely entity type: " entity-type ".")]
              (cache-put! task input-hash
                          (deterministic-envelope summary task
                                                  {:likely_entity_type entity-type})))))))))

;; ---------------------------------------------------------------------------
;; P3-B: Explain Target Strategy (Write Mode)
;; ---------------------------------------------------------------------------

(def ^:private explain-target-strategy-system-prompt
  "You are a data engineering assistant for BiTool, a medallion architecture platform.
The user has configured a target warehouse with a specific write mode. You need to
explain the tradeoffs of this choice.

Your job:
1. Explain why the chosen write mode is appropriate (or not) for this use case.
2. Compare with alternative modes: what would change with append, merge, replace, etc.
3. Note cost implications (e.g. merge is more expensive than append).
4. Note performance implications (e.g. partition pruning, clustering).
5. Note operational risks (e.g. data loss with replace, duplicate risk with append).

Respond as JSON with these keys:
  summary (string), confidence (number 0.0-1.0),
  tradeoffs (array of {mode, pros (array), cons (array)}),
  cost_notes (array of strings),
  performance_notes (array of strings),
  operational_risks (array of strings),
  recommendations (array of strings), open_questions (array of strings)")

(defn explain-target-strategy!
  "P3-B: Explain why a target write mode was chosen and its tradeoffs."
  [{:keys [target_config proposal] :as params}]
  (let [task       "explain_target_strategy"
        input-hash (stable-hash (select-keys params [:target_config]))
        cached     (cache-get task input-hash)]
    (if cached
      (assoc-in cached [:debug :source] "cached")
      (let [context {:target_kind       (:target_kind target_config)
                     :write_mode        (:write_mode target_config)
                     :table_format      (:table_format target_config)
                     :merge_keys        (:merge_keys target_config)
                     :partition_columns (:partition_columns target_config)
                     :cluster_by        (:cluster_by target_config)
                     :materialization   (when proposal
                                          (or (:materialization proposal)
                                              (:materialization_mode proposal)))
                     :has_compiled_sql  (boolean (:compiled_sql proposal))}]
        (try
          (let [raw    (llm/call-llm-text
                         explain-target-strategy-system-prompt
                         (json/generate-string context {:pretty true})
                         :max-tokens 800)
                parsed (try (json/parse-string raw true)
                            (catch Exception _ {:summary raw}))
                result (normalize-ai-envelope parsed task "deterministic_plus_ai")]
            (cache-put! task input-hash result))
          (catch Exception e
            (log/warn "LLM unavailable for explain-target-strategy, using deterministic fallback"
                      {:error (.getMessage e)})
            (let [wm   (or (:write_mode target_config) "append")
                  recs (cond-> [(str "Current write mode: " wm)]
                         (= wm "append")
                         (conj "Append is lowest-cost but risks duplicates without dedup")
                         (= wm "merge")
                         (conj "Merge handles updates correctly but is more expensive")
                         (= wm "replace")
                         (conj "Replace drops and recreates — simple but causes downtime")
                         (seq (:merge_keys target_config))
                         (conj (str "Merge keys: " (string/join ", " (:merge_keys target_config)))))
                  summary (str "Target uses " wm " write mode on "
                               (or (:target_kind target_config) "unknown") " ("
                               (or (:table_format target_config) "delta") ").")]
              (cache-put! task input-hash
                          (deterministic-envelope summary task
                                                  {:recommendations recs})))))))))

;; ---------------------------------------------------------------------------
;; P3-C: Generate Metric Glossary
;; ---------------------------------------------------------------------------

(def ^:private generate-metric-glossary-system-prompt
  "You are a data engineering assistant for BiTool, a medallion architecture platform.
The user has a Gold model proposal with columns representing dimensions and measures.
Generate a metric glossary that business users can reference.

Your job:
1. For each measure column, generate a business-friendly metric definition.
2. Identify which columns are dimensions vs measures.
3. Note any assumptions baked into the metric definitions.
4. Flag caveats that consumers should be aware of.

Respond as JSON with these keys:
  summary (string), confidence (number 0.0-1.0),
  metrics (array of {name, definition, source_columns (array), assumptions (array), caveats (array)}),
  definitions (array of strings — shared business definitions),
  assumptions (array of strings — shared assumptions),
  caveats (array of strings — shared caveats),
  recommendations (array of strings), open_questions (array of strings)")

(defn generate-metric-glossary!
  "P3-C: Generate a metric glossary for a Gold model proposal."
  [{:keys [proposal_id proposal brd_text] :as params}]
  (let [task       "generate_metric_glossary"
        input-hash (stable-hash (select-keys params [:proposal_id :proposal]))
        cached     (cache-get task input-hash)]
    (if cached
      (assoc-in cached [:debug :source] "cached")
      (let [columns (or (:columns proposal) (:target_columns proposal) [])
            context {:proposal_id  proposal_id
                     :columns      columns
                     :column_count (count columns)
                     :merge_keys   (or (:merge_keys proposal) [])
                     :brd_text     brd_text}]
        (try
          (let [raw    (llm/call-llm-text
                         generate-metric-glossary-system-prompt
                         (json/generate-string context {:pretty true})
                         :max-tokens 1000)
                parsed (try (json/parse-string raw true)
                            (catch Exception _ {:summary raw}))
                result (normalize-ai-envelope parsed task "deterministic_plus_ai")]
            (cache-put! task input-hash result))
          (catch Exception e
            (log/warn "LLM unavailable for generate-metric-glossary, using deterministic fallback"
                      {:error (.getMessage e)})
            (let [summary (str "Gold proposal " proposal_id
                               " has " (count columns) " columns. "
                               "Metric glossary generation requires AI assistance.")]
              (cache-put! task input-hash
                          (deterministic-envelope summary task {})))))))))

;; ---------------------------------------------------------------------------
;; P3-D: Explain Run or KPI Anomaly
;; ---------------------------------------------------------------------------

(def ^:private explain-anomaly-system-prompt
  "You are a data engineering assistant for BiTool, a medallion architecture platform.
A model run produced unexpected results, or a KPI value has shifted anomalously.
You are given the run history, validation history, recent drift events, and KPI
delta information.

Your job:
1. Identify the most likely upstream cause of the anomaly.
2. List impacted downstream assets (models, dashboards, KPIs).
3. Suggest concrete next checks the user should perform.
4. Do NOT speculate without evidence from the provided context.

Respond as JSON with these keys:
  summary (string), confidence (number 0.0-1.0),
  likely_causes (array of {cause, evidence}),
  impacted_assets (array of strings),
  next_checks (array of strings),
  recommendations (array of strings), open_questions (array of strings)")

(defn explain-run-or-kpi-anomaly!
  "P3-D: Explain why a model run or KPI value is anomalous."
  [{:keys [proposal_id run_history validation_history drift_events kpi_delta] :as params}]
  (let [task       "explain_anomaly"
        input-hash (stable-hash (select-keys params [:proposal_id :kpi_delta :run_history]))
        cached     (cache-get task input-hash)]
    (if cached
      (assoc-in cached [:debug :source] "cached")
      (let [context {:proposal_id        proposal_id
                     :run_count          (count (or run_history []))
                     :recent_runs        (take 5 (or run_history []))
                     :validation_count   (count (or validation_history []))
                     :recent_validations (take 3 (or validation_history []))
                     :drift_event_count  (count (or drift_events []))
                     :recent_drift       (take 3 (or drift_events []))
                     :kpi_delta          kpi_delta}]
        (try
          (let [raw    (llm/call-llm-text
                         explain-anomaly-system-prompt
                         (json/generate-string context {:pretty true})
                         :max-tokens 800)
                parsed (try (json/parse-string raw true)
                            (catch Exception _ {:summary raw}))
                result (normalize-ai-envelope parsed task "deterministic_plus_ai")]
            (cache-put! task input-hash result))
          (catch Exception e
            (log/warn "LLM unavailable for explain-anomaly, using deterministic fallback"
                      {:error (.getMessage e)})
            (let [run-ct   (count (or run_history []))
                  drift-ct (count (or drift_events []))
                  recs     (cond-> []
                             (pos? drift-ct)
                             (conj (str drift-ct " recent drift event(s) — check for schema changes"))
                             (pos? run-ct)
                             (conj (str "Review " run-ct " recent run(s) for row count or duration changes"))
                             kpi_delta
                             (conj "KPI delta detected — compare with previous period"))
                  summary  (str "Anomaly analysis for proposal " proposal_id
                                ": " (count recs) " potential factor(s) identified.")]
              (cache-put! task input-hash
                          (deterministic-envelope summary task
                                                  {:recommendations recs})))))))))
