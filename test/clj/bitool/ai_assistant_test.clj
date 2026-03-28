(ns bitool.ai-assistant-test
  (:require [clojure.test :refer :all]
            [bitool.ai.assistant :as assistant]
            [bitool.ai.llm :as llm]
            [bitool.ingest.grain-planner :as grain]
            [bitool.ops.schema-drift :as drift]
            [cheshire.core :as json]))

(use-fixtures :each (fn [f] (assistant/clear-cache!) (f)))

;; ---------------------------------------------------------------------------
;; Envelope normalization
;; ---------------------------------------------------------------------------

(deftest normalize-ai-envelope-fills-defaults
  (testing "missing keys get safe defaults"
    (let [result (assistant/normalize-ai-envelope {} "test_task" "test_source")]
      (is (= "" (:summary result)))
      (is (= 0.0 (:confidence result)))
      (is (= [] (:recommendations result)))
      (is (= {} (:edits result)))
      (is (= [] (:open_questions result)))
      (is (= [] (:warnings result)))
      (is (= "test_task" (get-in result [:debug :task])))
      (is (= "test_source" (get-in result [:debug :source]))))))

(deftest normalize-ai-envelope-preserves-values
  (testing "existing values are preserved"
    (let [input  {:summary "found 3 fields"
                  :confidence 0.85
                  :recommendations ["use id as PK"]
                  :edits {:pk ["id"]}
                  :open_questions ["is updated_at reliable?"]
                  :warnings ["low coverage on field X"]}
          result (assistant/normalize-ai-envelope input "task" "ai")]
      (is (= "found 3 fields" (:summary result)))
      (is (= 0.85 (:confidence result)))
      (is (= ["use id as PK"] (:recommendations result)))
      (is (= {:pk ["id"]} (:edits result)))
      (is (= ["is updated_at reliable?"] (:open_questions result)))
      (is (= ["low coverage on field X"] (:warnings result))))))

(deftest normalize-ai-envelope-passes-through-extra-keys
  (testing "task-specific keys like pk_reasoning survive the envelope"
    (let [input  {:summary "test"
                  :pk_reasoning "id is unique"
                  :record_grain "$.data[]"
                  :likely_causes ["missing PK"]}
          result (assistant/normalize-ai-envelope input "task" "ai")]
      (is (= "id is unique" (:pk_reasoning result)))
      (is (= "$.data[]" (:record_grain result)))
      (is (= ["missing PK"] (:likely_causes result)))))
  (testing "P2 task-specific keys survive: unmapped_requirements, impact_assessment, suggested_action, severity"
    (let [input  {:summary "drift found"
                  :confidence 0.9
                  :unmapped_requirements ["location aggregation"]
                  :impact_assessment "Silver models need updating"
                  :suggested_action "investigate"
                  :severity "warning"
                  :grain_label "one row per vehicle"
                  :ddl_available true}
          result (assistant/normalize-ai-envelope input "task" "ai")]
      (is (= ["location aggregation"] (:unmapped_requirements result)))
      (is (= "Silver models need updating" (:impact_assessment result)))
      (is (= "investigate" (:suggested_action result)))
      (is (= "warning" (:severity result)))
      (is (= "one row per vehicle" (:grain_label result)))
      (is (= true (:ddl_available result))))))

(deftest normalize-ai-envelope-normalizes-confidence-scale
  (testing "confidence > 1 is treated as 0-100 scale and divided by 100"
    (let [result (assistant/normalize-ai-envelope {:confidence 92} "task" "ai")]
      (is (= 0.92 (:confidence result)))))
  (testing "confidence <= 1 is preserved as-is"
    (let [result (assistant/normalize-ai-envelope {:confidence 0.85} "task" "ai")]
      (is (= 0.85 (:confidence result))))))

;; ---------------------------------------------------------------------------
;; P1-A: Explain Preview — deterministic fallback
;; ---------------------------------------------------------------------------

(def sample-inferred-fields
  [{:column_name "id" :path "$.data[].id" :type "STRING" :sample_coverage 1.0 :nullable false :enabled true}
   {:column_name "name" :path "$.data[].name" :type "STRING" :sample_coverage 0.95 :nullable true :enabled true}
   {:column_name "updated_at" :path "$.data[].updated_at" :type "TIMESTAMP" :sample_coverage 0.98 :nullable false :enabled true}])

(def sample-endpoint-config
  {:endpoint_name "vehicles"
   :endpoint_url "/fleet/vehicles"
   :load_type "incremental"
   :json_explode_rules [{:path "data"}]})

(deftest explain-preview-schema-deterministic-fallback
  (testing "returns deterministic result when LLM is unavailable"
    (with-redefs [llm/call-llm-text (fn [& _] (throw (ex-info "LLM down" {})))]
      (let [result (assistant/explain-preview-schema!
                    {:endpoint_config sample-endpoint-config
                     :inferred_fields sample-inferred-fields})]
        (is (string? (:summary result)))
        (is (pos? (count (:summary result))))
        (is (= "deterministic_only" (get-in result [:debug :source])))
        (is (some #(re-find #"AI explanation was unavailable" %) (:warnings result)))))))

(deftest explain-preview-schema-with-llm
  (testing "returns AI-enhanced result with task-specific fields preserved"
    (let [ai-response (json/generate-string
                       {:summary "The endpoint returns vehicle records"
                        :record_grain "$.data[]"
                        :pk_reasoning "id is the only unique identifier"
                        :watermark_reasoning "updated_at is a timestamp with 98% coverage"
                        :explode_reasoning "data array contains the record list"
                        :field_notes [{:field "id" :note "stable identifier"}]
                        :open_questions []})]
      (with-redefs [llm/call-llm-text (fn [& _] ai-response)]
        (let [result (assistant/explain-preview-schema!
                      {:endpoint_config sample-endpoint-config
                       :inferred_fields sample-inferred-fields})]
          (is (= "deterministic_plus_ai" (get-in result [:debug :source])))
          (is (= "The endpoint returns vehicle records" (:summary result)))
          ;; Task-specific keys should survive the envelope
          (is (= "$.data[]" (:record_grain result)))
          (is (= "id is the only unique identifier" (:pk_reasoning result)))
          (is (= "updated_at is a timestamp with 98% coverage" (:watermark_reasoning result)))
          (is (= "data array contains the record list" (:explode_reasoning result)))
          (is (= [{:field "id" :note "stable identifier"}] (:field_notes result))))))))

;; ---------------------------------------------------------------------------
;; P1-B: Suggest Bronze Keys — deterministic fallback
;; ---------------------------------------------------------------------------

(deftest suggest-bronze-keys-deterministic-fallback
  (testing "returns grain planner result when LLM is unavailable"
    (with-redefs [llm/call-llm-text (fn [& _] (throw (ex-info "LLM down" {})))]
      (let [result (assistant/suggest-bronze-keys!
                    {:endpoint_config sample-endpoint-config
                     :inferred_fields sample-inferred-fields})]
        (is (string? (:summary result)))
        (is (= "deterministic_only" (get-in result [:debug :source])))
        ;; Should still contain grain planner output
        (is (some #(re-find #"AI explanation was unavailable" %) (:warnings result)))))))

(deftest suggest-bronze-keys-with-llm
  (testing "returns AI-enhanced result with confidence normalized and extra keys preserved"
    (let [ai-response (json/generate-string
                       {:primary_key_fields ["id"]
                        :watermark_column "updated_at"
                        :grain_label "one row per vehicle per API call"
                        :confidence 92
                        :alternatives []
                        :summary "id is the unique identifier with 100% coverage"})]
      (with-redefs [llm/call-llm-text (fn [& _] ai-response)]
        (let [result (assistant/suggest-bronze-keys!
                      {:endpoint_config sample-endpoint-config
                       :inferred_fields sample-inferred-fields})]
          (is (= "deterministic_plus_ai" (get-in result [:debug :source])))
          (is (= "id is the unique identifier with 100% coverage" (:summary result)))
          ;; Confidence 92 (0-100 scale) should normalize to 0.92
          (is (= 0.92 (:confidence result)))
          ;; Task-specific keys preserved
          (is (= ["id"] (:primary_key_fields result)))
          (is (= "updated_at" (:watermark_column result)))
          (is (= "one row per vehicle per API call" (:grain_label result))))))))

;; ---------------------------------------------------------------------------
;; P1-C: Explain Proposal — deterministic fallback
;; ---------------------------------------------------------------------------

(deftest explain-proposal-deterministic-fallback
  (testing "returns deterministic summary when LLM is unavailable"
    (with-redefs [llm/call-llm-text (fn [& _] (throw (ex-info "LLM down" {})))]
      (let [result (assistant/explain-model-proposal!
                    {:proposal_id "prop-001"
                     :proposal_json (json/generate-string
                                    {:columns [{:name "id"} {:name "status"}]
                                     :materialization "streaming_table"
                                     :merge_keys ["id"]})})]
        (is (string? (:summary result)))
        (is (re-find #"prop-001" (:summary result)))
        (is (= "deterministic_only" (get-in result [:debug :source])))))))

;; ---------------------------------------------------------------------------
;; P1-D: Explain Validation Failure — deterministic fallback
;; ---------------------------------------------------------------------------

(deftest explain-validation-deterministic-fallback
  (testing "returns translated check results when LLM is unavailable"
    (with-redefs [llm/call-llm-text (fn [& _] (throw (ex-info "LLM down" {})))]
      (let [validation {:checks [{:status "FAIL" :name "pk_not_null" :message "Primary key column id has null values"}
                                  {:status "PASS" :name "col_types" :message "All column types valid"}
                                  {:status "WARN" :name "coverage" :message "Field x has only 40% coverage"}]}
            result (assistant/explain-proposal-validation!
                    {:proposal_id "prop-002"
                     :proposal_json "{}"
                     :validation_result validation})]
        (is (string? (:summary result)))
        (is (re-find #"1 check\(s\) failed" (:summary result)))
        (is (= "deterministic_only" (get-in result [:debug :source])))
        (is (= ["Primary key column id has null values"] (:likely_causes result)))
        ;; Warnings now include both the base "unavailable" message and passed warnings
        (is (some #(re-find #"AI explanation was unavailable" %) (:warnings result)))
        (is (some #(= "Field x has only 40% coverage" %) (:warnings result)))))))

;; ---------------------------------------------------------------------------
;; Caching behavior
;; ---------------------------------------------------------------------------

(deftest caching-prevents-duplicate-llm-calls
  (testing "second call with same input uses cache"
    (let [call-count (atom 0)
          ai-response (json/generate-string {:summary "cached" :confidence 0.9})]
      (with-redefs [llm/call-llm-text (fn [& _]
                                         (swap! call-count inc)
                                         ai-response)]
        (let [params {:endpoint_config sample-endpoint-config
                      :inferred_fields sample-inferred-fields}
              r1     (assistant/explain-preview-schema! params)
              r2     (assistant/explain-preview-schema! params)]
          (is (= 1 @call-count))
          (is (= "cached" (get-in r2 [:debug :source]))))))))

;; ---------------------------------------------------------------------------
;; P2-A: Suggest Silver Transforms — deterministic fallback
;; ---------------------------------------------------------------------------

(def sample-proposal
  {:columns [{:target_column "id" :source_columns ["id"] :data_type "STRING"}
             {:target_column "name" :source_columns ["name"] :data_type "STRING"}
             {:target_column "updated_at" :source_columns ["updated_at"] :data_type "TIMESTAMP"}]
   :materialization {:mode "streaming_table" :keys ["id"]}
   :column_mappings [{:source_field "id" :target_column "id"}
                     {:source_field "name" :target_column "name"}]})

(deftest suggest-silver-transforms-deterministic-fallback
  (testing "returns deterministic result when LLM is unavailable"
    (with-redefs [llm/call-llm-text (fn [& _] (throw (ex-info "LLM down" {})))]
      (let [result (assistant/suggest-silver-transforms!
                    {:proposal_id 1
                     :proposal sample-proposal})]
        (is (string? (:summary result)))
        (is (re-find #"3 columns" (:summary result)))
        (is (= "deterministic_only" (get-in result [:debug :source])))))))

(deftest suggest-silver-transforms-with-llm
  (testing "returns AI-enhanced result with edits categories"
    (let [ai-response (json/generate-string
                       {:summary "Consider type casting updated_at"
                        :confidence 0.8
                        :edits {:type_casts [{:description "Cast updated_at" :target_column "updated_at" :expression "CAST(updated_at AS TIMESTAMP)"}]
                                :derived_columns [{:description "Add date part" :target_column "updated_date" :expression "DATE(updated_at)"}]}})]
      (with-redefs [llm/call-llm-text (fn [& _] ai-response)]
        (let [result (assistant/suggest-silver-transforms!
                      {:proposal_id 1
                       :proposal sample-proposal})]
          (is (= "deterministic_plus_ai" (get-in result [:debug :source])))
          (is (= "Consider type casting updated_at" (:summary result)))
          (is (= 1 (count (get-in result [:edits :type_casts]))))
          (is (= 1 (count (get-in result [:edits :derived_columns])))))))))

;; ---------------------------------------------------------------------------
;; P2-B: Generate Silver Proposal from BRD — deterministic fallback
;; ---------------------------------------------------------------------------

(deftest generate-silver-from-brd-deterministic-fallback
  (testing "returns deterministic result when LLM is unavailable"
    (with-redefs [llm/call-llm-text (fn [& _] (throw (ex-info "LLM down" {})))]
      (let [result (assistant/generate-silver-proposal-from-brd!
                    {:brd_text "Need vehicle tracking with id, name, location"
                     :source_columns [{:column_name "id"} {:column_name "name"} {:column_name "lat"} {:column_name "lng"}]
                     :endpoint_config {:endpoint_name "vehicles"}})]
        (is (string? (:summary result)))
        (is (re-find #"BRD received" (:summary result)))
        (is (re-find #"4 Bronze columns" (:summary result)))
        (is (= "deterministic_only" (get-in result [:debug :source])))))))

(deftest generate-silver-from-brd-with-llm
  (testing "returns AI-generated proposal from BRD"
    (let [ai-response (json/generate-string
                       {:summary "Mapped 3 of 4 BRD requirements"
                        :confidence 0.75
                        :edits {:target_columns [{:target_column "vehicle_id" :source_column "id" :data_type "STRING"}]
                                :materialization "streaming_table"
                                :merge_keys ["vehicle_id"]}
                        :unmapped_requirements ["location aggregation"]})]
      (with-redefs [llm/call-llm-text (fn [& _] ai-response)]
        (let [result (assistant/generate-silver-proposal-from-brd!
                      {:brd_text "Need vehicle tracking"
                       :source_columns [{:column_name "id"}]
                       :endpoint_config {:endpoint_name "vehicles"}})]
          (is (= "deterministic_plus_ai" (get-in result [:debug :source])))
          (is (= ["location aggregation"] (:unmapped_requirements result)))
          (is (= 0.75 (:confidence result))))))))

;; ---------------------------------------------------------------------------
;; P2-C: Generate Gold Proposal from BRD — deterministic fallback
;; ---------------------------------------------------------------------------

(deftest generate-gold-from-brd-deterministic-fallback
  (testing "returns deterministic result when LLM is unavailable"
    (with-redefs [llm/call-llm-text (fn [& _] (throw (ex-info "LLM down" {})))]
      (let [result (assistant/generate-gold-proposal-from-brd!
                    {:brd_text "Need daily vehicle count by status"
                     :source_columns [{:column_name "id"} {:column_name "status"}]
                     :silver_proposal_id 10})]
        (is (string? (:summary result)))
        (is (re-find #"BRD received" (:summary result)))
        (is (= "deterministic_only" (get-in result [:debug :source])))))))

;; ---------------------------------------------------------------------------
;; P2-D: Suggest Gold Mart Design — deterministic fallback
;; ---------------------------------------------------------------------------

(deftest suggest-gold-mart-deterministic-fallback
  (testing "returns deterministic result when LLM is unavailable"
    (with-redefs [llm/call-llm-text (fn [& _] (throw (ex-info "LLM down" {})))]
      (let [result (assistant/suggest-gold-mart-design!
                    {:proposal_id 5
                     :proposal {:columns [{:target_column "status"} {:target_column "count"}]
                                :materialization "table"}
                     :source_table "silver.vehicles"})]
        (is (string? (:summary result)))
        (is (re-find #"2 columns" (:summary result)))
        (is (= "deterministic_only" (get-in result [:debug :source])))))))

;; ---------------------------------------------------------------------------
;; P2-E: Explain Schema Drift — deterministic fallback
;; ---------------------------------------------------------------------------

(def sample-drift-event
  {:event_id 42
   :endpoint_name "vehicles"
   :graph_id 1
   :api_node_id 2
   :drift_json (json/generate-string
                {:new_fields [{:column_name "gps_accuracy" :type "DOUBLE"}]
                 :missing_fields []
                 :type_changes [{:column_name "speed" :current_type "STRING" :inferred_type "DOUBLE"}]})
   :detected_at_utc "2026-03-28T10:00:00Z"})

(deftest explain-schema-drift-deterministic-fallback
  (testing "returns deterministic result when LLM is unavailable"
    (with-redefs [llm/call-llm-text (fn [& _] (throw (ex-info "LLM down" {})))
                  drift/get-drift-event (fn [_ _] sample-drift-event)]
      (let [result (assistant/explain-schema-drift!
                    {:event_id 42 :workspace_key "ws1"})]
        (is (string? (:summary result)))
        (is (re-find #"1 new field" (:summary result)))
        (is (re-find #"1 type change" (:summary result)))
        (is (= "deterministic_only" (get-in result [:debug :source])))
        (is (some? (:severity result)))))))

(deftest explain-schema-drift-with-llm
  (testing "returns AI-enhanced drift explanation"
    (let [ai-response (json/generate-string
                       {:summary "API added gps_accuracy and changed speed type"
                        :confidence 0.9
                        :likely_causes ["API version upgrade"]
                        :impact_assessment "Silver models using speed column will need type update"
                        :suggested_action "investigate"})]
      (with-redefs [llm/call-llm-text (fn [& _] ai-response)
                    drift/get-drift-event (fn [_ _] sample-drift-event)]
        (let [result (assistant/explain-schema-drift!
                      {:event_id 42 :workspace_key "ws1"})]
          (is (= "deterministic_plus_ai" (get-in result [:debug :source])))
          (is (= "investigate" (:suggested_action result)))
          (is (= ["API version upgrade"] (:likely_causes result)))
          (is (some? (:severity result))))))))

;; ---------------------------------------------------------------------------
;; P2-F: Suggest Drift Remediation — deterministic fallback
;; ---------------------------------------------------------------------------

(deftest explain-schema-drift-throws-404-when-event-not-found
  (testing "throws 404 when drift event does not exist"
    (with-redefs [drift/get-drift-event (fn [_ _] nil)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"not found"
            (assistant/explain-schema-drift! {:event_id 999 :workspace_key "ws1"}))))))

(deftest suggest-drift-remediation-throws-404-when-event-not-found
  (testing "throws 404 when drift event does not exist"
    (with-redefs [drift/get-drift-event (fn [_ _] nil)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"not found"
            (assistant/suggest-drift-remediation! {:event_id 999 :workspace_key "ws1"}))))))

(deftest suggest-drift-remediation-deterministic-fallback
  (testing "returns deterministic recommendations when LLM is unavailable"
    (with-redefs [llm/call-llm-text (fn [& _] (throw (ex-info "LLM down" {})))
                  drift/get-drift-event (fn [_ _] sample-drift-event)
                  drift/preview-schema-ddl (fn [_] nil)]
      (let [result (assistant/suggest-drift-remediation!
                    {:event_id 42 :workspace_key "ws1"})]
        (is (string? (:summary result)))
        (is (= "deterministic_only" (get-in result [:debug :source])))
        (is (seq (:recommendations result)))
        (is (some #(re-find #"new field" %) (:recommendations result)))
        (is (some #(re-find #"type change" %) (:recommendations result)))))))

;; ---------------------------------------------------------------------------
;; P3-A: Explain Endpoint Business Shape — deterministic fallback
;; (reuses sample-endpoint-config and sample-inferred-fields from P1 tests)
;; ---------------------------------------------------------------------------

(deftest explain-endpoint-business-shape-deterministic-fallback
  (testing "returns deterministic entity classification when LLM is unavailable"
    (with-redefs [llm/call-llm-text (fn [& _] (throw (ex-info "LLM down" {})))]
      (let [result (assistant/explain-endpoint-business-shape!
                    {:endpoint_config sample-endpoint-config
                     :inferred_fields sample-inferred-fields})]
        (is (string? (:summary result)))
        (is (= "deterministic_only" (get-in result [:debug :source])))
        (is (some? (:likely_entity_type result)))
        (is (some? (:confidence result)))))))

(deftest explain-endpoint-business-shape-with-llm
  (testing "returns AI-enhanced entity classification"
    (let [ai-response (json/generate-string
                       {:summary "This endpoint delivers real-time vehicle GPS pings"
                        :confidence 0.92
                        :entity_type "event"
                        :grain_hint "one row per vehicle per GPS reading"
                        :recommendations ["Consider partitioning by date"]})]
      (with-redefs [llm/call-llm-text (fn [& _] ai-response)]
        (let [result (assistant/explain-endpoint-business-shape!
                      {:endpoint_config sample-endpoint-config
                       :inferred_fields sample-inferred-fields})]
          (is (= "deterministic_plus_ai" (get-in result [:debug :source])))
          (is (= "event" (:entity_type result)))
          (is (some? (:grain_hint result))))))))

;; ---------------------------------------------------------------------------
;; P3-B: Explain Target Strategy — deterministic fallback
;; ---------------------------------------------------------------------------

(def sample-target-config
  {:target_kind       "databricks"
   :write_mode        "merge"
   :table_format      "delta"
   :merge_keys        ["id"]
   :partition_columns ["dt"]
   :cluster_by        []})

(deftest explain-target-strategy-deterministic-fallback
  (testing "returns deterministic strategy explanation when LLM is unavailable"
    (with-redefs [llm/call-llm-text (fn [& _] (throw (ex-info "LLM down" {})))]
      (let [result (assistant/explain-target-strategy!
                    {:target_config sample-target-config})]
        (is (string? (:summary result)))
        (is (= "deterministic_only" (get-in result [:debug :source])))
        (is (seq (:recommendations result)))
        (is (some? (:confidence result)))))))

(deftest explain-target-strategy-with-llm
  (testing "returns AI-enhanced strategy explanation"
    (let [ai-response (json/generate-string
                       {:summary "Merge mode with delta format is optimal for this SCD pattern"
                        :confidence 0.88
                        :write_mode_notes "MERGE ensures idempotent upserts"
                        :tradeoffs ["Higher compute cost vs append"
                                    "Enables point lookups"]
                        :recommendations ["Add cluster_by for frequently filtered columns"]})]
      (with-redefs [llm/call-llm-text (fn [& _] ai-response)]
        (let [result (assistant/explain-target-strategy!
                      {:target_config sample-target-config})]
          (is (= "deterministic_plus_ai" (get-in result [:debug :source])))
          (is (some? (:tradeoffs result))))))))

;; ---------------------------------------------------------------------------
;; P3-C: Generate Metric Glossary — deterministic fallback
;; ---------------------------------------------------------------------------

(def sample-gold-proposal
  {:columns [{:column_name "total_trips" :data_type "integer" :expression "count(trip_id)"}
             {:column_name "avg_speed" :data_type "double" :expression "avg(speed)"}
             {:column_name "vehicle_id" :data_type "string"}]
   :merge_keys ["vehicle_id"]})

(deftest generate-metric-glossary-deterministic-fallback
  (testing "returns deterministic summary when LLM is unavailable"
    (with-redefs [llm/call-llm-text (fn [& _] (throw (ex-info "LLM down" {})))]
      (let [result (assistant/generate-metric-glossary!
                    {:proposal_id 100
                     :proposal    sample-gold-proposal})]
        (is (string? (:summary result)))
        (is (re-find #"3 columns" (:summary result)))
        (is (= "deterministic_only" (get-in result [:debug :source])))
        (is (some? (:confidence result)))))))

(deftest generate-metric-glossary-with-llm
  (testing "returns AI-enhanced metric glossary"
    (let [ai-response (json/generate-string
                       {:summary "Generated definitions for 2 metrics"
                        :confidence 0.85
                        :glossary [{:metric "total_trips" :definition "Count of unique trips per vehicle"}
                                   {:metric "avg_speed" :definition "Average speed across all readings"}]
                        :recommendations ["Consider adding median_speed for outlier resistance"]})]
      (with-redefs [llm/call-llm-text (fn [& _] ai-response)]
        (let [result (assistant/generate-metric-glossary!
                      {:proposal_id 100
                       :proposal    sample-gold-proposal})]
          (is (= "deterministic_plus_ai" (get-in result [:debug :source])))
          (is (= 2 (count (:glossary result)))))))))

;; ---------------------------------------------------------------------------
;; P3-D: Explain Run/KPI Anomaly — deterministic fallback
;; ---------------------------------------------------------------------------

(deftest explain-run-or-kpi-anomaly-deterministic-fallback
  (testing "returns deterministic anomaly analysis when LLM is unavailable"
    (with-redefs [llm/call-llm-text (fn [& _] (throw (ex-info "LLM down" {})))]
      (let [result (assistant/explain-run-or-kpi-anomaly!
                    {:proposal_id 100
                     :run_history [{:status "success" :row_count 1000 :started_at "2026-03-27T10:00:00Z"}
                                   {:status "success" :row_count 500  :started_at "2026-03-26T10:00:00Z"}
                                   {:status "failed"  :row_count 0    :started_at "2026-03-25T10:00:00Z"}]
                     :validation_history [{:status "passed"} {:status "failed"}]
                     :drift_events [{:event_type "new_field"}]
                     :kpi_delta {:metric "row_count" :previous 500 :current 1000 :pct_change 100.0}})]
        (is (string? (:summary result)))
        (is (= "deterministic_only" (get-in result [:debug :source])))
        (is (some? (:confidence result)))))))

(deftest explain-run-or-kpi-anomaly-with-llm
  (testing "returns AI-enhanced anomaly explanation"
    (let [ai-response (json/generate-string
                       {:summary "Row count doubled due to upstream schema change adding new records"
                        :confidence 0.82
                        :likely_causes ["Schema drift event added new field, broadening ingestion"]
                        :recommendations ["Verify the additional rows are valid, not duplicates"]})]
      (with-redefs [llm/call-llm-text (fn [& _] ai-response)]
        (let [result (assistant/explain-run-or-kpi-anomaly!
                      {:proposal_id 100
                       :run_history [{:status "success" :row_count 1000}]
                       :kpi_delta {:metric "row_count" :previous 500 :current 1000 :pct_change 100.0}})]
          (is (= "deterministic_plus_ai" (get-in result [:debug :source])))
          (is (seq (:likely_causes result))))))))
