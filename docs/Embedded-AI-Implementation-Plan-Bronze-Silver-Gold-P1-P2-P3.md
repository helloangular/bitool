# Embedded AI Implementation Plan for Bronze, Silver, and Gold

## 1. Purpose

This document converts the embedded AI backlog into an engineering implementation plan for:

- `P1`
- `P2`
- `P3`

For each priority band, this document defines:

- frontend changes by file,
- backend routes and service functions,
- AI context and prompt shape,
- test plan,
- rollout sequence.

This plan assumes:

- Bronze remains centered in [apiComponent.js](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js),
- Silver and Gold remain centered in [modelingConsole.js](/Users/aaryakulkarni/bitool/resources/public/modelingConsole.js),
- AI is embedded into existing product flows rather than introduced as a separate UI shell.

---

## 2. Shared Architecture

### 2.1 Frontend pattern

Each AI action should follow the same UI pattern:

1. user clicks a targeted action button,
2. frontend sends current structured context to a dedicated backend route,
3. backend assembles deterministic context from graph, proposal, validation, schema, and compile data,
4. backend calls a shared AI orchestration helper,
5. backend returns structured JSON,
6. frontend renders:
   - short summary,
   - confidence,
   - recommendations,
   - editable proposed changes if any.

Use one shared inline UI pattern across all embedded AI actions:

- `ai-assist-trigger`
- `ai-assist-card`

Recommended `ai-assist-card` behavior:

- collapsed by default,
- summary + confidence always visible,
- expandable sections for:
  - recommendations
  - edits
  - open questions
  - warnings
- optional `Apply selected`
- optional `Undo`

This keeps Bronze, Silver, Gold, and target AI actions visually consistent and avoids button sprawl with unrelated interaction models.

Contextual visibility rules:

- only render an AI trigger when the local workflow state has enough context,
- avoid showing disabled AI controls with no usable data.

Examples:

- `Explain with AI` in Bronze
  - only after preview/recommendation data exists
- `Refine recommendation`
  - only when the recommendation card exists and ambiguity is meaningful
- `Explain Validation Failure`
  - only when validation has warned or failed
- BRD generation actions
  - only when there is a current proposal context to refine

### 2.2 Backend pattern

Add a small AI service layer rather than embedding prompt assembly directly in routes.

Recommended namespace:

- [src/clj/bitool/ai/assistant.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ai/assistant.clj)

LLM provider reuse:

- do not create a second provider integration path,
- reuse the Anthropic-first plus OpenAI fallback pattern that already exists in [src/clj/bitool/pipeline/intent.clj:277](/Users/aaryakulkarni/bitool/src/clj/bitool/pipeline/intent.clj#L277) through [src/clj/bitool/pipeline/intent.clj:336](/Users/aaryakulkarni/bitool/src/clj/bitool/pipeline/intent.clj#L336),
- extract the generic provider call and retry logic into a shared namespace:
  - [src/clj/bitool/ai/llm.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ai/llm.clj)
- then have `assistant.clj` depend on that shared namespace rather than on a duplicate implementation.

Recommended responsibilities:

- normalize request context,
- load graph/proposal/schema/validation records,
- compute deterministic summaries,
- build task-specific AI prompts,
- validate and normalize AI output,
- return structured JSON.

### 2.3 Route pattern

Add dedicated routes in [src/clj/bitool/routes/home.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/routes/home.clj) instead of one generic “AI” endpoint.

Reason:

- clearer authorization,
- easier caching and audit,
- simpler frontend wiring,
- task-specific testing.

### 2.3.1 Step 0: Create new namespaces

Before feature work starts, explicitly create:

- [src/clj/bitool/ai/llm.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ai/llm.clj)
- [src/clj/bitool/ai/assistant.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ai/assistant.clj)
- [test/clj/bitool/ai_llm_test.clj](/Users/aaryakulkarni/bitool/test/clj/bitool/ai_llm_test.clj)
- [test/clj/bitool/ai_assistant_test.clj](/Users/aaryakulkarni/bitool/test/clj/bitool/ai_assistant_test.clj)

These files do not exist yet and should be treated as creation tasks, not as if they already exist.

Extraction scope note:

- `ai/llm.clj` should extract both the retry path (`call-llm` + `translate-with-retry` pattern) and the single-shot call path used by `edit-intent` in [src/clj/bitool/pipeline/intent.clj:466](/Users/aaryakulkarni/bitool/src/clj/bitool/pipeline/intent.clj#L466),
- after extraction, `pipeline.intent` should also be refactored to depend on `ai.llm` rather than retaining private copies of the provider call functions.

### 2.4 Response contract

Use one shared envelope:

```json
{
  "summary": "short user-facing explanation",
  "confidence": 0.0,
  "recommendations": [],
  "edits": {},
  "open_questions": [],
  "warnings": [],
  "debug": {
    "task": "task_name",
    "source": "deterministic_plus_ai"
  }
}
```

The `debug` section is optional and should be safe for internal use only.

### 2.5 Budgets, caching, and rate control

Every AI button can trigger an LLM call, so the first implementation must include basic control measures.

Recommended defaults:

- `Explain Preview`
  - `max_tokens`: 700
- `Suggest Bronze Keys`
  - `max_tokens`: 500
- `Explain Proposal`
  - `max_tokens`: 900
- `Explain Validation Failure`
  - `max_tokens`: 700
- BRD generation tasks
  - `max_tokens`: 1200 to 1800 depending on context size

Required controls:

- frontend cooldown on repeated clicks for the same button/context,
- backend request fingerprinting and short-lived cache,
- cache key based on stable input hash,
- explicit timeout and provider failure handling,
- operator-visible message when result is cached versus newly generated.

Recommended cache scope:

- proposal-based explanations: cache by `proposal_id + proposal_checksum + task`
- preview-based explanations: cache by `graph_id + api_node_id + endpoint_name + inferred_fields_hash + task`
- validation explanations: cache by `proposal_id + validation_result_hash + task`

### 2.6 Deterministic fallback behavior

If the LLM is unavailable, each task should degrade gracefully.

Required fallback contract:

- never show a blank panel,
- return deterministic-only output when possible,
- clearly label that AI explanation was unavailable.

Fallback examples:

- `Suggest Bronze Keys`
  - run [src/clj/bitool/ingest/grain_planner.clj:204](/Users/aaryakulkarni/bitool/src/clj/bitool/ingest/grain_planner.clj#L204) first and return its output even if no LLM call succeeds
- `Explain Preview`
  - return deterministic summary from record path, inferred field counts, timestamp candidates, and planner output
- `Explain Proposal`
  - return deterministic summary from proposal fields, materialization mode, and compile metadata
- `Explain Validation Failure`
  - translate validation checks deterministically if the LLM is unavailable

### 2.7 Prompt versioning and evaluation

Prompt content should not live only as inline strings inside service functions.

Recommended approach:

- store prompts as versioned EDN or template resources under:
  - [resources/ai-prompts/](/Users/aaryakulkarni/bitool/resources/ai-prompts)
- include:
  - task name
  - prompt version
  - system prompt template
  - output contract notes

Recommended eval approach:

- add canned task fixtures under:
  - [test/fixtures/ai-responses/](/Users/aaryakulkarni/bitool/test/fixtures/ai-responses)
  - [test/fixtures/ai-contexts/](/Users/aaryakulkarni/bitool/test/fixtures/ai-contexts)
- run shape validation and manual spot-checks against representative Bronze, Silver, and Gold cases,
- track prompt version in the response debug envelope for regression debugging.

---

## 3. P1 Implementation Plan

## 3.1 Scope

`P1` includes:

- `Explain Preview`
- `Suggest Bronze Keys`
- `Explain Proposal`
- `Explain Validation Failure`

These are the highest-value additions because they improve user trust in workflows that already exist.

---

## 3.2 P1 Frontend changes

### 3.2.1 `resources/public/apiComponent.js`

Enhance the existing recommendation surface rather than introducing a second recommendation widget.

Existing recommendation surface:

- recommendation computation in [resources/public/apiComponent.js:1297](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L1297)
- rendered recommendation card in [resources/public/apiComponent.js:1384](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L1384)
- existing deterministic `Show reasoning` in [resources/public/apiComponent.js:1415](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L1415)

Add inline AI actions inside that card:

- `Explain with AI`
- `Refine recommendation` for ambiguous cases

Add UI elements:

- inline `ai-assist-card` attached to the existing recommendation card,
- status/loading state for AI request,
- optional “Apply suggestion” action for PK/watermark recommendations.

Suggested implementation points:

- inside or immediately below the schema recommendation card in [apiComponent.js:1200](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L1200)
- reuse existing endpoint tab rendering and status banner in [apiComponent.js:1816](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L1816)

State additions per endpoint config:

- `ai_preview_explanation`
- `ai_bronze_key_suggestion`
- `ai_loading_state`

### 3.2.2 `resources/public/modelingConsole.js`

Add buttons:

- `Explain Proposal`
- `Explain Validation Failure`

Add UI elements:

- proposal explanation panel,
- validation explanation panel attached to the current validation results area,
- “Apply suggested edits” action only when the backend returns structured edits.

Suggested placement:

- proposal detail header near compile/validate controls,
- validation results section for failure explanation.

Existing deterministic validation surface to preserve:

- validation rendering in [resources/public/modelingConsole.js:2103](/Users/aaryakulkarni/bitool/resources/public/modelingConsole.js#L2103) through [resources/public/modelingConsole.js:2154](/Users/aaryakulkarni/bitool/resources/public/modelingConsole.js#L2154)

State additions:

- `currentAiProposalExplanation`
- `currentAiValidationExplanation`
- `aiLoadingState`

---

## 3.3 P1 Backend changes

### 3.3.1 New routes in `src/clj/bitool/routes/home.clj`

Add:

- `POST /aiExplainPreviewSchema`
- `POST /aiSuggestBronzeKeys`
- `POST /aiExplainModelProposal`
- `POST /aiExplainProposalValidation`

Each route should:

- call `ensure-authorized!` with the same domain used by the base screen,
- validate required params,
- call the AI service layer,
- return normalized JSON,
- preserve existing error handling style.

### 3.3.2 New service namespace

Add:

- [src/clj/bitool/ai/assistant.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ai/assistant.clj)
- [src/clj/bitool/ai/llm.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ai/llm.clj)

Core public functions:

- `explain-preview-schema!`
- `suggest-bronze-keys!`
- `explain-model-proposal!`
- `explain-proposal-validation!`

Helper functions:

- `preview-schema-context`
- `bronze-key-context`
- `proposal-context`
- `validation-context`
- `call-task-model!`
- `normalize-ai-envelope`

### 3.3.3 Existing backend functions to reuse

Bronze context:

- [src/clj/bitool/routes/home.clj:1086](/Users/aaryakulkarni/bitool/src/clj/bitool/routes/home.clj#L1086)
- existing schema preview output from `preview-endpoint-schema!`
- deterministic Bronze recommendation output from [src/clj/bitool/ingest/grain_planner.clj:204](/Users/aaryakulkarni/bitool/src/clj/bitool/ingest/grain_planner.clj#L204)

Silver/Gold context:

- proposal load routes and helpers in [src/clj/bitool/routes/home.clj:1109](/Users/aaryakulkarni/bitool/src/clj/bitool/routes/home.clj#L1109) through [src/clj/bitool/routes/home.clj:1551](/Users/aaryakulkarni/bitool/src/clj/bitool/routes/home.clj#L1551)
- proposal generation and compile/validate logic in [src/clj/bitool/modeling/automation.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/modeling/automation.clj)

---

## 3.4 P1 AI context and prompt shape

### 3.4.1 `Explain Preview`

Context:

- endpoint name and URL,
- load type,
- pagination settings,
- inferred fields,
- schema recommendations,
- detected record path,
- explode rules,
- sample count.

Prompt intent:

- explain the current system output,
- do not invent fields not present,
- rank likely identifier and watermark fields,
- explain uncertainty.

UI behavior:

- trigger this from the existing recommendation card,
- treat it as a deeper explanation layer on top of `Show reasoning`,
- do not render a second independent explanation mechanism for the same recommendation.

Return:

- `summary`
- `record_grain`
- `pk_reasoning`
- `watermark_reasoning`
- `explode_reasoning`
- `field_notes[]`
- `open_questions[]`

### 3.4.2 `Suggest Bronze Keys`

Context:

- inferred fields,
- path coverage,
- enabled fields,
- timestamp-like fields,
- identifier-like fields,
- endpoint config.

Prompt intent:

- recommend PK and watermark,
- prefer deterministic inference from the existing grain planner,
- list alternatives if ambiguous.

Return:

- `primary_key_fields[]`
- `watermark_column`
- `grain_label`
- `confidence`
- `alternatives[]`
- `summary`

Implementation note:

- first run [src/clj/bitool/ingest/grain_planner.clj:204](/Users/aaryakulkarni/bitool/src/clj/bitool/ingest/grain_planner.clj#L204),
- then pass the deterministic result to the LLM only for explanation, ambiguity handling, and ranking,
- if the LLM fails, return the deterministic result with a fallback explanation.

Visibility rule:

- only show this action when the existing recommendation surface is present,
- and prefer showing it when confidence is below a configured threshold or when the user explicitly asks for deeper guidance.

Frontend migration note:

- the existing recommendation card runs `_computeRecommendations()` client-side in [apiComponent.js:1297](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L1297),
- this route should also return the deterministic grain planner output in its response,
- when the AI route has been called, the frontend should prefer the backend result over the client-side heuristic,
- this does not require a full migration of `_computeRecommendations`, but the AI-enhanced path should supersede it when both are present.

### 3.4.3 `Explain Proposal`

Context:

- proposal row,
- proposal JSON,
- source endpoint / source model,
- compiled SQL if available,
- materialization mode,
- processing policy.

Prompt intent:

- explain current proposal design,
- explain column selection, grain, keys, and materialization,
- surface unresolved assumptions.

Return:

- `summary`
- `business_shape`
- `materialization_reasoning`
- `key_reasoning`
- `column_reasoning[]`
- `open_questions[]`

### 3.4.4 `Explain Validation Failure`

Context:

- proposal JSON,
- validation result payload,
- compile result if available,
- proposal layer,
- failed checks.

Prompt intent:

- translate technical validation output into operator-facing explanation,
- identify likely causes,
- suggest next edits without pretending certainty.

Return:

- `summary`
- `likely_causes[]`
- `suggested_actions[]`
- `warnings[]`

---

## 3.5 P1 Test plan

### Frontend

Files:

- [resources/public/apiComponent.js](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js)
- [resources/public/modelingConsole.js](/Users/aaryakulkarni/bitool/resources/public/modelingConsole.js)

Tests:

- button renders only in correct screen state,
- loading state appears,
- success response renders explanation card,
- error response renders safe fallback,
- apply-suggestion action updates local form state correctly.
- AI actions attach to the existing recommendation and validation surfaces rather than rendering parallel duplicate panels.

### Backend route tests

Add route tests in:

- [test/clj/bitool/routes/home_test.clj](/Users/aaryakulkarni/bitool/test/clj/bitool/routes/home_test.clj)

Cases:

- valid request returns normalized JSON,
- missing params return bad request,
- auth failures return forbidden,
- backend AI service exceptions are handled cleanly.

### Service tests

Add tests in:

- [test/clj/bitool/ai_assistant_test.clj](/Users/aaryakulkarni/bitool/test/clj/bitool/ai_assistant_test.clj)
- [test/clj/bitool/ai_llm_test.clj](/Users/aaryakulkarni/bitool/test/clj/bitool/ai_llm_test.clj)

Cases:

- deterministic context assembly is correct,
- malformed AI output is normalized or rejected,
- missing optional context still produces safe envelope,
- explanation functions preserve current proposal/schema data accurately.

Fixture strategy:

- do not call live providers in normal tests,
- use canned LLM outputs from:
  - [test/fixtures/ai-responses/](/Users/aaryakulkarni/bitool/test/fixtures/ai-responses)
- use representative task contexts from:
  - [test/fixtures/ai-contexts/](/Users/aaryakulkarni/bitool/test/fixtures/ai-contexts)

---

## 3.6 P1 rollout

Order:

1. add backend AI service skeleton and route tests,
2. add `Explain Preview`,
3. add `Suggest Bronze Keys`,
4. add `Explain Proposal`,
5. add `Explain Validation Failure`.

Reason:

- Bronze and proposal explanation are lowest-risk and high-value,
- validation explanation naturally builds on proposal context loading.

---

## 4. P2 Implementation Plan

## 4.1 Scope

`P2` includes:

- BRD intake for Silver
- BRD intake for Gold
- transformation suggestions
- Gold mart design suggestions
- schema drift explanation
- schema drift remediation suggestions

This is where the product starts becoming a true AI-assisted modeling system rather than a pure explainability layer.

Important note:

- `P2` is only safe after the structured output contract from `P1` is stable,
- especially for BRD intake, because BRD-driven edits are more invasive than explain-only features.

---

## 4.2 P2 Frontend changes

### 4.2.1 `resources/public/modelingConsole.js`

Add:

- `Business Requirement` input panel,
- `Generate From BRD`,
- `Suggest Transformations`,
- `Suggest Mart Design`.

Suggested UI pattern:

- a collapsible side panel in proposal detail,
- editable textarea for BRD,
- result cards with explicit “Apply selected changes” flow.

Boundary with existing Pipeline Chat:

- Pipeline Chat in [resources/public/pipelineChatComponent.js](/Users/aaryakulkarni/bitool/resources/public/pipelineChatComponent.js) remains the conversational flow for creating or reshaping pipelines from natural language,
- BRD intake in Modeling Console is for refining an already-existing proposal in local context,
- both should share the same LLM backend and structured output contract.

BRD constraints:

- max BRD input length should be enforced in UI and backend,
- recommended initial limit: `8k` characters,
- larger documents should require upload/attachment design later rather than being pasted directly.

State additions:

- `draftBrdText`
- `aiSilverBrdResult`
- `aiGoldBrdResult`
- `aiTransformSuggestions`
- `aiMartSuggestions`

### 4.2.2 Drift/operations surface

Depending on current ops screen choice, add:

- `Explain Drift`
- `Suggest Fix`

If there is no obvious existing drift review panel, use the Modeling Console proposal detail first and attach drift actions to proposal/schema review context.

---

## 4.3 P2 Backend changes

### 4.3.1 New routes in `src/clj/bitool/routes/home.clj`

Add:

- `POST /aiGenerateSilverProposalFromBRD`
- `POST /aiGenerateGoldProposalFromBRD`
- `POST /aiSuggestSilverTransforms`
- `POST /aiSuggestGoldMartDesign`
- `POST /aiExplainSchemaDrift`
- `POST /aiSuggestDriftRemediation`

### 4.3.2 Service functions in `src/clj/bitool/ai/assistant.clj`

Add:

- `generate-silver-proposal-from-brd!`
- `generate-gold-proposal-from-brd!`
- `suggest-silver-transforms!`
- `suggest-gold-mart-design!`
- `explain-schema-drift!`
- `suggest-drift-remediation!`

### 4.3.3 Existing backend functions to reuse

Proposal context and compilation:

- [src/clj/bitool/modeling/automation.clj:2665](/Users/aaryakulkarni/bitool/src/clj/bitool/modeling/automation.clj#L2665)
- [src/clj/bitool/modeling/automation.clj:2752](/Users/aaryakulkarni/bitool/src/clj/bitool/modeling/automation.clj#L2752)

Schema snapshots and drift metadata:

- endpoint schema snapshot and Bronze inference data already referenced by existing Bronze production docs and current runtime metadata

Dependency note:

- drift explanation and remediation features depend on usable drift review context being exposed in product code,
- if that context is not yet fully surfaced, drift AI work should be sequenced after BRD and transform suggestion work.

---

## 4.4 P2 AI context and prompt shape

### 4.4.1 `Generate Silver From BRD`

Context:

- source endpoint,
- current Silver proposal if any,
- inferred Bronze fields,
- current mappings,
- BRD text.

Prompt intent:

- map business requirement into Silver schema edits,
- propose processing policy and materialization,
- identify missing source data.

Return:

- `summary`
- `schema_changes[]`
- `mapping_updates[]`
- `processing_policy_updates`
- `materialization_updates`
- `open_questions[]`

Graph context contract:

- `graph_context` should include:
  - source node id
  - graph id
  - endpoint name
  - source table
  - current proposal columns
  - current mappings
  - current processing policy

Open question handling:

- AI must return unresolved semantic questions explicitly,
- frontend should render them separately from edits,
- unresolved questions should not be silently applied into proposal state.

Apply behavior:

- all returned edits should be cherry-pickable,
- the user should be able to apply a subset,
- one-level undo should restore the pre-apply state.

### 4.4.2 `Generate Gold From BRD`

Context:

- source Silver proposal,
- current Gold proposal if any,
- current dimensions/measures,
- BRD text.

Prompt intent:

- propose Gold marts, grain, dimensions, and measures,
- surface ambiguities in KPI semantics.

Return:

- `summary`
- `marts[]`
- `dimensions[]`
- `measures[]`
- `grain`
- `open_questions[]`

Editor mapping requirement:

- generated dimensions, measures, and grain must map back to the current Gold proposal editor data model,
- do not return freeform narrative only,
- every suggested edit must be represented as machine-applyable proposal deltas.

### 4.4.3 `Suggest Transformations`

Context:

- proposal mappings,
- source columns,
- target columns,
- failed validations if any.

Prompt intent:

- recommend low-risk transformations that improve canonical modeling quality.

Return:

- `mapping_updates[]`
- `new_columns[]`
- `warnings[]`

### 4.4.4 `Explain Drift` and `Suggest Fix`

Context:

- before and after schema snapshots,
- affected proposal ids,
- current target schema,
- current validation status.

Prompt intent:

- classify change,
- explain impact,
- recommend minimally invasive edits.

Return:

- `summary`
- `change_classification`
- `impacted_models[]`
- `edits`
- `warnings[]`

---

## 4.5 P2 Test plan

### Frontend

Add UI tests for:

- BRD panel open/close,
- BRD text submission,
- applying returned schema or mapping edits,
- drift explanation rendering.

### Backend route tests

Add route coverage for each new endpoint in:

- [test/clj/bitool/routes/home_test.clj](/Users/aaryakulkarni/bitool/test/clj/bitool/routes/home_test.clj)

### Service tests

Add tests for:

- BRD context assembly,
- proposal edit normalization,
- drift classification normalization,
- safe handling when the AI omits optional fields.

Fixture strategy:

- add representative BRD fixtures,
- add representative Silver and Gold proposal fixtures,
- add drift before/after fixtures,
- keep provider calls mocked via canned response JSON.

### Integration tests

Targeted integration tests should cover:

- Silver BRD suggestion -> apply selected edits -> compile,
- Gold BRD suggestion -> apply selected edits -> compile,
- drift remediation suggestion -> validation path.

---

## 4.6 P2 rollout

Order:

1. `Suggest Transformations`
2. `Generate Silver From BRD`
3. `Suggest Mart Design`
4. `Generate Gold From BRD`
5. `Explain Drift`
6. `Suggest Fix`

Reason:

- transformation and proposal refinement are more central than drift assistance,
- Gold BRD support should follow Silver BRD support,
- drift work benefits from the same structured AI service layer already being stable.

---

## 5. P3 Implementation Plan

## 5.1 Scope

`P3` includes:

- KPI and mart generation assistant,
- metric glossary generation,
- warehouse tradeoff explanation,
- source business shape explanation,
- KPI/run anomaly assistant.

These are high-value features, but they should come after the product is already strong at Bronze/Silver/Gold explanation and guided model generation.

---

## 5.2 P3 Frontend changes

### 5.2.1 `resources/public/apiComponent.js`

Add:

- `What Does This Endpoint Represent?`

### 5.2.2 `resources/public/targetComponent.js`

Add:

- `Explain Write Mode`

### 5.2.3 `resources/public/modelingConsole.js`

Add:

- `Generate Metric Definitions`
- `Why did this change?`

If a metric explorer or operations console becomes the better home later, these can move there. For now, attach them to existing proposal and run-review flows.

These should follow the same inline `ai-assist-card` pattern as P1 and P2 rather than appearing as a second conversational AI system.

---

## 5.3 P3 Backend changes

### 5.3.1 New routes in `src/clj/bitool/routes/home.clj`

Add:

- `POST /aiExplainEndpointBusinessShape`
- `POST /aiExplainTargetStrategy`
- `POST /aiGenerateMetricGlossary`
- `POST /aiExplainRunOrKpiAnomaly`

### 5.3.2 Service functions in `src/clj/bitool/ai/assistant.clj`

Add:

- `explain-endpoint-business-shape!`
- `explain-target-strategy!`
- `generate-metric-glossary!`
- `explain-run-or-kpi-anomaly!`

---

## 5.4 P3 AI context and prompt shape

### 5.4.1 `Explain Endpoint Business Shape`

Context:

- endpoint metadata,
- inferred fields,
- schema recommendations,
- sample payload summary.

Return:

- `business_shape`
- `likely_entity_type`
- `downstream_silver_shapes[]`
- `downstream_gold_use_cases[]`

### 5.4.2 `Explain Target Strategy`

Context:

- target config,
- warehouse kind,
- proposal materialization,
- compiled SQL if available.

Return:

- `summary`
- `tradeoffs[]`
- `cost_notes[]`
- `performance_notes[]`
- `operational_risks[]`

### 5.4.3 `Generate Metric Glossary`

Context:

- Gold proposal,
- dimensions,
- measures,
- BRD text if available.

Return:

- `metrics[]`
- `definitions[]`
- `assumptions[]`
- `caveats[]`

### 5.4.4 `Explain Run or KPI Anomaly`

Context:

- model run history,
- validation history,
- schema drift history,
- recent config/model changes,
- KPI delta summary.

Return:

- `summary`
- `likely_causes[]`
- `impacted_assets[]`
- `next_checks[]`

---

## 5.5 P3 Test plan

### Frontend

Add rendering and action tests for:

- endpoint business-shape explanation,
- target strategy explanation,
- metric glossary rendering,
- anomaly explanation card.

### Backend

Route and service tests should verify:

- correct context assembly from historical run data,
- safe degradation when full KPI history is unavailable,
- normalized glossary output shape,
- clear error handling for insufficient anomaly context.

---

## 5.6 P3 rollout

Order:

1. `Explain Endpoint Business Shape`
2. `Explain Write Mode`
3. `Generate Metric Definitions`
4. `Explain Run or KPI Anomaly`

Reason:

- the first two are relatively self-contained,
- glossary generation depends on stronger Gold context,
- anomaly explanation depends on more operational data and is easiest to get wrong if rushed.

---

## 6. File-by-File Summary

### Frontend

- [resources/public/apiComponent.js](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js)
  - P1: explain preview, suggest bronze keys
  - P2: canonical naming suggestions
  - P3: endpoint business shape explanation

- [resources/public/modelingConsole.js](/Users/aaryakulkarni/bitool/resources/public/modelingConsole.js)
  - P1: explain proposal, explain validation failure
  - P2: BRD intake, transform suggestions, mart suggestions, drift support
  - P3: metric glossary, anomaly explanation

- [resources/public/targetComponent.js](/Users/aaryakulkarni/bitool/resources/public/targetComponent.js)
  - P2: suggest target defaults
  - P3: explain write mode

### Backend

- [src/clj/bitool/routes/home.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/routes/home.clj)
  - add task-specific AI routes

- [src/clj/bitool/ai/llm.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ai/llm.clj)
  - shared provider integration extracted from `pipeline.intent`

- [src/clj/bitool/ai/assistant.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ai/assistant.clj)
  - new orchestration layer

- [src/clj/bitool/modeling/automation.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/modeling/automation.clj)
  - reused for proposal/compile/validate context

### Tests

- [test/clj/bitool/routes/home_test.clj](/Users/aaryakulkarni/bitool/test/clj/bitool/routes/home_test.clj)
  - route coverage

- [test/clj/bitool/ai_assistant_test.clj](/Users/aaryakulkarni/bitool/test/clj/bitool/ai_assistant_test.clj)
  - service coverage

- [test/clj/bitool/ai_llm_test.clj](/Users/aaryakulkarni/bitool/test/clj/bitool/ai_llm_test.clj)
  - provider fallback and normalization coverage

- frontend UI tests
  - use the repo’s current frontend test approach where applicable

---

## 7. Recommended Delivery Strategy

If the team wants one clean delivery path:

### Release 1

- ship `P1`

### Release 2

- ship `P2`

### Release 3

- ship `P3`

If the team wants faster incremental value:

### Slice A

- `Explain Preview`
- `Explain Proposal`

### Slice B

- `Suggest Bronze Keys`
- `Explain Validation Failure`

### Slice C

- `Generate From BRD` for Silver
- `Suggest Transformations`

### Slice D

- `Suggest Mart Design`
- `Generate Gold From BRD`

### Slice E

- drift support
- glossary
- anomaly explanation

---

## 8. Final Recommendation

The best implementation sequence is:

1. build the shared AI service layer,
2. ship `P1`,
3. use `P1` feedback to refine structured output contracts,
4. ship `P2`,
5. only then move into `P3`.

That path gives Bitool a useful embedded AI layer quickly without destabilizing the Bronze/Silver/Gold workflows that already exist.
