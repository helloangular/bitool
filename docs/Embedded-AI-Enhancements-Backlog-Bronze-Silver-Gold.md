# Embedded AI Enhancements Backlog for Bronze, Silver, and Gold

## 1. Purpose

This document turns the current grounded gap analysis into a tactical implementation backlog.

It is designed to answer:

- which existing screen should get AI help,
- what exact user action should be added,
- what backend support is needed,
- what should be built first.

This is intentionally incremental.

It assumes:

- the existing API Configuration screen remains the Bronze workbench,
- the existing Modeling Console remains the Silver and Gold workbench,
- AI should be embedded into those existing screens rather than introduced as a separate parallel product.

---

## 2. Implementation Principles

1. Add AI where the user is already working.
2. Prefer “explain” and “suggest” before “auto-apply”.
3. Every AI action should produce structured output that the UI can render and the user can approve.
4. Avoid freeform chat as the first step. Start with targeted actions tied to specific workflow states.
5. Keep deterministic validation as the final gate before saving, compiling, publishing, or executing.
6. Enhance existing recommendation and validation surfaces before adding parallel AI widgets.

### 2.1 Shared UI pattern

Do not add one-off AI buttons and result blocks with different behavior on every screen.

Use one shared inline pattern:

- `ai-assist-trigger`
  - a compact icon-plus-label action
- `ai-assist-card`
  - collapsible result card with:
    - summary
    - confidence badge
    - expandable sections for recommendations, edits, open questions, warnings
    - optional `Apply selected`
    - optional `Undo`

This should be reused across:

- API recommendation card enhancements,
- Modeling Console explanation panels,
- target strategy explanations.

### 2.2 Contextual visibility rules

AI actions should only appear when they are relevant.

Examples:

- `Explain Preview`
  - only after `Preview Schema` has produced inferred fields or recommendations
- `Suggest Bronze Keys`
  - only when the existing recommendation card is present and confidence is ambiguous or user wants a deeper explanation
- `Explain Validation Failure`
  - only when validation has failed or warned
- BRD generation actions
  - only when there is a current proposal context to refine

Avoid showing disabled AI actions with no usable context on first load.

### 2.3 Chat vs embedded AI boundary

The product already has Pipeline Chat in [resources/public/pipelineChatComponent.js](/Users/aaryakulkarni/bitool/resources/public/pipelineChatComponent.js).

The intended division should be:

- Pipeline Chat
  - create or reshape pipelines conversationally across Bronze, Silver, and Gold
- Embedded AI
  - explain or refine one local workflow state inline

Both should share the same LLM infrastructure and context sources, but they should not compete visually or conceptually.

---

## 3. Priority Summary

### P1

- Bronze schema explanation
- Bronze PK/watermark/grain suggestions
- Silver proposal explanation
- Validation failure explanation

### P2

- BRD intake for Silver and Gold proposal generation
- Transformation suggestions in Modeling Console
- Gold dimension/measure suggestions
- Drift remediation suggestions

### P3

- KPI and mart generation assistant
- Root-cause assistant for KPI movement
- Gold business glossary and metric definition generator
- Cost/performance recommendation assistant

---

## 4. Screen-by-Screen Backlog

### 4.1 API Configuration screen

Existing surface:

- [resources/public/apiComponent.js](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js)

Current strengths:

- connection config,
- endpoint discovery,
- preview schema,
- inferred fields,
- watermark controls,
- Bronze naming,
- schema evolution settings.

#### P1-A: Explain Preview Schema

User action:

- add AI deep-dive action inside the existing recommendation card, for example:
  - `Explain with AI`

UI placement:

- inside the existing recommendation card rendered from [resources/public/apiComponent.js:1384](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js#L1384),
- not as a second parallel explanation block next to `Show reasoning`

Expected output:

- why this record path was chosen,
- why these fields were inferred,
- why certain fields were suggested as watermark candidates,
- why certain fields look like identifiers,
- why explode rules were or were not suggested.

Suggested backend endpoint:

- `POST /aiExplainPreviewSchema`

Request shape:

- `base_url`
- `endpoint_config`
- `inferred_fields`
- `schema_recommendations`

Response shape:

- `summary`
- `record_grain`
- `watermark_reasoning`
- `pk_reasoning`
- `explode_reasoning`
- `field_notes[]`

Priority:

- `P1`

#### P1-B: Suggest PK / watermark / grain

User action:

- add AI enhancement inside the existing recommendation card, for example:
  - `Refine recommendation`
  - or `Explain ambiguity`

UI placement:

- inside the existing recommendation card,
- not as a separate recommendation card that duplicates current PK/watermark/grain output

Expected output:

- recommended `primary_key_fields`
- recommended `watermark_column`
- confidence score
- alternatives if ambiguous

Implementation note:

- this should not be a parallel LLM-only recommender,
- it should run the existing deterministic grain planner first in [src/clj/bitool/ingest/grain_planner.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ingest/grain_planner.clj),
- then use AI only to explain, rank, or soften ambiguity when deterministic signals are weak.

Suggested backend endpoint:

- `POST /aiSuggestBronzeKeys`

Request shape:

- `endpoint_config`
- `inferred_fields`
- `sample_payload_summary`

Response shape:

- `primary_key_fields[]`
- `watermark_column`
- `grain_label`
- `confidence`
- `alternatives[]`
- `explanation`

Priority:

- `P1`

#### P2-A: Suggest field naming and canonicalization

User action:

- add button: `Suggest Canonical Names`

Expected output:

- proposed `column_name` changes
- semantic labels like `id`, `timestamp`, `amount`, `status`
- optional target type override suggestions

Suggested backend endpoint:

- `POST /aiSuggestBronzeFieldCanonicalization`

Priority:

- `P2`

#### P3-A: Explain source business shape

User action:

- add button: `What Does This Endpoint Represent?`

Expected output:

- whether endpoint is likely event/fact/reference/snapshot,
- likely downstream Silver model shapes,
- likely Gold use cases.

Suggested backend endpoint:

- `POST /aiExplainEndpointBusinessShape`

Priority:

- `P3`

---

### 4.2 Target screen

Existing surface:

- [resources/public/targetComponent.js](/Users/aaryakulkarni/bitool/resources/public/targetComponent.js)

Current strengths:

- target warehouse selection,
- write mode,
- target location,
- Silver/Gold execution settings,
- advanced options JSON.

#### P2-B: Suggest target configuration defaults

User action:

- add button: `Suggest Target Defaults`

Expected output:

- recommended write mode,
- merge keys,
- table format,
- clustering/partitioning hints,
- cost/performance notes.

Suggested backend endpoint:

- `POST /aiSuggestTargetConfig`

Request shape:

- `target_kind`
- `current_target`
- `proposal_json`
- `compiled_sql` if available

Response shape:

- `write_mode`
- `merge_keys[]`
- `cluster_by[]`
- `partition_columns[]`
- `notes[]`

Priority:

- `P2`

#### P3-B: Explain warehouse tradeoffs

User action:

- add button: `Explain Write Mode`

Expected output:

- why `append`, `merge`, `replace`, `update`, `delete`, or `copy_into` would be appropriate,
- cost and performance implications,
- operational risk notes.

Suggested backend endpoint:

- `POST /aiExplainTargetStrategy`

Priority:

- `P3`

---

### 4.3 Modeling Console: Silver

Existing surface:

- [resources/public/modelingConsole.js](/Users/aaryakulkarni/bitool/resources/public/modelingConsole.js)

Current strengths:

- proposal generation,
- compile,
- validate,
- review,
- publish,
- execute.

#### P1-C: Explain proposal

User action:

- add button: `Explain Proposal`

UI placement:

- proposal detail header near compile/validate actions

Expected output:

- why these columns were selected,
- why this materialization mode was chosen,
- why these merge keys were chosen,
- what source fields drive each target field,
- what business shape the model represents.

Suggested backend endpoint:

- `POST /aiExplainModelProposal`

Request shape:

- `proposal_id`
- `proposal_json`
- `compile_result` if available

Response shape:

- `summary`
- `business_shape`
- `materialization_reasoning`
- `key_reasoning`
- `column_reasoning[]`
- `open_questions[]`

Priority:

- `P1`

#### P2-C: Suggest transformations

User action:

- add button: `Suggest Transformations`

Expected output:

- casts,
- date normalization,
- enum cleanup,
- field splitting,
- derived fields,
- dedupe hints.

Suggested backend endpoint:

- `POST /aiSuggestSilverTransforms`

Response shape:

- `mapping_updates[]`
- `new_columns[]`
- `processing_policy_updates`
- `explanation`

Priority:

- `P2`

#### P2-D: BRD to Silver proposal

User action:

- new panel: `Business Requirement`
- action: `Generate From BRD`

Expected output:

- suggested Silver schema updates,
- suggested processing policy,
- suggested materialization strategy,
- explicit assumptions.

Suggested backend endpoint:

- `POST /aiGenerateSilverProposalFromBRD`

Request shape:

- `proposal_id` or `source_context`
- `brd_text`
- `graph_context`

Priority:

- `P2`

#### P1-D: Explain validation failure

User action:

- add button or panel link: `Explain Validation Failure`

Expected output:

- plain-language explanation of failed checks,
- likely root cause,
- suggested next edits.

UI note:

- this should appear below or alongside the existing deterministic validation display in [resources/public/modelingConsole.js](/Users/aaryakulkarni/bitool/resources/public/modelingConsole.js),
- it should not replace the existing `PASS` / `WARN` / `FAIL` check rendering.

Suggested backend endpoint:

- `POST /aiExplainProposalValidation`

Request shape:

- `proposal_id`
- `validation_result`
- `proposal_json`

Priority:

- `P1`

---

### 4.4 Modeling Console: Gold

Existing surface:

- same Modeling Console,
- Gold routes already present in [resources/public/modelingConsole.js:1018](/Users/aaryakulkarni/bitool/resources/public/modelingConsole.js#L1018)

#### P2-E: Suggest dimensions and measures

User action:

- add button: `Suggest Mart Design`

Expected output:

- candidate dimensions,
- candidate measures,
- recommended grain,
- aggregation choices,
- likely metric groupings.

Suggested backend endpoint:

- `POST /aiSuggestGoldMartDesign`

Request shape:

- `proposal_id`
- `silver_context`
- `current_gold_proposal`

Response shape:

- `dimensions[]`
- `measures[]`
- `grain`
- `group_by[]`
- `materialization`
- `explanation`

Priority:

- `P2`

#### P2-F: BRD to Gold proposal

User action:

- BRD panel in Gold flow
- action: `Generate Gold From BRD`

Expected output:

- candidate marts,
- measures,
- dimensions,
- metric definitions,
- unresolved semantic questions.

Suggested backend endpoint:

- `POST /aiGenerateGoldProposalFromBRD`

Priority:

- `P2`

#### P3-C: KPI and glossary generation

User action:

- add button: `Generate Metric Definitions`

Expected output:

- metric glossary,
- assumptions,
- business definitions,
- caveats.

Suggested backend endpoint:

- `POST /aiGenerateMetricGlossary`

Priority:

- `P3`

---

### 4.5 Schema drift and operations

Existing surface:

- operational and validation surfaces exist in the platform,
- drift-related tech design also exists in docs, but AI/operator guidance is still thin in UI.

Dependency note:

- drift explanation and remediation should be treated as dependent on the schema-drift review surface and usable drift context being available in product code,
- these are not suitable as the first AI implementation tasks even though they are valuable.

#### P2-G: Explain schema drift

User action:

- add button: `Explain Drift`

Expected output:

- what changed,
- which models are impacted,
- whether change is additive, breaking, or low-risk,
- suggested remediation.

Suggested backend endpoint:

- `POST /aiExplainSchemaDrift`

Request shape:

- `endpoint_schema_snapshot_before`
- `endpoint_schema_snapshot_after`
- affected proposals

Priority:

- `P2`

#### P2-H: Suggest drift remediation

User action:

- add button: `Suggest Fix`

Expected output:

- proposal updates,
- type changes,
- additive column adoption,
- validation notes.

Suggested backend endpoint:

- `POST /aiSuggestDriftRemediation`

Priority:

- `P2`

#### P3-D: Explain KPI or run anomalies

User action:

- add button: `Why did this change?`

Expected output:

- likely upstream drivers,
- impacted entities,
- likely code/model/config changes,
- suggestions for deeper checks.

Suggested backend endpoint:

- `POST /aiExplainRunOrKpiAnomaly`

Priority:

- `P3`

---

## 5. Suggested Backend Design Pattern

For all of the above AI endpoints, use one shared pattern:

1. Build structured context from existing graph, proposal, validation, and schema data.
2. Run deterministic summarization and rule checks first.
3. Send a compact, structured prompt to the AI layer only for ranking, explanation, and proposal generation.
4. Return structured JSON, not only prose.
5. Keep user-facing text plus machine-readable edits separate.

Recommended response pattern:

```json
{
  "summary": "short explanation",
  "confidence": 0.82,
  "recommendations": [],
  "edits": {},
  "open_questions": [],
  "warnings": []
}
```

This keeps the AI useful for the UI without making the frontend parse prose.

## 5.1 Apply and undo pattern

For any AI result that proposes edits:

- render each proposed edit as a separate selectable row,
- default to review-first, not auto-apply,
- support `Apply selected`,
- preserve previous state in a local `ai_previous_state` buffer for one-level undo.

This is especially important for:

- Bronze key suggestions,
- transformation suggestions,
- BRD-driven proposal edits.

---

## 6. Recommended Implementation Order

### First milestone

- `Explain Preview`
- `Suggest Bronze Keys`
- `Explain Proposal`
- `Explain Validation Failure`

Reason:

- these fit existing screens,
- they are high-value,
- they do not require a new workflow concept,
- they improve trust in the current automation.

### Second milestone

- `Generate From BRD` for Silver
- `Generate Gold From BRD`
- `Suggest Transformations`
- `Suggest Mart Design`
- `Explain Drift`

Reason:

- these begin turning the product into a true AI-assisted modeling system.

### Third milestone

- metric glossary
- KPI anomaly assistant
- warehouse tradeoff assistant
- deeper business-shape explanation

Reason:

- these are valuable, but are less critical than improving the core operator workflow.

---

## 7. Recommended Frontend Additions by File

### `resources/public/apiComponent.js`

Add:

- `Explain Preview`
- `Suggest Bronze Keys`
- later: `Suggest Canonical Names`

### `resources/public/targetComponent.js`

Add:

- `Suggest Target Defaults`
- later: `Explain Write Mode`

### `resources/public/modelingConsole.js`

Add:

- `Explain Proposal`
- `Explain Validation Failure`
- `Generate From BRD`
- `Suggest Transformations`
- `Suggest Mart Design`
- later: metric glossary helpers

---

## 8. Recommended First Build

If only one AI feature cluster is built first, build this:

1. `Explain Preview` in API Configuration
2. `Explain Proposal` in Modeling Console
3. `Explain Validation Failure` in Modeling Console

That combination does the most to improve user trust in the system that already exists.

It also avoids pretending that the product lacks Bronze/Silver/Gold support today.

---

## 9. Final Product Guidance

The right narrative for Bitool is not:

- “we need to build Bronze, Silver, and Gold UI from scratch.”

It is:

- “we already have the main Bronze/Silver/Gold workflow, and now we should make it easier to understand, trust, and steer with embedded AI.”

That is the implementation path this backlog supports.
