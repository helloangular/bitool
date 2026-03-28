# JSON Explode Auto-Planning Tech Design

## 1. Summary

This document defines a production-safe design for automatically recommending:

- `json_explode_rules`
- `primary_key_fields`
- `watermark_column`
- optional child-entity Bronze candidates

for `API -> Bronze` ingestion in Bitool.

The goal is to make API onboarding much more reliable without letting an LLM silently invent invalid record paths or incorrect table grain.

The design uses:

- deterministic structural inference from sample JSON or OpenAPI-derived schema
- deterministic scoring and validation
- optional LLM ranking and explanation only for ambiguous cases

The final system is intended to help Bitool answer questions like:

- what should one Bronze row represent?
- which JSON array should be exploded?
- which field is the most likely primary key?
- which field is the most likely incremental watermark?
- should a nested child collection become a separate Bronze table?

## 2. Problem

Today, `API -> Bronze` configuration depends heavily on operator judgment for:

- selecting the correct explode path
- selecting the correct row grain
- choosing stable key fields
- choosing a usable watermark field

This causes recurring failures:

- `json_explode_rules` set too high or too low in the JSON tree
- row grain mismatch between Bronze and the intended entity
- `source_record_id` blank because the chosen key does not exist at the selected grain
- `max_watermark = null` because the chosen timestamp does not exist or is usually null
- repeated reinserts because the watermark never advances
- deeply nested child arrays flattened into the wrong table

The system already has useful building blocks:

- schema inference
- field descriptor generation
- automatic path-based column naming
- optional detection of record paths in runtime preview
- UI-side PK and watermark heuristics

But the selection of the correct Bronze grain remains too manual.

## 3. Goals

### Functional

- Automatically recommend the best `json_explode_rules` path for one endpoint.
- Automatically recommend `primary_key_fields`.
- Automatically recommend `watermark_column`.
- Suggest separate child-entity Bronze candidates when nested arrays represent real entities.
- Produce human-readable reasoning and confidence.
- Integrate into preview and save flows.

### Non-functional

- Deterministic by default.
- Safe: never invent paths not present in schema/sample evidence.
- Explainable.
- Backward-compatible with existing endpoint configs.
- Works without LLM.
- LLM augmentation is optional and constrained.

## 4. Non-goals

- Full automatic creation of multiple Bronze tables from one endpoint in v1.
- Automatic Silver modeling.
- Arbitrary SQL or transformation generation.
- Blindly trusting LLM output as executable source of truth.

## 5. Current System

The relevant current components are:

- [schema_infer.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ingest/schema_infer.clj)
  - builds inferred field descriptors
  - detects logical records from JSON paths
- [bronze.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ingest/bronze.clj)
  - converts extracted records into Bronze rows
  - derives `source_record_id` and `event_time_utc`
- [jsontf.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/api/jsontf.clj)
  - traverses nested JSON
  - supports per-context row extraction and array handling
- [runtime.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ingest/runtime.clj)
  - preview-time schema detection
  - inferred field persistence
- [apiComponent.js](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js)
  - currently applies frontend heuristics for watermark and PK defaults

Current behavior:

- runtime can infer fields and sometimes detect a record path
- UI can heuristically fill PK/watermark in some cases
- but `json_explode_rules` selection is still shallow and mostly manual
- nested child entities are not formally planned as separate Bronze candidates

## 6. Core Design

### 6.1 New Planner Layer

Add a deterministic planner stage after schema inference:

`sample JSON / OpenAPI -> StructuralFacts -> GrainCandidates -> Recommendation`

This planner will produce:

```clojure
{:recommended_records_path "data"
 :recommended_primary_key_fields ["INVOICE_ID"]
 :recommended_watermark_column "data_items_LAST_UPDATE_DATE"
 :child_entity_candidates
 [{:path "data.items"
   :entity_name "invoice_line"
   :recommended_primary_key_fields ["INVOICE_ID" "LINE_NUMBER"]
   :recommended_watermark_column "data_items_LAST_UPDATE_DATE"
   :confidence 0.81}]
 :confidence 0.93
 :reasons
 ["Path data is an array of objects"
  "Path data contains stable ID candidate INVOICE_ID"
  "Path data contains watermark candidate LAST_UPDATE_DATE"
  "Nested array data.lines exists and may be a separate child entity"]}
```

### 6.2 Three-stage decision model

#### Stage A: Deterministic structural extraction

Build a compact structural graph from sample JSON and/or OpenAPI:

- object paths
- array-of-object paths
- array-of-scalar paths
- scalar fields per path
- likely ID fields
- likely timestamp fields
- nullability / sample coverage
- nested repeated arrays
- parent-child path relationships

#### Stage B: Deterministic scoring

Generate candidate explode paths and score them.

#### Stage C: Optional LLM ranking

Only used when deterministic confidence is below threshold or multiple candidates are close.

The LLM does not invent paths. It receives only structured candidate facts and must choose among them.

## 7. StructuralFacts Model

Introduce a normalized planner input model:

```clojure
{:endpoint_name "ap/invoices"
 :endpoint_url "ap/invoices"
 :sample_count 50
 :root_path "$"
 :array_paths
 [{:path "data"
   :kind :array-of-objects
   :depth 1
   :field_count 206
   :id_candidates ["INVOICE_ID" "INVOICE_NUM"]
   :timestamp_candidates ["LAST_UPDATE_DATE" "CREATION_DATE"]
   :parent_keys_available []
   :nested_array_paths ["data.LINES" "data.DISTRIBUTIONS"]}]
 :object_paths
 [...]
 :scalar_fields
 [{:path "data.INVOICE_ID"
   :column_name "data_items_INVOICE_ID"
   :type "STRING"
   :nullable false
   :sample_coverage 1.0}
  ...]}
```

## 8. Candidate Generation

### 8.1 Primary explode candidates

Generate one candidate for each array-of-objects path.

Typical candidates:

- `data`
- `items`
- `orders`
- `orders.items`
- `results.records`

### 8.2 Child-entity candidates

If a chosen candidate contains nested arrays of objects, also generate child suggestions.

Example:

- chosen Bronze path: `data`
- nested child arrays:
  - `data.lines`
  - `data.distributions`

These are not automatically materialized in v1, but they are surfaced as preview suggestions.

## 9. Scoring Rules

Each candidate receives a weighted deterministic score.

### Positive signals

- path is an array of objects: `+50`
- contains endpoint-matching ID like `INVOICE_ID` for `ap/invoices`: `+50`
- contains generic stable key `*_ID`: `+40`
- contains `*_KEY`: `+30`
- contains watermark candidate like `UPDATED_AT`, `LAST_UPDATE_DATE`: `+30`
- low nullability key candidate: `+20`
- sample coverage >= 0.99 for key or watermark: `+15`
- parent key available for nested child candidate: `+20`
- shallow path depth: `+10`

### Negative signals

- no ID candidates: `-40`
- no timestamp candidates on an incremental endpoint: `-25`
- contains nested repeated arrays, suggesting this is a parent not final grain: `-15`
- array elements are mostly scalars rather than objects: `-50`
- likely cartesian-risk if multiple repeated structures would be flattened together: `-35`

### Output thresholds

- `>= 80`: auto-apply
- `50-79`: suggest in preview, operator confirm
- `< 50`: no automatic choice

## 10. PK Recommendation Rules

For the selected explode candidate:

1. Prefer endpoint-matching ID fields
   - `INVOICE_ID` for invoices
   - `VEHICLE_ID` or `id` for vehicles

2. Prefer non-null, high-coverage fields.

3. Prefer immutable identifiers over names or display values.

4. For nested child entities, recommend composite keys if needed.

Examples:

- parent grain:
  - `["INVOICE_ID"]`
- child grain:
  - `["INVOICE_ID" "LINE_NUMBER"]`

If no acceptable PK exists:

- do not auto-fill
- return warning:
  - `No stable primary key candidate found at chosen grain`

## 11. Watermark Recommendation Rules

For the selected explode candidate:

1. Prefer fields containing:
   - `UPDATED`
   - `LAST_UPDATE`
   - `MODIFIED`
   - `UPDATED_AT`
   - `TIMESTAMP`

2. Prefer timestamp-like types from inference.

3. Prefer high sample coverage and low nullability.

4. Penalize business-event timestamps that are often null or conditional:
   - `CANCELLED_DATE`
   - `CLOSED_DATE`
   - `DELETED_AT`
   unless the endpoint semantics specifically require them.

5. If incremental load is enabled and no valid watermark exists at grain:
   - return suggestion to use full load or manual selection

## 12. Optional LLM Layer

### 12.1 When to use

Use LLM only if:

- deterministic top score is below threshold, or
- top two candidates are very close, or
- endpoint/entity semantics are ambiguous

### 12.2 LLM input

Provide only compact structured candidate facts:

```json
{
  "endpoint_name": "ap/invoices",
  "endpoint_url": "ap/invoices",
  "candidates": [
    {
      "path": "data",
      "kind": "array-of-objects",
      "id_candidates": ["INVOICE_ID", "INVOICE_NUM"],
      "timestamp_candidates": ["LAST_UPDATE_DATE", "CREATION_DATE"],
      "nested_array_paths": ["data.LINES"]
    },
    {
      "path": "data.LINES",
      "kind": "array-of-objects",
      "id_candidates": ["LINE_NUMBER"],
      "timestamp_candidates": [],
      "parent_keys_available": ["INVOICE_ID"]
    }
  ]
}
```

### 12.3 LLM output constraints

The LLM may only:

- choose one of the provided candidates
- rank provided PK candidates
- rank provided watermark candidates
- explain why

The LLM may not:

- invent new paths
- invent fields not present in the evidence
- directly mutate config without deterministic validation

## 13. Validation Layer

Before applying recommendations:

- verify explode path exists in sample/OpenAPI-derived structure
- verify chosen path is array-of-objects or valid root object grain
- verify PK fields exist at that grain
- verify watermark field exists and is timestamp-like
- reject if recommendation would create obvious multi-array cartesian flattening

Invalid recommendations are downgraded to preview suggestions with explicit warnings.

## 14. Runtime/UI Integration

### 14.1 Backend

Add a new planner namespace:

- `src/clj/bitool/ingest/grain_planner.clj`

Public functions:

```clojure
(analyze-endpoint-structure body endpoint-config)
(recommend-endpoint-grain endpoint-config structure-facts)
(recommend-endpoint-config endpoint-config structure-facts)
```

### 14.2 Preview route

Extend preview response to include:

```clojure
{:inferred_fields [...]
 :detected_records_path "data"
 :recommendations
 {:json_explode_rules [{:path "data"}]
  :primary_key_fields ["INVOICE_ID"]
  :watermark_column "data_items_LAST_UPDATE_DATE"
  :child_entity_candidates [...]}}
```

### 14.3 UI behavior

The planner output should be surfaced in the existing endpoint configuration flow, not as a separate wizard.

Placement:

- recommendations appear in the `Schema` tab immediately after a successful `Preview Schema`
- the `Basic`, `Pagination`, and `Request` tabs remain unchanged
- planner output is shown above the inferred fields table so the operator sees row-grain decisions before field-level details

Updated `Schema` tab layout:

```text
+----------------------------------------------------------+
| Schema                                                   |
+----------------------------------------------------------+
| [Recommendation Card]                                    |
| +------------------------------------------------------+ |
| | Record Grain: data[]                       [Auto]     | |
| | Primary Key:  INVOICE_ID                   [Auto]     | |
| | Watermark:    LAST_UPDATE_DATE             [Auto]     | |
| | Confidence:   93%                                     | |
| |                                                      | |
| | [Show reasoning]                                     | |
| +------------------------------------------------------+ |
|                                                          |
| [Possible Child Entities] (collapsed by default)         |
| +------------------------------------------------------+ |
| | invoice_line                                [Suggest] | |
| | Path: data.LINES[]                                   | |
| | Keys: INVOICE_ID + LINE_NUMBER                       | |
| | [Create as separate endpoint]                        | |
| +------------------------------------------------------+ |
|                                                          |
| Inferred Fields                                          |
| On | Path | Column | Type | WM | Override | Coverage    |
| ...                                                      |
+----------------------------------------------------------+
```

#### 14.3.1 Recommendation card

The recommendation card is the primary UX surface for planner output.

It displays:

- recommended record grain / `json_explode_rules`
- recommended `primary_key_fields`
- recommended `watermark_column`
- overall confidence score
- short human-readable reasoning

Behavior:

- card is always shown after preview if planner returns any recommendation
- reasoning is collapsed by default behind a `Show reasoning` toggle
- operator edits still happen in the existing form inputs; the card is guidance and action surface, not a duplicate form

#### 14.3.2 Confidence states

The UI should use three recommendation states, not just auto-apply vs suggest.

| Score | Badge | Color | Behavior |
| --- | --- | --- | --- |
| `>= 80` | `Auto-detected` | Green | Pre-fill field, show editable badge |
| `50-79` | `Suggested` | Amber | Leave field unchanged/empty, show one-click accept |
| `< 50` | `Manual` | Gray | Do not fill, show manual-selection hint |

Rules:

- confidence is computed per recommended field and for the overall grain recommendation
- field-level badges can differ from overall card confidence
- operator can always override any value

#### 14.3.3 Acceptance interaction

For each recommended value:

- `Auto-detected`
  - form field is pre-filled automatically
  - green badge appears next to the input
  - operator may edit the value directly
- `Suggested`
  - form field is not overwritten automatically
  - amber suggestion pill appears next to the relevant input
  - clicking the pill applies the suggested value
  - clicking dismiss removes the suggestion for that preview result
- `Manual`
  - no value is filled
  - gray helper text indicates manual selection is required

If an existing saved value is present but invalid for the inferred structure:

- keep the existing value visible
- show inline warning
- show replacement suggestion from the planner
- never silently overwrite an already-saved non-empty value

#### 14.3.4 Reasoning display

Reasoning should be available without overwhelming the form.

Use a collapsed details block inside the recommendation card.

Example reasoning bullets:

- `data[] is an array of 206-field objects`
- `INVOICE_ID is non-null, stable, and matches endpoint semantics`
- `LAST_UPDATE_DATE is timestamp-like with high coverage`

Rules:

- maximum 3 to 5 bullets in the default card
- no raw planner internals or giant path dumps in the first view
- detailed structural facts can remain in console/debug mode only

#### 14.3.5 Child entity candidates

Child entity suggestions must be actionable.

If nested repeated child arrays are detected, show a collapsible `Possible child entities` section below the recommendation card.

Each child entity card shows:

- suggested entity/table name
- child array path
- recommended composite key
- confidence
- short rationale
- `Create as separate endpoint` action

This section is:

- collapsed by default
- omitted completely when there are no viable child candidates

#### 14.3.6 Create as separate endpoint action

Clicking `Create as separate endpoint` should:

1. Add a new endpoint config entry to `endpoint_configs`.
2. Copy shared parent settings:
   - base URL
   - auth
   - request configuration
   - pagination mode where still applicable
3. Pre-fill child-specific settings:
   - derived endpoint name
   - same endpoint URL
   - child `json_explode_rules`
   - recommended child `primary_key_fields`
   - recommended child watermark if available
4. Switch the UI to the new endpoint tab immediately.
5. Show a transient hint such as `Created from parent child-entity recommendation`.

Suggested naming pattern:

- parent endpoint: `ap/invoices`
- suggested child endpoint label: `ap/invoices.invoice_line`

This keeps child-entity planning one-click actionable instead of informational only.

### 14.4 Save behavior

Save path remains explicit.

Rules:

- auto-filled recommendations are treated as normal editable values
- suggestion acceptance writes into the normal endpoint config model
- dismissed suggestions are UI-local and are recomputed on the next preview
- save does not persist hidden planner state as source of truth
- only the final chosen endpoint config is persisted

## 15. Example

Input endpoint:

- `/ap/invoices`

Observed structure:

- `data[]`
  - fields include `INVOICE_ID`, `INVOICE_NUM`, `LAST_UPDATE_DATE`
  - nested arrays include `LINES[]`

Planner output:

```clojure
{:json_explode_rules [{:path "data"}]
 :primary_key_fields ["INVOICE_ID"]
 :watermark_column "data_items_LAST_UPDATE_DATE"
 :child_entity_candidates
 [{:path "data.LINES"
   :entity_name "invoice_line"
   :recommended_primary_key_fields ["INVOICE_ID" "LINE_NUMBER"]
   :confidence 0.79}]}
```

Operator result:

- parent invoice Bronze configured correctly
- future enhancement may spin off invoice-lines Bronze from suggestion

## 16. Failure Modes

### No arrays at all

- use root object grain
- no `json_explode_rules`

### Multiple equally plausible arrays

- use LLM explanation or operator prompt
- do not silently choose low-confidence candidate

### Missing stable keys

- do not auto-populate PK
- warn clearly

### Missing timestamp

- do not auto-populate watermark
- suggest full load or manual selection

### Child array without parent key

- warn that child table recommendation is incomplete

## 17. Metrics

Track:

- preview recommendation acceptance rate
- manual override rate
- rate of `source_record_id` blank after recommendation
- rate of `max_watermark = null` after recommendation
- reinsertion/no-op correctness after recommendation
- operator time to first successful run

## 18. Rollout Plan

### Phase 1

- deterministic candidate extraction
- deterministic scoring
- backend recommendation response
- UI display only

### Phase 2

- auto-apply high-confidence recommendations
- warnings for invalid existing PK/watermark

### Phase 3

- optional constrained LLM ranking
- child entity suggestion UX

### Phase 4

- optional multi-Bronze planning from one endpoint

## 19. Risks

- false confidence on semantically wrong but structurally plausible paths
- endpoint-specific naming conventions may vary widely
- operators may over-trust automatic recommendations

Mitigations:

- confidence thresholds
- explicit preview reasoning
- deterministic validation
- audit trail of recommended vs accepted config

## 20. Recommendation

Implement the planner as a deterministic backend feature first.

Use LLM only as an ambiguity resolver and explanation layer.

This keeps the system:

- safe
- testable
- explainable
- compatible with existing schema inference and Bronze runtime

while materially reducing operator error around:

- `json_explode_rules`
- row grain
- PK selection
- watermark selection
