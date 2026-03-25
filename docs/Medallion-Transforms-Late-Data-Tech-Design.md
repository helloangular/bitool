# Medallion Transforms and Late-Data Handling — Technical Design

## 1. Purpose

This document defines the product and implementation design for adding:

- rich column transforms for Bronze -> Silver and Silver -> Gold,
- freeform expression editing comparable to projection and filter nodes,
- late / out-of-order data handling for medallion pipelines,
- a single canonical transform and processing-policy model shared by:
  - the Modeling Console,
  - synthesized graph artifacts,
  - main canvas nodes,
  - the SQL compiler and execution paths.

This design is implementation-oriented. It is meant to guide code changes in the current Bitool codebase, not just describe product intent.

---

## 2. Scope

This design covers:

- Modeling Console proposal editing UX
- proposal JSON schema
- graph synthesis
- compile-time validation
- Silver and Gold SQL generation
- late-data / event-time policy modeling
- PostgreSQL, Snowflake, and Databricks compilation expectations
- rollout and test strategy

This design does not fully specify:

- warehouse-specific streaming engines
- full CDC semantics across every connector type
- non-medallion ad hoc graph authoring beyond compatibility requirements

---

## 3. Related Documents

This document extends and operationalizes:

- [Silver-Gold-Automation-Tech-Design.md](./Silver-Gold-Automation-Tech-Design.md)
- [GIL-for-Medallion-Modeling-Tech-Design.md](./GIL-for-Medallion-Modeling-Tech-Design.md)
- [Bitool Graph -> SQL Compiler -> Databricks Job Execution Design.md](./Bitool%20Graph%20-%3E%20SQL%20Compiler%20-%3E%20Databricks%20Job%20Execution%20Design.md)
- [API-to-Bronze-Production-Tech-Design.md](./API-to-Bronze-Production-Tech-Design.md)
- [Node Tech Design - All Nodes.md](./Node%20Tech%20Design%20-%20All%20Nodes.md)

Role boundaries:

- Silver/Gold automation defines proposal generation.
- This document defines the proposal editing and runtime semantics for transforms and late-data handling.
- GIL / graph synthesis turns approved proposals into graph artifacts.
- compiler dialects produce warehouse-native SQL.

---

## 4. Problem Statement

Bitool currently has useful building blocks, but the medallion workflow still has major product gaps:

- transform editing in modeling has been weaker than graph-node editing,
- dropdown transforms and freeform expressions have not shared one canonical model,
- late / out-of-order data policy is not first-class in Silver and Gold proposals,
- event-time semantics, merge semantics, and reprocessing windows are not modeled explicitly,
- canvas nodes and Modeling Console risk diverging unless they share the same underlying schema.

Without solving those together, the product ends up with:

- duplicated transform systems,
- confusing UX differences between modeling and canvas,
- incorrect latest-state behavior for late data,
- weak explainability for merge and reprocessing behavior,
- incomplete warehouse compilation semantics.

---

## 5. Goals

Bitool should:

- support three column authoring modes:
  - direct mapping,
  - transform chain,
  - advanced expression
- expose an expression editor in modeling comparable to projection/filter node UX
- support a first-class processing policy for Silver and Gold models
- allow users to configure:
  - business keys,
  - event-time column,
  - sequence/version column,
  - late-data tolerance,
  - too-late behavior,
  - reprocessing window,
  - ordering strategy,
  - materialization mode
- compile those semantics into deterministic SQL
- preserve compatibility with synthesized graph artifacts and main canvas nodes
- keep one canonical proposal schema for all medallion modeling behavior

---

## 6. Non-Goals

This design does not aim to:

- replace warehouse-native optimization engines
- add arbitrary unsafe SQL passthrough
- create a separate transform engine only for modeling
- fully solve general streaming watermark execution inside Bitool itself
- make the main canvas the primary medallion UX

---

## 7. Product Positioning

### 7.1 Preferred user workflow

The Modeling Console is the primary UX for:

- Bronze -> Silver proposal authoring
- Silver -> Gold proposal authoring
- transform editing
- late-data policy configuration
- compile / validate / review / publish / execute lifecycle

### 7.2 Main canvas role

The main canvas remains:

- the low-level graph authoring and inspection surface
- the execution representation synthesized from approved proposals
- the advanced escape hatch for non-standard flows

### 7.3 Design rule

Bitool must not create two medallion transform systems.

There must be:

- one canonical column transform model
- one canonical processing-policy model
- one canonical validation model

Both Modeling Console and canvas nodes should read and write compatible representations.

---

## 8. Design Principles

1. One source of truth.
   Proposal JSON is canonical. UI state, graph synthesis, and SQL compilation all derive from it.

2. Assistive on the surface, deterministic underneath.
   Users can edit visually, but compiled output must be deterministic and auditable.

3. Event time over arrival time.
   Latest-state and late-data semantics must prefer business event time unless explicitly configured otherwise.

4. Idempotent execution.
   Silver and Gold execution must be safe to rerun over reprocessing windows.

5. Strong safe-expression boundary.
   Allow rich scalar expressions, but reject statement-level SQL or dangerous tokens.

6. Reuse current components where it helps.
   Projection/filter expression UX should be reused rather than reimplemented.

---

## 9. Current Code Anchors

The design intentionally builds on existing modules:

### Frontend

- Modeling Console: [resources/public/modelingConsole.js](/Users/aaryakulkarni/bitool/resources/public/modelingConsole.js)
- Shared transform editor: [resources/public/transformEditorComponent.js](/Users/aaryakulkarni/bitool/resources/public/transformEditorComponent.js)
- Shared expression editor: [resources/public/expressionComponent.js](/Users/aaryakulkarni/bitool/resources/public/expressionComponent.js)
- Projection node UX: [resources/public/projectionComponent.js](/Users/aaryakulkarni/bitool/resources/public/projectionComponent.js)
- Calculated column UX: [resources/public/calculatedColumnComponent.js](/Users/aaryakulkarni/bitool/resources/public/calculatedColumnComponent.js)
- Filter node UX: [resources/public/filterComponent.js](/Users/aaryakulkarni/bitool/resources/public/filterComponent.js)

### Backend

- Modeling proposal generation / validation / compile / review / publish / execute:
  [src/clj/bitool/modeling/automation.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/modeling/automation.clj)
- Compiler entry:
  [src/clj/bitool/compiler/core.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/compiler/core.clj)
- PostgreSQL dialect:
  [src/clj/bitool/compiler/dialect/postgresql.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/compiler/dialect/postgresql.clj)
- Graph synthesis / persistence:
  [src/clj/bitool/graph2.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/graph2.clj)

### Existing tests

- Modeling backend tests:
  [test/clj/bitool/modeling_automation_test.clj](/Users/aaryakulkarni/bitool/test/clj/bitool/modeling_automation_test.clj)
- Real UI E2E:
  `playwright/tests/e2e/specs/28-bitool-modeling-console-full.ui.spec.ts`

---

## 10. Canonical Proposal Schema

### 10.1 Column-level schema

Each proposal column should support:

```json
{
  "target_column": "driver_name",
  "type": "STRING",
  "nullable": true,
  "role": "attribute",
  "description": "Canonical driver display name",
  "source_columns": ["name", "alias"],
  "source_paths": ["$.data[].name", "$.data[].alias"],
  "base_expression": "silver.\"name\"",
  "transform": ["TRIM()", "UPPERCASE()"],
  "expression": "UPPER(TRIM(silver.\"name\"))",
  "rule_source": "manual_expression",
  "confidence": 0.87
}
```

Interpretation:

- `base_expression`
  - authoritative source seed for transform-chain generation
  - may be absent for fully manual expressions
- `transform`
  - optional transform chain as selected from the dropdown editor
  - only retained when it still matches the final expression
- `expression`
  - authoritative compiled expression for the column
- `source_columns`
  - lineage list used for validation, review, and downstream explainability

### 10.2 Mapping-level schema

Mappings mirror column expressions:

```json
{
  "target_column": "driver_name",
  "source_columns": ["name", "alias"],
  "base_expression": "silver.\"name\"",
  "transform": ["TRIM()", "UPPERCASE()"],
  "expression": "UPPER(TRIM(silver.\"name\"))",
  "rule_source": "manual_expression"
}
```

Rule:

- `columns[].expression` and `mappings[].expression` must stay synchronized.

### 10.3 Processing policy schema

Each Silver or Gold proposal should gain:

```json
{
  "processing_policy": {
    "business_keys": ["vehicle_id"],
    "event_time_column": "event_time",
    "sequence_column": "record_version",
    "ordering_strategy": "event_time_then_sequence",
    "late_data_tolerance": {
      "value": 10,
      "unit": "minutes"
    },
    "late_data_mode": "merge",
    "too_late_behavior": "quarantine",
    "reprocess_window": {
      "value": 24,
      "unit": "hours"
    }
  }
}
```

Supported semantics:

- `business_keys`
  - logical primary keys for dedupe / merge
- `event_time_column`
  - business event timestamp
- `sequence_column`
  - optional version or monotonic tie-breaker
  - supported initial types:
    - integer / bigint
    - timestamp
    - lexically ordered string versions only when explicitly configured
  - if present, it must be comparable within the target warehouse
  - ties after `event_time_column` and `sequence_column` comparison must be resolved deterministically by the engine using an additional internal tiebreaker
- `ordering_strategy`
  - `latest_event_time_wins`
  - `latest_sequence_wins`
  - `event_time_then_sequence`
  - `append_only`
- `late_data_tolerance`
  - maximum lateness within which records still affect the current model
- `late_data_mode`
  - `merge`
  - `append`
- `too_late_behavior`
  - `quarantine`
  - `drop`
  - `accept_anyway`
- `reprocess_window`
  - recent time range to rescan on every run for deterministic correction

### 10.4 Full Silver proposal example

The following example shows a full Silver proposal shape with schema, mappings, materialization, and processing policy together:

```json
{
  "layer": "silver",
  "target_warehouse": "postgresql",
  "target_model": "silver_entity_latest",
  "target_table": "public.silver_entity_latest",
  "source_layer": "bronze",
  "source_alias": "bronze",
  "source_table": "public.bronze_entity_events",
  "columns": [
    {
      "target_column": "entity_id",
      "type": "STRING",
      "nullable": false,
      "role": "business_key",
      "source_columns": ["entity_id"],
      "source_paths": ["$.data[].id"],
      "base_expression": "bronze.\"entity_id\"",
      "expression": "bronze.\"entity_id\"",
      "rule_source": "direct_mapping"
    },
    {
      "target_column": "event_time",
      "type": "TIMESTAMP",
      "nullable": false,
      "role": "timestamp",
      "source_columns": ["event_time_raw"],
      "source_paths": ["$.data[].event_time"],
      "base_expression": "bronze.\"event_time_raw\"",
      "transform": ["TO_TIMESTAMP()"],
      "expression": "CAST(bronze.\"event_time_raw\" AS TIMESTAMP)",
      "rule_source": "transform_chain"
    },
    {
      "target_column": "display_name",
      "type": "STRING",
      "nullable": true,
      "role": "attribute",
      "source_columns": ["name", "alias"],
      "source_paths": ["$.data[].name", "$.data[].alias"],
      "expression": "COALESCE(NULLIF(TRIM(bronze.\"name\"), ''), TRIM(bronze.\"alias\"))",
      "rule_source": "manual_expression"
    }
  ],
  "mappings": [
    {
      "target_column": "entity_id",
      "source_columns": ["entity_id"],
      "base_expression": "bronze.\"entity_id\"",
      "expression": "bronze.\"entity_id\"",
      "rule_source": "direct_mapping"
    },
    {
      "target_column": "event_time",
      "source_columns": ["event_time_raw"],
      "base_expression": "bronze.\"event_time_raw\"",
      "transform": ["TO_TIMESTAMP()"],
      "expression": "CAST(bronze.\"event_time_raw\" AS TIMESTAMP)",
      "rule_source": "transform_chain"
    },
    {
      "target_column": "display_name",
      "source_columns": ["name", "alias"],
      "expression": "COALESCE(NULLIF(TRIM(bronze.\"name\"), ''), TRIM(bronze.\"alias\"))",
      "rule_source": "manual_expression"
    }
  ],
  "materialization": {
    "mode": "merge",
    "keys": ["entity_id"]
  },
  "processing_policy": {
    "business_keys": ["entity_id"],
    "event_time_column": "event_time",
    "sequence_column": "record_version",
    "ordering_strategy": "event_time_then_sequence",
    "late_data_tolerance": {
      "value": 10,
      "unit": "minutes"
    },
    "late_data_mode": "merge",
    "too_late_behavior": "quarantine",
    "reprocess_window": {
      "value": 24,
      "unit": "hours"
    }
  }
}
```

---

## 11. UX Design

## 11.1 Modeling Console as primary medallion UX

The Modeling Console should expose four editable layers:

1. Schema
2. Processing Policy
3. Transformation Mapping
4. SQL / Validation / Review / Publish / Execute

### 11.2 Column authoring modes

Each mapping row should support:

- `Direct`
  - simple source reference
- `Transform`
  - dropdown chain editor
- `Expression`
  - full expression editor using the shared `expression-component`

Recommended row actions:

- `Expression`
- `Transform`
- later optional `Reset`

### 11.3 Expression editor

The shared expression editor should be embedded in Modeling Console as a modal panel.

Requirements:

- use the same component family as projection/filter authoring
- show:
  - functions
  - columns
  - operators
  - predicates
  - case expressions
- allow direct typing
- allow quoting-compatible PostgreSQL references
- hide unrelated graph-association features

### 11.4 Processing Policy card

Add a new card in proposal detail:

- `Business Keys`
- `Event Time Column`
- `Sequence Column`
- `Ordering Strategy`
- `Late Data Tolerance`
- `Late Data Mode`
- `Too-Late Behavior`
- `Reprocess Window`

Recommended controls:

- multiselect for key columns
- dropdowns for event-time and sequence columns
- select for strategy
- integer + unit inputs for tolerance and reprocess window

### 11.5 Layer-specific copy

Silver copy:

- focus on canonicalization, dedupe, latest state, and correction

Gold copy:

- focus on mart grain, window refresh, aggregate correctness, and late-arriving re-aggregation

---

## 12. Transform Semantics

### 12.1 Direct mappings

Examples:

```sql
silver."driver_id"
bronze.trip_id
CAST(silver."event_time" AS DATE)
```

### 12.2 Transform chain

Transform chain is a structured convenience path that generates expressions.

Examples:

- `TRIM()`
- `UPPERCASE()`
- `LOWERCASE()`
- `TO_DATE()`
- `TO_VARCHAR()`
- `SUBSTRING(1,3)`

The generated expression must become the canonical `expression`.

### 12.3 Manual expressions

Manual expressions allow richer logic:

```sql
CASE
  WHEN silver."status" = 'active' THEN UPPER(silver."driver_name")
  ELSE COALESCE(silver."driver_alias", silver."driver_name")
END
```

```sql
silver."first_name" || ' ' || silver."last_name"
```

### 12.4 Expression lineage

When a manual expression is saved:

- Bitool should infer `source_columns` by scanning referenced source fields
- if nested base expressions appear inside the expression, their lineage should be preserved

This is needed for:

- validation
- review diffs
- impact analysis
- generated documentation

This lineage extraction should be treated as best-effort in the initial implementation.

Recommended initial behavior:

- use regex or token-based extraction for:
  - `bronze.col`
  - `silver."QuotedCol"`
  - known JSON extraction patterns
- preserve prior lineage if parsing fails
- mark lineage as `unknown` or `partial` when extraction is incomplete

The system should not block proposal save solely because lineage extraction could not fully reconstruct all referenced source fields.

---

## 13. Safe Expression Boundary

### 13.1 Allowed

Bitool should allow:

- source references:
  - `bronze.col`
  - `silver."MixedCaseCol"`
- JSON extraction references already used by API->Bronze to Silver modeling
- scalar functions:
  - `TRIM`
  - `UPPER`
  - `LOWER`
  - `SUBSTRING`
  - `CAST`
  - `COALESCE`
  - `NULLIF`
  - `TO_DATE`
- arithmetic
- concatenation
- `CASE WHEN`
- aggregate functions where appropriate:
  - `SUM`
  - `AVG`
  - `MIN`
  - `MAX`
  - `COUNT(*)`

### 13.2 Rejected

Bitool should reject:

- statement-level SQL:
  - `DROP`
  - `ALTER`
  - `INSERT`
  - `DELETE`
  - `UPDATE`
  - `MERGE`
  - `COPY`
  - `PUT`
  - `CALL`
- comments:
  - `--`
  - `/* ... */`
- semicolon-terminated multi-statement input

### 13.3 Backend enforcement

This must be implemented in [automation.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/modeling/automation.clj), not only in frontend code.

The validator should:

- reject obviously unsafe tokens
- still allow expression strings complex enough to support real medallion modeling
- ensure at least one source reference or allowed JSON-extraction reference appears in non-aggregate expressions

---

## 14. Late / Out-of-Order Data Design

## 14.1 Problem

Bronze arrival order is not reliable.

Examples:

- webhook retries
- delayed API backfill pages
- connector retries
- batched event delivery
- out-of-order event payloads

If Silver and Gold rely on ingestion order rather than business ordering:

- latest-state tables become wrong
- dedupe logic keeps stale rows
- Gold aggregates drift
- dashboards lose consistency

### 14.2 Core rule

When present, `event_time_column` is authoritative over ingestion time.

### 14.3 Processing policy behaviors

#### `latest_event_time_wins`

Use:

- `business_keys`
- `event_time_column`

Semantics:

- latest event-time record per key wins

#### `latest_sequence_wins`

Use:

- `business_keys`
- `sequence_column`

Semantics:

- record with highest sequence/version per key wins

#### `event_time_then_sequence`

Use:

- `business_keys`
- `event_time_column`
- `sequence_column`

Semantics:

- order by event time descending
- tie-break by sequence descending
- if rows still tie after those comparisons, the runtime must apply a deterministic internal tiebreaker; behavior must never be left non-deterministic

#### `append_only`

Semantics:

- no latest-state correction
- used for immutable event logs or facts

### 14.4 Late-data tolerance

`late_data_tolerance` is the threshold within which late records are still merged into the target model.

Example:

- tolerance = `10 minutes`
- a record with `event_time` 09:50 arriving at 09:58 is accepted and merged
- a record with `event_time` 08:00 arriving at 09:58 may be:
  - quarantined
  - dropped
  - accepted anyway

### 14.5 Reprocess window

`reprocess_window` defines a rolling historical slice that is recomputed every run.

Examples:

- last 1 hour
- last 24 hours
- last 7 days

This is essential for idempotent correction of moderately late data.

---

## 15. Silver Compilation Design

Silver compilation should interpret `processing_policy`.

### 15.1 Canonical compile shape

Required logical structure for merge-capable materializations:

```sql
WITH staged AS (
  SELECT ...
  FROM bronze_source
  WHERE event_time >= now() - interval '24 hours'
),
ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY business_key
           ORDER BY event_time DESC, record_version DESC
         ) AS rn
  FROM staged
),
latest AS (
  SELECT * FROM ranked WHERE rn = 1
)
MERGE INTO silver_table t
USING latest s
ON ...
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT
```

Important rule:

- the relation used by `MERGE` must already be uniqueness-safe on the merge key
- therefore ranking or equivalent deduplication must happen before `MERGE`
- this is required, not optional, whenever:
  - a reprocess window is used, or
  - multiple source rows for the same business key may exist in the merge source

Without this rule, warehouses such as Snowflake and Databricks can fail with multiple-match merge errors.

### 15.2 Strategy mapping

- `latest_event_time_wins`
  - `ORDER BY event_time DESC`
- `latest_sequence_wins`
  - `ORDER BY sequence DESC`
- `event_time_then_sequence`
  - `ORDER BY event_time DESC, sequence DESC`
- `append_only`
  - no ranking CTE
  - `INSERT INTO ... SELECT ...`

### 15.3 Too-late handling

Required behavior:

- `accept_anyway`
  - include rows even if older than tolerance
- `quarantine`
  - write rows to a shared audit quarantine table
  - exclude from target merge
- `drop`
  - exclude and record metric

Initial quarantine contract:

- use a shared audit table:
  - `audit.model_quarantine`
- do not reuse Bronze `bad_records` directly, because late-data quarantine is a different semantic class

Suggested columns:

- `quarantine_id`
- `model_layer`
- `target_model`
- `target_table`
- `business_key_json`
- `event_time_utc`
- `arrival_time_utc`
- `ordering_strategy`
- `late_data_tolerance_json`
- `decision`
- `reason_code`
- `source_row_json`
- `run_id`
- `created_at_utc`

Suggested reason codes:

- `late_beyond_tolerance`
- `missing_event_time`
- `missing_business_key`
- `invalid_sequence_value`

---

## 16. Gold Compilation Design

Gold models are more varied, but the same processing policy still matters.

### 16.1 Latest-state marts

If Gold is a latest-state mart:

- use the same ranking semantics as Silver
- treat Silver as the source relation

### 16.2 Aggregate marts

If Gold is aggregate-oriented:

- use `reprocess_window` to determine the recomputation slice
- use `event_time_column` to derive aggregate windows and refresh scope
- require join-cardinality validation for structured joins used in metric computation

Examples:

- daily driver activity
- vehicle utilization by hour
- daily exceptions by fleet

### 16.3 Gold late-arrival correction

Late-arriving Silver rows should:

- trigger recomputation for the affected window or partition
- not require full-table rebuilds when a bounded refresh window is configured

For structured joins in Gold:

- the right side of a join should declare expected uniqueness where applicable
- if uniqueness cannot be proven or declared, validation should emit at least a warning about possible fan-out
- publish may optionally block for joins marked as `must_be_unique`

---

## 17. Warehouse-Specific Expectations

## 17.1 PostgreSQL

Expected implementation:

- `CREATE TABLE IF NOT EXISTS`
- merge-like behavior via:
  - delete + insert for keyed upserts
  - insert-only for append
- double-quoted identifiers for mixed-case columns
- timestamp/date casts using PostgreSQL syntax
- ranked-before-materialization still applies before delete + insert merge emulation

Important note:

- PostgreSQL support in this codebase should not assume native `MERGE`
- the expected path remains:
  - produce a uniqueness-safe ranked source CTE
  - materialize via delete + insert or append depending on mode

Current relevant dialect file:

- [src/clj/bitool/compiler/dialect/postgresql.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/compiler/dialect/postgresql.clj)

## 17.2 Snowflake

Expected implementation:

- native `MERGE`
- standard SQL expression compilation
- warehouse validation by explain/parse path

## 17.3 Databricks

Expected implementation:

- Delta-style materialization
- `MERGE INTO`
- partition-aware refresh and reprocessing
- eventually watermark-friendly semantics for streaming-oriented jobs

---

## 18. Graph Synthesis Design

The approved proposal should synthesize into standard graph primitives, not a separate engine.

### 18.1 Silver graph shape

```text
table -> projection -> mapping -> optional filter -> target -> output
```

Required mappings:

- `projection.columns[].expression` from proposal expressions
- `mapping.mapping[].expression` from proposal expressions
- preserve `transform` metadata when the expression still matches the transform chain
- target config should include:
  - `write_mode`
  - `merge_keys`
  - `event_time_column`
  - `sequence_column`
  - `late_data_tolerance`
  - `too_late_behavior`
  - `reprocess_window`

### 18.2 Gold graph shape

Same as Silver, but source relation is Silver.

### 18.3 Main canvas compatibility

When users inspect or edit the synthesized graph:

- expressions should remain intelligible
- mapping and projection nodes should preserve the same semantics
- no medallion-only hidden execution behavior should exist outside stored config

Schema evolution during reprocessing must also be predictable:

- if a new target column is added and historical source rows do not contain that field, recompilation should yield `NULL` unless a defaulting expression is explicitly modeled
- the engine should not require historical source payloads to already contain newly added fields
- explicit backfill logic remains a modeling concern, not an automatic schema-migration side effect

---

## 19. Frontend Implementation Plan

## 19.1 Modeling Console

Primary file:

- [resources/public/modelingConsole.js](/Users/aaryakulkarni/bitool/resources/public/modelingConsole.js)

Changes:

1. Add `Processing Policy` card
2. Add `Expression` action per mapping row
3. Host `expression-component` in a modal
4. Keep `Transform` button for quick chains
5. Sync edited expressions into:
   - `columns`
   - `mappings`
   - `source_columns`
   - `base_expression`
   - `transform` metadata when applicable
6. Add dirty-state handling for policy edits
7. Surface policy in review tab and compiled artifact summary

## 19.2 Shared expression editor reuse

Relevant file:

- [resources/public/expressionComponent.js](/Users/aaryakulkarni/bitool/resources/public/expressionComponent.js)

Needed behavior:

- allow a synthetic `selectedRectangle.items` source list
- avoid dependence on graph associations for modeling usage
- keep current function/operator UX

## 19.3 Shared transform editor reuse

Relevant file:

- [resources/public/transformEditorComponent.js](/Users/aaryakulkarni/bitool/resources/public/transformEditorComponent.js)

Needed behavior:

- continue to drive transform chains
- eventually expand available transform catalog

---

## 20. Backend Implementation Plan

## 20.1 Proposal validation and persistence

Primary file:

- [src/clj/bitool/modeling/automation.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/modeling/automation.clj)

Add or extend:

- richer `safe-proposal-expression?`
- policy validation helpers
- proposal defaults for `processing_policy`
- synchronization between `columns` and `mappings`
- static validation messages specific to policy errors

## 20.2 Compiler

Primary files:

- [src/clj/bitool/compiler/core.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/compiler/core.clj)
- dialect files under `src/clj/bitool/compiler/dialect/`

Add:

- compile-time interpretation of `processing_policy`
- ranking CTE generation
- reprocess-window source filtering
- warehouse-specific materialization for merge / append

## 20.3 Graph synthesis

Primary area:

- [src/clj/bitool/modeling/automation.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/modeling/automation.clj)

Extend synthesized target config with medallion policy fields.

---

## 21. Audit and Observability

Late-data handling should be visible, not implicit.

Recommended runtime reason codes:

- `accepted_in_window`
- `merged_latest`
- `quarantined_too_late`
- `dropped_too_late`
- `reprocessed_window`

Recommended metadata:

- target model
- business key
- event time
- arrival time
- ordering strategy
- tolerance used
- decision taken

Where possible, this should land in the audit schema already used by API-to-Bronze execution.

---

## 22. Testing Strategy

## 22.1 Backend tests

Primary file:

- [test/clj/bitool/modeling_automation_test.clj](/Users/aaryakulkarni/bitool/test/clj/bitool/modeling_automation_test.clj)

Add tests for:

- direct mappings
- transform-chain generated expressions
- manual expression acceptance
- manual expression rejection for statement-level SQL
- PostgreSQL quoted identifiers
- policy validation:
  - missing event-time column
  - missing business keys
  - invalid strategy combinations
- compile shape for each ordering strategy
- Silver latest-state merge SQL
- Gold aggregate refresh-window SQL

## 22.2 UI and E2E tests

Primary E2E spec:

- `playwright/tests/e2e/specs/28-bitool-modeling-console-full.ui.spec.ts`

Required coverage:

- create Silver proposal
- edit with transform chain
- edit with manual expression
- set processing policy
- compile / validate / review / publish / execute
- create Gold proposal
- repeat same editing flows
- assert DB-backed success
- assert Silver and Gold target tables have rows

## 22.3 Late-data correctness tests

Need dedicated fixtures with out-of-order records:

- earlier event arrives later
- same key with newer event-time overwrites old row
- same key with older event-time does not overwrite newer row
- very late row beyond tolerance is quarantined or dropped based on policy

---

## 23. Rollout Plan

### Phase 1

- Modeling Console expression editor for mappings
- keep dropdown transforms
- richer backend expression validation
- PostgreSQL path first

### Phase 2

- add `processing_policy` schema and UI
- compile Silver latest-state strategies
- support reprocess windows

### Phase 3

- add Gold late-data and aggregate refresh semantics
- add quarantine/drop runtime behavior
- expand transform catalog

### Phase 4

- expose same transform and policy concepts in main canvas nodes
- keep proposal JSON as canonical source of truth
- improve explainability and diffs in review workflow

---

## 24. Open Questions

1. Should `too_late_behavior = accept_anyway` bypass tolerance entirely or only bypass quarantine/drop while still flagging the row?
2. Should Gold always expose `processing_policy`, or only when the target pattern is latest-state or windowed aggregate?
3. For Databricks, should watermark semantics remain advisory at compile-time or become a first-class runtime orchestration concept?
4. Should transform catalogs remain warehouse-agnostic, or should the UI expose warehouse-specific function sets once target warehouse is known?

---

## 25. Recommendation

Implement this as a hybrid product model:

- Modeling Console is the opinionated medallion authoring UX
- graph synthesis remains the execution representation
- canvas remains available for advanced users

But all of them must share:

- one proposal schema
- one transform representation
- one processing-policy representation
- one validation boundary

That is the only way to avoid product drift while still giving users both a guided medallion workflow and a low-level graph authoring escape hatch.

---

## 26. Gold Transform and Modeling Extension

Gold modeling should be treated as a business-modeling system, not just a second pass of column cleanup.

The product should therefore extend beyond column-level transforms into:

- aggregations
- reusable business metrics
- cross-domain joins
- dimensional modeling
- time intelligence
- SCD-aware lookup semantics
- serving optimization

### 26.1 Core Gold transform families

#### A. Aggregation transforms

These should be first-class, not encoded only as raw SQL strings.

Examples:

- `sum`
- `avg`
- `min`
- `max`
- `count`
- `count_distinct`
- `sum_if`
- `count_if`
- weighted average
- ratio metrics

Suggested schema:

```json
{
  "metrics": [
    {
      "name": "total_distance",
      "kind": "aggregate",
      "aggregation": "sum",
      "expression": "silver.\"distance\"",
      "filter_expression": null
    }
  ]
}
```

#### B. Business metric transforms

Gold should support reusable named metrics that are more semantic than plain aggregates.

Examples:

- utilization rate
- idle time percent
- harsh event rate
- fuel efficiency
- safety score
- SLA compliance

These should compile to deterministic SQL but be represented in proposal JSON as named semantic definitions.

Suggested schema:

```json
{
  "metrics": [
    {
      "name": "utilization_rate",
      "kind": "derived_metric",
      "expression": "SUM(silver.\"drive_minutes\") / NULLIF(SUM(silver.\"available_minutes\"), 0)"
    }
  ]
}
```

#### C. Join and enrichment transforms

Gold often requires combining multiple Silver models.

These joins should be structured, not only manual SQL.

Examples:

- fact-to-dimension join
- latest dimension join
- as-of join
- bridge join
- lookup enrichment join

Suggested schema:

```json
{
  "joins": [
    {
      "name": "vehicle_lookup",
      "join_type": "left",
      "source_model": "silver_vehicle",
      "alias": "vehicle",
      "right_side_uniqueness": {
        "mode": "declared_unique",
        "keys": ["vehicle_id"]
      },
      "conditions": [
        {
          "left_expression": "silver.\"vehicle_id\"",
          "right_expression": "vehicle.\"vehicle_id\""
        }
      ],
      "join_semantics": "latest_dimension"
    }
  ]
}
```

#### D. Dimensional modeling transforms

Gold should support explicit modeling constructs:

- fact tables
- dimension tables
- bridge tables
- snapshot marts

This should be reflected in proposal metadata:

- `entity_kind`
- `grain`
- `dimension_columns`
- `measure_columns`

#### E. Time-intelligence transforms

Gold needs time-aware semantics beyond simple casts.

Examples:

- daily / weekly / monthly rollups
- rolling 7d / 30d / 90d metrics
- period-over-period deltas
- MTD / QTD / YTD
- lag / lead
- running totals

Suggested schema:

```json
{
  "grain": {
    "dimensions": ["vehicle_id", "event_date"],
    "time_bucket": "day"
  },
  "time_intelligence": {
    "window_type": "rolling_30d",
    "event_time_column": "event_time"
  }
}
```

### 26.2 Gold UX extension in Modeling Console

To support the above, Gold authoring should expand from a single mapping card to five editing areas:

1. `Columns`
   - direct mapping
   - transform chain
   - manual expression
2. `Metrics`
   - named measures
   - aggregate functions
   - derived KPI formulas
3. `Joins`
   - source models
   - join types
   - join conditions
   - SCD/as-of mode
4. `Grain`
   - fact grain
   - dimensions
   - time bucket
   - partitioning intent
5. `Serving`
   - deferred from this document unless and until `serving_policy` is fully specified

### 26.3 Gold proposal JSON extension

Gold proposals should evolve from:

- `columns`
- `mappings`
- `group_by`

to:

- `columns`
- `mappings`
- `metrics`
- `joins`
- `grain`
- `processing_policy`
- `semantic_contract`

Suggested example:

```json
{
  "layer": "gold",
  "target_model": "gold_daily_entity_metrics",
  "source_model": "silver_entity_events",
  "grain": {
    "dimensions": ["entity_id", "event_date"],
    "time_bucket": "day"
  },
  "metrics": [
    {
      "name": "total_distance",
      "kind": "aggregate",
      "aggregation": "sum",
      "expression": "silver.\"distance\""
    },
    {
      "name": "avg_speed",
      "kind": "aggregate",
      "aggregation": "avg",
      "expression": "silver.\"speed\""
    },
    {
      "name": "utilization_rate",
      "kind": "derived_metric",
      "expression": "SUM(silver.\"drive_minutes\") / NULLIF(SUM(silver.\"available_minutes\"), 0)"
    }
  ]
}
```

### 26.4 Gold compilation direction

Compiler changes should support structured Gold authoring instead of only flat column-expression compilation.

High-level compile flow:

1. build source CTEs
2. apply joins
3. apply row filters
4. derive projected columns
5. compute aggregates and metrics
6. apply refresh-window / late-data rules
7. materialize into the Gold target

This will likely require:

- extending compiler IR
- adding structured metric compilation
- adding structured join compilation
- adding window-expression support

---

## 27. Expanded Transform Catalog

This section captures a broader brainstorm of transforms Bitool should eventually support.

### 27.1 Bronze -> Silver transform catalog

Bronze -> Silver should support more than direct mappings and light string cleanup.

Recommended additions:

- JSON flatten / explode
- root vs child-entity extraction
- path rename / alias standardization
- boolean normalization
- timestamp parsing with timezone handling
- numeric cleanup and normalization
- `coalesce`
- `null_if`
- dedupe by:
  - event time
  - sequence
  - latest non-null
- standard derived columns:
  - `event_date`
  - `ingest_date`
  - `source_system`
  - `load_batch_id`
- survivorship rules:
  - prefer source A
  - prefer non-null
  - prefer latest timestamp
- quality-flag derivation:
  - invalid enum
  - missing key
  - parse failure
- quarantine / reject routing

These should be split across:

- column transforms
- processing policy
- quality / reject policy

### 27.2 Silver -> Gold transform catalog

Gold needs a richer business-modeling catalog.

Recommended additions:

- aggregate measures
- reusable KPIs
- semantic metrics
- fact vs dimension tagging
- surrogate key generation
- SCD1 and SCD2 dimension handling
- as-of joins
- bridge-table generation
- top-N / ranking
- percentile / median / approximate distinct
- anomaly flags
- cohort segmentation
- business calendar transforms
- hierarchy rollups:
  - fleet
  - region
  - org
- dashboard-serving denormalization
- pre-aggregation / serving tables

### 27.3 Warehouse-aware transform support

The UI should remain warehouse-agnostic at the product layer where possible, but the compiler must know which features are portable and which are dialect-specific.

Recommended split:

- warehouse-agnostic transform catalog in proposal JSON
- dialect adapter layer in compiler
- validation warnings when a transform is only partially portable

Examples:

- portable:
  - `TRIM`
  - `UPPER`
  - `LOWER`
  - `CASE`
  - `COALESCE`
- less portable:
  - warehouse-specific timestamp parsing
  - approximate distinct functions
  - serving optimization hints
  - Z-order / clustering directives

### 27.4 Out of scope note

Domain-specific template packs, starter marts, or vertical accelerators are intentionally out of scope for this implementation document.

They belong in a separate product or accelerators design once the generic transform and processing-policy engine is complete.

### 27.5 Recommended implementation order

To avoid overbuilding too early:

1. complete manual expression support in Modeling Console
2. add Gold metrics as first-class structured objects
3. add Gold grain and time-bucket support
4. add structured joins
5. add late-data-aware aggregate refresh
6. add reusable template packs

This order maximizes business value while keeping the compiler and UI evolution incremental.
