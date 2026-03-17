# Schema Inference and Optional Type Inference Tech Design

## 1. Purpose

This document defines the design for adding schema inference and optional type inference to BiTool’s `API -> Bronze` runtime.

The goal is to improve usability and reduce manual field configuration while preserving the Bronze principles defined in [API to Bronze Best Practices.md](/Users/aaryakulkarni/bitool/docs/API%20to%20Bronze%20Best%20Practices.md).

This design is implementation-oriented and intended to be followed by code changes in this repository.

---

## 2. Current Behavior

Today, BiTool Bronze creation works like this:

- `json_explode_rules` defines record extraction,
- `selected_nodes` defines promoted Bronze columns,
- promoted columns are created from configured paths,
- promoted columns are currently tolerant and largely string-based,
- the raw source object is always preserved in `payload_json`.

This behavior is simple and safe, but it requires users to manually define promoted fields and does not help with evolving source schemas.

Relevant implementation points:

- Bronze row building in [src/clj/bitool/ingest/bronze.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ingest/bronze.clj)
- Runtime table creation in [src/clj/bitool/ingest/runtime.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ingest/runtime.clj)
- API config normalization in [src/clj/bitool/graph2.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/graph2.clj)

---

## 3. Goals

### 3.1 Functional goals

- Infer candidate Bronze columns from API record shape.
- Optionally infer column data types.
- Allow users to accept, override, or disable inferred fields.
- Support schema evolution without breaking ingestion.
- Preserve raw payload as the source of truth.

### 3.2 Non-goals

- Fully automatic flattening of every nested field into Bronze columns.
- Replacing Silver typing and normalization.
- Strict schema-on-write validation of all incoming payloads.
- Perfect semantic typing of arbitrary JSON.

---

## 4. Product Principles

The feature should follow these rules:

1. Inference is advisory first, not destructive.
2. Raw payload remains authoritative.
3. Missing inferred fields do not fail the run.
4. New fields should be surfaced safely.
5. Type inference is optional and conservative.
6. Bronze remains replayable and schema-drift tolerant.

---

## 5. User Experience

### 5.1 API node behavior

The `Ap` node should support three promoted-schema modes per endpoint:

- `manual`
- `infer`
- `hybrid`

#### `manual`

Only use explicitly configured `selected_nodes`.

#### `infer`

Infer promoted fields from sampled API records.

#### `hybrid`

Use configured `selected_nodes` plus inferred candidates.

### 5.2 UI behavior

The API editor should provide:

- `schema_mode` selector,
- `sample_records` count,
- `max_inferred_columns`,
- `type_inference_enabled`,
- inferred field preview table,
- confidence indicator,
- accept/reject/rename controls,
- “pin as manual field” control.

### 5.3 Runtime behavior

At runtime:

- if inferred schema already exists and is approved, use it,
- if endpoint is configured for inference refresh, resample and compare,
- if drift is additive and safe, optionally evolve,
- if drift is incompatible, flag it and continue with safe behavior.

---

## 6. Data Model Changes

### 6.1 Endpoint config additions

Extend endpoint config in `Ap` with:

```clojure
{:schema_mode "manual"               ; manual | infer | hybrid
 :sample_records 100
 :max_inferred_columns 100
 :type_inference_enabled true
 :schema_evolution_mode "additive"   ; none | additive | advisory
 :inferred_fields []
 :field_overrides []
 :inference_version 1}
```

### 6.2 Field descriptor shape

Add a stable inferred-field descriptor shape:

```clojure
{:path "$.data[].vehicle.id"
 :column_name "data_vehicle_id"
 :source_kind "inferred"             ; inferred | manual
 :enabled true
 :type "STRING"                      ; inferred or overridden final type
 :observed_types ["STRING" "NULL"]
 :nullable true
 :confidence 0.97
 :sample_coverage 0.94
 :depth 3
 :array_mode "scalar"                ; scalar | object | array_scalar | array_object
 :override_type nil
 :notes nil}
```

### 6.3 Audit/control tables

Add product tables for schema inference state, for example:

- `audit.inferred_schema_snapshot`
- `audit.schema_drift_event`

#### `audit.inferred_schema_snapshot`

Recommended columns:

- `source_system`
- `endpoint_name`
- `graph_version_id`
- `schema_hash`
- `fields_json`
- `sample_size`
- `created_at_utc`
- `created_by`

#### `audit.schema_drift_event`

Recommended columns:

- `run_id`
- `source_system`
- `endpoint_name`
- `drift_type`
- `field_path`
- `old_descriptor_json`
- `new_descriptor_json`
- `severity`
- `created_at_utc`

---

## 7. Inference Scope

### 7.1 What to infer

Infer only fields inside the logical record after `json_explode_rules` are applied.

That means inference happens after record extraction, not on the whole page envelope.

### 7.2 Which fields are eligible

Eligible:

- scalar values,
- nested scalar leaves,
- arrays of scalars,
- low-depth nested object leaves.

Not eligible by default:

- very deep structures,
- arrays of objects beyond a configured depth,
- large embedded blobs,
- high-cardinality nested collections that would explode the schema.

### 7.3 Recommended limits

Use guardrails:

- max depth: `4`
- max inferred columns: configurable, default `100`
- max sampled records: configurable, default `100`
- skip paths with giant arrays or objects above size threshold

---

## 8. Schema Inference Algorithm

### 8.1 Step 1: sample records

Sample logical records after page extraction.

Recommended sources:

- manual preview run in UI,
- runtime sample from first N records,
- optionally persisted historical sample.

### 8.2 Step 2: flatten candidate paths

Walk each sampled record and emit candidate leaf paths.

Example:

```json
{
  "id": "t1",
  "vehicle": {"id": "v1", "name": "truck-1"},
  "metrics": {"speed": 42.5},
  "tags": ["cold", "regional"]
}
```

Candidate paths:

- `$.id`
- `$.vehicle.id`
- `$.vehicle.name`
- `$.metrics.speed`
- `$.tags[]`

### 8.3 Step 3: aggregate observations

For each path collect:

- observed type set,
- presence count,
- null count,
- sample coverage,
- max depth,
- array mode.

### 8.4 Step 4: apply inclusion heuristics

Include fields that satisfy configurable heuristics such as:

- coverage above minimum threshold,
- scalar or allowed array mode,
- not on denylist,
- within depth and column limits.

### 8.5 Step 5: derive normalized column names

Convert path to safe column name:

- remove root markers,
- replace separators with `_`,
- normalize arrays,
- deduplicate conflicting names.

Example:

- `$.vehicle.id` -> `vehicle_id`
- `$.tags[]` -> `tags`
- `$.driver.profile.name` -> `driver_profile_name`

### 8.6 Step 6: attach confidence

Confidence should reflect:

- type consistency,
- field presence rate,
- path stability,
- naming ambiguity.

Confidence should be used for UI guidance, not for hard failure.

---

## 9. Optional Type Inference

### 9.1 Design principle

Type inference should be conservative and opt-in.

Raw payload remains the truth even if inferred type is wrong.

### 9.2 Supported inferred Bronze types

Use a narrow Bronze-safe type set:

- `STRING`
- `BOOLEAN`
- `INT`
- `BIGINT`
- `DOUBLE`
- `DATE`
- `TIMESTAMP`

Fallback type:

- `STRING`

### 9.3 Type observation rules

Map observed JSON values to normalized logical types:

- string -> `STRING`
- boolean -> `BOOLEAN`
- integer-sized number -> `INT` or `BIGINT`
- fractional number -> `DOUBLE`
- ISO date string -> `DATE`
- ISO timestamp string -> `TIMESTAMP`
- null -> `NULL`
- everything else -> `STRING`

### 9.4 Type decision rules

Recommended inference rules:

- all null or mixed incompatible types -> `STRING`
- integer + null -> `INT`
- bigint + int + null -> `BIGINT`
- double + int + null -> `DOUBLE`
- boolean + null -> `BOOLEAN`
- timestamp strings consistently parseable -> `TIMESTAMP`
- date strings consistently parseable -> `DATE`
- anything ambiguous -> `STRING`

### 9.5 Safety rule

If confidence is below threshold or observed types are mixed:

- set final Bronze type to `STRING`.

Do not make ingestion fragile in pursuit of stronger typing.

---

## 10. Schema Evolution Model

### 10.1 Modes

Support:

- `none`
- `advisory`
- `additive`

#### `none`

Use the currently approved inferred/manual schema only.

#### `advisory`

Detect and log drift, but do not change Bronze schema automatically.

#### `additive`

Allow new inferred columns to be added automatically when they satisfy safe rules.

### 10.2 Compatible changes

Safe additive changes:

- new optional field,
- wider nullable interpretation,
- `INT` to `BIGINT`,
- numeric to `STRING` fallback.

### 10.3 Incompatible changes

Log and do not auto-apply:

- object replacing scalar,
- scalar replacing object,
- timestamp/date instability,
- field-name collisions,
- large nested arrays becoming promoted fields.

---

## 11. Runtime Integration

### 11.1 New namespace

Add:

- `src/clj/bitool/ingest/schema_infer.clj`

Responsibilities:

- flatten sampled records,
- aggregate field observations,
- infer candidate schema,
- compare with prior approved schema,
- emit drift events.

### 11.2 Bronze writer changes

Extend [bronze.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ingest/bronze.clj) to:

- build promoted columns from manual + inferred descriptors,
- use descriptor column names instead of only path-derived names,
- optionally coerce values for inferred types,
- fall back safely to string rendering.

### 11.3 Runtime changes

Extend [runtime.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ingest/runtime.clj) to:

- sample records during preview or runtime,
- resolve effective schema before table creation,
- compare current inferred schema with stored snapshot,
- create/evolve target DDL when allowed,
- record schema drift events.

### 11.4 Graph/model changes

Extend [graph2.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/graph2.clj) to:

- normalize schema inference config,
- persist inferred field descriptors and overrides,
- validate inference mode options.

---

## 12. DDL Strategy

### 12.1 Initial table creation

When Bronze table does not exist:

- create standard Bronze envelope columns,
- add manual promoted columns,
- add enabled inferred columns,
- use inferred or overridden types when enabled,
- fall back to `STRING` when unsafe.

### 12.2 Existing table evolution

For additive mode:

- detect missing columns,
- add only safe new columns,
- never drop columns automatically,
- never narrow types automatically.

### 12.3 Type evolution

Allowed automatic widening:

- `INT` -> `BIGINT`
- `INT`/`BIGINT` -> `DOUBLE`
- any type -> `STRING`

Disallowed automatic narrowing:

- `STRING` -> typed
- `DOUBLE` -> `INT`
- `TIMESTAMP` -> `DATE`

Those need manual approval.

---

## 13. UI Design

### 13.1 API panel additions

Add to the API endpoint editor:

- `Schema Mode`
- `Sample Records`
- `Max Inferred Columns`
- `Type Inference Enabled`
- `Schema Evolution Mode`
- `Preview Inferred Schema`

### 13.2 Inferred schema table

Display:

- path,
- column name,
- inferred type,
- observed types,
- nullable,
- coverage,
- confidence,
- enabled toggle,
- override type control.

### 13.3 Preview workflow

Recommended flow:

1. user selects endpoint,
2. clicks preview inference,
3. system samples records,
4. UI shows inferred fields,
5. user edits/accepts,
6. save persists approved descriptors.

---

## 14. Observability

### 14.1 Run detail additions

Add to endpoint run detail:

- inferred schema version used,
- schema drift count,
- columns added count,
- type coercion fallback count.

### 14.2 Alerts

Schema drift should optionally alert when:

- incompatible drift occurs,
- inference exceeds max columns,
- field collisions occur,
- type instability crosses threshold.

---

## 15. Failure Handling

### 15.1 Safe fallback

If schema inference fails:

- do not fail the entire endpoint by default,
- fall back to manual schema if present,
- otherwise write only Bronze envelope plus `payload_json`,
- record an inference failure event.

### 15.2 Unsafe drift

If drift is incompatible and mode is not additive-safe:

- keep using prior approved schema,
- log drift event,
- mark run with warning or partial-success metadata,
- do not auto-mutate production schema.

---

## 16. Phased Implementation

### Phase 1: advisory inference

- sample records,
- infer candidate fields,
- expose preview in UI,
- save approved inferred descriptors,
- no automatic schema evolution yet,
- types mostly default to `STRING`.

### Phase 2: optional type inference

- conservative type inference,
- UI override support,
- runtime coercion with fallback to string,
- additive DDL with safe types.

### Phase 3: schema evolution and drift management

- additive schema evolution mode,
- drift event persistence,
- drift monitoring UI,
- alerts.

---

## 17. Acceptance Criteria

This feature is ready when:

- users can preview inferred fields from sampled API records,
- approved inferred fields are persisted in endpoint config,
- Bronze tables can be created from manual + inferred descriptors,
- optional type inference is available and conservative,
- incompatible drift does not break ingestion,
- drift events are recorded,
- raw payload remains preserved regardless of inferred schema state.

---

## 18. Recommended First Implementation Slice

Implement this first:

1. `schema_mode` and inference config persistence in `Ap`.
2. New `schema_infer.clj` namespace with record-path inference.
3. API UI preview of inferred fields.
4. Bronze writer support for approved inferred descriptors.
5. Advisory-only drift logging.

That slice delivers real user value without taking on risky automatic schema mutation too early.
