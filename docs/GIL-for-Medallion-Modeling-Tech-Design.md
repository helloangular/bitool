# GIL for Medallion Modeling — Technical Design

## 1. Purpose

This document defines how Bitool should use a medallion-focused extension of GIL (Graph Intent Language) so that a user can express intent such as:

- "create Silver schema from current Bronze schema"
- "generate Bronze to Silver mapping for trips"
- "create Gold mart for daily fleet utilization"
- "add dedupe and merge logic to the Silver trip model"

and have an LLM produce a constrained intermediate representation that Bitool validates and deterministically compiles into a real graph.

This is the Bitool equivalent of the ISL and IRL pattern used in the sibling `medtronic-cicd` project:

- natural language -> intermediate intent language,
- intermediate intent language -> deterministic compiler,
- compiler output -> executable artifact.

Here, the executable artifact is:

- a Bitool graph,
- and eventually compiled SQL / Databricks job definitions for Silver and Gold execution.

---

## 2. Why This Exists

Bitool already has:

- IGL as the general graph representation,
- GIL as the generic LLM-to-graph proposal contract,
- transformation nodes like projection, filter, join, mapping, aggregation, sorter, and target.

What is missing is a domain-specific intent layer for medallion modeling.

Without that layer, the user still has to manually translate intent into:

- entity boundaries,
- target schemas,
- merge keys,
- mapping rules,
- incremental semantics,
- node selection and graph shape.

This document fills that gap.

---

## 3. Relationship to Existing Docs

This document sits on top of:

- [GIL_Design_v1_0.md](./GIL_Design_v1_0.md)
- [Silver-Gold-Automation-Tech-Design.md](./Silver-Gold-Automation-Tech-Design.md)
- [Bitool Graph -> SQL Compiler -> Databricks Job Execution Design.md](./Bitool%20Graph%20-%3E%20SQL%20Compiler%20-%3E%20Databricks%20Job%20Execution%20Design.md)

Role boundaries:

- generic GIL defines the base NL -> GIL -> graph pipeline,
- this document defines medallion-specific GIL intents and schemas,
- Silver/Gold automation defines proposal heuristics and metadata,
- the compiler doc defines graph -> SQL -> Databricks execution.

---

## 4. Design Principles

1. LLM proposes, compiler decides.
   The LLM may draft medallion GIL, but the compiler is deterministic and authoritative.

2. Medallion semantics must be explicit.
   Silver and Gold models require explicit materialization, keys, grain, and dependency rules.

3. Intent must compile into standard graph primitives.
   The system should reuse current Bitool node types rather than inventing a second execution engine.

4. SQL remains the downstream execution target.
   GIL builds the graph; the graph later compiles to SQL and Databricks jobs.

5. Every generated model is reviewable.
   Dry-run, diff, validation, and explanation are required before apply.

---

## 5. User Intents

The medallion GIL layer should support a small set of high-value intents first.

### 5.1 Schema proposal intents

- `propose_silver_schema`
- `propose_gold_schema`

Example prompts:

- "Create Silver schema from Bronze trips and drivers"
- "Propose Gold marts for vehicle utilization and idle time"

### 5.2 Mapping intents

- `build_bronze_to_silver_mapping`
- `build_silver_to_gold_mapping`
- `patch_mapping`

Example prompts:

- "Map current Bronze trip schema into a canonical Silver trip model"
- "Create Gold aggregation by day and region"

### 5.3 Materialization intents

- `set_materialization`
- `add_incremental_merge`
- `add_partition_refresh`

### 5.4 Graph patch intents

- `add_node`
- `remove_node`
- `replace_node`
- `insert_between`
- `update_contract`

These patch intents are important because most real use is incremental, not greenfield.

---

## 6. Conceptual Architecture

```text
User intent
  ->
LLM proposes Medallion GIL
  ->
normalize
  ->
validate
  ->
compile to Bitool graph plan
  ->
dry-run preview / diff / explanation
  ->
apply graph mutation
  ->
compile graph to SQL artifact
  ->
Databricks execution
```

The medallion GIL layer is not the final execution format. It is the structured proposal language between user intent and graph mutation.

---

## 7. Medallion GIL Top-Level Shape

Suggested top-level structure:

```clojure
{:mgil-version "1.0"
 :intent :build_model
 :layer "silver"
 :model_name "silver_trip"
 :source_models [{:layer "bronze" :name "trip_raw"}]
 :goal "canonical trip model with merge semantics"
 :contract {...}
 :graph_plan {...}
 :explanations [...]
 :confidence 0.93}
```

Top-level fields:

- `:mgil-version`
- `:intent`
- `:layer`
- `:model_name`
- `:source_models`
- `:goal`
- `:contract`
- `:graph_plan`
- `:validation_hints`
- `:explanations`
- `:confidence`

---

## 8. Model Contract

The medallion GIL contract must make explicit what generic GIL does not.

### 8.1 Required contract fields

```clojure
{:target_table "sheetz_telematics.silver.trip"
 :materialization {:mode "merge"
                   :keys ["trip_id"]
                   :partition_columns ["event_date"]
                   :refresh_strategy "incremental"}
 :grain {:type "entity"
         :keys ["trip_id"]}
 :columns [{:name "trip_id" :type "STRING" :nullable false}
           {:name "vehicle_id" :type "STRING" :nullable true}
           {:name "start_time_utc" :type "TIMESTAMP" :nullable true}]
 :tests [{:type "unique_key" :columns ["trip_id"]}
         {:type "not_null" :columns ["trip_id"]}]}
```

### 8.2 Why this matters

This contract gives the compiler explicit ownership for:

- merge keys,
- partition columns,
- target schema,
- model grain,
- validation tests,
- refresh strategy.

Without it, the LLM would be guessing at graph semantics and the SQL compiler would have nowhere authoritative to read from.

---

## 9. Graph Plan Representation

The medallion GIL graph plan should compile into actual Bitool nodes.

Example:

```clojure
{:nodes [{:node-ref "src_trip"
          :type "source_model"
          :config {:layer "bronze"
                   :model "trip_raw"}}
         {:node-ref "proj_trip"
          :type "projection"
          :config {:columns [...]}}
         {:node-ref "map_trip"
          :type "mapping"
          :config {:mapping [...]}}
         {:node-ref "filter_trip"
          :type "filter"
          :config {:sql "trip_id is not null"}}
         {:node-ref "target_trip"
          :type "target"
          :config {:target_table "sheetz_telematics.silver.trip"
                   :materialization {:mode "merge"
                                     :keys ["trip_id"]}}}]
 :edges [{:from "src_trip" :to "proj_trip"}
         {:from "proj_trip" :to "map_trip"}
         {:from "map_trip" :to "filter_trip"}
         {:from "filter_trip" :to "target_trip"}]}
```

The key point is:

- the medallion GIL is not raw graph2 params only,
- it is a semantically richer layer that later lowers into actual node configs.

---

## 10. New Logical Node Concepts

Bitool may need logical node concepts in the medallion GIL layer even if they compile to existing physical nodes.

### 10.1 `source_model`

Represents a Bronze, Silver, or Gold model as a graph input.

This is not necessarily a brand-new canvas node. It may compile into an existing table/source node shape.

### 10.2 `materialization_contract`

Represents:

- mode,
- keys,
- partitions,
- refresh strategy.

This may compile into Output or Target configuration rather than a visible node.

### 10.3 `model_test`

Represents:

- uniqueness,
- non-null,
- accepted values,
- row-count bounds,
- referential checks.

These may compile into metadata and downstream validation jobs rather than visible graph nodes.

### 10.4 `dependency_ref`

Represents upstream model references for Silver and Gold dependency orchestration.

---

## 11. Lowering Rules

The medallion GIL compiler should lower intent into physical graph structures using stable rules.

### 11.1 Silver schema proposal

Intent:

- `propose_silver_schema`

Compiler behavior:

- read Bronze profile and schema snapshot metadata,
- derive candidate entity contract,
- generate projection + mapping + target plan,
- attach merge contract if entity keys are present,
- emit dry-run graph plan.

### 11.2 Bronze to Silver mapping

Intent:

- `build_bronze_to_silver_mapping`

Compiler behavior:

- resolve source paths to promoted Bronze columns,
- generate mapping expressions,
- insert filter, dedupe, and cast rules as needed,
- attach model tests and target materialization contract.

### 11.3 Silver to Gold mapping

Intent:

- `build_silver_to_gold_mapping`

Compiler behavior:

- resolve input Silver models,
- derive joins and aggregation grain,
- attach Gold materialization mode,
- generate aggregate and presentation projections.

---

## 12. NL -> Medallion GIL Prompting Contract

The LLM should not be asked to emit arbitrary graph JSON.

It should emit only medallion GIL according to a constrained schema.

Required prompt inputs:

- current graph context if patching,
- source schema and sample profile,
- existing model contracts,
- allowed node types,
- allowed materialization modes,
- allowed test types,
- validation errors from previous attempt if retrying.

Required LLM outputs:

- intent,
- target layer,
- target model contract,
- graph plan,
- rationale,
- uncertainty flags.

---

## 13. Validation

Medallion GIL validation must happen before graph compilation.

### 13.1 Structural validation

- valid top-level intent,
- supported layer,
- model name present,
- source models present,
- graph references valid.

### 13.2 Contract validation

- materialization mode allowed,
- merge keys required for merge mode,
- grain required for aggregate models,
- target columns unique,
- tests reference valid columns.

### 13.3 Semantic validation

- Bronze paths resolve to known promoted columns or known source paths,
- aggregate expressions match declared grain,
- join intents specify left/right semantics,
- Gold models do not reference undefined Silver models.

### 13.4 Safety validation

- no unsupported node types,
- no invalid target overwrite pattern,
- no ambiguous self-join aliasing,
- no illegal cycle introduction during patch.

---

## 14. Dry-Run and Review

Before apply, Bitool should show:

- proposed graph diff,
- proposed model contract,
- proposed mapping table,
- proposed SQL preview if compilation is available,
- validation warnings,
- confidence score,
- rationale from the LLM,
- lineage impact.

This is critical. Medallion GIL should be a reviewable planning language, not a direct mutation tool.

---

## 15. Compiler Outputs

A valid medallion GIL proposal should produce:

1. Graph mutation plan
2. Normalized model contract
3. Mapping specification
4. Lineage preview
5. Optional SQL preview

After apply, the resulting graph becomes the authoritative IGL/graph representation.

---

## 16. Relationship to SQL Compilation

The sequence should be:

1. user intent -> medallion GIL
2. medallion GIL -> graph
3. graph -> logical plan
4. logical plan -> SQL IR
5. SQL IR -> Databricks SQL

This keeps the boundaries clean:

- medallion GIL owns intent-to-graph construction,
- the SQL compiler owns graph-to-SQL compilation.

The LLM should not skip the graph and emit final SQL as the primary system path for Bitool modeling.

---

## 17. Metadata Model

Bitool should persist medallion GIL artifacts such as:

- `mgil_proposal`
- `mgil_validation_result`
- `mgil_compile_plan`
- `mgil_apply_result`

Recommended fields:

- proposal id,
- graph id,
- graph version,
- target model,
- layer,
- prompt,
- normalized mgil json,
- validation result,
- compiler version,
- approved by,
- created at,
- applied at.

This is separate from but linked to:

- `model_proposal`
- `model_release`
- `compiled_model_artifact`

`mgil_proposal` is about intent-to-graph construction.

`model_release` and `compiled_model_artifact` are about executable model lifecycle.

---

## 18. Example End-to-End Intents

### 18.1 Bronze to Silver

Prompt:

- "Create a Silver trip model from Bronze trip_raw. Use trip_id as merge key, keep latest record by updated_at, and add vehicle status from the Silver vehicle model."

Expected medallion GIL result:

- one source model for Bronze trip_raw,
- one source model for Silver vehicle,
- projection node,
- mapping node,
- join node,
- filter or dedupe node,
- target with merge contract.

### 18.2 Silver to Gold

Prompt:

- "Create a Gold daily vehicle utilization mart aggregated by day, region, and vehicle type."

Expected medallion GIL result:

- Silver source models,
- join node if dimensions required,
- aggregation node with explicit grain,
- output contract with Gold materialization mode.

### 18.3 Patch

Prompt:

- "Add a not-null check on trip_id and change the Silver trip model to partition by event_date."

Expected medallion GIL result:

- patch intent,
- update existing model contract,
- add or update test metadata,
- no unnecessary rebuild of unrelated graph structure.

---

## 19. Testing Strategy

This layer needs its own golden tests, similar to the sibling ISL/IRL pattern.

Required tests:

- NL -> medallion GIL schema conformance,
- medallion GIL normalization,
- invalid contract rejection,
- merge-key ownership validation,
- Bronze path -> promoted column resolution,
- aggregate grain validation,
- graph patch correctness,
- dry-run diff stability.

Golden fixtures should cover:

- single-source Silver build,
- multi-source Silver build,
- Gold aggregate mart,
- patching an existing model,
- invalid ambiguous prompt corrected via retry.

---

## 20. Phased Implementation

### Phase A: Schema and validator

- define medallion GIL grammar,
- define JSON schema and EDN normalization,
- add validator,
- add dry-run API.

### Phase B: Graph compiler

- lower medallion GIL into actual graph mutation plans,
- support Silver schema and Bronze-to-Silver mapping,
- support patch operations.

### Phase C: Gold support

- add Gold schema and Silver-to-Gold mapping intents,
- add aggregate grain validation,
- add dependency refs.

### Phase D: SQL preview

- integrate with the graph-to-SQL compiler,
- show SQL preview in dry-run mode,
- show lineage and target contract diff.

### Phase E: Product hardening

- approval workflow,
- prompt/example registry,
- audit trail,
- replayable proposal history,
- production activation guardrails.

---

## 21. Recommendation

Bitool should absolutely have a medallion-specific GIL layer.

That is the cleanest way to support:

- "create Silver schema"
- "generate mapping from Bronze"
- "create Gold mart"

without forcing the LLM to directly mutate graphs or generate final SQL blindly.

The right architecture is:

- IGL is the stored graph,
- generic GIL is the base intent language,
- medallion GIL is the domain-specific extension for Bronze/Silver/Gold modeling,
- SQL compiler and Databricks jobs remain downstream deterministic execution layers.
