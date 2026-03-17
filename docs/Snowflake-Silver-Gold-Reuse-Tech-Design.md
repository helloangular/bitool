# Snowflake Silver-Gold Reuse Tech Design

## 1. Purpose

This document defines how Bitool should reuse the existing Bronze-to-Silver and Silver-to-Gold automation design for Snowflake.

The main conclusion is:

- the Silver/Gold control-plane design is largely reusable,
- the logical graph and SQL compiler pipeline are reusable,
- the review and approval workflow are reusable,
- the warehouse dialect backend and execution packaging must be Snowflake-specific.

This is a reuse design, not a new parallel Silver/Gold system.

---

## 2. Summary

Bitool should not build a separate Snowflake-specific modeling product.

Bitool should instead keep:

- one shared schema proposal system,
- one shared mapping proposal system,
- one shared intermediate graph and logical plan model,
- one shared review and approval lifecycle,
- one shared release and lineage model,
- one compiler framework with multiple dialect backends.

In that model:

- Databricks is one execution backend,
- Snowflake is another execution backend.

---

## 3. Reuse Decision

The existing Bronze-to-Silver and Silver-to-Gold design for Databricks is reusable for Snowflake in all design-time layers.

The main split is:

1. Reusable across warehouses
   - schema profiling
   - semantic inference
   - model proposal generation
   - mapping proposal generation
   - human review and approval
   - lineage capture
   - model release lifecycle
   - intermediate graph generation
   - logical plan generation
   - SQL IR generation
   - validation framework shape

2. Snowflake-specific
   - physical type mapping
   - SQL dialect emission
   - semi-structured access patterns
   - materialization templates
   - execution packaging
   - warehouse-native validation execution
   - operational deployment conventions

---

## 4. Goals

Bitool should:

- reuse the current Silver/Gold automation architecture for Snowflake,
- generate Snowflake-ready Silver and Gold schema proposals,
- generate Snowflake-specific Bronze-to-Silver and Silver-to-Gold SQL,
- preserve the same human review and finalization workflow,
- keep the graph, lineage, approval, and release model warehouse-agnostic,
- avoid hard-forking the design into separate Databricks and Snowflake products.

---

## 5. Non-Goals

This design does not aim to:

- make Snowflake use the Databricks SQL dialect,
- remove warehouse-specific compilation differences,
- force one materialization strategy across all warehouses,
- replace Snowflake-native execution with JVM-side execution,
- define Gold business semantics automatically without review.

---

## 6. Architecture Principle

Bitool should use a layered architecture:

```text
Intent / Review UI
  ->
Intermediate Graph / Logical Model
  ->
Logical Plan
  ->
Dialect-neutral SQL IR
  ->
Warehouse dialect backend
  ->
Warehouse-native execution package
```

Only the last two layers should vary between Databricks and Snowflake.

---

## 7. What Is Reusable As-Is

### 7.1 Schema generation

The same proposal workflow can be reused for Snowflake:

- profile Bronze tables,
- infer candidate entities and facts,
- infer candidate keys and timestamps,
- propose Silver contracts,
- propose Gold marts,
- attach confidence and explanation metadata,
- require user review before publish.

This logic should stay warehouse-agnostic.

### 7.2 Human review and finalization

The same review lifecycle should be reused:

- draft proposal,
- validation,
- human edits,
- approval,
- promotion,
- release publish,
- rollback to prior release if needed.

No Snowflake-specific review model should be created.

### 7.3 Transformation generation at the logical level

The same graph and logical operations are reusable:

- projection,
- mapping,
- filter,
- join,
- aggregation,
- deduplicate,
- merge contract,
- materialization contract.

These should compile into the same logical plan regardless of warehouse.

### 7.4 Metadata model

The same metadata inventory should remain authoritative:

- profile snapshots,
- model proposals,
- validation results,
- compiled artifacts,
- releases,
- lineage,
- execution runs.

Snowflake should reuse these tables rather than introducing a parallel metadata stack.

---

## 8. What Must Be Snowflake-Specific

### 8.1 SQL dialect backend

The SQL emitter must generate Snowflake SQL, including:

- identifier quoting,
- type names,
- timestamp semantics,
- merge syntax,
- JSON and semi-structured expressions,
- flatten semantics,
- warehouse/session settings where required.

### 8.2 Physical type mapping

Logical types should map differently by warehouse.

Example:

| Logical type | Databricks | Snowflake |
|--------------|------------|-----------|
| string | STRING | VARCHAR |
| integer | BIGINT / INT | NUMBER / INTEGER |
| decimal | DECIMAL(p,s) | NUMBER(p,s) |
| boolean | BOOLEAN | BOOLEAN |
| timestamp | TIMESTAMP | TIMESTAMP_NTZ or TIMESTAMP_TZ |
| json object | STRUCT / MAP / STRING | VARIANT |
| json array | ARRAY / STRING | ARRAY or VARIANT |

Type recommendation remains shared. Final physical type selection is dialect-specific.

### 8.3 Semi-structured transformation rules

Bronze data often includes JSON payloads. Snowflake requires different access patterns:

- `VARIANT` storage for raw payloads,
- `:` path accessors,
- `FLATTEN` for array expansion,
- `TRY_PARSE_JSON`, `PARSE_JSON`, `TO_VARIANT` where needed.

The logical mapping can stay the same, but the emitted SQL must be Snowflake-aware.

### 8.4 Materialization templates

Snowflake requires its own templates for:

- `CREATE OR REPLACE TABLE`
- `CREATE OR REPLACE VIEW`
- `MERGE`
- append insert
- partition-like incremental rebuild patterns
- transient or permanent table choices
- clustering keys

Bitool should not reuse Databricks templates directly.

### 8.5 Execution packaging

Databricks execution packages produce Databricks jobs or workflows.

Snowflake execution packages should instead target Snowflake-native options such as:

- SQL statements executed over JDBC or Snowflake SQL API,
- stored procedure wrappers where needed,
- tasks for scheduled execution,
- optional streams/tasks integration in later phases.

The release model stays shared, but the execution wrapper changes.

The current codebase already persists Snowflake target options such as `sf_load_method`, `sf_stage_name`, `sf_warehouse`, `sf_file_format`, `sf_on_error`, and `sf_purge` on the `Tg` node. What does not exist yet is the execution-layer router that reads those fields and decides which Snowflake load path to use.

Required runtime routing design:

1. The routing decision should happen in the compiled-model execution path, not in generic Bronze `load-rows!`.
   Bronze ingestion already has its own target dispatch concerns. Silver and Gold execution should use compiled artifact execution metadata plus target config to select the runtime path.

2. `sf_load_method` should be treated as an execution contract:
   - `jdbc`
   - `stage_copy`
   - `merge`

3. Recommended runtime split:
   - `bitool.compiler.package.snowflake` emits an execution package with the chosen load method and resolved target contract,
   - `bitool.operations` or the compiled-model execution service triggers the Snowflake run,
   - a Snowflake execution adapter performs the selected path.

4. Early implementation guidance:
   - Silver/Gold `table_replace` and `append` can start with JDBC-executed SQL,
   - bulk stage + `COPY INTO` should be added as a later operational optimization,
   - `merge` should be implemented as compiled SQL execution, not by reusing Bronze write-path behavior.

---

## 9. Reuse Matrix

| Capability | Reuse level | Notes |
|------------|-------------|-------|
| schema profiling model | High | same profile shape, different warehouse query adapters |
| semantic inference | High | warehouse-agnostic |
| Silver schema proposal | High | mostly shared |
| Gold schema proposal | High | mostly shared |
| mapping proposal model | High | shared |
| human review UI | High | shared |
| lineage model | High | shared |
| intermediate graph generation | High | shared |
| logical plan generation | High | shared |
| SQL IR | High | shared |
| SQL codegen | Low | dialect-specific |
| DDL templates | Low | dialect-specific |
| merge/upsert templates | Low | merge keys exist in the graph model, but execution templates are not implemented for any warehouse yet |
| validation execution | Medium | shared framework, Snowflake-native runner |
| job packaging | Low | execution-backend specific |

---

## 10. Snowflake-Specific Schema Generation Rules

The proposal engine should stay shared but add Snowflake-aware finalization rules:

1. Prefer `VARIANT` for raw semi-structured carry-through fields.
2. Prefer explicit scalar columns for reviewed Silver/Gold contracts.
3. Use layer-aware timestamp defaults:
   - Bronze raw storage may keep `TIMESTAMP_TZ` to preserve current connector/runtime behavior,
   - reviewed Silver and Gold contracts should default to `TIMESTAMP_NTZ` unless timezone semantics are explicitly required.
4. Use `NUMBER(p,s)` for reviewed decimal fields instead of unbounded numeric defaults.
5. Keep Snowflake object naming conventions explicit:
   - database
   - schema
   - object name
6. Allow table class selection later:
   - permanent
   - transient
   - temporary

These are finalization rules, not a separate schema proposal engine.

---

## 11. Snowflake-Specific Transformation Patterns

### 11.1 Bronze to Silver

Shared logical steps:

- flatten nested payloads,
- cast and rename fields,
- dedupe by business key,
- derive audit columns,
- materialize canonical Silver tables.

Snowflake-specific SQL concerns:

- `payload_json:field::string`
- `LATERAL FLATTEN`
- `TRY_TO_TIMESTAMP`
- `TRY_TO_NUMBER`
- `MERGE INTO`

### 11.2 Silver to Gold

Shared logical steps:

- join conformed dimensions,
- aggregate facts,
- apply KPI formulas,
- publish marts/views.

Snowflake-specific SQL concerns:

- aggregate expression syntax and null handling,
- window frame behavior,
- clustering strategies,
- view/table replacement semantics.

---

## 12. Compiler Design

Bitool should keep one compiler architecture:

1. normalize graph
2. build logical plan
3. build dialect-neutral SQL IR
4. select warehouse dialect backend
5. emit warehouse SQL
6. package execution artifact

Recommended compiler interface:

```clojure
(compile-model
  {:graph graph
   :target-warehouse :snowflake
   :target-layer :silver
   :materialization {:mode "merge" :keys ["trip_id"]}})
```

Important implementation note:

This compiler abstraction does not exist in the codebase yet. Today the SQL emission path is still inlined in `automation.clj` through `compile-select-sql`, `compile-materialization-sql`, and `compile-sql-ir`. The SQL IR is reusable, but Databricks-oriented SQL emission is not yet extracted behind a dialect interface.

Recommended backend split:

```clojure
bitool.compiler.core
bitool.compiler.ir
bitool.compiler.dialect.databricks
bitool.compiler.dialect.snowflake
bitool.compiler.package.databricks
bitool.compiler.package.snowflake
```

This keeps the shared compiler core intact and isolates dialect logic cleanly.

---

## 13. Validation Model

Validation should stay shared at the framework level:

- identifier validity,
- source-field resolution,
- materialization contract completeness,
- merge-key presence,
- grain declaration,
- lineage completeness,
- SQL compilation success.

Snowflake-specific validation should add:

- Snowflake identifier and reserved-word checks,
- dialect parse/compile check,
- `EXPLAIN` or limited sample execution against Snowflake,
- semi-structured expression validation,
- object existence checks for referenced databases/schemas/tables.

---

## 14. Human Review Workflow

The same human review path should be reused:

1. propose schema
2. propose mappings
3. generate intermediate graph
4. compile draft SQL
5. run validation
6. show diffs, lineage, and sample results
7. approve and publish release

For Snowflake, review screens should additionally surface:

- generated Snowflake SQL,
- Snowflake type mappings,
- `VARIANT` / `FLATTEN` usage,
- merge key contract,
- database/schema target resolution.

The workflow remains the same. The warehouse detail panel changes.

---

## 15. Intermediate Graph Reuse

The Bitool intermediate graph should be the main reusable layer across Databricks and Snowflake.

LLM-assisted generation should still produce:

- source nodes,
- projection nodes,
- mapping nodes,
- join nodes,
- aggregate nodes,
- output/target nodes,
- materialization metadata.

That graph should not contain raw Databricks-specific SQL.

Instead:

- the graph captures intent,
- the compiler backend decides whether the final SQL is Databricks or Snowflake.

This is the main reason Snowflake reuse is practical.

---

## 16. Recommended Implementation Order

### Phase A: Shared-model hardening

- confirm shared metadata model remains warehouse-agnostic,
- confirm SQL IR is not Databricks-specific,
- confirm intermediate graph and proposal model carry all materialization contracts needed by Snowflake,
- confirm profiling adapters can read Snowflake metadata using `INFORMATION_SCHEMA`, `SHOW COLUMNS`, and sample queries instead of Databricks metastore assumptions.

### Phase A.5: Databricks extraction

- extract the current Databricks SQL emission from `automation.clj`,
- move Databricks SQL generation into `bitool.compiler.dialect.databricks`,
- move Databricks execution packaging into `bitool.compiler.package.databricks`,
- make the current Databricks path use the same compiler interface proposed for Snowflake.

This phase is required before Snowflake backend work can remain parallel and clean. Without it, Snowflake support will become a second inline codepath rather than a backend implementation.

### Phase B: Snowflake dialect backend

- implement Snowflake type mapper,
- implement Snowflake expression emitter,
- implement DDL/DML templates,
- implement merge and replace templates,
- implement semi-structured helper templates.

### Phase C: Snowflake validation runner

- add Snowflake compile/validate path,
- add sample execution support,
- add release artifact packaging for Snowflake.

### Phase D: Review UI extensions

- show Snowflake SQL preview,
- show type mapping diffs,
- show target database/schema/object placement,
- keep the same approval and publish lifecycle.

### Phase E: Operational execution

- execute published Snowflake releases through Snowflake-native runtime,
- record run metadata in the same compiled model run tables,
- keep observability in Bitool, execution in Snowflake.

---

## 17. Risks

1. Databricks assumptions leaked into the logical IR.
   If the current IR embeds Databricks-only semantics, Snowflake support will become a rewrite instead of a backend.

2. The immediate leak is in SQL emission, not the IR.
   Today `compile-select-sql` and `compile-materialization-sql` emit SQL directly from `automation.clj` without a dialect abstraction. This is the primary near-term blocker for Snowflake support.

3. Semi-structured transformations are under-specified.
   Snowflake support depends heavily on a clean logical representation of array explosion and nested field access.

4. Materialization contracts are incomplete.
   Merge keys exist in the graph model, but merge execution is not implemented for any warehouse yet. Clustering strategy, replace semantics, and runtime selection still need a complete compiler/runtime contract.

5. Validation becomes too runtime-dependent.
   The compiler should remain correct even before warehouse execution is attempted.

6. Snowflake credential support may lag operational needs.
   Snowflake commonly uses key-pair authentication in addition to username/password. The secret and connection model must support RSA private key material and passphrase handling, not only basic credentials.

---

## 18. Conclusion

Yes, Bitool should reuse the existing Databricks-oriented Silver/Gold design for Snowflake.

The correct architecture is:

- shared control plane,
- shared proposal and review system,
- shared intermediate graph,
- shared logical plan and SQL IR,
- separate Snowflake dialect and execution backend.

That lets Bitool support:

- Silver and Gold schema generation with human review,
- Bronze-to-Silver and Silver-to-Gold transformation generation,
- Snowflake-specific SQL emission,
- warehouse-native Snowflake execution,

without building a separate Snowflake-only modeling product.

---

## 19. Relationship to Existing Docs

This document extends:

- [Silver-Gold-Automation-Tech-Design.md](./Silver-Gold-Automation-Tech-Design.md)
- [Bitool Graph -> SQL Compiler -> Databricks Job Execution Design.md](./Bitool%20Graph%20-%3E%20SQL%20Compiler%20-%3E%20Databricks%20Job%20Execution%20Design.md)
- [Connector-Tech-Design-Kafka-File-Snowflake.md](./Connector-Tech-Design-Kafka-File-Snowflake.md)

Interpretation:

- `Silver-Gold-Automation-Tech-Design.md` remains the warehouse-agnostic automation design,
- the Databricks compiler design remains one backend implementation,
- this document defines how Snowflake becomes the second backend without changing the shared control-plane model.
