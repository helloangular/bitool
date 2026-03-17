# Bitool Graph -> SQL Compiler -> Databricks Job Execution Design

## 1. Purpose

This document defines how Bitool transformation graphs should compile into SQL artifacts and how those SQL artifacts should run as Databricks jobs.

The target pattern is:

- Bitool graph models the transformation,
- Bitool compiler emits validated SQL,
- Databricks runs that SQL,
- Bitool records lineage, releases, and execution metadata.

---

## 2. Scope

This design covers:

- graph normalization,
- logical planning,
- SQL generation,
- job packaging,
- Databricks execution,
- runtime metadata,
- release and rollback behavior.

It assumes Bronze ingestion is already handled by Bitool’s runtime.

---

## 3. Non-Goals

This design does not attempt to:

- make Bitool execute all transformations inside the JVM,
- replace Spark with a custom compute engine,
- support every vendor-specific SQL feature from day one,
- infer business semantics without an explicit model contract,
- make notebooks the only execution format.

---

## 4. Current Product Boundary

Today Bitool already has graph nodes and editor behavior for:

- projection,
- filter,
- join,
- aggregation,
- sorter,
- mapping,
- function,
- target.

Near-term production direction should be:

- Bitool owns graph design, compilation, validation, orchestration, and metadata,
- Databricks owns SQL execution, storage optimization, and runtime scaling.

---

## 5. Supported Execution Targets

The compiler should target these Databricks execution modes:

1. Databricks SQL Warehouse job
   Best for pure SQL models.

2. Databricks notebook job
   Best when SQL must be wrapped with orchestration or parameter logic.

3. Databricks workflow task chain
   Best for model dependency ordering across Silver and Gold.

Default recommendation:

- pure SQL artifact plus Databricks SQL or notebook execution wrapper.

---

## 6. Supported Materialization Modes

The compiler should support explicit materialization contracts:

- `view`
- `table_replace`
- `append`
- `merge`
- `incremental_partition_overwrite`

Typical usage:

- Gold semantic layer: `view`
- small rebuild model: `table_replace`
- append-only fact: `append`
- deduped or corrected Silver entity: `merge`
- partitioned daily rebuild: `incremental_partition_overwrite`

Bitool should not default to plain `insert into select *`.

---

## 7. Compiler Inputs

The compiler should accept:

- graph definition,
- node configs,
- source model contracts,
- target model contract,
- environment context,
- warehouse dialect target,
- incremental strategy,
- release metadata.

Input graph nodes should be normalized into a stable internal representation before SQL generation.

---

## 8. Compiler Pipeline

### 8.1 Graph normalization

Convert the user graph into a normalized DAG:

- resolve node types,
- validate connections,
- resolve input/output order,
- normalize aliases,
- resolve model references,
- reject cycles where unsupported.

### 8.2 Semantic planning

Build a logical plan with stable relational semantics:

- source scans,
- projections,
- filters,
- joins,
- derived expressions,
- aggregations,
- sort/limit if allowed,
- target materialization.

### 8.3 SQL intermediate representation

Compile the logical plan into a SQL-oriented IR:

```clojure
{:cte-order [...]
 :sources [...]
 :joins [...]
 :select [...]
 :where [...]
 :group-by [...]
 :having [...]
 :materialization {:mode "merge"
                   :target "silver.trip"
                   :keys ["trip_id"]}}
```

The SQL IR should be dialect-neutral enough to support later warehouse extensions.

The single-structure example above is only the simplest case.

For real Bitool graphs, the IR must represent node boundaries and multi-source plans explicitly. A practical shape is:

```clojure
{:sources [{:node_id 10
            :alias "bronze_trip"
            :relation "sheetz_telematics.bronze.trip_raw"}
           {:node_id 11
            :alias "silver_vehicle"
            :relation "sheetz_telematics.silver.vehicle"}]
 :ctes [{:name "src_trip"
         :node_id 10
         :sql {:select [...]
               :from [{:relation "sheetz_telematics.bronze.trip_raw"
                       :alias "bronze_trip"}]}}
        {:name "src_vehicle"
         :node_id 11
         :sql {:select [...]
               :from [{:relation "sheetz_telematics.silver.vehicle"
                       :alias "silver_vehicle"}]}}
        {:name "joined_trip_vehicle"
         :node_id 21
         :sql {:select [...]
               :from [{:cte "src_trip" :alias "t"}]
               :joins [{:type "left"
                        :cte "src_vehicle"
                        :alias "v"
                        :on ["t.vehicle_id = v.vehicle_id"]}]}}
        {:name "filtered_trip_vehicle"
         :node_id 22
         :sql {:select [...]
               :from [{:cte "joined_trip_vehicle" :alias "j"}]
               :where ["j.event_time_utc >= :watermark_start"]}}]
 :cte_order ["src_trip" "src_vehicle" "joined_trip_vehicle" "filtered_trip_vehicle"]
 :final_select {:from [{:cte "filtered_trip_vehicle" :alias "f"}]
                :select [...]}
 :materialization {:mode "merge"
                   :target "silver.trip"
                   :keys ["trip_id"]}}
```

Requirements:

- each intermediate node can become a named CTE boundary,
- self-joins must carry unique aliases even if the same relation appears more than once,
- nested filter-then-join or join-then-aggregate patterns must preserve node order in the IR,
- dialect compilation must read from the IR, not infer structure back from raw SQL strings.

### 8.4 Dialect compilation

Compile SQL IR into Databricks SQL with:

- explicit column lists,
- explicit casts,
- canonical aliases,
- merge clauses where needed,
- partition predicates,
- audit columns.

### 8.5 Artifact packaging

Emit:

- compiled SQL text,
- parameter schema,
- target contract,
- lineage edges,
- validation report,
- release checksum.

---

## 9. Node-to-SQL Mapping Rules

### 9.1 Projection

Projection nodes compile into explicit `SELECT` expressions.

Never emit `SELECT *` for published Silver or Gold artifacts unless the model contract explicitly allows it.

### 9.2 Filter

Filter nodes compile into `WHERE` or `HAVING` clauses depending on position and aggregate context.

### 9.3 Join

Join nodes compile into explicit `JOIN ... ON ...` clauses with join type required.

Unsupported or ambiguous joins must fail compilation.

### 9.4 Mapping

Mapping nodes compile into:

- renames,
- casts,
- transform expressions,
- default values,
- conditional expressions.

### 9.5 Aggregation

Aggregation nodes compile into:

- `GROUP BY`,
- aggregate select expressions,
- optional `HAVING`.

Every aggregate model must declare grain.

### 9.6 Sorter

Sorter nodes should compile only when:

- target materialization is `view`,
- or the sort is part of a window function or limited operation.

For table materializations, plain final `ORDER BY` should generally be disallowed because it is not a durable storage contract.

---

## 10. Incremental Semantics

The compiler should support declarative incremental behavior.

### 10.1 Append

Use:

```sql
INSERT INTO target_table (col1, col2, ...)
SELECT ...
FROM source
WHERE ...
```

### 10.2 Replace

Use:

```sql
CREATE OR REPLACE TABLE target_table AS
SELECT ...
```

### 10.3 Merge

Use:

```sql
MERGE INTO target_table t
USING source_cte s
ON t.business_key = s.business_key
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT (...)
VALUES (...)
```

This should be the default for many Silver canonical tables.

Merge key ownership:

- merge keys must live in a materialization contract attached to the Output node or model contract,
- the Target node owns physical location and connection information,
- the Output/materialization contract owns logical semantics such as `mode`, `keys`, `partition_columns`, and `refresh_strategy`,
- proposal metadata may suggest merge keys, but compilation must read the approved contract from the graph or published model release.

### 10.4 Partition overwrite

For partition-scoped refresh:

- compute changed partitions,
- write only affected partitions,
- avoid full-table rebuild unless configured.

---

## 11. Databricks Job Contract

Bitool should package compiled SQL plus run context into a job payload such as:

```json
{
  "model_name": "silver_trip",
  "layer": "silver",
  "materialization_mode": "merge",
  "compiled_sql_path": "s3://.../compiled/sql/silver_trip/v12.sql",
  "target_table": "sheetz_telematics.silver.trip",
  "run_id": "uuid",
  "release_id": "release-123",
  "changed_partition_dates": ["2026-03-14"],
  "watermark_start": "2026-03-14T00:00:00Z",
  "watermark_end": "2026-03-14T01:00:00Z"
}
```

Databricks execution should consume this payload and run the exact compiled artifact for that release.

Early-phase fallback:

- before object-store-backed artifact publishing exists, compiled SQL may be stored in the control-plane database,
- job payloads may then carry either `compiled_sql_path` or `compiled_sql_inline_ref`,
- object storage should become the preferred path in later phases, but compiler work must not be blocked on it.

---

## 12. Artifact Storage

Compiled model artifacts should be persisted, not reconstructed ad hoc.

Required artifacts:

- normalized graph JSON,
- logical plan JSON,
- SQL IR JSON,
- compiled SQL text,
- validation report,
- lineage report,
- release manifest.

Suggested object layout:

```text
tenant=<tenant>/workspace=<workspace>/layer=<layer>/model=<model>/release=<release-id>/
```

---

## 13. Metadata Model

This compiler design shares the same metadata model described in:

- [Silver-Gold-Automation-Tech-Design.md](./Silver-Gold-Automation-Tech-Design.md)

Compiler-specific focus:

- `model_release` is the authoritative released model version,
- `compiled_model_artifact` stores the compiled SQL, SQL IR, and validation outputs for that release,
- `lineage_edge` stores lineage with release context and may be populated at proposal-preview time or compile time,
- `compiled_model_run` stores Databricks execution outcomes for compiled releases,
- `databricks_job_binding` stores environment-specific execution bindings.

Compiler-oriented tables or views:

- `compiled_model_artifact`
- `compiled_model_run`
- `databricks_job_binding`

Key fields:

- model name,
- layer,
- graph id,
- graph version,
- release id,
- artifact checksum,
- materialization mode,
- target table,
- SQL path,
- last validation status.

---

## 14. Validation Before Execution

Before a Databricks job is triggered, Bitool should validate:

1. Graph validity
   - required node config present,
   - no unsupported cycles,
   - target exists.

2. SQL validity
   - generated SQL parses,
   - referenced columns exist,
   - materialization mode is legal,
   - merge keys are declared when needed.

3. Contract validity
   - compiled output columns match target contract,
   - no duplicate target columns,
   - no ambiguous aliases.

4. Runtime safety
   - environment bindings exist,
   - target connection is configured,
   - release is approved for execution.

Optional but recommended:

- run `EXPLAIN`,
- run sample/staging query,
- run uniqueness checks for merge keys.

### 14.5 Compiler correctness and regression testing

The compiler itself must be treated as production code with regression safety.

Required compiler tests:

- golden-file snapshot tests for known graph inputs and expected SQL output,
- node-level unit tests for projection, filter, join, mapping, aggregation, and sorter compilation,
- multi-node integration tests for common graph shapes such as source -> filter -> join -> aggregate -> target,
- self-join and aliasing tests,
- round-trip validation between logical plan and SQL IR,
- dialect-specific regression tests for Databricks SQL behavior.

CI policy:

- a frozen set of reference graphs must compile identically unless an intentional change updates the golden artifacts,
- any SQL diff in those fixtures should require explicit review,
- validation success alone is not enough; generated SQL stability must also be checked.

---

## 15. Lineage

The compiler should emit lineage at:

- graph node level,
- model level,
- column level.

For each target column, Bitool should persist:

- source columns,
- transform expression,
- join dependencies,
- aggregate dependencies,
- release version.

Lineage granularity:

- add `lineage_kind` with values such as `passthrough`, `expression`, `conditional`, `constant`,
- preserve both raw source-column references and dependency role when possible,
- impact analysis should use `lineage_kind` instead of treating all lineage edges as identical text references.

This is required for impact analysis and safe schema changes.

---

## 16. Release Model

Compiled SQL should be versioned and published like code artifacts.

Release lifecycle:

1. Draft graph
2. Validated compile artifact
3. Approved release
4. Active release bound to environment
5. Executable Databricks job reference

This prevents “run whatever the latest draft graph means right now” behavior.

---

## 17. Runtime Execution Flow

### 17.1 Bronze to Silver

1. Bronze run completes successfully.
2. Bitool resolves dependent Silver models.
3. Bitool selects the active compiled release.
4. Bitool sends Databricks job payload with run context.
5. Databricks executes compiled SQL.
6. Bitool records job result, lineage, and downstream eligibility.

### 17.2 Silver to Gold

1. Silver model refresh succeeds.
2. Dependent Gold models are resolved by dependency graph.
3. Bitool triggers Databricks jobs for Gold releases.
4. Results are recorded in the model-run metadata.

---

## 18. Failure Handling

Bitool should distinguish:

- compile failure,
- validation failure,
- Databricks submission failure,
- Databricks runtime failure,
- partial workflow failure,
- downstream dependency block.

Retry policy should differ by failure type:

- compile or validation errors are terminal until config changes,
- submission failures are retryable,
- runtime failures are retryable based on job class and error taxonomy.

---

## 19. Security and Governance

Required controls:

- RBAC for compile, approve, and run,
- immutable release artifacts,
- audit logs for who approved SQL,
- environment promotion controls,
- secret-safe Databricks auth integration,
- ability to diff current vs proposed SQL release.

For regulated environments, generated SQL should be reviewable before activation.

---

## 20. Observability

Bitool should expose:

- compile success/failure rate,
- validation failure reasons,
- Databricks job latency,
- model refresh freshness,
- lineage completeness,
- release adoption,
- model-level error rate.

Key SLOs:

- compiled release activation latency,
- Silver freshness lag,
- Gold freshness lag,
- failed-run recovery time.

---

## 21. Recommended SQL Patterns

Preferred generated SQL styles:

- explicit `SELECT` lists,
- explicit cast expressions,
- CTE-based decomposition for readability,
- `MERGE` for incremental Silver,
- `CREATE OR REPLACE VIEW` for semantic Gold views,
- controlled use of `CREATE OR REPLACE TABLE` for rebuild models.

Avoid by default:

- `SELECT *`,
- hidden implicit casts,
- unbounded cartesian joins,
- final `ORDER BY` for table materializations,
- hand-written warehouse-specific behavior leaking into generic graph semantics.

---

## 22. Phased Implementation

### Phase A: Core compiler

- graph normalization,
- logical plan,
- SQL IR,
- Databricks SQL generation for projection/filter/join/mapping/aggregation.

### Phase B: Release packaging

- compiled artifact storage,
- release metadata,
- validation reports,
- job payload generation.

### Phase C: Databricks execution integration

- Jobs API bindings,
- SQL warehouse execution path,
- run metadata persistence,
- dependency-aware task chaining.

### Phase D: Incremental hardening

- merge semantics,
- partition overwrite,
- contract diffing,
- lineage enrichment,
- rollback to prior compiled release.

### Phase E: Product maturity

- approval workflow,
- environment promotion,
- explain-plan analysis,
- cost-aware compiler hints,
- richer Gold templates.

---

## 23. Recommendation

The right near-term production architecture is:

- Bitool graph as authoring layer,
- Bitool compiler as control and validation layer,
- SQL as the executable artifact,
- Databricks as the runtime engine.

This keeps Bitool aligned with its strengths while giving it a credible path toward production-grade Silver and Gold orchestration.
