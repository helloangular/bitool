# Silver-Gold-Automation-Tech-Design

## 1. Purpose

This document defines how Bitool should automate parts of Bronze-to-Silver and Silver-to-Gold design without pretending that all business modeling can be fully inferred.

The target outcome is:

- Bitool proposes Silver and Gold schemas,
- Bitool proposes Bronze-to-Silver and Silver-to-Gold mappings,
- users review and approve those proposals,
- approved mappings compile into executable SQL,
- Databricks or another warehouse-native engine executes the compiled models.

This is an automation and compiler design, not a claim that Bitool should own all Silver/Gold business logic itself.

---

## 2. Problem Statement

Today Bitool has useful transformation primitives:

- projection,
- mapping,
- filter,
- join,
- aggregation,
- sorter,
- target,
- graph-level orchestration.

Those primitives are necessary but not sufficient for product-grade Silver and Gold modeling.

The missing layer is:

- automated schema proposal,
- automated mapping proposal,
- semantic understanding of source fields,
- repeatable validation,
- compilation to trustworthy SQL,
- approval and governance around proposed changes.

Without that layer, every Silver and Gold model is largely custom work.

---

## 3. Goals

Bitool should:

- infer candidate Silver entity schemas from Bronze structures,
- infer candidate Gold mart schemas from Silver contracts and business intents,
- generate draft mappings and transforms,
- attach confidence scores and explanation metadata,
- validate proposals before execution,
- compile approved graphs into SQL suitable for Databricks execution,
- preserve lineage from source fields to published models.

---

## 4. Non-Goals

This design does not aim to:

- replace Databricks as the main compute engine,
- fully automate business semantics with no review,
- generate arbitrary Spark code as the primary path,
- infer correct KPI definitions without business context,
- eliminate the need for data contracts, testing, or approvals.

---

## 5. Design Principles

1. Assistive, not fully autonomous.
   Bitool should propose models and mappings, not silently publish them.

2. Rules first, LLM second.
   Deterministic heuristics should handle stable cases. LLMs should fill semantic gaps, rank candidates, and explain decisions.

3. SQL is the primary execution artifact.
   The output of Silver/Gold automation should be a logical plan that compiles to SQL.

4. Every proposal is explainable.
   A generated column or mapping must carry provenance, confidence, and validation status.

5. Execution remains warehouse-native.
   Bitool should compile and orchestrate. Databricks should execute and optimize.

---

## 6. High-Level Architecture

Bitool should add a Silver/Gold automation layer with five major components:

1. Schema profiler
   Reads Bronze or Silver schemas, sample rows, null rates, cardinality, distinct counts, and drift history.

2. Semantic inference engine
   Applies deterministic rules and optional LLM reasoning to label fields as identifiers, timestamps, facts, dimensions, enums, foreign keys, measures, or audit columns.

3. Model proposal engine
   Produces candidate Silver canonical tables and Gold marts from the profile plus graph context.

4. Mapping proposal engine
   Produces field-level mappings, transform expressions, join rules, filters, and aggregation grain.

5. Validation and approval workflow
   Runs static checks, sample execution checks, lineage checks, and user review before publish.

Bronze prerequisite:

- this profiler depends on API-to-Bronze schema infrastructure that is only partially complete today,
- `endpoint_schema_snapshot` is a useful start but is not yet enough by itself for Silver automation,
- Phase A must first extend Bronze-side profiling metadata with retention, indexes, and profile metrics such as null rates, cardinality, and distinct counts,
- see the API-to-Bronze readiness gaps in [API-to-Bronze-Production-Readiness-Checklist.md](./API-to-Bronze-Production-Readiness-Checklist.md).

---

## 7. Silver Schema Automation

### 7.1 Target output

Bitool should propose Silver schemas as canonical operational models such as:

- fact-style event tables,
- dimension tables,
- bridge tables,
- flattened canonical entity tables,
- latest-state snapshots.

### 7.2 Core patterns

Silver schema proposals should follow repeatable patterns:

- nested Bronze arrays become child entity or fact tables,
- repeated object structures become dimensions when keys are stable,
- status and enum-like fields remain descriptive dimensions or typed attributes,
- timestamps become event, created, updated, or effective-time candidates,
- raw payload-only fields remain in Bronze unless explicitly promoted.

### 7.3 Entity inference

For each Bronze endpoint, Bitool should identify likely entities by:

- root object type,
- nested object repetition,
- primary key stability,
- repeated path groups,
- natural key candidates,
- API naming hints.

Example:

- `$.data[].vehicle.id`, `$.data[].vehicle.name`, `$.data[].vehicle.status` suggest a `vehicle` dimension,
- `$.data[].trip.id`, `$.data[].trip.start_time`, `$.data[].trip.distance` suggest a `trip` fact.

### 7.4 Column design rules

Silver proposals should apply:

- stable naming convention,
- explicit types,
- declared nullability,
- candidate primary key,
- candidate foreign keys,
- audit columns,
- source lineage references.

---

## 8. Gold Schema Automation

### 8.1 Target output

Gold proposals should produce analytics-ready models such as:

- KPI marts,
- daily rollups,
- hourly operational aggregates,
- conformed dimensions,
- presentation views,
- executive summary tables.

### 8.2 Gold proposal inputs

Gold cannot be inferred from raw schema alone. It should require:

- Silver schema,
- declared business domain,
- optional metric catalog,
- optional grain specification,
- optional business glossary,
- optional example dashboards or report contracts.

### 8.3 Gold patterns

Bitool should support reusable Gold templates such as:

- daily entity activity,
- latest-status rollup,
- utilization by asset,
- SLA or freshness summary,
- exception counts by day,
- dimension + measure star schema.

---

## 9. Mapping Automation

### 9.1 Bronze-to-Silver

Bitool should generate draft mappings that define:

- source column or JSON path,
- target column,
- cast or normalize expression,
- null/default behavior,
- dedupe rule,
- join rule if multiple sources are involved,
- audit lineage.

### 9.2 Silver-to-Gold

Bitool should generate:

- dimension joins,
- aggregation expressions,
- grouping grain,
- filter predicates,
- derived KPI formulas,
- window definitions when needed.

### 9.3 Mapping representation

Mapping proposals should be stored in a structured form, not only free-form SQL:

Representation rule:

- `source_paths` carries stable logical JSON or model paths,
- `source_columns` carries resolved physical SQL column names,
- for Bronze mappings, promoted column names depend on stable identifier normalization from schema inference,
- compiler resolution must always happen from `source_paths` to `source_columns` so proposals remain stable across recompilation.

```clojure
{:source_layer "bronze"
 :target_layer "silver"
 :target_model "silver_trip"
 :mappings [{:target_column "trip_id"
             :expression "bronze.trip_id"
             :source_paths ["$.data[].id"]
             :source_columns ["bronze.trip_id"]
             :confidence 0.99
             :rule_source "exact_name_match"}
            {:target_column "vehicle_id"
             :expression "bronze.data_items_vehicle_id"
             :source_paths ["$.data[].vehicle.id"]
             :source_columns ["bronze.data_items_vehicle_id"]
             :confidence 0.97
             :rule_source "path_semantics"}]
 :filters []
 :joins []
 :materialization {:mode "merge"
                   :keys ["trip_id"]}}
```

---

## 10. Deterministic Rules vs LLM Usage

### 10.1 Deterministic rules

Rules should handle:

- name normalization,
- type compatibility,
- exact field matches,
- path-pattern matches,
- common key detection,
- timestamp detection,
- enum detection,
- additive schema evolution checks.

### 10.2 LLM-assisted tasks

LLMs are useful for:

- semantic grouping of related fields,
- choosing the most likely entity split,
- proposing human-friendly target names,
- suggesting join intent,
- drafting KPI definitions from glossary text,
- explaining why a mapping was proposed,
- ranking multiple possible target models.

### 10.3 LLM constraints

LLM output must never be trusted directly for execution.

Every LLM proposal must be:

- captured as a draft,
- validated by deterministic checks,
- test-executed on sample data,
- approved before publish.

---

## 11. Validation Pipeline

Every generated Silver/Gold proposal should pass:

1. Schema validation
   - valid identifiers,
   - supported types,
   - unique columns,
   - materialization contract present.

2. Mapping validation
   - all source references resolve,
   - all target columns have expressions,
   - joins have keys,
   - aggregate models define grain.

3. Sample execution validation
   - generated SQL runs on a sample or staging target,
   - row counts are plausible,
   - primary-key uniqueness is checked,
   - null-rate shifts are measured,
   - type casts succeed.

4. Contract validation
   - declared target schema matches compiled SQL output,
   - required fields are present,
   - breaking changes are flagged.

5. Approval validation
   - model owner approves draft,
   - diff against current published model is reviewed,
   - lineage impact is shown.

---

## 12. Metadata Model

Bitool should use one metadata model with two linked layers:

- proposal metadata,
- compiled release metadata.

Proposal metadata is the authority for draft design state.

Compiled release metadata is the authority for what is executable and production-active.

Bitool should add metadata tables such as:

- `schema_profile_snapshot`
- `semantic_annotation`
- `model_contract`
- `model_proposal`
- `mapping_proposal`
- `model_validation_result`
- `model_release`
- `lineage_edge`
- `compiled_model_artifact`
- `compiled_model_run`

Relationship rules:

- `model_proposal` describes candidate models and mappings before publish,
- `model_release` describes the approved model version and is the parent of the compiled artifact,
- `compiled_model_artifact` stores the exact compiled SQL and validation outputs for that release,
- `lineage_edge` is shared across proposal preview and compiled release output, with release/version context,
- `compiled_model_run` stores execution outcomes for compiled releases.

Authority rules:

- `model_release.active=true` is the control-plane source of truth for what is production-active,
- `compiled_model_artifact` is the executable artifact bound to that active release,
- proposal tables must never be treated as executable truth by themselves.

Suggested `model_proposal` shape:

- `proposal_id`
- `tenant_key`
- `workspace_key`
- `layer`
- `target_model`
- `status`
- `source_graph_id`
- `proposal_json`
- `compiled_sql`
- `confidence_score`
- `created_by`
- `created_at_utc`

---

## 13. User Workflow

### 13.1 Silver proposal flow

1. User selects one or more Bronze endpoints.
2. Bitool profiles Bronze schema and sample data.
3. Bitool proposes Silver entities, keys, and mappings.
4. User reviews and edits model contract.
5. Bitool validates and compiles proposal.
6. Approved model is published as executable SQL.

### 13.2 Gold proposal flow

1. User selects one or more Silver models.
2. User optionally provides metric intent or dashboard goal.
3. Bitool proposes mart grain, dimensions, and measures.
4. Bitool validates aggregation semantics.
5. User reviews and publishes.

---

## 14. Compiler Boundary

The output of this design is not a direct row executor.

The output is:

- logical model contract,
- structured mapping contract,
- compiled SQL artifact,
- executable Databricks job specification.

That compiler and execution path is defined in the companion document:

- `Bitool Graph -> SQL Compiler -> Databricks Job Execution Design`

---

## 15. Security and Governance

Automation for Silver and Gold must include:

- RBAC for proposal generation and publish,
- audit trail for model edits,
- approval workflow for production activation,
- environment-aware compilation,
- secret-safe execution context,
- lineage and impact analysis before publish.

LLM prompts and outputs should also be auditable for regulated environments.

---

## 16. Risks

Main risks:

- over-trusting semantic inference,
- generating superficially valid but semantically wrong KPIs,
- ambiguous business keys,
- hidden many-to-many joins,
- unstable naming across proposal revisions,
- low-quality sample data leading to wrong contracts.

Mitigation:

- confidence scoring,
- explicit approval gates,
- reusable model templates,
- deterministic validation,
- sample execution checks,
- model tests after publish.

---

## 17. Phased Implementation

### Phase A: Proposal foundation

- Bronze prerequisite:
  - extend Bronze-side schema snapshot infrastructure with indexes, retention, and profile metrics,
  - add a profile snapshot shape that includes null rates, cardinality, and distinct counts,
  - do not start Silver profiler implementation as if `endpoint_schema_snapshot` alone is enough.
- add schema profiling snapshots,
- add semantic annotations,
- add Silver schema proposal engine,
- add Bronze-to-Silver mapping proposal engine.

Repo-local subset now implemented:

- a deterministic Silver proposal API exists for Bronze API endpoints,
- the current implementation reads the latest Bronze schema snapshot when available and falls back to endpoint-config inferred fields when needed,
- `schema_profile_snapshot` and `model_proposal` metadata are now persisted in the Bitool metadata store,
- rules-based validation, SQL compilation, sample execution validation, and publish metadata now exist for Silver proposals,
- `model_validation_result`, `model_release`, and `compiled_model_artifact` now back the repo-local publish path,
- current proposals are still rules-based only and do not yet include full profile metrics, semantic annotations, proposal review UI, or LLM-assisted ranking.

### Phase B: Validation and publish

Repo-local subset now implemented:

- structured mapping validation now runs before sample execution and before compiled SQL is persisted to proposal state,
- sample execution validation now exists as a bounded repo-local JDBC path with best-effort timeout behavior,
- compile, validate, publish, list, get, update, and graph-synthesis APIs now exist for Silver proposals,
- compiled SQL artifacts, validation results, model releases, intermediate Bitool graph artifacts, and release execution run records now back the repo-local publish path,
- published Silver releases now synthesize a deterministic intermediate Bitool graph before execution,
- repo-local execution can trigger a configured Databricks Silver job from the generated graph target while passing release and compiled SQL metadata,
- proposal review UI, warehouse-native validation jobs, richer approval workflow, and full graph-to-SQL compiler execution are still pending.

### Phase C: Gold automation

- add Gold mart templates,
- add Silver-to-Gold mapping proposals,
- add metric catalog integration.

### Phase D: LLM assist

- add LLM-based semantic ranking,
- add explanation generation,
- add proposal refinement workflows.

### Phase E: Product hardening

- add approval workflow,
- add impact analysis,
- add stronger lineage and tests,
- add production governance for model releases.

---

## 18. Recommendation

The pragmatic production path is:

- automate Silver and Gold proposal generation,
- keep execution warehouse-native,
- require approval before publish,
- treat LLM output as assistive metadata, not authoritative logic.

This gives Bitool a realistic path toward Informatica-like transformation assistance without trying to replace Databricks as the compute and optimization layer.
