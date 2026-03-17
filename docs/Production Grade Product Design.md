# Production Grade Product Design

## 1. Purpose

This document defines the target design for turning the current BiTool graph runtime into a production-grade data integration product.

It is intentionally broader than the Sheetz Bronze MVP design. The MVP proved that BiTool can:

- model API ingestion with graph nodes,
- run scheduled and manual API extractions,
- write Bronze data and audit state,
- trigger downstream Databricks Jobs,
- expose a richer API/target/scheduler UI.

This document answers the next question:

How does BiTool become a reliable product that enterprise teams can run in production across many tenants, pipelines, developers, and environments?

---

## 2. Product Thesis

BiTool should compete on three things at the same time:

1. A visual, graph-native modeling surface for ingestion and transformation.
2. A programmable execution engine that is more expressive than SQL-only modeling.
3. Production-grade operating characteristics: reliability, observability, governance, repeatability, and safe change management.

The product does not win just because it is Turing-complete or graph-based. It wins if it combines expressive modeling with platform discipline.

---

## 3. Current State

### 3.1 What exists now

The current codebase already has:

- graph-backed node modeling in [src/clj/bitool/graph2.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/graph2.clj),
- API connector execution in [src/clj/bitool/connector/api.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/connector/api.clj),
- Bronze runtime in [src/clj/bitool/ingest/runtime.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ingest/runtime.clj),
- checkpoint and audit persistence in [src/clj/bitool/ingest/checkpoint.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ingest/checkpoint.clj),
- Bronze envelope creation in [src/clj/bitool/ingest/bronze.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ingest/bronze.clj),
- scheduler execution in [src/clj/bitool/ingest/scheduler.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/ingest/scheduler.clj),
- Databricks Jobs API triggering in [src/clj/bitool/databricks/jobs.clj](/Users/aaryakulkarni/bitool/src/clj/bitool/databricks/jobs.clj),
- richer API, target, and scheduler editors in:
  - [resources/public/apiComponent.js](/Users/aaryakulkarni/bitool/resources/public/apiComponent.js)
  - [resources/public/targetComponent.js](/Users/aaryakulkarni/bitool/resources/public/targetComponent.js)
  - [resources/public/schedulerComponent.js](/Users/aaryakulkarni/bitool/resources/public/schedulerComponent.js)

### 3.2 What is still missing for product-grade operation

The current implementation is functional, but not yet a full platform. Major gaps remain:

- no deployment model across dev/stage/prod environments,
- no versioned publish/activate workflow for graphs,
- no queue-based execution isolation,
- no worker pool and concurrency controls per tenant/source,
- limited monitoring UI,
- no alerting framework,
- no RBAC model,
- no secret management UX,
- no policy enforcement layer,
- no lineage, impact analysis, or change review UX,
- no packaged artifact export for CI/CD,
- no tenant isolation model,
- no rate-control governance at platform level,
- no SLA/status dashboard,
- no reconciliation and replay tooling.

---

## 4. Product Goals

### 4.1 Functional goals

- Let users visually model ingestion and transformation graphs.
- Support API, database, webhook, and scheduled sources.
- Support Bronze, Silver, and Gold execution boundaries.
- Support both visual transformations and code-backed extensibility.
- Support manual, scheduled, and event-driven execution.
- Support safe replay and reprocessing.

### 4.2 Non-functional goals

- Idempotent execution.
- Strong observability.
- Tenant isolation.
- Horizontal scaling.
- Safe rollout and rollback.
- Clear ownership, auditability, and access control.
- Recovery after worker crash or partial failure.
- Deterministic environment promotion.

### 4.3 Business goals

- Reduce time to onboard new data sources.
- Reduce engineering dependency for straightforward pipelines.
- Keep enough expressiveness for complex pipelines without escaping to a second orchestration product too early.
- Provide a platform that can plausibly replace parts of Informatica-class ETL plus parts of dbt-class transformation authoring for selected workloads.

---

## 5. Product Scope

BiTool should be designed as four cooperating layers.

### 5.1 Design layer

This is the visual graph product:

- node editors,
- canvas,
- dependency graph,
- schema browsing,
- config validation,
- environment-aware configuration.

### 5.2 Control plane

This manages:

- graph versions,
- publish/activate state,
- schedules,
- secrets references,
- connection metadata,
- policy validation,
- execution requests,
- lineage metadata,
- ownership and access control.

### 5.3 Execution plane

This runs:

- ingestion tasks,
- transformation tasks,
- scheduler polling,
- worker processes,
- retries,
- checkpointing,
- job dispatch,
- failure handling.

### 5.4 Observability plane

This exposes:

- run history,
- step-level logs,
- alerts,
- freshness state,
- SLA state,
- replay support,
- bad-record inspection,
- throughput and error metrics.

---

## 6. Architecture

### 6.1 Recommended high-level architecture

```text
Browser UI
  -> API server / control plane
  -> graph version store
  -> connection + secret references
  -> execution request queue

Scheduler / webhook / manual trigger
  -> dispatcher
  -> worker pool
  -> source connectors
  -> target writers
  -> downstream job triggers

Execution state
  -> run tables
  -> checkpoint tables
  -> bad records
  -> metrics/logs/events

Consumption
  -> run monitoring UI
  -> alerts
  -> Databricks / warehouse / APIs / dashboards
```

### 6.2 Core architectural separation

BiTool should separate:

- configuration metadata from runtime state,
- authoring from published execution,
- control-plane APIs from worker-plane execution,
- user-facing graph versions from immutable run snapshots.

That separation is mandatory for production safety.

---

## 7. Execution Model

### 7.1 Current model

Today, request-triggered runtime and scheduler polling happen in the application process. That is acceptable for MVP, but not ideal for a larger product.

### 7.2 Target model

Move to a dispatcher plus worker model:

- control-plane server accepts publish and run requests,
- dispatcher resolves the active graph version,
- execution requests are placed on a queue,
- stateless workers claim work,
- workers emit structured events and heartbeats,
- workers update run state transactionally.

### 7.3 Why this matters

This gives:

- isolation between UI traffic and heavy execution,
- safer horizontal scaling,
- worker crash recovery,
- concurrency limits by source and tenant,
- delayed retry and backoff support,
- better run cancellation and pause/resume control.

### 7.4 Recommended queue model

Use a durable queue abstraction, for example:

- Postgres-backed job queue first, if operational simplicity matters,
- then optionally Redis/SQS/Kafka class infrastructure when scale requires it.

Queue payload must reference:

- published graph version id,
- environment,
- trigger type,
- tenant/workspace id,
- run id,
- override params,
- trace/correlation id.

---

## 8. Graph Versioning and Release Model

### 8.1 Required model

Graphs must support these states:

- draft,
- validated,
- published,
- active,
- archived.

### 8.2 Required guarantees

- Every run references an immutable published graph version.
- Draft edits never affect active runs.
- Rollback is switching active version, not mutating the old one.
- Schedules point to active published versions only.

### 8.3 Environment promotion

Support:

- dev,
- test,
- stage,
- prod.

Promotion model:

1. author draft,
2. validate,
3. publish version,
4. promote config bundle to next environment,
5. activate there,
6. retain prior active version for rollback.

### 8.4 Artifact model

Each published graph should compile into a deterministic artifact:

- graph json/edn snapshot,
- resolved node config,
- schema contract snapshot,
- environment variable references,
- execution policy snapshot.

This is the bridge to CI/CD.

---

## 9. Runtime Guarantees

### 9.1 Idempotency

Every step should define its idempotency behavior:

- source fetch idempotency,
- checkpoint update semantics,
- target write semantics,
- downstream trigger semantics.

### 9.2 Checkpoint safety

Current Bronze runtime already handles core checkpoint semantics. Production grade requires:

- transactional checkpoint updates tied to target write success,
- explicit successful vs attempted checkpoints,
- replay mode without corrupting production watermark state,
- operator controls for reset / fast-forward / backfill.

### 9.3 Retry semantics

Retry policy must exist at three levels:

- connector request retry,
- node execution retry,
- whole-run retry.

These must have different policies and observability.

### 9.4 Concurrency control

Required concurrency controls:

- per graph,
- per endpoint,
- per connection,
- per external source,
- per tenant.

Without this, a flexible runtime becomes a self-DOS tool.

### 9.5 Backpressure

The runtime should not let fast producers overload targets or downstream systems. Workers need:

- bounded in-flight tasks,
- target write batching,
- optional page buffering limits,
- global and tenant-level quotas.

---

## 10. Transformation Product Strategy

### 10.1 Positioning

BiTool should not imitate dbt exactly. The product advantage is:

- visual graph composition,
- non-SQL source and middleware support,
- Turing-complete extensibility,
- mixed ingestion + transformation + orchestration in one model.

### 10.2 What is needed to make transformation product-grade

Projection, filter, aggregation, sorter, function, join, and mapping nodes are not enough by themselves.

To become a serious Silver/Gold product, BiTool still needs:

- materialization semantics,
- model dependency references,
- reusable subgraphs,
- column-level lineage,
- test/assertion nodes,
- environment-aware compilation,
- diff and impact analysis,
- declarative incremental semantics,
- SQL generation and review support where appropriate.

### 10.3 Recommended boundary

Near term:

- keep Bronze orchestration and selected visual transforms in BiTool,
- keep heavy Silver/Gold processing in Databricks or warehouse-native engines.

Medium term:

- add compiled transformation execution modes:
  - warehouse SQL generation,
  - Spark/Databricks SQL generation,
  - function-backed execution,
  - hybrid graph execution.

Related design docs:

- `Silver-Gold-Automation-Tech-Design.md`
- `Bitool Graph -> SQL Compiler -> Databricks Job Execution Design.md`
- `GIL-for-Medallion-Modeling-Tech-Design.md`

---

## 11. Security Design

### 11.1 Identity and access

BiTool needs:

- workspace or tenant model,
- user roles,
- graph ownership,
- environment-specific permissions,
- secret usage permissions,
- approval workflow for production activation.

### 11.2 Secret management

Do not store live secrets in graph state.

Required model:

- graph stores only secret references,
- secret value resolved from provider at runtime,
- provider can be env vars first, then Vault/cloud secrets later,
- secret access is audited.

### 11.3 Audit logging

Every production mutation should be audit logged:

- connection updates,
- secret reference changes,
- graph publication,
- schedule changes,
- manual runs,
- checkpoint resets,
- replay operations,
- approval actions.

---

## 12. Observability Design

### 12.1 Minimum operator views

The product needs dedicated UI pages for:

- all runs,
- currently running jobs,
- failed jobs,
- scheduler state,
- endpoint freshness,
- bad records,
- alert history,
- target job trigger history.

### 12.2 Structured run model

Track:

- run id,
- graph id,
- graph version id,
- tenant id,
- environment,
- trigger type,
- node-level states,
- retry counts,
- timings,
- row counts,
- output references,
- failure class,
- linked external job ids.

### 12.3 Metrics

Emit:

- runs started/completed/failed,
- mean time by node type,
- rows written,
- API response classes,
- retry counts,
- freshness lag,
- scheduler claim lag,
- bad-record counts,
- downstream trigger failures.

### 12.4 Logs

All worker logs must be structured and correlated by:

- run id,
- node id,
- tenant/workspace id,
- external job id,
- graph version id.

### 12.5 Alerts

Alerts should support:

- failure alerts,
- repeated failure alerts,
- freshness breach alerts,
- target latency alerts,
- retry exhaustion alerts,
- scheduler silent-failure alerts.

Delivery channels:

- email,
- Slack,
- webhook,
- PagerDuty later.

---

## 13. Data Model Additions

### 13.1 Control-plane tables

Add durable product tables for:

- `graph_version`
- `graph_release`
- `workspace`
- `workspace_member`
- `execution_request`
- `execution_run`
- `node_run`
- `alert_rule`
- `alert_event`
- `secret_ref`
- `connection_policy`
- `run_annotation`

### 13.2 Runtime tables

Continue using or expanding:

- `ingestion_checkpoint`
- `endpoint_run_detail`
- `bad_records`
- `scheduler_run_state`

### 13.3 Why this matters

The current graph table alone is not enough. A production product needs first-class release and run entities instead of inferring everything from mutable graph state.

---

## 14. UI and UX Design

### 14.1 Authoring UX

The authoring surface should support:

- schema-aware forms,
- inline validation,
- config previews,
- diff view vs active version,
- test-run from draft,
- publish flow,
- environment overrides,
- reusable templates.

### 14.2 Operations UX

Operators need:

- run list,
- run detail timeline,
- node-by-node status,
- replay actions,
- checkpoint controls,
- bad-record explorer,
- scheduler history,
- alert console.

### 14.3 Product UX rule

Do not force operators into raw tables for routine support tasks. Production-grade means supportability through the product UI.

---

## 15. Extensibility Strategy

### 15.1 Connector contract

Connectors should implement a stable contract:

- validate config,
- build request,
- fetch with retry/backoff,
- emit records/pages,
- classify failure,
- report checkpoint signals.

### 15.2 Node execution contract

Each executable node should declare:

- input contract,
- output contract,
- side effects,
- retryability,
- idempotency class,
- concurrency class,
- metrics emitted.

### 15.3 Plugin direction

Longer term, make connectors and execution adapters pluggable. The platform should not require editing core runtime code for every new source type.

---

## 16. Reliability and Failure Model

### 16.1 Failure classes

Standardize failure categories:

- user config error,
- source auth error,
- source rate limit,
- source transport error,
- source schema drift,
- target transient write error,
- target contract error,
- downstream trigger error,
- worker crash,
- scheduler failure.

### 16.2 Recovery actions

The platform should support:

- retry,
- skip,
- replay from checkpoint,
- replay from time window,
- replay bad records,
- backfill by partition/window,
- rollback active graph version.

### 16.3 Chaos and fault testing

Before claiming production grade, test:

- worker crash during checkpoint update,
- partial page ingestion,
- duplicate trigger delivery,
- schedule overlap,
- target outage,
- Databricks Jobs API failure,
- queue redelivery.

---

## 17. Multi-Tenancy

### 17.1 Required model

BiTool should support either:

- workspace-level tenancy first, or
- hard tenant id isolation from the start.

### 17.2 Isolation boundaries

At minimum isolate:

- graph visibility,
- connection visibility,
- secret refs,
- scheduler ownership,
- run history,
- alerting,
- quotas.

### 17.3 Operational limits

Per tenant limits are required for:

- runs per minute,
- concurrent workers,
- API request budgets,
- storage retention.

---

## 18. Compliance and Governance

### 18.1 Core governance needs

- change history,
- approvals for production changes,
- secret access audit,
- run audit history,
- ownership metadata,
- data classification tags.

### 18.2 Lineage

BiTool should produce lineage at:

- graph level,
- node level,
- table level,
- column level where possible.

This is one of the places where a graph product can beat raw notebook workflows.

---

## 19. Delivery Plan

### Phase 1: Runtime hardening

- queue-backed execution requests,
- worker separation,
- immutable published graph versions,
- structured execution tables,
- richer runtime metrics/logging.

### Phase 2: Operations product

- run monitoring UI,
- bad-record viewer,
- scheduler monitoring UI,
- alerts,
- checkpoint reset and replay UI.

### Phase 3: Governance product

- RBAC,
- publish/approve/activate workflow,
- secret provider abstraction,
- environment promotion model,
- audit trail UX.

### Phase 4: Transformation product

- materialization semantics,
- reusable models/subgraphs,
- assertions/tests,
- lineage and impact analysis,
- compile-to-SQL / compile-to-runtime modes.

### Phase 5: Platform scale

- worker autoscaling,
- queue partitioning,
- tenant quotas,
- usage metering,
- plugin SDK.

---

## 20. Acceptance Criteria For “Production Grade”

BiTool should not be called production-grade until all of the following are true:

- runs use immutable published graph versions,
- execution can survive worker failure without corrupting checkpoints,
- concurrency and retry policies are enforced centrally,
- operators have a usable monitoring UI,
- alerting exists,
- RBAC and secret controls exist,
- environment promotion and rollback exist,
- replay and checkpoint reset are product features,
- structured run metadata is queryable and retained,
- product behavior is deterministic across environments.

---

## 21. Recommended Immediate Next Steps

The highest-leverage next implementation steps are:

1. Add immutable graph version and release tables.
2. Introduce queue-backed execution requests and separate workers from the web process.
3. Add run-monitoring and scheduler-monitoring UI.
4. Add alert rules and alert delivery.
5. Add checkpoint reset and replay tooling.
6. Add RBAC and secret-provider abstraction.

That sequence raises the product from “working runtime” to “operable platform” fastest.
