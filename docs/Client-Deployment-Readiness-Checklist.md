# Client Deployment Readiness Checklist

## 1. Purpose

This checklist defines what Bitool still needs before it should be deployed to a client as a supported product, not just as a guided demo or tightly supervised pilot.

It is broader than:

- [API-to-Bronze-Production-Readiness-Checklist.md](./API-to-Bronze-Production-Readiness-Checklist.md)
- [Production-Scale-Reliability-Tech-Design.md](./Production-Scale-Reliability-Tech-Design.md)
- [Snowflake-Silver-Gold-Reuse-Tech-Design.md](./Snowflake-Silver-Gold-Reuse-Tech-Design.md)

This document consolidates the remaining product gaps across:

- security and governance,
- source-to-Bronze runtime semantics,
- Kafka and File/Mainframe completeness,
- Bronze-to-Silver-to-Gold automation,
- Databricks and Snowflake warehouse parity,
- operations and monitoring,
- deployment packaging and client support readiness.

---

## 2. Deployment Tiers

### 2.1 Guided pilot

Bitool can be used with a client when:

- scope is narrow,
- one team operates it directly,
- concurrency is controlled,
- manual intervention is acceptable,
- production support expectations are limited.

### 2.2 Standard client deployment

Bitool should not be presented as client-deployable in the normal sense until every `P0` item in this checklist is complete.

### 2.3 Broad product rollout

Bitool should not be treated as broadly productized until both `P0` and `P1` are complete, with `P2` underway or intentionally deferred.

---

## 3. Status Legend

- `Done`: implemented in a way that materially satisfies the requirement.
- `Partial`: useful work exists, but the product claim is still incomplete.
- `P0`: required before first standard client deployment.
- `P1`: required before repeatable multi-client rollout.
- `P2`: productization work that should follow once the first real deployments are stable.

---

## 4. Checklist

### 4.1 Security and governance

- `Partial / P0`: managed secrets support AES-GCM encryption, but the repo still allows a plaintext fallback path when encryption key material is not configured. Client deployment should require envelope encryption or an external secret manager.
  Reference: [Production-Scale-Reliability-Tech-Design.md](./Production-Scale-Reliability-Tech-Design.md)
- `Partial / P0`: RBAC is now present on ops mutations and the main ingest control routes, but client deployment still requires complete coverage across graph editing, medallion publish/approval, checkpoint reset, admin configuration, and any remaining legacy mutation paths.
  Reference: [Production-Scale-Reliability-Tech-Design.md](./Production-Scale-Reliability-Tech-Design.md)
- `Partial / P0`: publish and activate flows need explicit approval gates, not just route-level mutation controls. API schema approval is further along than the modeling publish path; Silver/Gold still do not have a separate enforced approval gate beyond state transitions.
  Reference: [Production-Scale-Reliability-Tech-Design.md](./Production-Scale-Reliability-Tech-Design.md)
- `Partial / P0`: environment promotion must require auditable approval for production release.
  Reference: [Production-Scale-Reliability-Tech-Design.md](./Production-Scale-Reliability-Tech-Design.md)
- `Partial / P0`: audit trail already covers secret access, config changes, queue mutations, and much of the operator surface. Remaining gaps still need closure for replay-from-checkpoint, bulk-ignore bad-record actions, and modeling publish/review transitions so the full release and recovery path is traceable.
- `Done / P0`: ops configuration changes are versioned and auditable with preview, validate, apply, and rollback semantics plus optimistic concurrency checks.
- `P0`: tenant and workspace administration must be policy-driven and safe for real operators, not only repo-local tables and direct mutation routes.
- `P1`: SSO, enterprise identity integration, and role mapping should be supported for typical client deployments.
- `P1`: data access policies need explicit handling for PII, masked fields, and environment-specific secret scopes.

### 4.2 Runtime correctness and recovery

- `Partial / P0`: retry behavior must use a deterministic failure state machine, not only best-effort retry loops.
  Reference: [Production-Scale-Reliability-Tech-Design.md](./Production-Scale-Reliability-Tech-Design.md)
- `Partial / P0`: delayed retry scheduling and DLQ routing must be complete for all request kinds, not only the API-first paths.
  Reference: [Production-Scale-Reliability-Tech-Design.md](./Production-Scale-Reliability-Tech-Design.md)
- `Partial / P0`: checkpoint movement must be impossible unless the corresponding Bronze batch is durably committed. This is materially stronger on transactional targets than on non-transactional warehouse paths, so the remaining gap is target-specific, not uniform.
  Reference: [API-to-Bronze-Production-Readiness-Checklist.md](./API-to-Bronze-Production-Readiness-Checklist.md)
- `Partial / P0`: replay exists and replay-from-checkpoint is available, but the full contract still needs to guarantee resume from last committed checkpoint plus configured overlap rather than only replaying a manually selected point.
- `Partial / P0`: downstream triggers must be idempotent and must not cause duplicate medallion runs on retry or worker restart.
  Reference: [Production-Scale-Reliability-Tech-Design.md](./Production-Scale-Reliability-Tech-Design.md)
- `P0`: explicit source- and run-level pause/resume semantics are still missing as a product capability, even though some connector-local pause hooks exist. Long-running runs and stream-shaped work still need a real paused state, resume contract, and operator surface.
  Reference: [Production-Scale-Reliability-Tech-Design.md](./Production-Scale-Reliability-Tech-Design.md)
- `Partial / P0`: orphan recovery, lease heartbeats, and abandoned-run reconciliation must be proven across all request kinds.
  Reference: [Production-Scale-Reliability-Tech-Design.md](./Production-Scale-Reliability-Tech-Design.md)
- `P0`: batch rollback, archive, replay, and checkpoint reset must all operate with a clear consistency contract and audit trail.
- `P1`: fair-share scheduling, priority rules, and quota enforcement should be proven under mixed-tenant load.
- `P1`: failure classification should be stable enough that retry policy can distinguish transient, deterministic, security, configuration, and data-quality failures.

### 4.3 Source to Bronze completeness

#### API

- `Partial / P0`: circuit breaker automation should go beyond cooldown/backoff heuristics into a stronger operator-grade control loop.
  Reference: [API-to-Bronze-Production-Readiness-Checklist.md](./API-to-Bronze-Production-Readiness-Checklist.md)
- `P1`: source-host, credential, and endpoint-level throttling should be provable under representative production traffic.

#### Kafka stream and Kafka consumer

- `Partial / P0`: native Kafka consumer wiring must be complete for queue-driven worker execution, not only injected `poll-fn` test paths.
- `Partial / P0`: long-lived consumer lifecycle must be explicit where streaming mode is promised.
- `Partial / P0`: offset commit, manifest commit, and checkpoint durability must be proven together for crash recovery. The current path still relies on ordered sequencing rather than an atomic Kafka-offset-plus-manifest commit.
- `P0`: duplicate and lost-record handling still need an explicit Kafka product contract. Operator visibility exists, but there is no implemented deduplication layer in the Kafka consumer path today.
- `P0`: rebalance handling still needs to be implemented as a first-class concern. A native consumer path exists, but there is no full rebalance-listener contract yet for assignment changes and recovery under load.
- `P0`: overlap protection and idempotency contract must apply to Kafka the same way it applies to API ingestion.
- `P1`: Kafka consumer observability should expose lag, partition assignment, rebalance count, stall detection, commit latency, and retention runway in a production-credible way.

#### File and Mainframe files

- `Partial / P0`: remote transports must be complete for the supported claim set: local, S3, SFTP, Azure Blob, and any client-promised HTTP/object-store mode.
- `Partial / P0`: object-store-backed file ingest workflow must be complete for staged/cloud sources.
- `Partial / P0`: fixed-width and copybook parsing need stronger production coverage for REDEFINES, OCCURS, packed decimal edge cases, broader PIC handling, and EBCDIC support.
- `Partial / P0`: malformed record handling must quarantine per-record failures instead of killing whole-file ingestion where feasible.
- `Partial / P0`: local-file checksum and changed-file logic are implemented and test-covered. The remaining gap is carrying that same confidence into remote transport and staged/object-store-backed file flows so new files are never silently skipped.
- `P1`: remote transport timeouts, retries, resume behavior, and partial-download cleanup should be hardened for unstable networks.

### 4.4 Bronze to Silver to Gold automation

- `Done / P0`: Silver proposal generation now supports the promised Bronze inputs: API, Kafka, and File/Mainframe Bronze sources.
- `Partial / P0`: Gold automation is materially implemented with generate, validate, review, publish, and execute paths, but it still needs stronger end-to-end warehouse-backed proof before it should be treated as fully closed for client delivery.
- `Partial / P0`: schema generation, transform generation, review, validation, publish, and execute now use a largely consistent lifecycle across Silver and Gold. The remaining gap is complete proof and audit coverage across the whole release flow, not the absence of core functions.
- `Done / P0`: proposal state machine enforcement now blocks invalid transitions, stale validation reuse, and unsafe mutation after review/publish by using guarded transitions and checksum-based validation currency.
- `P0`: integration tests must cover end-to-end `Bronze -> Silver -> Gold` generation and execution for the promised warehouse targets.
- `P1`: release packaging, artifact traceability, and rollback semantics should be consistent across medallion layers.

### 4.5 Databricks and Snowflake warehouse parity

- `Done / P0`: Databricks SQL emission now goes through a compiler backend with a shared `compile-model` interface instead of remaining only as inline generation logic.
  Reference: [Snowflake-Silver-Gold-Reuse-Tech-Design.md](./Snowflake-Silver-Gold-Reuse-Tech-Design.md)
- `Done / P0`: Snowflake dialect backend now exists for type mapping, expressions, DDL/DML generation, and validation SQL through the shared compiler interface.
  Reference: [Snowflake-Silver-Gold-Reuse-Tech-Design.md](./Snowflake-Silver-Gold-Reuse-Tech-Design.md)
- `Partial / P0`: Snowflake execution routing must be explicit for JDBC insert, staged load, bulk copy, and merge/upsert modes where those are exposed in the UI.
  Reference: [Snowflake-Silver-Gold-Reuse-Tech-Design.md](./Snowflake-Silver-Gold-Reuse-Tech-Design.md)
- `Partial / P0`: merge/upsert compilation and execution paths now exist for Databricks and Snowflake, including emitted `MERGE INTO` SQL. The remaining gap is stronger end-to-end validation, operational proof, and confidence across the promised warehouse targets.
  Reference: [Snowflake-Silver-Gold-Reuse-Tech-Design.md](./Snowflake-Silver-Gold-Reuse-Tech-Design.md)
- `P0`: Databricks and Snowflake validation and publish flows must produce consistent review artifacts and operator visibility.
- `P1`: layer-aware timestamp/type rules should be explicit so Bronze, Silver, and Gold do not drift by warehouse.
- `P1`: if BigQuery or other warehouses are a likely roadmap target, compiler and runtime boundaries should be kept generic enough now to avoid another refactor later.

### 4.6 Operations and monitoring

- `Done / P0`: the ops console now covers the main operator loop with a live backend and frontend for pipeline overview, queue/workers, source health, batches/manifests, checkpoints/replay, bad records, schema/medallion, and admin policies.
  Reference: [Ops-Console-Backend-Design.md](./Ops-Console-Backend-Design.md)
- `Partial / P0`: operator actions exist for replay-from-checkpoint, bad-record replay/ignore/export, worker drain/undrain/force-release, alert lifecycle, circuit-breaker reset, and versioned config rollback. Client deployment still needs one explicit contract for source-level pause/resume and any remaining rollback-by-batch or checkpoint-reset workflows that still live outside the unified ops surface.
  Reference: [Ops-Console-Backend-Design.md](./Ops-Console-Backend-Design.md)
- `Partial / P0`: alerting now has lifecycle states, deduplication, silence/unsilence, workspace scoping, and audit hooks. What still remains for client readiness is proving the notification fan-out integrations against real delivery channels.
  Reference: [Ops-Console-Backend-Design.md](./Ops-Console-Backend-Design.md)
- `Partial / P0`: queue and worker status now surface request backlog, worker drain state, force-release, DLQ visibility, and lease/heartbeat health. Remaining product work is better partition-level hotspot visibility and broader stuck-work diagnostics under scale.
  Reference: [Ops-Console-Backend-Design.md](./Ops-Console-Backend-Design.md)
- `Done / P0`: source observability now exists for API, Kafka, and File/Mainframe paths with freshness, lag, circuit-breaker state, bad-record visibility, replay views, and operator drill-down.
  Reference: [Ops-Console-Backend-Design.md](./Ops-Console-Backend-Design.md)
- `Partial / P0`: data-loss-risk indicators are now operator-visible for stale sources, high lag, and checkpoint-manifest gaps. The remaining gap is extending this into stronger Kafka-native retention/partition-gap detection and source-specific reconciliation rules.
  Reference: [Ops-Console-Backend-Design.md](./Ops-Console-Backend-Design.md)
- `Partial / P0`: bulk operator actions are now bounded and guarded with row-level locking, advisory locking, caps, and audit events. Client deployment still needs these safety rules applied consistently to every destructive operator workflow.
  Reference: [Ops-Console-Backend-Design.md](./Ops-Console-Backend-Design.md)
- `Partial / P0`: frontend polling resilience is now implemented with per-screen refresh isolation, jitter, exponential backoff, connection state, and stale-data indication. Client deployment still needs this behavior validated under real outage and latency conditions.
  Reference: [Ops-Console-Backend-Design.md](./Ops-Console-Backend-Design.md)
- `Partial / P0`: Kafka stream monitoring now exists, but it is still not fully warehouse- or broker-grade. For client production claims it should expose credible consumer telemetry such as per-partition lag, rebalance events, consumer-group state, stall detection, and commit latency from the native consumer path.
  Reference: [Ops-Console-Backend-Design.md](./Ops-Console-Backend-Design.md)
- `Partial / P1`: time-series rollups, sparklines, and day-over-day deltas are implemented. Anomaly views, richer trend analysis, and historical SLO reporting remain to be completed.
  Reference: [Ops-Console-Backend-Design.md](./Ops-Console-Backend-Design.md)
- `Partial / P1`: notification dispatch is pluggable today, but real email, Slack, PagerDuty, or client-specific channel implementations still need to be wired, secured, and proven in production-like environments.
  Reference: [Ops-Console-Backend-Design.md](./Ops-Console-Backend-Design.md)
- `P1`: cross-environment lineage and impact analysis should be available for release operations.

### 4.7 Deployability and supportability

- `Partial / P0`: repo-local packaging exists, including a Dockerfile and baseline repo documentation, but client deployment still needs a complete install playbook covering required services, environment variables, external dependencies, migrations, and bootstrap order.
- `P0`: database migration, upgrade, and rollback process must be scripted and operator-safe.
- `P0`: backup, restore, and disaster recovery procedures must be defined and tested.
- `P0`: health checks, readiness checks, and liveness semantics must be available for app, workers, scheduler, and any stream consumers.
- `P0`: configuration must be environment-driven and documented well enough that a client or delivery team can stand up the product repeatably.
- `Partial / P0`: some runbook-style documentation exists, but client deployment still needs a broader operational set for stuck queue, repeated retries, lost lease, replay, checkpoint correction, bad-record spikes, source outage, warehouse outage, and medallion release recovery.
- `P1`: autoscaling guidance and workload sizing guidance should be documented from measured evidence, not only estimates.
- `P1`: storage growth, retention cost, and metadata DB tuning guidance should be documented for large tenants.
- `P1`: support boundaries must be explicit: supported topologies, supported warehouses, supported source types, scale limits, and recovery expectations.

### 4.8 Validation and production evidence

- `P0`: dedicated load tests still need to be created for representative client volume across API, Kafka, and File/Mainframe paths. The current test suite is broad, but it is not a true load harness.
- `P0`: dedicated failure-injection tests still need to be created for worker crash, network stall, target slowdown, partial batch failure, replay, rollback, and checkpoint recovery.
- `P0`: dedicated concurrency and stress tests still need to be created for overlapping runs, multi-worker claims, rebalance-like source behavior, retry storms, and queue contention.
- `Partial / P0`: medallion integration tests must cover Databricks and Snowflake paths that are part of the product promise.
- `P0`: release signoff must require evidence artifacts, operator approval, and traceable environment/version metadata.
- `P1`: soak tests and long-duration resilience tests should be part of the standard release bar.

### 4.9 Adjacent product gaps that still affect deployment readiness

- `Partial / P1`: AI orchestration is not yet at its own done line. Classifier breadth, guardrails, broader query handling, idempotent multi-step resume, and integration coverage still need closure if AI-assisted product flows are part of the client story.
  Reference: `docs/AI_Agent_Orchestration_Design_v2_1.md`
- `Partial / P1`: product UX still needs a cleaner client-facing admin and operator surface where current repo-local workflows remain engineering-oriented.
- `P2`: tenant-facing onboarding, self-service diagnostics, and polished support workflows should follow after the first stable deployments.

---

## 5. Exit Criteria

Bitool should not be called ready for a standard client deployment until all of the following are true:

- every `P0` item above is `Done`,
- the client-promised source types are complete end-to-end,
- the client-promised warehouse targets are complete end-to-end,
- runtime recovery semantics are documented and demonstrated by tests,
- operators can diagnose, pause, replay, recover, and audit the main failure modes without direct code intervention,
- security controls match the client environment expectations,
- installation, upgrade, backup, restore, and DR procedures are documented and tested,
- release evidence exists for load, fault, soak, and operator-workflow validation.

---

## 6. Recommended Delivery Order

1. Close `P0` security and governance gaps first.
2. Close `P0` runtime correctness and source-to-Bronze gaps next, especially Kafka/File semantics if they are part of the immediate client promise.
3. Finish `P0` Snowflake and Databricks medallion parity only for the warehouse targets actually being sold.
4. Complete `P0` operator workflows and deployment packaging before broadening scope.
5. Use `P1` to move from first-client readiness to repeatable multi-client rollout.
6. Use `P2` for product polish, self-service, and broader platform ergonomics.

---

## 7. Relationship to Existing Docs

- [API-to-Bronze-Production-Readiness-Checklist.md](./API-to-Bronze-Production-Readiness-Checklist.md) remains the narrow API-ingestion checklist.
- [Production-Scale-Reliability-Tech-Design.md](./Production-Scale-Reliability-Tech-Design.md) remains the runtime and platform hardening design.
- [Snowflake-Silver-Gold-Reuse-Tech-Design.md](./Snowflake-Silver-Gold-Reuse-Tech-Design.md) remains the Snowflake medallion reuse design and phase plan.

This document is the consolidated client-deployment checklist across those narrower designs.
