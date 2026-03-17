# Production-Scale Reliability Tech Design for Bitool

## 1. Purpose

This document defines what Bitool still needs in order to be production-ready for large-scale, enterprise data workloads.

The target is not "works for a demo" or "works for a few scheduled API pulls." The target is a platform that can run many tenants, many pipelines, and large data volumes with predictable reliability, controlled concurrency, safe recovery, and clear operational ownership.

This design is grounded in the current Bitool implementation, especially:

- graph authoring and persistence in `src/clj/bitool/graph2.clj` and `src/clj/bitool/db.clj`,
- queueing and run tracking in `src/clj/bitool/ingest/execution.clj`,
- scheduler polling in `src/clj/bitool/ingest/scheduler.clj`,
- API ingestion runtime in `src/clj/bitool/ingest/runtime.clj`,
- paginated connector execution in `src/clj/bitool/connector/api.clj`,
- downstream Databricks job triggering in `src/clj/bitool/databricks/jobs.clj`,
- process startup in `src/clj/bitool/core.clj`.

## 2. Current Architecture Snapshot

Bitool already has a useful foundation:

- graph definitions are versioned in the `graph` table and loaded by graph id,
- execution requests and runs are persisted in Postgres-backed queue tables,
- scheduler polling enqueues work instead of always running inline,
- workers claim queued requests with `FOR UPDATE SKIP LOCKED`,
- execution request dedup already uses `pg_advisory_xact_lock` plus request-key uniqueness,
- request leasing already exists with configurable lease duration on `execution_request`,
- structured `execution_run` and `node_run` rows already capture run-level and node-level outcomes,
- API ingestion supports pagination, retries, checkpoint state, bad records, and Bronze writes,
- Databricks jobs can be triggered after Bronze success,
- there are tests covering scheduler, execution routing, runtime behavior, and schema inference.

The current architecture is still fundamentally single-product-process oriented:

- the HTTP server, scheduler loop, and execution worker all start from the same JVM in `src/clj/bitool/core.clj`,
- metadata storage uses a single hardcoded admin datasource in `src/clj/bitool/db.clj`,
- ingestion runtime builds full row collections in memory before writing,
- queueing is durable but still minimal,
- isolation, governance, and observability are not yet platform-grade.

## 3. What Is Still Pending

### 3.1 Topology and service separation

Pending:

- split control-plane API, scheduler, dispatcher, and worker roles into independently deployable processes,
- support multiple worker replicas without relying on a single shared JVM lifecycle,
- separate interactive UI traffic from heavy execution traffic,
- add explicit environment topology for dev, stage, and prod.

Why:

- today a noisy ingest run can compete with UI/API traffic,
- deploys, restarts, or memory pressure affect the entire product at once,
- horizontal scaling is possible only in a limited form.

### 3.2 Large-data runtime behavior

Pending:

- replace "collect all rows for endpoint, then write" with streaming page-to-batch writes,
- add bounded batch flush thresholds by rows, bytes, and time,
- add spill-to-object-storage support for very large payloads and replay artifacts,
- add chunked checkpoint commits tied to durable write manifests.

Recommended default worker memory budget:

- each worker should keep at most `50 MiB` of in-flight row payloads before flushing,
- expose this as `BITOOL_WORKER_MAX_BATCH_BYTES=52428800`,
- combine the byte ceiling with row-count and elapsed-time flush triggers so small rows do not over-buffer and large rows do not exhaust memory.

Why:

- `run-api-node!` currently accumulates all rows and bad records for an endpoint before calling `load-rows!`,
- `load-rows!` passes the full row set into one bulk insert path,
- this creates avoidable memory pressure and weakens backpressure for large runs.

### 3.3 Queueing, retries, and worker leases

Pending:

- add lease heartbeats and orphan recovery for long-running requests,
- add an expired-lease sweeper and deterministic requeue policy,
- implement delayed retry scheduling instead of only recording `retry_count`,
- add dead-letter handling for poison requests,
- add request priority, fairness, and tenant-aware admission control,
- add run cancellation, timeout, and pause semantics.

Why:

- `execution_request` and related run tables already provide durable queueing, dedup, leasing, and run metadata,
- what is still missing is lease renewal, expired-lease recovery, a full retry state machine, and DLQ routing,
- one queue is not enough for predictable behavior under mixed workloads.

### 3.4 Concurrency and rate governance

Pending:

- enforce concurrency limits per tenant, source system, host, endpoint, and target,
- add global and per-tenant quotas,
- centralize rate-limit state instead of relying only on connector-local sleep and retry behavior,
- prevent one high-volume source from starving the rest of the platform.

Why:

- current concurrency is mostly implicit,
- `connector/api.clj` has HTTP pooling and retry logic, but there is no platform-level admission control policy.

### 3.5 Reliability semantics

Pending:

- define formal run states and terminal semantics,
- move from best-effort checkpoint updates to transactional checkpoint progression,
- add idempotency keys for all externally visible writes and downstream triggers,
- add replay manifests and deterministic rerun behavior,
- add partial-success policy by endpoint and by batch.

Why:

- production systems need precise answers for duplicates, partial writes, retries, and recovery after crash.

### 3.6 Multi-tenancy and isolation

Pending:

- add tenant and workspace identity to all control-plane and run-plane entities,
- isolate queue scheduling, quotas, secrets, and lineage by tenant,
- support tenant-aware encryption and audit,
- add namespace conventions for Bronze, Silver, and audit objects.

Why:

- current runtime tables are graph-centric, not tenant-centric,
- Informatica and Databricks-class usage requires strong isolation boundaries.

### 3.7 Observability and operations

Pending:

- structured logs with run, tenant, graph, node, and request correlation ids,
- metrics for throughput, lag, queue depth, lease expiry, checkpoint age, retry volume, bad record rate, and downstream trigger success,
- distributed tracing across API, scheduler, worker, connector, and Databricks trigger calls,
- operator dashboards, alerting, and SLA/freshness views,
- reconciliation and replay tooling.

Why:

- current code persists run metadata, but product-grade operations require live signals and operator workflows.

### 3.8 Security and governance

Pending:

- replace env-only secret resolution with a real secrets backend,
- add RBAC for graph authoring, publishing, execution, replay, and secret usage,
- add change approval and audit trails for production activation,
- add policy enforcement for egress domains, PII tagging, and retention.

Why:

- secret resolution in runtime currently maps refs to environment variables,
- that is not sufficient for multi-tenant enterprise operation.

### 3.9 Delivery and release engineering

Pending:

- immutable graph artifacts with checksums and promotion history,
- reproducible environment promotion,
- blue/green or canary activation for runtime workers,
- schema migration discipline for metadata tables,
- performance, chaos, and soak test gates before release.

Why:

- production reliability depends as much on delivery discipline as on runtime code.

## 4. Target Production Architecture

```text
Authoring UI
  -> Control Plane API
     -> Metadata DB
     -> Graph artifact store
     -> Secret reference store
     -> Policy engine

Manual trigger / scheduler / webhook
  -> Dispatcher
     -> Durable execution queue
     -> Admission control
     -> Priority and fairness rules

Worker pool
  -> Source connectors
  -> Stream/batch processors
  -> Bronze writers
  -> Checkpoint manager
  -> Replay manifest writer
  -> Downstream trigger adapter

Platform state
  -> run tables
  -> execution events
  -> lease heartbeats
  -> bad records
  -> DLQ
  -> metrics/logs/traces

Consumption
  -> operations UI
  -> alerts
  -> Databricks / warehouse / APIs / downstream services
```

### 4.1 Service split

Bitool should run as four logical services:

1. `bitool-control-plane`
   Handles graph CRUD, validation, publishing, release activation, secrets references, and run requests.

2. `bitool-scheduler`
   Polls schedules and webhooks, computes due work, and submits execution requests.

3. `bitool-dispatcher`
   Applies quotas, priorities, and concurrency policy before making work claimable.

4. `bitool-worker`
   Executes API ingestion, writes Bronze batches, advances checkpoints, records events, and triggers downstream jobs.

Sequencing matters:

- Phase 1 should use role-based startup from the same codebase and deployment artifact, for example `BITOOL_ROLE=api|scheduler|worker|all`,
- Phase 5 is where these roles become independently deployable services with separate scaling, rollout, and failure domains.

These may share a repo and most libraries, but they should not be forced to share a process forever.

### 4.2 Storage split

Bitool needs three storage classes:

- metadata store:
  Postgres for graphs, releases, queue metadata, leases, policy, and audit records.

- object storage:
  S3, ADLS, or GCS for run manifests, raw payload spill files, replay artifacts, and large bad-record payloads.

- analytical target:
  Databricks Delta, Postgres, or warehouse targets for Bronze and downstream curated data.

The metadata store should not be used as the storage layer for large payload retention.

### 4.3 Storage lifecycle and partitioning

Bitool needs an explicit storage temperature model:

- `hot`:
  active queue state, recent runs, recent execution events, recent scheduler state, recent checkpoint rows, and recent bad-record metadata.

- `warm`:
  recent but less frequently queried run history, execution events, and summarized operational records.

- `archive`:
  raw payload spill files, replay artifacts, old manifests, and long-term audit exports in object storage.

The current codebase already writes append-heavy operational data or is moving in that direction. These datasets should not remain as unbounded heap tables forever.

Recommended partition targets in Postgres metadata storage:

- `execution_request`
- `execution_run`
- `node_run`
- future `execution_event`
- future `execution_dlq`
- future `execution_lease_heartbeat`

Recommended partition strategy:

- partition append-heavy operational tables by time, usually monthly,
- sub-partition logically by environment or tenant only when scale proves it is necessary,
- keep current and near-future partitions pre-created,
- run scheduled partition creation and retention cleanup jobs.

Migration rule:

- existing non-partitioned tables must be migrated in stages,
- do not assume an in-place conversion,
- create partitioned replacement tables,
- backfill in bounded time windows,
- validate counts and checksums,
- then cut writers and readers over deliberately.

### 4.4 Bronze partitioning and retention strategy

Bitool also needs a product-level partitioning opinion for Bronze outputs, not just a low-level `partition_columns` field.

Current reality:

- `ingest.runtime` writes Bronze rows with `partition_date`,
- target configuration supports `partition_columns`,
- Databricks table creation already supports partitioned Delta tables through `db/create-table!`.

What is still missing is the operating standard.

Recommended Bronze partition rules:

- default partition key:
  `partition_date` derived from ingestion time for operational stability.

- retain source event time separately:
  `event_time_utc` stays as a data column and may be used downstream for Silver/Gold semantics.

- avoid over-partitioning:
  do not partition Bronze by high-cardinality ids such as tenant, source record id, driver id, or API cursor.

- optional secondary layout hints:
  for Delta targets, use `cluster_by`, Z-order, or similar layout optimization on common filter keys such as `source_system`, `endpoint_name`, `event_time_utc`, or business ids.

- late-arriving data:
  allow event-time skew without rewriting the partitioning strategy; Bronze remains operationally partitioned, not business-perfectly partitioned.

Recommended table families:

- Bronze raw records:
  partition by `partition_date`

- bad records:
  partition by `created_at` or `partition_date`

- endpoint run detail:
  partition by `started_at_utc` month in metadata storage if retained at large scale

- batch manifests:
  partition by `committed_at_utc` month in metadata storage

Retention guidance:

- recent Bronze remains hot in the analytical target,
- raw payload spill files move to object storage after the hot replay window,
- old batch manifests and replay artifacts can be compacted or archived after SLA and audit windows expire,
- bad-record payload bodies should be archived separately from searchable bad-record metadata.

Default retention windows:

- hot operational metadata: 30 days,
- warm queryable run history: 90 days,
- archive for raw payloads, replay artifacts, and audit exports: 365 days by default unless compliance requires longer,
- bad-record metadata searchable in hot or warm storage: 90 days,
- bad-record payload bodies archived after 30 days unless active incident handling requires extension.

## 5. Reliability Model

### 5.1 Execution contract

Bitool should explicitly adopt this contract:

- request delivery is at-least-once,
- Bronze write semantics are idempotent at the batch level,
- Bronze schema evolution is monotonic: type widening is allowed, type narrowing is not,
- Bronze writers and Bronze DDL generation must agree on effective column types before idempotency can be claimed,
- checkpoint advancement is monotonic and happens only after durable writes,
- downstream triggers are idempotent and tied to run manifests,
- replay is first-class and deterministic against a published graph artifact.

This is the correct reliability model for Bitool. Exactly-once across every source, queue, target, and downstream system is not realistic.

### 5.1.1 Schema enforcement policy

Schema inference alone is not enough. Bitool also needs an explicit enforcement mode per endpoint or target:

- `strict`:
  reject writes that do not match the declared Bronze schema.

- `additive`:
  allow new columns, but reject incompatible type changes or column removal from the effective contract.

- `permissive`:
  accept drift into raw payload storage, log the mismatch, and keep promoted-column changes advisory only.

Recommended default:

- Bronze endpoints should default to `additive`,
- high-risk regulated sources may opt into `strict`,
- `permissive` should be allowed only with explicit operator acknowledgement.

### 5.2 Run lifecycle

Full target run lifecycle:

- `requested`
- `admitted`
- `queued`
- `leased`
- `running`
- `partially_committed`
- `succeeded`
- `failed_retryable`
- `failed_terminal`
- `cancelled`
- `timed_out`

Every state transition must be recorded as an append-only execution event.

Phase sequencing:

- Phase 1 should extend the current model only to:
  `queued -> leased -> running -> succeeded | failed | cancelled | timed_out`
- Phase 2 can add:
  `requested`, `admitted`, `partially_committed`, `failed_retryable`, and `failed_terminal`

This keeps the first hardening pass close to the existing `queued -> running -> success/failed` implementation in `ingest/execution.clj`.

### 5.3 Checkpoint safety

Checkpoint progression should use these rules:

- read last successful checkpoint when the run starts,
- record attempted cursor and watermark as run-scoped progress, not global truth,
- write Bronze batches,
- write a batch manifest containing row count, byte count, partition keys, and high watermark,
- only after the manifest is durable, advance the endpoint checkpoint,
- on retry or replay, resume from the last committed checkpoint plus configured overlap.

The current success/failure checkpoint helpers are a good foundation, but they need to be attached to durable batch commits rather than only end-of-endpoint completion.

Prerequisite:

- path-derived identifiers used in promoted columns, manifests, and checkpoint-related metadata must be sanitized into valid stable identifiers before batch-level checkpointing is introduced.

### 5.3.1 Atomic batch commit protocol

Batch manifests are not enough unless commit atomicity is defined.

Required protocol:

1. write the batch to a staging target or staging partition,
2. validate row count, checksum, schema mode, and partition metadata,
3. write the batch manifest row,
4. atomically promote the batch to committed state,
5. only then advance the endpoint checkpoint,
6. only after checkpoint success may downstream triggers consume the committed batch.

If any step after staging fails:

- the batch remains uncommitted,
- the checkpoint must not move,
- replay or retry may safely re-attempt the batch using the same batch key.

Implementation note:

- for Postgres targets, this may mean a transaction boundary with staging tables,
- for Delta or object-storage-backed targets, this means manifest plus commit-marker semantics rather than assuming bulk insert alone is atomic.

### 5.3.2 Batch versioning and rollback

Bitool should support rollback by committed batch, even before implementing warehouse-native time travel.

Minimum model:

- every committed Bronze flush gets a stable `batch_id`,
- every row or output file is attributable to that batch,
- batches can be marked `active` or `rolled_back`,
- replay and downstream processing can filter on active batches only.

This is the practical first step toward data-level undo for Bitool.

### 5.4 Failure isolation

Failure domains must be separated:

- control-plane API failure must not stop workers already running,
- one tenant must not exhaust all worker capacity,
- one source host rate-limit event must not block unrelated connectors,
- a downstream Databricks trigger failure must not corrupt Bronze success state,
- bad records must be quarantined without poisoning the whole run by default.

### 5.5 Replay model

Replay must support:

- rerun from last checkpoint,
- rerun a time window,
- rerun from a prior published graph version,
- replay bad records only,
- replay downstream triggers from existing Bronze manifests without re-extracting source data when possible.

### 5.5.1 Deterministic replay

Replay should be split into two levels:

- `best-effort replay`:
  re-fetch source data using the original graph artifact and current credentials.

- `deterministic replay`:
  replay from stored source pages, stored Bronze manifests, or stored bad-record payloads captured during the original execution.

Required controls for deterministic replay:

- pin replay to a specific graph artifact version,
- retain source page artifacts in object storage for the configured replay window,
- verify schema compatibility before replay writes begin,
- allow downstream-trigger replay from committed manifests without re-fetching the source.

## 6. Scalability Design

### 6.1 Control-plane scalability

Requirements:

- stateless API servers behind a load balancer,
- metadata DB connection pooling with sane limits and no hardcoded local credentials,
- immutable graph artifacts cached by version id,
- read/write separation where needed for heavy history queries.

### 6.2 Queue scalability

Recommended evolution:

Phase 1:

- keep Postgres as the durable queue because it matches the current implementation and lowers operational cost,
- partition requests by environment and tenant,
- add indexes for claim order, retry schedule, and lease expiry,
- add a sweeper that requeues expired leases.

Phase 2:

- move hot-path dispatching to SQS, Kafka, or another dedicated queue when queue depth, worker count, or scheduling latency justifies it,
- keep Postgres as the source of truth for run metadata even if the claim path moves.

### 6.3 Worker scalability

Workers need:

- independent autoscaling,
- work-type specific pools such as `api-small`, `api-large`, `replay`, and `downstream-trigger`,
- CPU and memory sizing by workload class,
- hard per-run memory ceilings,
- lease renewal heartbeats while long runs are active.

Compute isolation should follow workload class, not only queue type:

- `api-small`:
  lightweight scheduled pulls with small row budgets.

- `api-large`:
  high-volume or high-memory endpoints with larger batch budgets and stricter concurrency caps.

- `replay`:
  lower priority by default, isolated from production freshness paths.

- `downstream-trigger`:
  lightweight orchestration or job-trigger work that should not share heap limits with large ingest.

### 6.4 Connector concurrency

Concurrency must be governed at multiple levels:

- `tenant max concurrent runs`
- `graph max concurrent runs`
- `source credential or API key max in-flight requests`
- `source host max in-flight requests`
- `endpoint max pages in parallel`
- `target max concurrent writers`
- `downstream trigger max concurrent jobs`

`connector/api.clj` already has a reusable HTTP connection manager. That should become one layer inside a wider platform concurrency controller, not the primary control surface.

### 6.5 Large data handling

For large endpoints, Bitool should not materialize all rows in memory.

Target flow:

1. fetch a page,
2. extract logical records,
3. convert to Bronze rows,
4. accumulate into bounded batch buffers,
5. flush batch to target or spill file,
6. emit batch manifest,
7. continue until endpoint completion,
8. advance checkpoint at safe commit points.

Flush thresholds should be configurable by:

- row count,
- serialized byte size,
- wall-clock age,
- bad-record ratio.

### 6.6 Target strategy

Bitool should not try to become a distributed compute engine inside the JVM.

Production boundary:

- Bitool owns orchestration, connector execution, Bronze normalization, metadata, reliability, and control-plane concerns,
- Databricks or another warehouse engine owns heavy distributed transformation when data volume requires cluster-scale compute,
- lightweight row-wise transforms can stay in Bitool workers,
- heavy joins, repartitions, wide aggregations, and very large backfills should execute in downstream compute engines.

That is the practical path to being "Databricks/Informatica-like" without building a new Spark runtime inside Bitool.

### 6.7 Dependency-aware execution

Bitool should grow from single-graph execution into dependency-aware orchestration when Bronze, Silver, and Gold flows are modeled as separate graphs or packages.

Required behavior:

- downstream graphs may declare dependencies on upstream graph success,
- dispatcher admission checks upstream completion state before claiming dependent work,
- replay of an upstream graph may optionally trigger dependent reprocessing,
- dependency state must be version-aware so a downstream graph can depend on a specific upstream published artifact or committed batch.

This is not a Phase 1 requirement, but it is a necessary Phase 3+ capability if Bitool wants Informatica-like workflow orchestration.

## 7. Backend Concurrency and Robust Execution Design

### 7.1 Admission control

Before a request becomes claimable, the dispatcher should evaluate:

- tenant quota,
- source quota,
- endpoint concurrency policy,
- schedule priority,
- replay vs production priority,
- maintenance windows,
- dependency locks,
- fair-share eligibility so one tenant cannot monopolize claim bandwidth even inside quota.

If limits are exceeded, the request stays deferred with a next-check timestamp.

Default fairness model:

- weighted round-robin across tenant bins,
- FIFO within each tenant bin,
- starvation alerting when tenant wait time exceeds `2x` platform p95 claim latency.

### 7.2 Worker leasing

Lease model:

- worker claims request with lease expiry,
- worker heartbeats every `lease_duration / 3`,
- sweeper claims expired requests into a dedicated `recovering_orphan` transition before terminal retry/DLQ routing,
- run event log records lease owner changes,
- repeated orphaning eventually routes request to DLQ for inspection.

Recommended defaults:

- default lease duration: 300 seconds,
- default heartbeat interval: 100 seconds,
- orphan detection should require at least two missed heartbeat windows before reclaim unless the worker is known dead through a stronger signal.

### 7.3 Timeouts and cancellation

Every run needs:

- max queue wait,
- max total wall-clock runtime,
- max idle time between progress events,
- cancellation token checked at page and batch boundaries.

### 7.4 Retry classes

Retry policy should be class-based:

- transport and transient 5xx: exponential backoff with jitter,
- 429: respect `Retry-After`, plus platform quota dampening,
- auth refreshable: one refresh path, then bounded retries,
- schema drift advisory: continue if policy allows,
- target write conflict: retry only if idempotent batch key allows it,
- validation or configuration errors: terminal failure, no retry.

Bitool should make this explicit through a reusable error taxonomy:

- `transient_network`
- `rate_limited`
- `auth_expired`
- `schema_drift`
- `target_conflict`
- `config_error`
- `poison_payload`
- `dependency_blocked`

Connector and runtime code should classify errors into one of these failure classes before retry or DLQ policy is chosen.

### 7.4.1 Circuit breakers and adaptive backpressure

Circuit breakers should be first-class, not connector-specific conventions.

Recommended breaker scope:

- per `(source_host, credential)` by default,
- with optional endpoint-level overrides when one path is materially different.

State machine:

- `closed`
- `open`
- `half_open`

Trip inputs:

- consecutive failure threshold,
- error-rate threshold in a sliding window,
- repeated `429` or auth failures.

Backpressure behavior should also be explicit:

- if a source is rate-limited, scheduler and dispatcher should reduce new admissions for that source during cooldown,
- if queue depth exceeds platform thresholds, user-triggered work may receive `429` with retry guidance,
- if target writes slow down, workers should reduce fetch concurrency before exhausting memory.

### 7.5 Dead-letter queue

DLQ is required for:

- requests that exceed retry budget,
- repeated lease orphaning,
- unrecoverable payload parsing failures,
- policy violations that require operator action.

DLQ entries must preserve:

- request metadata,
- graph version id,
- tenant id,
- checkpoint state,
- failure class,
- last error,
- artifact references.

## 8. Data Model Additions

The current tables are a start. A production platform also needs:

- `tenant`
- `workspace`
- `secret_ref`
- `graph_artifact`
- `graph_release_history`
- `graph_dependency`
- `graph_edit_lock` or optimistic version column support
- `execution_event`
- `execution_lease_heartbeat`
- `execution_dlq`
- `run_batch_manifest`
- `run_output_manifest`
- `endpoint_schema_snapshot`
- `rate_limit_policy`
- `concurrency_policy`
- `replay_request`
- `alert_rule`
- `lineage_edge`
- `data_contract`

Key requirements:

- all run-plane rows carry `tenant_id`, `workspace_id`, `environment`, and `graph_version_id`,
- event tables are append-only,
- checkpoint tables use upsert semantics rather than delete-then-insert where possible,
- schema inference snapshots capture inferred fields, confidence or provenance, sample record count, and inference timestamp,
- graph save and publish paths use optimistic concurrency so concurrent edits return a conflict instead of silently overwriting drafts,
- large payload columns are references to object storage rather than always inline text.

## 9. Security and Governance

### 9.1 Secret management

Target design:

- store only secret references in Bitool metadata,
- resolve secrets at runtime from Vault, AWS Secrets Manager, Azure Key Vault, or GCP Secret Manager,
- before adding new secret backends, make the current env-var resolution path fail loudly on missing references instead of returning `nil`,
- rotate without graph edits,
- audit every secret access by worker identity.

### 9.2 RBAC

Minimum roles:

- viewer,
- developer,
- publisher,
- operator,
- tenant-admin,
- platform-admin.

Permissions must distinguish:

- edit draft,
- publish,
- activate in prod,
- trigger manual run,
- replay,
- view secrets metadata,
- manage quotas and policies.

### 9.3 Policy enforcement

Add policy checks for:

- outbound host allowlists,
- approved target connections,
- PII classification,
- minimum retry and timeout configuration,
- mandatory owner and SLA tags before publish.

## 10. Observability and SRE Design

### 10.1 Metrics

Bitool should expose at least:

- queue depth by tenant and request kind,
- scheduler lag,
- claim latency,
- active leases,
- expired leases,
- run duration,
- page fetch latency,
- rows extracted and rows written,
- bad-record rate,
- checkpoint age,
- Databricks trigger success rate,
- secret resolution failure count.

### 10.2 Logs

All logs should be structured and include:

- `tenant_id`
- `workspace_id`
- `environment`
- `graph_id`
- `graph_version_id`
- `run_id`
- `request_id`
- `node_id`
- `endpoint_name`
- `worker_id`
- `trace_id`

### 10.3 Tracing

Trace spans should cover:

- API request ingress,
- enqueue,
- dispatch decision,
- worker claim,
- connector request,
- batch write,
- checkpoint commit,
- downstream trigger.

### 10.4 SLOs

Suggested initial production SLOs:

- control-plane API availability: 99.9%
- scheduler enqueue latency for due jobs: p95 under 60 seconds
- worker claim latency after admission: p95 under 30 seconds
- successful run completion for healthy sources: 99% within configured SLA window
- checkpoint freshness alert when lag exceeds tenant SLA
- Bronze target freshness: data age at target must not exceed the endpoint-configured SLA window, and should alert when freshness lag exceeds `2x` the scheduled interval

These are target operating objectives and should be tuned with real workload data.

## 11. Immediate Code Implications for This Repo

The current codebase should evolve in these concrete ways:

### 11.1 `src/clj/bitool/core.clj`

- stop starting HTTP, scheduler, and worker unconditionally in the same runtime,
- introduce role-based startup such as `BITOOL_ROLE=api|scheduler|worker|all`.

### 11.2 `src/clj/bitool/db.clj`

- externalize admin datasource config,
- remove hardcoded localhost credentials,
- support pooled and environment-specific metadata DB configuration,
- add real upsert helpers and chunked batch insert helpers,
- add commit-helper support for staged batch promotion and manifest persistence,
- prepare for optimistic concurrency checks on graph save and publish.

### 11.3 `src/clj/bitool/ingest/execution.clj`

- add event log table writes,
- add lease heartbeat updates,
- add expired-lease sweeper,
- implement retry scheduling and DLQ flow,
- add priority and tenant-aware claim rules,
- classify runtime failures into stable failure classes before retry policy is applied,
- add fair-share tenant claim ordering.

### 11.4 `src/clj/bitool/ingest/bronze.clj`

- stop unconditional `(str v)` coercion for promoted values,
- align writer value coercion with inferred and declared Bronze column types,
- enforce monotonic schema evolution rules so reruns do not narrow types unexpectedly,
- make bad-record routing explicit for value/type conversion failures.

### 11.5 `src/clj/bitool/ingest/runtime.clj`

- replace endpoint-wide row accumulation with streaming batch writers,
- checkpoint after durable batch manifests,
- separate Bronze success from downstream trigger success,
- emit richer run-state events,
- validate or sanitize dynamic table identifiers used by `fetch-checkpoint` and `replace-row!`,
- make `resolve-secret-ref` fail loudly on missing references instead of returning `nil`,
- enforce endpoint schema mode before batch commit,
- persist replayable source-page artifacts and batch commit metadata,
- support rollback-by-batch semantics.

### 11.6 `src/clj/bitool/utils.clj` and `src/clj/bitool/ingest/schema_infer.clj`

- fix `path->name` so brackets and other non-identifier characters are sanitized deterministically,
- ensure schema inference and promoted-column generation share the same identifier normalization rules,
- prevent path-derived column names from drifting across runs.

### 11.7 `src/clj/bitool/connector/api.clj`

- add platform-aware throttling hooks,
- add circuit-breaker integration by credential, host, and endpoint,
- add request metrics and trace propagation,
- support cancellation checks between pages and retries,
- expose enough error context for failure classification and deterministic replay artifact capture.

### 11.8 `src/clj/bitool/databricks/jobs.clj`

- make downstream trigger idempotent,
- persist trigger attempts and responses,
- support asynchronous follow-up status polling when required by SLA.

### 11.9 `src/clj/bitool/ingest/scheduler.clj`

- stop swallowing top-level loop exceptions without structured context,
- record scheduler poll failures as metrics and structured logs,
- keep enqueue failure handling explicit so slot state and operator visibility stay correct,
- support source-level cooldown propagation when circuit breakers or rate limits are active.

### 11.10 `src/clj/bitool/graph2.clj` and graph persistence paths

- add optimistic concurrency checks for draft graph saves,
- return conflict responses on stale publish attempts,
- make dependency declarations explicit for cross-graph orchestration rather than encoding them only in naming or manual schedules.

## 12. Phased Implementation Plan

### Phase 0: Correctness bugs that block hardening

- fix dynamic table-name validation in runtime paths such as checkpoint fetch and replace,
- fix Bronze writer and DDL type mismatch caused by unconditional stringification,
- fix `path->name` identifier sanitization for `[]` and other invalid characters,
- fix silent missing-secret resolution,
- fix top-level scheduler exception swallowing so failures are visible before scale work begins.

### Phase 1: Hardening the current runtime

- role-based process startup,
- externalized metadata DB config,
- lease heartbeat and expired-lease recovery,
- retry scheduler and DLQ,
- structured logs and metrics,
- chunked batch writes,
- explicit Phase 1 run state machine: `queued`, `leased`, `running`, `succeeded`, `failed`, `cancelled`, `timed_out`,
- schema enforcement modes,
- failure taxonomy and classifier,
- circuit breakers with source cooldown propagation.

### Phase 2: Large-data reliability

- streaming endpoint execution,
- batch manifests,
- repo-local artifact persistence first, with object-storage spill and replay artifacts required to complete Phase 2,
- checkpoint commit-at-batch-boundary,
- target-specific writer abstractions,
- atomic batch commit protocol,
- deterministic replay from stored source artifacts,
- rollback-by-batch.

Current repo-local landing is only a Phase 2 core runtime subset:

- bounded streaming execution,
- pluggable artifact persistence (`local`, `http`, `none`) with local default,
- batch manifests and batch-boundary checkpoint progression,
- atomic batch commit path for transactional targets (Postgres/MySQL), with non-transactional fallback behavior unchanged.

Still deferred before Bitool can claim full Phase 2 delivery:

- object-store-backed artifact retention and replay,
- atomic manifest plus checkpoint commit semantics,
- deterministic replay from stored source artifacts rather than re-fetch only.

### Phase 3: Multi-tenant control plane

- tenant/workspace model,
- quotas and concurrency policies,
- secret manager integration,
- RBAC and publish/activate approval workflow,
- fair-share scheduling across tenants,
- dependency-aware cross-graph execution,
- optimistic concurrency for graph editing and publishing.

Current repo-local landing is a Phase 3 control-plane subset:

- metadata-backed tenant/workspace records with graph-to-workspace assignment,
- per-workspace queue and concurrency limits enforced at enqueue/claim time,
- weighted fair-share worker claim ordering by workspace activity,
- dependency metadata plus enqueue-time blockers for unmet upstream graph success,
- DB-backed secret resolution for ingest auth refs; current repo-local secret values are stored plaintext in Postgres and still require envelope encryption or external secret-manager backing before production rollout,
- optimistic graph-save guards on the main ingestion-related save routes.

Still deferred before Bitool can claim full Phase 3 delivery:

- RBAC and approval workflow for publish/activate,
- policy-driven tenant administration UX,
- encrypted secret storage or external secret-manager integration for control-plane secrets,
- multi-step release promotion with explicit prod approval gates.

### Phase 4: Operations product

- run explorer,
- freshness dashboard,
- replay tooling,
- SLA alerts,
- lineage and impact views.

Current repo-local landing is a Phase 4 operations subset:

- enriched run explorer filters by graph, tenant, workspace, endpoint, and request kind,
- metadata-backed endpoint freshness status and overdue alert views,
- replay of prior execution runs back into the queue with `trigger_type = replay`,
- graph dependency lineage endpoints for upstream/downstream impact inspection.

Still deferred before Bitool can claim full Phase 4 delivery:

- UI-grade operator workflows,
- notification fan-out for SLA alerts,
- replay from stored source artifacts rather than queueing a new live fetch,
- cross-environment impact analysis beyond graph dependency metadata.

### Phase 5: Scale-out evolution

Current repo-local Phase 5 landing:

- queue partitioning on `execution_request` and `execution_run` via deterministic `queue_partition`,
- workload-specific worker pools through `BITOOL_WORKER_QUEUE_PARTITIONS` and `BITOOL_WORKER_WORKLOAD_CLASSES`,
- tenant-level queue/concurrency quotas layered on top of workspace limits,
- autoscaler-facing demand snapshots grouped by tenant, workspace, partition, and workload class,
- daily usage metering for execution requests by tenant/workspace/request kind/workload/partition/status,
- minimal execution plugin registry so new request kinds can be added without hardcoding more dispatch branches.

Still deferred before Bitool can claim full Phase 5 delivery:

- dedicated dispatcher service separated from workers,
- broker-backed hot-path queue offload beyond Postgres,
- automatic worker autoscaling controller that consumes the demand snapshot,
- regional deployment, failover orchestration, and disaster recovery runbooks,
- tenant billing/reporting workflows built on top of usage metering,
- stronger plugin packaging/versioning beyond the current in-process registry.

Deferred beyond the current roadmap unless scale proves the need:

- warehouse-native time travel equivalents beyond rollback-by-batch,
- broad pushdown optimization for non-API sources,
- append-only checkpoint-log replacement for current checkpoint rows,
- fully distributed metadata storage beyond tuned Postgres plus replicas and queue offload.

## 13. Production Readiness Exit Criteria

Bitool should not be called production-ready for large-scale workloads until all of the following are true:

- API, scheduler, and worker roles can be deployed independently,
- no endpoint requires unbounded in-memory accumulation to complete,
- worker leases are renewed and recovered automatically,
- retries, DLQ, and replay are first-class product behaviors,
- every run is traceable with structured logs, metrics, and event history,
- checkpoints advance only after durable writes,
- tenant quotas and concurrency controls are enforced,
- secrets come from a managed backend,
- production publish and activation are auditable,
- load, failover, and soak tests have passed against representative data volumes.

## 14. Recommended Next Moves

The highest-value next steps for Bitool are:

1. split runtime roles in `bitool.core`,
2. harden `ingest.execution` with lease heartbeats, requeue, retries, and DLQ,
3. refactor `ingest.runtime` to stream batches instead of collecting full endpoint result sets,
4. externalize metadata DB and secrets configuration,
5. add metrics, structured logs, and operator-visible run history,
6. define tenant/workspace identity before expanding production usage.

Without these six moves, Bitool remains a strong prototype with useful production-oriented pieces, but not yet a production-scale data platform.
