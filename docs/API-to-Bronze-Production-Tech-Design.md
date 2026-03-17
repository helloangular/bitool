# API-to-Bronze Production Tech Design

## 1. Purpose

This document defines the remaining technical design required to make Bitool's `API -> Bronze` path production-ready.

It assumes the current repo-local baseline already provides:

- paginated API extraction,
- streaming batch flushes,
- batch manifests,
- checkpoint progression at batch boundaries,
- bad-record quarantine,
- execution retries and DLQ,
- a Phase 2 core runtime subset in `src/clj/bitool/ingest/runtime.clj` and `src/clj/bitool/ingest/execution.clj`.

This document focuses on the remaining work for correctness, replayability, durability, and scale.

---

## 2. Scope

In scope:

- API extraction to Bronze,
- schema inference and promoted columns,
- batch manifests,
- checkpoint and watermark progression,
- bad-record capture,
- replay and rollback,
- artifact retention,
- API-to-Bronze observability and SLOs.

Out of scope:

- Silver and Gold transformation logic,
- warehouse-native optimization beyond what API-to-Bronze requires,
- full platform RBAC and approval workflow details beyond how they affect API-to-Bronze execution.

---

## 3. Current State

The current API-to-Bronze runtime in `src/clj/bitool/ingest/runtime.clj` already does the following:

- resolves checkpoint state,
- fetches pages asynchronously,
- samples pages for schema inference,
- converts logical records into Bronze rows and bad records,
- flushes bounded batches,
- writes manifest rows,
- advances checkpoint state,
- records endpoint freshness after success.

Operationally, the runtime now works as a bounded streaming loop rather than a full-endpoint accumulator:

- pages are fetched from the source,
- page payloads are converted into Bronze rows and bad records,
- rows accumulate into an in-memory batch buffer bounded by row count and byte count,
- the buffer is flushed through the manifest plus checkpoint path at safe commit points,
- the loop continues until a terminal page message is reached.

This is materially better than the old "collect everything then bulk insert" model. The remaining gap is not basic ingestion support. The remaining gap is production semantics.

The most important missing properties are:

- deterministic replay,
- object-store-backed artifact durability,
- target-agnostic atomic commit semantics,
- rollback-by-batch,
- formal schema enforcement policy,
- production-grade operator controls and SLOs.

---

## 4. Design Goals

The API-to-Bronze path must satisfy this contract:

- request execution is at-least-once,
- batch writes are idempotent by stable `batch_id`,
- checkpoint movement is monotonic and happens only after durable committed state,
- replay can be deterministic within a configured retention window,
- bad records are isolated from normal record success,
- operators can inspect, replay, and roll back safely.

---

## 5. Target Reliability Model

### 5.1 Batch commit contract

Every API-to-Bronze flush must follow one durable contract:

1. materialize batch output in a staging target or staging state,
2. validate row count, checksum, schema mode, and partition metadata,
3. persist manifest metadata,
4. mark the batch committed,
5. advance checkpoint state,
6. expose the batch to downstream consumers.

If any step fails before commit:

- checkpoint must not move,
- batch must remain non-committed,
- retry must be safe with the same `batch_id`.

### 5.2 Durable batch identity

Each batch needs:

- `batch_id`,
- `run_id`,
- `graph_version_id`,
- `source_system`,
- `endpoint_name`,
- `batch_seq`,
- `status`,
- `artifact_path`,
- `artifact_checksum`,
- `max_watermark`,
- `next_cursor`,
- `row_count`,
- `bad_record_count`,
- `partition_dates`.

Current `run_batch_manifest` is a good base. The remaining requirement is stronger commit semantics and replay usage.

### 5.3 Bronze visibility contract

Downstream readers and replay tooling must consume only committed and active batches.

This requires:

- manifest state that distinguishes staged vs committed vs rolled_back,
- a batch activity flag or equivalent filter,
- downstream trigger logic tied to committed manifests instead of endpoint success alone.

---

## 6. Schema Design

### 6.1 Enforcement modes

Every endpoint should declare a Bronze schema mode:

- `strict`: schema drift fails the batch,
- `additive`: new columns are allowed, incompatible type changes fail,
- `permissive`: raw payload stays authoritative and promoted-column drift is advisory.

Recommended default:

- `additive`.

Repo-local subset now implemented:

- endpoint configs accept `schema_enforcement_mode`,
- runtime enforces `strict`, `additive`, and `permissive` behavior during inference,
- additive mode allows widening and rejects narrowing or incompatible type changes,
- legacy `schema_evolution_mode` values still map into the new enforcement modes for backward compatibility.

### 6.2 Effective schema rules

The runtime should compute one effective schema from:

- inferred fields,
- manually configured selected fields,
- override types,
- reserved Bronze columns.

Rules:

- identifier normalization must be stable,
- duplicate normalized names fail fast,
- type widening is allowed,
- type narrowing is rejected,
- writer coercion and DDL types must share the same canonical type map.

### 6.3 Schema snapshots

Add `endpoint_schema_snapshot` metadata:

- `graph_id`,
- `api_node_id`,
- `graph_version_id`,
- `inferred_fields_json`,
- `sample_record_count`,
- `captured_at_utc`.

This gives operators a queryable history of schema inference changes and supports replay compatibility checks.

Repo-local subset now implemented:

- `endpoint_schema_snapshot` rows are persisted during inference-enabled runs,
- snapshots capture inferred fields, sample record count, and capture timestamp,
- replay compatibility checks against those snapshots are still deferred.

---

## 7. Replay Design

### 7.1 Replay levels

Support two replay modes:

- `best_effort`: current behavior, re-fetch from source using pinned graph artifact.
- `deterministic`: replay from stored source pages, bad-record payloads, or committed manifests without live source dependency.

### 7.2 Required replay artifacts

For deterministic replay, preserve:

- source page payloads,
- source response metadata,
- page ordering,
- checkpoint state at batch boundaries,
- manifest rows,
- bad-record payloads,
- graph artifact version reference.

### 7.3 Artifact storage

The current `local|http|none` artifact modes are not enough for production replay.

Dependency note:

- deterministic replay depends on this artifact-writer layer being complete first,
- best-effort replay is available immediately,
- deterministic replay reader work should not begin until source-page artifact persistence is durable and queryable.

Target design:

- object-store-backed artifact writer abstraction,
- implementations for S3, ADLS, and GCS,
- retention policy by artifact class,
- checksum validation on read,
- path convention by tenant, workspace, endpoint, run, and batch.

Recommended object path shape:

```text
tenant=<tenant>/workspace=<workspace>/endpoint=<endpoint>/run=<run-id>/batch=<batch-id>/
```

Stored objects:

- `pages/<page-seq>.json`,
- `manifest.json`,
- `bad-records/<record-id>.json`.

### 7.4 Replay entry points

Operators should be able to:

- replay from last committed checkpoint,
- replay a time window,
- replay a specific batch,
- replay bad records only,
- replay downstream triggers from committed manifests.

---

## 8. Rollback Design

### 8.1 Rollback-by-batch

Add rollback metadata to manifest state:

- `active`,
- `rolled_back_at_utc`,
- `rolled_back_by`,
- `rollback_reason`.

Rollback behavior:

- mark target batch inactive,
- do not move checkpoint backward automatically unless explicitly requested,
- downstream consumers ignore inactive batches,
- replay can create a new active batch after rollback.

### 8.2 Target-specific behavior

- Postgres/MySQL Bronze targets:
  use transactional update/delete semantics keyed by `batch_id`.
- Delta/object-store-backed Bronze targets:
  use manifest + commit marker + active-batch filter semantics.

### 8.3 Checkpoint behavior on rollback

Rollback must not silently rewrite global checkpoint truth.

Default rule:

- rolling back a batch does not move the endpoint checkpoint backward automatically.

Reason:

- the checkpoint represents the last committed ingestion progress at the time of write,
- automatic regression would make rollback mutate extraction state implicitly,
- different rollback intents exist: data undo, full re-ingest, or selective corrective replay.

Operator behavior:

- if the operator wants to re-ingest from before the rolled-back batch, replay must accept an explicit watermark or cursor override,
- if the operator only wants to suppress the bad batch in Bronze, rollback alone is sufficient and checkpoint remains unchanged,
- if the rolled-back batch established the current checkpoint, the system should mark the endpoint as requiring operator review before a backward replay.

---

## 9. Checkpoint and Watermark Design

### 9.1 Global checkpoint rules

Checkpoint state must represent only committed truth:

- `last_successful_watermark`,
- `last_successful_cursor`,
- `last_successful_run_id`,
- `rows_ingested`,
- `status`,
- `updated_at_utc`.

Attempted but uncommitted progress should remain run-scoped, not endpoint-global.

### 9.2 Recommended model addition

Add run-scoped progress tracking:

- `run_progress_watermark`,
- `run_progress_cursor`,
- `last_page_seq`,
- `last_attempted_at_utc`.

This prevents failure recovery from depending on global checkpoint rows for in-progress work.

---

## 10. Bad-Record Design

### 10.1 Quarantine contract

Bad records should capture:

- `batch_id`,
- `run_id`,
- `source_system`,
- `endpoint_name`,
- `error_class`,
- `error_message`,
- `payload_json`,
- `payload_checksum`,
- `created_at`.

### 10.2 Retention

Recommended policy:

- bad-record metadata searchable for 90 days,
- payload bodies archived after 30 days,
- extend retention only during active incidents or regulated workloads.

### 10.3 Replay

Bad-record-only replay should:

- read archived payloads,
- re-run only the record conversion and write path,
- not re-fetch the source,
- write a new corrective batch with a new `batch_id`.

---

## 11. Partitioning and Retention

### 11.1 Bronze partition policy

Default Bronze partitioning should be operational, not business-perfect:

- partition by `partition_date`,
- keep `event_time_utc` as a separate query column,
- avoid partitioning by high-cardinality IDs.

### 11.2 Manifest and audit retention

Recommended defaults:

- hot metadata: 30 days,
- queryable run history: 90 days,
- replay artifacts and archived payloads: 365 days,
- bad-record payload bodies: 30 days unless extended.

---

## 12. Observability and SLOs

### 12.1 Required metrics

Add API-to-Bronze metrics for:

- pages fetched,
- rows written,
- bad-record count and ratio,
- batch flush latency,
- manifest commit latency,
- checkpoint lag,
- checkpoint age,
- replay success/failure,
- artifact write/read failures,
- source rate-limit incidents.

### 12.2 Required SLOs

- Bronze freshness: target data age must stay within endpoint SLA.
- Checkpoint lag: alert when lag exceeds `2x` scheduled interval.
- Bad-record rate: alert when ratio exceeds configured threshold.
- Replay success: alert on repeated deterministic replay failures.

---

## 13. Failure Scenarios

The design must explicitly handle:

- worker crash after source fetch but before manifest commit,
- manifest write failure after Bronze staging,
- checkpoint write failure after manifest persistence,
- object-store artifact write failure,
- source API rate limit or outage,
- target write slowdown or outage,
- schema drift detected mid-run,
- replay against incompatible current Bronze schema.

Expected invariant:

- no failure path may advance checkpoint beyond committed Bronze truth.

### 13.1 Failure scenario to checklist mapping

| Failure scenario | Checklist items validated | Primary design sections |
| --- | --- | --- |
| Worker crash after source fetch but before manifest commit | `4.3`, `4.4` | `5.1`, `9` |
| Manifest write failure after Bronze staging | `4.3`, `4.4` | `5.1`, `9` |
| Checkpoint write failure after manifest persistence | `4.3`, `4.4`, `4.6` | `5.1`, `9` |
| Object-store artifact write failure | `4.6`, `4.8`, `4.10` | `7`, `11`, `12` |
| Source API rate limit or outage | `4.1`, `4.10`, `4.11` | `12`, `14` |
| Target write slowdown or outage | `4.3`, `4.4`, `4.11` | `5.1`, `14` |
| Schema drift detected mid-run | `4.2`, `4.6` | `6`, `7` |
| Replay against incompatible current Bronze schema | `4.2`, `4.6`, `4.7` | `6`, `7`, `8` |

---

## 14. Implementation Plan

### Phase A: Finish Phase 2 for API-to-Bronze

- object-store artifact writer abstraction,
- source-page artifact retention,
- deterministic replay reader path, after the artifact writer and retained source-page persistence are complete,
- target-agnostic commit protocol state model.

### Phase B: Schema and rollback controls

- schema enforcement modes,
- schema snapshot persistence,
- active vs rolled_back batch semantics,
- rollback-by-batch operator workflow.

### Phase C: Operator and SLO layer

- checkpoint/freshness/bad-record dashboards,
- alert fan-out,
- replay and rollback APIs,
- incident-friendly audit views.

### Phase D: Production proof

- representative load test,
- long-run soak test,
- failure-injection scenarios,
- documented runbooks for replay, checkpoint reset, rollback, and source outage.

---

## 15. Exit Criteria

API-to-Bronze can be called production-ready when:

- deterministic replay works from stored source artifacts for the configured retention window,
- checkpoint movement is coupled only to committed Bronze truth,
- bad records can be inspected and replayed independently,
- rollback-by-batch exists and downstream readers respect it,
- object-store artifact retention replaces repo-local-only persistence,
- schema enforcement policy is explicit and tested,
- load, soak, and failure-injection tests pass.
