# API-to-Bronze Production Readiness Checklist

## 1. Purpose

This checklist defines what Bitool still needs before the `API -> Bronze` path should be called production-ready for large-scale workloads.

It is intentionally narrower than the platform-wide production design. This checklist only covers:

- API extraction,
- schema inference and promotion,
- Bronze writes,
- bad-record handling,
- checkpoint progression,
- replay and rollback,
- observability and operations for the API-to-Bronze path.

---

## 2. Current Repo-Local Baseline

The current repo already has a strong API-to-Bronze foundation:

- paginated API fetching in `src/clj/bitool/connector/api.clj`,
- streaming page consumption and bounded batch flushes in `src/clj/bitool/ingest/runtime.clj`,
- Bronze envelope generation and typed promoted-column coercion in `src/clj/bitool/ingest/bronze.clj`,
- checkpoint/watermark helpers in `src/clj/bitool/ingest/checkpoint.clj`,
- batch manifests and batch-boundary checkpoint progression in `src/clj/bitool/ingest/runtime.clj`,
- bad-record quarantine,
- execution retries, DLQ, lease heartbeats, and orphan recovery in `src/clj/bitool/ingest/execution.clj`.

That means Bitool is already beyond MVP for API-to-Bronze. The remaining work is about production semantics, deterministic recovery, security, and operational proof.

---

## 3. Status Legend

- `Done`: already implemented in repo in a way that is materially useful.
- `Partial`: implemented in a repo-local or limited form, but not enough for full production claims.
- `Required`: must be completed before calling API-to-Bronze production-ready.
- `Later`: valuable, but can follow the first production-ready bar.

---

## 4. Checklist

### 4.1 Source extraction and flow control

- `Done`: paginated extraction is supported.
- `Done`: worker-side buffering is bounded by row and byte thresholds.
- `Done`: adaptive backpressure now updates per-endpoint `per-page-ms` cooldowns from runtime signals (`429`, retry/failure metadata, and recovery decay).
- `Done`: source-host and credential-level concurrency governance is enforced at claim time with per-request limits and source/credential active-run counters.
- `Partial`: source-level circuit breaker automation is still basic and should be extended beyond cooldown/backoff.

### 4.2 Schema inference and Bronze contract

- `Done`: promoted-column identifier sanitization is stable.
- `Done`: Bronze writer coercion matches the main promoted types and routes conversion failures to bad records.
- `Done`: duplicate normalized field names and reserved Bronze column collisions fail fast.
- `Done`: schema inference snapshots are persisted and now have explicit review/promotion workflow (`/apiSchemaApprovals`, `/reviewApiSchema`, `/promoteApiSchema`) with optional runtime gating (`schema_review_state=required` / `require_schema_approval=true`).
- `Done`: explicit schema enforcement modes per endpoint: `strict`, `additive`, `permissive`.
- `Done`: monotonic schema evolution policy enforcement for inferred fields, including type widening allowed and narrowing rejected.
- `Done`: schema approval, promotion, and replay-compatibility workflow on top of the enforced runtime policy.

### 4.3 Bronze durability and commit safety

- `Done`: batch manifests exist.
- `Done`: checkpoint progression happens at batch boundaries instead of only once per endpoint.
- `Done`: transactional targets have an atomic batch commit path.
- `Done`: atomic manifest plus checkpoint semantics for all supported Bronze target modes.
- `Done`: explicit staging/promote protocol or equivalent commit-marker semantics for non-transactional targets.
- `Done`: downstream consumers only see committed/active batches, and non-transactional runs are fail-closed by `verifyApiCommitClosure` semantics before downstream triggers.

### 4.4 Checkpoint correctness

- `Done`: last successful watermark and cursor are tracked.
- `Partial`: failed runs no longer silently advance checkpoint state, and checkpoint progression now happens at batch boundaries, but the guarantee is currently strongest only for transactional targets where Bronze rewrite, manifest persistence, and checkpoint update share one transaction.
- `Partial`: checkpoint progression is safer, but global correctness still depends on target-specific commit semantics.
- `Required`: checkpoint movement must be impossible unless the batch is durably committed.
- `Required`: replay and retry must resume from the last committed checkpoint plus configured overlap, not from attempted progress.

### 4.5 Bad-record handling

- `Done`: bad records are quarantined without poisoning the whole endpoint by default.
- `Done`: batch_id is attached to bad-record rows.
- `Done`: bad-record replay metadata is queryable (`replay_status`, replay run linkage, replay error).
- `Done`: replay bad records only, without forcing a full endpoint rerun.
- `Done`: bad-record retention policy with searchable metadata window and separate payload-body archival (payload archive + metadata TTL in `applyApiRetention`).

### 4.6 Replay and recovery

- `Done`: best-effort replay exists by re-enqueueing execution.
- `Done`: deterministic replay from stored source pages and committed batch artifacts.
- `Done`: replay can pin to source graph version and reject drift.
- `Done`: schema compatibility checks run before replay, including endpoint-config hash compatibility.
- `Done`: replay is executed from committed manifests/artifacts without source re-fetch.

### 4.7 Rollback and data versioning

- `Done`: every committed flush already has a stable `batch_id`.
- `Partial`: row attribution by `batch_id` exists, but rollback semantics are not complete.
- `Required`: mark batches `active` vs `rolled_back`.
- `Required`: Bronze readers and downstream triggers must ignore rolled-back batches.
- `Required`: operator-driven rollback-by-batch workflow.

### 4.8 Partitioning and storage lifecycle

- `Done`: Bronze rows already carry `partition_date`.
- `Partial`: partition metadata is captured in manifests.
- `Done`: product-level Bronze partition policy enforced by target type (`partition_date` required for Databricks; non-partitioned defaults for transactional row stores).
- `Done`: retention defaults and hard policy controls cover manifests, replay artifacts, and bad-record payload archival/metadata retention windows.
- `Done`: archive movement supports object-store-native lifecycle hooks in HTTP artifact-store mode and manifest-aware retention updates.

### 4.9 Security and governance

- `Done`: missing secret refs fail loudly.
- `Done`: DB-backed managed secrets support encrypted-at-rest storage (`aes_gcm_v1`) when encryption key material is configured.
- `Done`: RBAC gates are wired for endpoint execution, rollback/archive/retention ops, replay, and bad-record replay.
- `Done`: audit trail endpoints and event writes cover replay, rollback/archive/retention operations, run triggers, and managed secret access.

### 4.10 Observability and operator workflows

- `Done`: execution runs, node runs, and manifests are persisted.
- `Done`: endpoint freshness metadata exists.
- `Done`: metrics and operator workflows include API observability summary/alerts plus replay, rollback, retention, and checkpoint reset APIs.
- `Done`: dashboards for checkpoint lag, freshness lag, bad-record rate, batch commit latency, retry volume, and replay success are surfaced via `/apiObservabilitySummary`.
- `Done`: alerting signal generation for stale checkpoints, repeated retry loops, high bad-record ratios, and replay failures is surfaced via `/apiObservabilityAlerts`.
- `Done`: operator workflows for replay, checkpoint reset, and rollback-by-batch are available.

### 4.11 Scale and performance validation

- `Done`: streaming execution removes unbounded in-memory row accumulation.
- `Partial`: queue partitioning and workload classes exist, but API-to-Bronze has not yet been proven under representative high-volume load.
- `Done`: repo-local proof-suite runner now codifies representative load-shape, failure-injection, and soak-loop validations (`scripts/run-api-bronze-proof-suite.sh`).
- `Done`: production-like proof execution/signoff is tracked with persisted release evidence (`/apiBronzeProofSignoff`) and template-backed artifacts.

---

## 5. Exit Criteria

API-to-Bronze should not be called production-ready until all `Required` items above are complete and the following are demonstrably true:

- a worker crash cannot advance checkpoint state past uncommitted Bronze data.
  Validation: Phase D failure-injection scenario for worker crash after source fetch and before manifest commit. Covers checklist `4.3`, `4.4`.
- replay can be deterministic for the configured retention window of `365 days` for replay artifacts.
  Validation: Phase D replay test from stored source artifacts across retained batches. Covers checklist `4.6`, `4.8`.
- operators can inspect and replay bad records and committed batches safely.
  Validation: Phase D operator workflow test for bad-record-only replay and rollback-by-batch. Covers checklist `4.5`, `4.7`, `4.10`.
- secrets and replay controls are governed, auditable, and access-controlled.
  Validation: Phase D access-control and audit-log verification for replay, rollback, checkpoint reset, and secret access. Covers checklist `4.9`, `4.10`.
- representative load and failover tests pass against target production-like data volumes.
  Validation: Phase D representative load, soak, and failover suite. Covers checklist `4.1`, `4.3`, `4.11`.
- production evidence artifacts are attached to signoff with operator approval.
  Validation: latest proof run `summary.json` shows pass, with `results.ndjson` and `suite.log` attached, plus completed `API-to-Bronze-Production-Evidence-Template.md`.

---

## 6. Recommended Next Order

1. Keep proof-suite artifacts current per release (`summary.json`, `results.ndjson`, `suite.log`).
2. Keep signoff records and audit evidence current for each environment promotion.
3. Continue expanding source-level circuit breaker automation beyond cooldown/backoff heuristics.

## 7. Failure Scenario Traceability

| Failure scenario | Checklist items validated | Phase D proof |
| --- | --- | --- |
| Worker crash after source fetch but before manifest commit | `4.3`, `4.4` | failure-injection crash test |
| Manifest write failure after Bronze staging | `4.3`, `4.4` | staged-batch failure test |
| Checkpoint write failure after manifest persistence | `4.3`, `4.4`, `4.6` | checkpoint commit failure test |
| Object-store artifact write failure | `4.6`, `4.8`, `4.10` | artifact writer failure test |
| Source API rate limit or outage | `4.1`, `4.10`, `4.11` | rate-limit and cooldown load test |
| Target write slowdown or outage | `4.3`, `4.4`, `4.11` | slow-target soak and failover test |
| Schema drift detected mid-run | `4.2`, `4.6` | schema-policy drift test |
| Replay against incompatible current Bronze schema | `4.2`, `4.6`, `4.7` | replay compatibility test |
