# API-to-Bronze Operator Runbook

## Required Roles

When `bitool-rbac-enabled=true`, these role grants are required:

- `api.execute`: trigger API ingestion.
- `api.ops`: inspect batches, rollback, archive, retention apply, execution replay.
- `api.replay_bad_records`: bad-record-only replay.
- `secrets.write`: managed secret writes.
- `api.audit`: inspect control-plane audit events.

## Core Endpoints

- `POST /runApiIngestion`
- `GET /apiBatches`
- `GET /apiBadRecords`
- `GET /apiSchemaApprovals`
- `POST /reviewApiSchema`
- `POST /promoteApiSchema`
- `POST /replayApiBadRecords`
- `POST /archiveApiBatch`
- `POST /rollbackApiBatch`
- `POST /applyApiRetention`
- `GET /verifyApiCommitClosure`
- `POST /resetApiCheckpoint`
- `GET /apiObservabilitySummary`
- `GET /apiObservabilityAlerts`
- `POST /executionRuns/:run_id/replay`
- `POST /apiBronzeProofSignoff`
- `GET /apiBronzeProofSignoff`
- `GET /controlPlane/auditEvents`
- `POST /controlPlane/secrets`

## Workflow: Trigger API Ingestion

1. Call `POST /runApiIngestion` with `gid`, `id`, and optional `endpoint_name`.
2. If request scope overlaps with an active run, expect dedupe reuse or `409` (all-endpoint conflict).
3. Track execution via `GET /executionRuns` and batch state via `GET /apiBatches`.

## Workflow: Replay Bad Records Only

1. Call `GET /apiBadRecords` with `gid`, `id`, `endpoint_name`, and optional `batch_id`/`run_id`.
2. Confirm candidate bad-record rows and replay status.
3. Call `POST /replayApiBadRecords` with the same identifiers.
4. Validate replay manifest creation and updated bad-record replay statuses.

## Workflow: Schema Approval and Promotion

1. Call `GET /apiSchemaApprovals` with `gid`, `api_node_id`, and `endpoint_name`.
2. Review latest snapshot hash and inferred fields.
3. Record review decision with `POST /reviewApiSchema` (`review_state=pending|approved|rejected`).
4. Promote approved schema with `POST /promoteApiSchema` (or `reviewApiSchema` with `promote=true`).
5. For strict gating, set endpoint config `schema_review_state=required` (or `require_schema_approval=true`).

## Workflow: Rollback by Batch

1. Inspect batch from `GET /apiBatches`.
2. Only rollback `committed` active batches; `preparing`/`pending_checkpoint` batches are rejected.
3. Call `POST /rollbackApiBatch`.
4. Confirm:
- manifest `status=rolled_back`,
- `active=false`,
- operator metadata (`rolled_back_by`, `rolled_back_at_utc`) populated.

## Workflow: Archive + Retention

1. For one batch, call `POST /archiveApiBatch`.
2. For policy sweep, call `POST /applyApiRetention` with:
  - manifest/archive controls: `archive_days`, `retention_days`, `limit`
  - bad-record controls: `bad_record_payload_archive_days`, `bad_record_retention_days`
3. Confirm manifest `artifact_path` and archive metadata updates.
4. Confirm bad-record payload archive refs and payload redaction for archived payload rows.
4. For HTTP/object-store mode, verify archive/delete lifecycle endpoints are configured and healthy.

## Workflow: Checkpoint Reset

1. Confirm no in-flight manifests for the endpoint (`GET /verifyApiCommitClosure`).
2. Call `POST /resetApiCheckpoint` with `gid`, `api_node_id`, `endpoint_name`, and required `reason`.
3. Optional overrides:
  - `reset_to_cursor`
  - `reset_to_watermark`
4. Validate audit event `api.reset_checkpoint`.

## Workflow: Commit-Closure Verification

1. Call `GET /verifyApiCommitClosure` for each production endpoint before downstream release triggers.
2. Proceed only when `ready=true` and incomplete/active-non-committed lists are empty.

## Workflow: Observability and Alerts

1. Use `GET /apiObservabilitySummary` for endpoint-level checkpoint lag, bad-record ratio, commit latency, retry volume, and replay outcomes.
2. Use `GET /apiObservabilityAlerts` for current alert-grade conditions.
3. Route alerts through existing incident workflows before reruns or checkpoint resets.

## Workflow: Deterministic Replay

1. Trigger `POST /executionRuns/:run_id/replay`.
2. Replay uses stored artifacts and manifest batch ordering; no live source fetch is expected.
3. Use `replay_source_graph_version` to pin to graph version and reject drift.
4. Optionally scope replay by `replay_source_batch_ids` for targeted recovery.

## Workflow: Production Proof Gate

1. Run `scripts/run-api-bronze-proof-suite.sh` (set `INGEST_SOAK_ITERATIONS` for extended soak).
2. Collect generated artifacts from `tmp/api-bronze-proof/<UTC_TIMESTAMP>/`.
3. Confirm `summary.json` has `"status": "passed"` and `"failed_tests": 0`.
4. Attach `summary.json`, `results.ndjson`, and `suite.log` to the release signoff record.
5. Complete `docs/API-to-Bronze-Production-Evidence-Template.md` with operator and reviewer signoff.
6. Persist signoff record with `POST /apiBronzeProofSignoff`.
7. Verify persisted signoff history with `GET /apiBronzeProofSignoff`.

## Incident Playbooks

### Source API 429/5xx bursts

- Verify adaptive per-page cooldown increase in endpoint run result metadata.
- Reduce endpoint-level `source_max_concurrency` / `credential_max_concurrency` when needed.
- Re-run affected execution after source stabilization.

### Target DB/warehouse degradation

- Pause new enqueue if target write latency grows.
- Inspect manifests for `pending_checkpoint` rows.
- Run replay or rollback only after manifest state is reconciled.

### Secret compromise or rotation

1. Write new value with `POST /controlPlane/secrets`.
2. Re-run impacted API nodes.
3. Verify `secret.read` and update events in `GET /controlPlane/auditEvents`.

## Audit Validation Checklist

For each operational action, verify corresponding audit events:

- `api.run_ingestion`
- `api.rollback_batch`
- `api.archive_batch`
- `api.apply_retention`
- `api.bad_record_replay`
- `execution.replay_run`
- `secret.read`
