# API-to-Bronze Production Evidence Template

Use this template to record release-grade evidence for API -> Bronze production readiness.

## Run Metadata

- Date (UTC):
- Environment:
- Git commit SHA:
- Operator:
- Reviewer:
- Soak iterations:

## Proof Suite Artifacts

- Summary file: `tmp/api-bronze-proof/<UTC_TIMESTAMP>/summary.json`
- Results file: `tmp/api-bronze-proof/<UTC_TIMESTAMP>/results.ndjson`
- Log file: `tmp/api-bronze-proof/<UTC_TIMESTAMP>/suite.log`

Required checks:

- `summary.json.status = "passed"`
- `summary.json.failed_tests = 0`
- `summary.json.total_tests` matches expected phase/iteration count

## Operational Checks

- No `pending_checkpoint` manifests remain after run.
- No duplicate active API requests exist for the same API node scope.
- Replay guard check verified: re-running bad-record replay for already-committed IDs produces noop or skips duplicates.

## Incident Drill Notes

- Source API outage/rate-limit behavior observed:
- Target write degradation behavior observed:
- Manifest reconciliation behavior observed:
- Deterministic replay behavior observed:

## Signoff

- Operator signoff:
- Reviewer signoff:
- Date (UTC):
