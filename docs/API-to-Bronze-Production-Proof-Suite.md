# API-to-Bronze Production Proof Suite

## Purpose

This suite is the production validation gate for API -> Bronze semantics:

- retry correctness under source and target failures,
- checkpoint and manifest integrity,
- replay/rollback/archive operator safety,
- lease/orphan recovery stability under contention.

## Runner

Use the repository runner:

```bash
scripts/run-api-bronze-proof-suite.sh
```

Optional soak duration:

```bash
INGEST_SOAK_ITERATIONS=50 scripts/run-api-bronze-proof-suite.sh
```

Optional output root override:

```bash
API_BRONZE_PROOF_OUTPUT_ROOT=tmp/api-bronze-proof scripts/run-api-bronze-proof-suite.sh
```

The runner writes evidence artifacts per run:

- `suite.log`
- `results.ndjson`
- `summary.json`

Default output path: `tmp/api-bronze-proof/<UTC_TIMESTAMP>/`.

## Phases

1. Load-shape coverage
- stream flush and manifest persistence path (`run-api-node-streams-batches-and-persists-manifests`).
- manifest-aware retention sweep (`cleanup-ingest-retention-manifest-aware-sweeps-discovered-api-targets`).
- enqueue overlap control across all-endpoint vs endpoint-scoped runs.
- queue-claim filters for inactive workspace contexts.

2. Failure-injection coverage
- zero-row checkpoint ordering safety (`run-detail` must persist before post-stream checkpoint write).
- stale preparing/pending manifest reconcile fail-closed behavior.
- failed-run checkpoint mutation preserving last successful batch identity.
- fetch-stream drain/cancel correctness when post-fetch validation fails early.

3. Soak loop
- repeated cursor resume and deterministic replay paths.
- repeated retry backoff and orphan sweep behavior across iterations.

## Exit Criteria

Treat API -> Bronze as production-ready only when:

- proof suite passes with zero failures,
- soak loop passes for agreed iteration count in a production-like environment,
- no stuck `pending_checkpoint` manifests remain after test completion,
- no duplicate active runs are present for the same API node scope,
- proof artifacts (`summary.json`, `results.ndjson`, `suite.log`) are attached to the release signoff record.

## Recommended CI Gate

Run these namespaces at minimum on protected branches:

```bash
lein test \
  bitool.ingest-execution-test \
  bitool.ingest-runtime-test \
  bitool.operations-test \
  bitool.routes.home-test
```

Run full proof suite nightly with higher soak iterations.

## Evidence Hand-off

For production readiness signoff, attach:

1. `summary.json` from the latest passing run.
2. `results.ndjson` and `suite.log` for traceability and debugging.
3. Completed operator signoff template from `docs/API-to-Bronze-Production-Evidence-Template.md`.
4. Persisted release signoff row via `POST /apiBronzeProofSignoff` and retrievable via `GET /apiBronzeProofSignoff`.
