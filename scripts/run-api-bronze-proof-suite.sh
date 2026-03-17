#!/usr/bin/env bash
set -euo pipefail

# Production proof suite runner for API -> Bronze.
# Uses focused tests for load-shape behavior, failure injection semantics, and
# long-run soak repetitions.

SOAK_ITERATIONS="${INGEST_SOAK_ITERATIONS:-10}"
RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUTPUT_ROOT="${API_BRONZE_PROOF_OUTPUT_ROOT:-tmp/api-bronze-proof}"
RUN_DIR="${OUTPUT_ROOT}/${RUN_TS}"
RESULTS_FILE="${RUN_DIR}/results.ndjson"
SUMMARY_FILE="${RUN_DIR}/summary.json"
LOG_FILE="${RUN_DIR}/suite.log"
TOTAL_TESTS=0
FAILED_TESTS=0
PHASE_NAME="init"

mkdir -p "${RUN_DIR}"
touch "${RESULTS_FILE}"
touch "${LOG_FILE}"

log() {
  echo "$*" | tee -a "${LOG_FILE}"
}

run_test() {
  local test_var="$1"
  local start_ts end_ts duration status
  start_ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  start_epoch="$(date +%s)"
  log "[api-bronze-proof][test][${PHASE_NAME}] ${test_var}"
  if lein test :only "${test_var}" 2>&1 | tee -a "${LOG_FILE}"; then
    status="passed"
  else
    status="failed"
    FAILED_TESTS=$((FAILED_TESTS + 1))
  fi
  end_ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  end_epoch="$(date +%s)"
  duration=$((end_epoch - start_epoch))
  TOTAL_TESTS=$((TOTAL_TESTS + 1))
  printf '{"phase":"%s","test":"%s","status":"%s","started_at_utc":"%s","finished_at_utc":"%s","duration_seconds":%s}\n' \
    "${PHASE_NAME}" "${test_var}" "${status}" "${start_ts}" "${end_ts}" "${duration}" >> "${RESULTS_FILE}"
}

log "[api-bronze-proof] run_dir=${RUN_DIR}"
log "[api-bronze-proof] Phase 1/3: Load-shape coverage"
PHASE_NAME="load_shape"
run_test bitool.ingest-runtime-test/run-api-node-streams-batches-and-persists-manifests
run_test bitool.ingest-runtime-test/cleanup-ingest-retention-manifest-aware-sweeps-discovered-api-targets
run_test bitool.ingest-execution-test/enqueue-api-request-reuses-all-endpoints-run-for-overlapping-specific-endpoint
run_test bitool.ingest-execution-test/claim-next-request-filters-inactive-workspaces

log "[api-bronze-proof] Phase 2/3: Failure-injection coverage"
PHASE_NAME="failure_injection"
run_test bitool.ingest-runtime-test/run-api-node-does-not-advance-zero-row-checkpoint-before-run-detail
run_test bitool.ingest-runtime-test/abort-preparing-batches-fails-closed-when-reconciliation-query-errors
run_test bitool.ingest-runtime-test/checkpoint-row-for-failure-preserves-last-successful-batch-identity
run_test bitool.ingest-runtime-test/run-api-node-drains-errors-when-post-fetch-validation-throws-early

log "[api-bronze-proof] Phase 3/3: Soak loop (${SOAK_ITERATIONS} iterations)"
PHASE_NAME="soak"
for i in $(seq 1 "${SOAK_ITERATIONS}"); do
  log "[api-bronze-proof][soak] iteration ${i}/${SOAK_ITERATIONS}"
  run_test bitool.ingest-runtime-test/run-api-node-resumes-from-checkpoint-cursor-and-persists-last-cursor
  run_test bitool.ingest-runtime-test/run-api-node-replays-deterministically-from-stored-batch-artifacts
  run_test bitool.ingest-execution-test/retry-decision-and-backoff-follow-request-budget
  run_test bitool.ingest-execution-test/sweep-expired-leases-claims-each-orphan-before-processing
done

cat > "${SUMMARY_FILE}" <<EOF
{
  "run_timestamp_utc": "${RUN_TS}",
  "output_dir": "${RUN_DIR}",
  "soak_iterations": ${SOAK_ITERATIONS},
  "total_tests": ${TOTAL_TESTS},
  "failed_tests": ${FAILED_TESTS},
  "status": "$(if [ "${FAILED_TESTS}" -eq 0 ]; then echo passed; else echo failed; fi)",
  "results_file": "${RESULTS_FILE}",
  "log_file": "${LOG_FILE}"
}
EOF

log "[api-bronze-proof] summary_file=${SUMMARY_FILE}"
if [ "${FAILED_TESTS}" -ne 0 ]; then
  log "[api-bronze-proof] failed_tests=${FAILED_TESTS}"
  exit 1
fi

log "[api-bronze-proof] complete"
