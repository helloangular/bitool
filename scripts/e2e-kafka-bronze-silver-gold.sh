#!/usr/bin/env bash
# =============================================================================
# E2E Test: Kafka -> Bronze -> Silver -> Gold
# =============================================================================
#
# Prerequisites:
#   1. Kafka running on localhost:9092 (docker compose -f docker-compose.kafka.yml up -d)
#   2. bitool running on localhost:8080 (lein run)
#   3. A graph with a Kafka source node + Target node configured
#   4. Snowflake/Databricks connection configured in that graph
#
# Usage:
#   ./scripts/e2e-kafka-bronze-silver-gold.sh <GRAPH_ID> <KAFKA_NODE_ID>
#
# Example:
#   ./scripts/e2e-kafka-bronze-silver-gold.sh 1 2
#
# =============================================================================

set -euo pipefail

BASE="http://localhost:8080"
GID="${1:?Usage: $0 <GRAPH_ID> <KAFKA_NODE_ID>}"
KF_NODE_ID="${2:?Usage: $0 <GRAPH_ID> <KAFKA_NODE_ID>}"
ENDPOINT_NAME="${3:-test_orders}"
TOPIC="${4:-test-orders}"

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

step() { echo -e "\n${CYAN}===== STEP: $1 =====${NC}"; }
ok()   { echo -e "${GREEN}  OK: $1${NC}"; }
fail() { echo -e "${RED}  FAIL: $1${NC}"; exit 1; }
info() { echo -e "${YELLOW}  $1${NC}"; }

json_field() {
  # Extract a top-level field from JSON. Usage: json_field '{"a":1}' a
  python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(d.get(sys.argv[2],''))" "$1" "$2" 2>/dev/null
}

# =============================================================================
# STEP 0: Produce test data to Kafka
# =============================================================================
step "Produce test data to Kafka topic '$TOPIC'"

NUM_MESSAGES=20
for i in $(seq 1 "$NUM_MESSAGES"); do
  ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  statuses=("pending" "shipped" "delivered" "cancelled")
  status=${statuses[$(( RANDOM % 4 ))]}
  cat <<JSON
{"order_id": "ORD-$(printf '%04d' $i)", "customer_id": "CUST-$(( (RANDOM % 50) + 1 ))", "amount": $(printf '%.2f' "$(echo "scale=2; ($RANDOM % 10000) / 100" | bc)"), "currency": "USD", "status": "$status", "items": [{"sku": "SKU-$(( RANDOM % 999 ))", "qty": $(( RANDOM % 5 + 1 )), "price": $(printf '%.2f' "$(echo "scale=2; ($RANDOM % 5000) / 100" | bc)")}], "updated_at": "$ts"}
JSON
done | docker exec -i bitool-kafka kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic "$TOPIC" 2>/dev/null

ok "Produced $NUM_MESSAGES messages to topic '$TOPIC'"

# =============================================================================
# STEP 1: Trigger Kafka ingestion (Bronze)
# =============================================================================
step "Run Kafka ingestion -> Bronze"

INGEST_RESP=$(curl -s -X POST "$BASE/runKafkaIngestion" \
  -H "Content-Type: application/json" \
  -d "{\"gid\": $GID, \"id\": $KF_NODE_ID, \"endpoint_name\": \"$ENDPOINT_NAME\"}")

echo "  Response: $INGEST_RESP"
REQUEST_ID=$(json_field "$INGEST_RESP" "request_id")
RUN_ID=$(json_field "$INGEST_RESP" "run_id")

if [ -z "$REQUEST_ID" ] || [ "$REQUEST_ID" = "None" ]; then
  fail "Kafka ingestion enqueue failed"
fi
ok "Ingestion enqueued: request_id=$REQUEST_ID, run_id=$RUN_ID"

info "Waiting for ingestion to complete..."
sleep 10

# =============================================================================
# STEP 2: Propose Silver schema from Bronze
# =============================================================================
step "Propose Silver schema from Kafka node"

SILVER_RESP=$(curl -s -X POST "$BASE/proposeSilverSchema" \
  -H "Content-Type: application/json" \
  -d "{\"gid\": $GID, \"id\": $KF_NODE_ID, \"endpoint_name\": \"$ENDPOINT_NAME\"}")

echo "  Response: $SILVER_RESP" | head -c 500
echo ""
SILVER_PROPOSAL_ID=$(json_field "$SILVER_RESP" "proposal_id")

if [ -z "$SILVER_PROPOSAL_ID" ] || [ "$SILVER_PROPOSAL_ID" = "None" ]; then
  fail "Silver proposal creation failed. Response: $SILVER_RESP"
fi
ok "Silver proposal created: proposal_id=$SILVER_PROPOSAL_ID"

# =============================================================================
# STEP 3: Compile Silver proposal (generates SQL)
# =============================================================================
step "Compile Silver proposal"

COMPILE_RESP=$(curl -s -X POST "$BASE/compileSilverProposal" \
  -H "Content-Type: application/json" \
  -d "{\"proposal_id\": $SILVER_PROPOSAL_ID}")

echo "  Response: $COMPILE_RESP" | head -c 500
echo ""
ok "Silver proposal compiled"

# =============================================================================
# STEP 4: Validate Silver proposal
# =============================================================================
step "Validate Silver proposal"

VALIDATE_RESP=$(curl -s -X POST "$BASE/validateSilverProposal" \
  -H "Content-Type: application/json" \
  -d "{\"proposal_id\": $SILVER_PROPOSAL_ID}")

echo "  Response: $VALIDATE_RESP" | head -c 500
echo ""
VALIDATE_STATUS=$(json_field "$VALIDATE_RESP" "status")
info "Validation status: $VALIDATE_STATUS"

# =============================================================================
# STEP 5: Publish Silver proposal (creates release)
# =============================================================================
step "Publish Silver proposal"

PUBLISH_RESP=$(curl -s -X POST "$BASE/publishSilverProposal" \
  -H "Content-Type: application/json" \
  -d "{\"proposal_id\": $SILVER_PROPOSAL_ID}")

echo "  Response: $PUBLISH_RESP" | head -c 500
echo ""
SILVER_RELEASE_ID=$(json_field "$PUBLISH_RESP" "release_id")

if [ -z "$SILVER_RELEASE_ID" ] || [ "$SILVER_RELEASE_ID" = "None" ]; then
  fail "Silver publish failed. Response: $PUBLISH_RESP"
fi
ok "Silver published: release_id=$SILVER_RELEASE_ID"

# =============================================================================
# STEP 6: Execute Silver release (Bronze -> Silver transform)
# =============================================================================
step "Execute Silver release (Bronze -> Silver transform)"

EXEC_SILVER_RESP=$(curl -s -X POST "$BASE/executeSilverRelease" \
  -H "Content-Type: application/json" \
  -d "{\"release_id\": $SILVER_RELEASE_ID}")

echo "  Response: $EXEC_SILVER_RESP" | head -c 500
echo ""
SILVER_RUN_ID=$(json_field "$EXEC_SILVER_RESP" "model_run_id")
ok "Silver execution started: model_run_id=$SILVER_RUN_ID"

# Poll for completion
if [ -n "$SILVER_RUN_ID" ] && [ "$SILVER_RUN_ID" != "None" ]; then
  info "Polling Silver execution status..."
  for i in $(seq 1 30); do
    sleep 5
    POLL_RESP=$(curl -s -X POST "$BASE/pollSilverModelRun" \
      -H "Content-Type: application/json" \
      -d "{\"model_run_id\": $SILVER_RUN_ID}")
    STATUS=$(json_field "$POLL_RESP" "status")
    info "  Poll $i: status=$STATUS"
    if [ "$STATUS" = "succeeded" ] || [ "$STATUS" = "failed" ]; then
      break
    fi
  done
  if [ "$STATUS" = "succeeded" ]; then
    ok "Silver transform completed successfully"
  else
    fail "Silver transform did not succeed. Status: $STATUS"
  fi
fi

# =============================================================================
# STEP 7: Propose Gold schema from Silver
# =============================================================================
step "Propose Gold schema from Silver proposal"

GOLD_RESP=$(curl -s -X POST "$BASE/proposeGoldSchema" \
  -H "Content-Type: application/json" \
  -d "{\"silver_proposal_id\": $SILVER_PROPOSAL_ID}")

echo "  Response: $GOLD_RESP" | head -c 500
echo ""
GOLD_PROPOSAL_ID=$(json_field "$GOLD_RESP" "proposal_id")

if [ -z "$GOLD_PROPOSAL_ID" ] || [ "$GOLD_PROPOSAL_ID" = "None" ]; then
  fail "Gold proposal creation failed. Response: $GOLD_RESP"
fi
ok "Gold proposal created: proposal_id=$GOLD_PROPOSAL_ID"

# =============================================================================
# STEP 8: Compile Gold proposal
# =============================================================================
step "Compile Gold proposal"

COMPILE_GOLD_RESP=$(curl -s -X POST "$BASE/compileGoldProposal" \
  -H "Content-Type: application/json" \
  -d "{\"proposal_id\": $GOLD_PROPOSAL_ID}")

echo "  Response: $COMPILE_GOLD_RESP" | head -c 500
echo ""
ok "Gold proposal compiled"

# =============================================================================
# STEP 9: Validate Gold proposal
# =============================================================================
step "Validate Gold proposal"

VALIDATE_GOLD_RESP=$(curl -s -X POST "$BASE/validateGoldProposal" \
  -H "Content-Type: application/json" \
  -d "{\"proposal_id\": $GOLD_PROPOSAL_ID}")

echo "  Response: $VALIDATE_GOLD_RESP" | head -c 500
echo ""
ok "Gold proposal validated"

# =============================================================================
# STEP 10: Publish Gold proposal
# =============================================================================
step "Publish Gold proposal"

PUBLISH_GOLD_RESP=$(curl -s -X POST "$BASE/publishGoldProposal" \
  -H "Content-Type: application/json" \
  -d "{\"proposal_id\": $GOLD_PROPOSAL_ID}")

echo "  Response: $PUBLISH_GOLD_RESP" | head -c 500
echo ""
GOLD_RELEASE_ID=$(json_field "$PUBLISH_GOLD_RESP" "release_id")

if [ -z "$GOLD_RELEASE_ID" ] || [ "$GOLD_RELEASE_ID" = "None" ]; then
  fail "Gold publish failed. Response: $PUBLISH_GOLD_RESP"
fi
ok "Gold published: release_id=$GOLD_RELEASE_ID"

# =============================================================================
# STEP 11: Execute Gold release (Silver -> Gold transform)
# =============================================================================
step "Execute Gold release (Silver -> Gold transform)"

EXEC_GOLD_RESP=$(curl -s -X POST "$BASE/executeGoldRelease" \
  -H "Content-Type: application/json" \
  -d "{\"release_id\": $GOLD_RELEASE_ID}")

echo "  Response: $EXEC_GOLD_RESP" | head -c 500
echo ""
GOLD_RUN_ID=$(json_field "$EXEC_GOLD_RESP" "model_run_id")
ok "Gold execution started: model_run_id=$GOLD_RUN_ID"

# Poll for completion
if [ -n "$GOLD_RUN_ID" ] && [ "$GOLD_RUN_ID" != "None" ]; then
  info "Polling Gold execution status..."
  for i in $(seq 1 30); do
    sleep 5
    POLL_RESP=$(curl -s -X POST "$BASE/pollGoldModelRun" \
      -H "Content-Type: application/json" \
      -d "{\"model_run_id\": $GOLD_RUN_ID}")
    STATUS=$(json_field "$POLL_RESP" "status")
    info "  Poll $i: status=$STATUS"
    if [ "$STATUS" = "succeeded" ] || [ "$STATUS" = "failed" ]; then
      break
    fi
  done
  if [ "$STATUS" = "succeeded" ]; then
    ok "Gold transform completed successfully"
  else
    fail "Gold transform did not succeed. Status: $STATUS"
  fi
fi

# =============================================================================
# SUMMARY
# =============================================================================
echo ""
echo -e "${GREEN}=============================================${NC}"
echo -e "${GREEN} E2E Pipeline Complete!${NC}"
echo -e "${GREEN}=============================================${NC}"
echo ""
echo "  Kafka Topic:          $TOPIC"
echo "  Messages Produced:    $NUM_MESSAGES"
echo "  Bronze Ingestion:     run_id=$RUN_ID"
echo "  Silver Proposal:      proposal_id=$SILVER_PROPOSAL_ID"
echo "  Silver Release:       release_id=$SILVER_RELEASE_ID"
echo "  Gold Proposal:        proposal_id=$GOLD_PROPOSAL_ID"
echo "  Gold Release:         release_id=$GOLD_RELEASE_ID"
echo ""
echo "  Check your warehouse for:"
echo "    - Bronze table: bronze_${ENDPOINT_NAME}"
echo "    - Silver table: silver_${ENDPOINT_NAME}"
echo "    - Gold table:   gold_${ENDPOINT_NAME}_daily (or similar)"
echo ""
