#!/usr/bin/env bash
# =============================================================================
# E2E Test: Kafka -> Bronze
# =============================================================================
#
# Produces test JSON messages to a local Kafka topic, then triggers bitool
# ingestion which creates the bronze table and lands the data.
#
# Prerequisites:
#   1. Kafka running:  docker compose -f docker-compose.kafka.yml up -d
#   2. bitool running: lein run  (port 8080)
#   3. A model (graph) with:
#        - Kafka source node (btype "Kf") configured with bootstrap_servers, topic_configs
#        - Connected: Kf -> Output -> Target (with valid warehouse connection_id)
#
# Usage:
#   ./scripts/e2e-kafka-to-bronze.sh <GRAPH_ID> <KAFKA_NODE_ID> [ENDPOINT_NAME] [TOPIC] [NUM_MESSAGES]
#
# Example:
#   ./scripts/e2e-kafka-to-bronze.sh 1 2 test_orders test-orders 20
#
# After this completes, open the Modeling console in the UI to:
#   - Propose Silver schema
#   - Validate & publish Silver
#   - Propose Gold schema from Silver
#   - Execute transforms
# =============================================================================

set -euo pipefail

BASE="http://localhost:8080"
GID="${1:?Usage: $0 <GRAPH_ID> <KAFKA_NODE_ID> [ENDPOINT_NAME] [TOPIC] [NUM_MESSAGES]}"
KF_NODE_ID="${2:?Usage: $0 <GRAPH_ID> <KAFKA_NODE_ID> [ENDPOINT_NAME] [TOPIC] [NUM_MESSAGES]}"
ENDPOINT_NAME="${3:-test_orders}"
TOPIC="${4:-test-orders}"
NUM_MESSAGES="${5:-20}"

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
  python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(d.get(sys.argv[2],''))" "$1" "$2" 2>/dev/null
}

# =============================================================================
# STEP 1: Verify Kafka is running
# =============================================================================
step "Check Kafka broker"

if ! docker exec bitool-kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
  fail "Kafka broker not reachable. Run: docker compose -f docker-compose.kafka.yml up -d"
fi
ok "Kafka broker is up"

# =============================================================================
# STEP 2: Create topic (idempotent)
# =============================================================================
step "Ensure topic '$TOPIC' exists"

docker exec bitool-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1 2>/dev/null || true
ok "Topic '$TOPIC' ready"

# =============================================================================
# STEP 3: Produce test data
# =============================================================================
step "Produce $NUM_MESSAGES test messages"

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

ok "Produced $NUM_MESSAGES messages to '$TOPIC'"

# Verify messages are readable
MSG_COUNT=$(docker exec bitool-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic "$TOPIC" \
  --from-beginning --timeout-ms 5000 2>/dev/null | wc -l | tr -d ' ')
info "Topic '$TOPIC' now has ~$MSG_COUNT total messages"

# =============================================================================
# STEP 4: Verify bitool is running
# =============================================================================
step "Check bitool server"

if ! curl -s -o /dev/null -w "%{http_code}" "$BASE/" | grep -q "200"; then
  fail "bitool not reachable at $BASE. Run: lein run"
fi
ok "bitool is up at $BASE"

# =============================================================================
# STEP 5: Trigger Kafka ingestion -> Bronze
# =============================================================================
step "Run Kafka ingestion (graph=$GID, node=$KF_NODE_ID, endpoint=$ENDPOINT_NAME)"

INGEST_RESP=$(curl -s -X POST "$BASE/runKafkaIngestion" \
  -H "Content-Type: application/json" \
  -d "{\"gid\": $GID, \"id\": $KF_NODE_ID, \"endpoint_name\": \"$ENDPOINT_NAME\"}")

echo "  Response: $INGEST_RESP"
REQUEST_ID=$(json_field "$INGEST_RESP" "request_id")
RUN_ID=$(json_field "$INGEST_RESP" "run_id")

if [ -z "$REQUEST_ID" ] || [ "$REQUEST_ID" = "None" ]; then
  fail "Kafka ingestion enqueue failed. Response: $INGEST_RESP"
fi
ok "Ingestion enqueued: request_id=$REQUEST_ID, run_id=$RUN_ID"

# =============================================================================
# STEP 6: Wait for ingestion to complete
# =============================================================================
step "Wait for bronze ingestion to finish"

info "Ingestion runs async. Waiting 15s for consumer to poll, batch, and flush..."
sleep 15

# Check batches landed
BATCHES_RESP=$(curl -s "$BASE/bronzeSourceBatches?gid=$GID&node_id=$KF_NODE_ID&endpoint_name=$ENDPOINT_NAME" 2>/dev/null || echo "{}")
info "Batches response: $(echo "$BATCHES_RESP" | head -c 300)"

# =============================================================================
# SUMMARY
# =============================================================================
echo ""
echo -e "${GREEN}=============================================${NC}"
echo -e "${GREEN} Kafka -> Bronze Complete!${NC}"
echo -e "${GREEN}=============================================${NC}"
echo ""
echo "  Kafka Topic:        $TOPIC"
echo "  Messages Produced:  $NUM_MESSAGES"
echo "  Graph ID:           $GID"
echo "  Kafka Node ID:      $KF_NODE_ID"
echo "  Endpoint:           $ENDPOINT_NAME"
echo "  Run ID:             $RUN_ID"
echo ""
echo "  Bronze table created in your warehouse:"
echo "    bronze_${ENDPOINT_NAME}"
echo ""
echo -e "${CYAN}  Next steps (in the UI):${NC}"
echo "    1. Open the Modeling console (Tools > Modeling)"
echo "    2. Propose Silver schema from this bronze endpoint"
echo "    3. Validate & publish Silver"
echo "    4. Propose Gold schema from Silver"
echo "    5. Validate & publish Gold"
echo "    6. Execute transforms to materialize data"
echo ""
