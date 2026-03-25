#!/usr/bin/env bash
# Produce sample JSON messages to the local Kafka 'test-orders' topic.
# Usage: ./scripts/kafka-test-producer.sh [NUM_MESSAGES]
#
# Each message is a JSON order record that exercises:
#   - primary_key_fields  (order_id)
#   - watermark_column    (updated_at)
#   - promoted columns    (order_id, customer_id, amount, status, updated_at)
#   - nested JSON         (items array stored in payload_json)

set -euo pipefail

NUM=${1:-10}
TOPIC="test-orders"
BROKER="localhost:9092"

for i in $(seq 1 "$NUM"); do
  ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  statuses=("pending" "shipped" "delivered" "cancelled")
  status=${statuses[$(( RANDOM % 4 ))]}
  cat <<JSON
{"order_id": "ORD-$(printf '%04d' $i)", "customer_id": "CUST-$(( (RANDOM % 50) + 1 ))", "amount": $(printf '%.2f' "$(echo "scale=2; ($RANDOM % 10000) / 100" | bc)"), "currency": "USD", "status": "$status", "items": [{"sku": "SKU-$(( RANDOM % 999 ))", "qty": $(( RANDOM % 5 + 1 )), "price": $(printf '%.2f' "$(echo "scale=2; ($RANDOM % 5000) / 100" | bc)")}], "updated_at": "$ts"}
JSON
done | docker exec -i bitool-kafka kafka-console-producer \
    --bootstrap-server "$BROKER" \
    --topic "$TOPIC"

echo "Produced $NUM messages to $TOPIC"
