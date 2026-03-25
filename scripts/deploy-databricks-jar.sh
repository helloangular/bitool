#!/usr/bin/env bash
# Deploy Bronze Ingestion JAR to Databricks
#
# Prerequisites:
#   - Databricks CLI installed: pip install databricks-cli
#   - DATABRICKS_HOST and DATABRICKS_TOKEN env vars set
#   - Leiningen installed
#
# Usage:
#   ./scripts/deploy-databricks-jar.sh
#   ./scripts/deploy-databricks-jar.sh --create-job

set -euo pipefail

JAR_NAME="bitool-bronze-ingest.jar"
DBFS_PATH="/FileStore/jars/${JAR_NAME}"
JOB_NAME="bitool-bronze-ingest"

echo "=== Step 1: Build uberjar ==="
lein with-profile +databricks uberjar
JAR_PATH="target/uberjar/${JAR_NAME}"

if [ ! -f "$JAR_PATH" ]; then
  JAR_PATH="target/${JAR_NAME}"
fi

if [ ! -f "$JAR_PATH" ]; then
  echo "ERROR: JAR not found at target/uberjar/${JAR_NAME} or target/${JAR_NAME}"
  exit 1
fi

echo "  Built: ${JAR_PATH} ($(du -h "$JAR_PATH" | cut -f1))"

echo ""
echo "=== Step 2: Upload to DBFS ==="
databricks fs cp --overwrite "$JAR_PATH" "dbfs:${DBFS_PATH}"
echo "  Uploaded to dbfs:${DBFS_PATH}"

if [ "${1:-}" = "--create-job" ]; then
  echo ""
  echo "=== Step 3: Create Databricks Job ==="

  JOB_JSON=$(cat <<'JOBEOF'
{
  "name": "bitool-bronze-ingest",
  "tasks": [
    {
      "task_key": "bronze_ingest",
      "spark_jar_task": {
        "main_class_name": "bitool.databricks_job.bronze_ingest",
        "parameters": [
          "--endpoints", "fleet/vehicles,fleet/drivers",
          "--source-system", "samsara",
          "--base-url", "https://api.samsara.com",
          "--token-env", "SAMSARA_API_TOKEN",
          "--catalog", "main",
          "--schema", "bronze"
        ]
      },
      "libraries": [
        {"jar": "DBFS_PATH_PLACEHOLDER"}
      ],
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "num_workers": 0,
        "node_type_id": "i3.xlarge",
        "spark_conf": {
          "spark.master": "local[*]",
          "spark.databricks.cluster.profile": "singleNode"
        },
        "custom_tags": {
          "ResourceClass": "SingleNode"
        }
      }
    }
  ],
  "max_concurrent_runs": 1
}
JOBEOF
)

  # Replace placeholder with actual DBFS path
  JOB_JSON=$(echo "$JOB_JSON" | sed "s|DBFS_PATH_PLACEHOLDER|dbfs:${DBFS_PATH}|g")

  # Check if job already exists
  EXISTING_JOB_ID=$(databricks jobs list --output json 2>/dev/null | \
    python3 -c "import sys,json; jobs=json.load(sys.stdin).get('jobs',[]); matches=[j['job_id'] for j in jobs if j['settings']['name']=='${JOB_NAME}']; print(matches[0] if matches else '')" 2>/dev/null || echo "")

  if [ -n "$EXISTING_JOB_ID" ]; then
    echo "  Updating existing job: ${EXISTING_JOB_ID}"
    echo "$JOB_JSON" | databricks jobs reset --job-id "$EXISTING_JOB_ID" --json-file /dev/stdin
  else
    echo "  Creating new job..."
    echo "$JOB_JSON" | databricks jobs create --json-file /dev/stdin
  fi

  echo "  Done. Run with: databricks jobs run-now --job-id <JOB_ID>"
fi

echo ""
echo "=== Deployment complete ==="
echo "  JAR: dbfs:${DBFS_PATH}"
echo "  To run manually:"
echo "    databricks jobs run-now --job-id <JOB_ID>"
echo "  Or from a notebook:"
echo "    %sh java -jar /dbfs${DBFS_PATH} --endpoints fleet/vehicles --source-system samsara"
