#!/usr/bin/env bash
set -euo pipefail
# Fetch tables dynamically from API
TABLES_JSON=$(curl -s http://localhost/api/v1/export/tables || echo "{}")
TABLES=($(echo "$TABLES_JSON" | python3 -c "import sys, json; print(' '.join([t['name'] for t in json.load(sys.stdin).get('tables', [])]))" 2>/dev/null \
  || echo "$TABLES_JSON" | python -c "import sys, json; print(' '.join([t['name'] for t in json.load(sys.stdin).get('tables', [])]))" 2>/dev/null \
  || echo "products gov_data sales_records"))
echo "| Table | Duration (s) | Exit Code |"
echo "|-------|--------------|----------|"
for tbl in "${TABLES[@]}"; do
  start=$(date +%s)
  echo "Running quality engine for $tbl"
  exit_code=0
  docker exec sdoqap-spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/spark_quality_engine.py "$tbl" || exit_code=$?
  end=$(date +%s)
  duration=$((end - start))
  echo "| $tbl | $duration | $exit_code |"
done
