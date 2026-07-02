#!/usr/bin/env bash
set -euo pipefail
TABLES=(orders customers products inventory)
echo "| Table | Duration (s) | Exit Code |"
echo "|-------|--------------|----------|"
for tbl in "${TABLES[@]}"; do
  start=$(date +%s)
  echo "Running quality engine for $tbl"
  docker exec sdoqap-spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/spark_quality_engine.py $tbl
  exit_code=$?
  end=$(date +%s)
  duration=$((end - start))
  echo "| $tbl | $duration | $exit_code |"
done
