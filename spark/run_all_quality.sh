#!/usr/bin/env bash
set -euo pipefail
TABLES=(orders customers products inventory)
output_file="/opt/spark-apps/quality_metrics.md"
{ echo "| Table | Duration (s) | Exit Code |"; echo "|-------|--------------|----------|"; } > "$output_file"
for tbl in "${TABLES[@]}"; do
  start=$(date +%s)
  echo "Running quality engine for $tbl"
  spark-submit --master spark://spark-master:7077 --jars /opt/spark/jars/delta-core_2.12-${DELTA_VERSION}.jar,/opt/spark/jars/delta-storage_2.12-${DELTA_VERSION}.jar /opt/spark-apps/spark_quality_engine.py $tbl
  exit_code=$?
  end=$(date +%s)
  duration=$((end - start))
  echo "| $tbl | $duration | $exit_code |" >> "$output_file"
done
