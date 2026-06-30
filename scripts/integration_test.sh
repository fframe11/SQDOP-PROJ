#!/usr/bin/env bash
# ---------------------------------------------------------------
# Integration test for the Data Engineering Platform
# ---------------------------------------------------------------
set -euo pipefail

echo "=== Checking Kafka health ==="
# Verify Kafka port is reachable via Docker exec
if ! docker exec kafka bash -c "nc -z localhost 9092" >/dev/null 2>&1; then
  echo "❌ Kafka not reachable"
  exit 1
fi

echo "Kafka reachable"

# Consumer lag check skipped (no consumer group in this test)
# KAFKA_LAG=$(docker exec kafka bash -c "kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group reddit-consumer" | grep reddit_raw | awk '{print \$6}')
# echo "Kafka consumer lag: $KAFKA_LAG"
# if [[ -z "$KAFKA_LAG" || $KAFKA_LAG -gt 10 ]]; then
#   echo "❌ Consumer lag too high"
#   exit 1
# fi

echo "=== Verifying Spark processing ==="
# Check Spark UI for RUNNING app
SPARK_RUNNING=$(docker exec spark-master bash -c "curl -s http://localhost:8080/api/v1/applications | grep RUNNING || true")
if [[ -z "$SPARK_RUNNING" ]]; then
  echo "❌ Spark job not running"
  exit 1
fi

echo "Spark job is RUNNING"

echo "=== Checking HDFS parquet output ==="
HDFS_COUNT=$(docker exec hdfs-namenode bash -c "hdfs dfs -count -q /data/reddit/parquet | awk '{print \$2}'")
echo "Parquet files in HDFS: $HDFS_COUNT"
if [[ -z "$HDFS_COUNT" || $HDFS_COUNT -lt 1 ]]; then
  echo "❌ No parquet files found"
  exit 1
fi

echo "=== Checking Elasticsearch indexing ==="
ES_COUNT=$(curl -s "http://localhost:9200/reddit/posts/_count" | jq .count)
echo "Documents indexed in ES: $ES_COUNT"
if [[ -z "$ES_COUNT" || $ES_COUNT -lt 1 ]]; then
  echo "❌ No documents indexed"
  exit 1
fi

echo "=== All checks passed! ==="
