#!/bin/bash
set -e

echo "=== Waiting for NameNode RPC (port 9000) to be ready ==="
for i in $(seq 1 120); do
  if docker exec sdoqap-namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "Safe mode"; then
    echo "NameNode RPC is reachable (attempt $i)"
    break
  fi
  echo "Attempt $i: NameNode not ready yet, waiting 2s..."
  sleep 2
done

echo "=== Leaving safe mode ==="
docker exec sdoqap-namenode hdfs dfsadmin -safemode leave
echo "Safe mode is OFF"

echo "=== Verifying HDFS data ==="
docker exec sdoqap-namenode hdfs dfs -ls -R /data/raw

echo "=== Installing requests in Spark (if needed) ==="
docker exec sdoqap-spark-master pip install --no-deps \
  /opt/spark-apps/wheels/certifi-2026.6.17-py3-none-any.whl \
  /opt/spark-apps/wheels/charset_normalizer-3.4.7-cp311-cp311-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl \
  /opt/spark-apps/wheels/idna-3.18-py3-none-any.whl \
  /opt/spark-apps/wheels/urllib3-2.7.0-py3-none-any.whl \
  /opt/spark-apps/wheels/requests-2.34.2-py3-none-any.whl 2>/dev/null || true

echo ""
echo "=== Running Spark Quality Check: products ==="
docker exec -e HADOOP_USER_NAME=root sdoqap-spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/spark_quality_engine.py products

echo ""
echo "=== Running Spark Quality Check: gov_data ==="
docker exec -e HADOOP_USER_NAME=root sdoqap-spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/spark_quality_engine.py gov_data

echo ""
echo "=== Running Spark Quality Check: sales_records ==="
docker exec -e HADOOP_USER_NAME=root sdoqap-spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/spark_quality_engine.py sales_records

echo ""
echo "=== ALL SPARK QUALITY CHECKS COMPLETE ==="
