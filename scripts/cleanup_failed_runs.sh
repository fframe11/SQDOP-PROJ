#!/usr/bin/env bash
# Cleanup HDFS quarantine directories owned by root (or non-spark owners) that caused permission errors

docker exec sdoqap-namenode hdfs dfs -ls -R /data/quarantine | awk '/root/ {print $8}' | while read -r dir; do
  echo "Deleting problematic HDFS directory: $dir"
  docker exec sdoqap-namenode hdfs dfs -rm -r -skipTrash "$dir"
done
