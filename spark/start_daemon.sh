#!/bin/bash
if [ "$SPARK_MODE" = "master" ]; then
    echo "Starting Spark Trigger Daemon inside /docker-entrypoint-initdb.d..."
    python /opt/spark-apps/spark_trigger_daemon.py &
fi
