# Dockerfile for Spark 3.4.1 with Delta Lake (Option B)
# Extends the official Bitnami Spark image and bundles the Delta Lake core JAR
# and the matching Python delta-spark package. This creates an immutable,
# self‑contained image suitable for production‑grade workloads.

FROM bitnamilegacy/spark:3.4.1

USER root
RUN apt-get update && apt-get install -y bash
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

ENV SPARK_CONF_DIR=/opt/bitnami/spark/conf

# Download the Delta Lake core JAR (compatible with Spark 3.4 / Scala 2.12)
ENV DELTA_VERSION=2.4.0
RUN curl -L -o /opt/bitnami/spark/jars/delta-core_2.12-${DELTA_VERSION}.jar https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_VERSION}/delta-core_2.12-${DELTA_VERSION}.jar && \
    curl -L -o /opt/bitnami/spark/jars/delta-storage-${DELTA_VERSION}.jar https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar
# Add Spark config for Delta Lake and Prometheus metrics
RUN echo "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" >> ${SPARK_CONF_DIR}/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" >> ${SPARK_CONF_DIR}/spark-defaults.conf && \
    echo "spark.ui.prometheus.enabled=true" >> ${SPARK_CONF_DIR}/spark-defaults.conf

# Install the matching Python package (used by spark_quality_engine.py)
RUN pip install --no-cache-dir delta-spark==${DELTA_VERSION} && pip install --no-cache-dir pyyaml requests kafka-python

# Download Kafka & ES connectors
RUN curl -L -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.4.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/kafka-clients-3.3.2.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar && \
    curl -L -o /opt/bitnami/spark/jars/elasticsearch-spark-30_2.12-8.10.2.jar https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.10.2/elasticsearch-spark-30_2.12-8.10.2.jar && \
    curl -L -o /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Ensure Spark picks up JARs from the default /opt/spark/jars directory (already on classpath)
# No further configuration is required.

# Copy trigger daemon script and automatic initialization script
COPY start_daemon.sh /docker-entrypoint-initdb.d/start_daemon.sh
RUN chmod +x /docker-entrypoint-initdb.d/start_daemon.sh

# Pre-resolve and download Spark Streaming Kafka and ES dependencies to local Ivy cache for offline execution
RUN spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.10.2 --version

# Copy spark application scripts into the image
COPY spark_quality_engine.py spark_gold_layer.py spark_trigger_daemon.py streaming_job.py rules_config.json reddit_stream.py dynamic_rules_engine.py ai_rule_advisor.py data_profile_store.py /opt/spark-apps/

# Restore default CMD from base image
CMD ["/opt/bitnami/scripts/spark/run.sh"]
