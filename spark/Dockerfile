# Dockerfile for Spark 3.4.1 with Delta Lake (Option B)
# Extends the official Bitnami Spark image and bundles the Delta Lake core JAR
# and the matching Python delta-spark package. This creates an immutable,
# self‑contained image suitable for production‑grade workloads.

FROM bitnamilegacy/spark:3.4.1

USER root
RUN apt-get update && apt-get install -y bash
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

ENV SPARK_CONF_DIR=/opt/bitnami/spark/conf
ENV SPARK_CLASSPATH=/opt/bitnami/spark/jars/*
ENV SPARK_DIST_CLASSPATH=/opt/bitnami/spark/jars/*

# Download the Delta Lake core JAR (compatible with Spark 3.4 / Scala 2.12)
ENV DELTA_VERSION=2.4.0
RUN curl -L -o /opt/bitnami/spark/jars/delta-core_2.12-${DELTA_VERSION}.jar https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_VERSION}/delta-core_2.12-${DELTA_VERSION}.jar && \
    curl -L -o /opt/bitnami/spark/jars/delta-storage-${DELTA_VERSION}.jar https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar
# Add Spark config for Delta Lake
RUN echo "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" >> ${SPARK_CONF_DIR}/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" >> ${SPARK_CONF_DIR}/spark-defaults.conf

# Install the matching Python package (used by spark_quality_engine.py)
RUN pip install --no-cache-dir delta-spark==${DELTA_VERSION} && pip install --no-cache-dir pyyaml requests

# Ensure Spark picks up JARs from the default /opt/spark/jars directory (already on classpath)
# No further configuration is required.

# Preserve the entrypoint from the base image
CMD ["/opt/bitnami/scripts/spark/run.sh"]
