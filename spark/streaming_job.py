import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Spark session
HDFS_URL = "hdfs://namenode:9000"

def run_streaming_job():
    spark = SparkSession.builder \
        .appName("RedditStreaming") \
        .config("spark.sql.streaming.checkpointLocation", f"{HDFS_URL}/tmp/checkpoints/reddit") \
        .getOrCreate()

    # Define schema for incoming Reddit JSON
    define_schema = """
    subreddit STRING,
    id STRING,
    title STRING,
    created_utc DOUBLE,
    author STRING,
    score INT
    """

    try:
        # Read from Kafka
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "reddit_raw") \
            .load()

        # Convert binary value to string and parse JSON
        json_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
            .select(F.from_json(F.col("json"), define_schema).alias("data")) \
            .select("data.*") \
            .withColumn("ts", F.from_unixtime(F.col("created_utc")).cast("timestamp"))

        # Write Parquet to HDFS
        parquet_query = json_df.writeStream \
            .format("parquet") \
            .option("path", f"{HDFS_URL}/data/reddit/parquet") \
            .option("checkpointLocation", f"{HDFS_URL}/tmp/checkpoints/parquet") \
            .outputMode("append") \
            .partitionBy("subreddit") \
            .trigger(processingTime="2 seconds") \
            .start()

        # Write to Elasticsearch (requires elasticsearch-hadoop connector on Spark image)
        es_query = json_df.writeStream \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", "reddit") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("checkpointLocation", f"{HDFS_URL}/tmp/checkpoints/es") \
            .outputMode("append") \
            .start()

        spark.streams.awaitAnyTermination()
    except Exception as e:
        print(f"Streaming job failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_streaming_job()
