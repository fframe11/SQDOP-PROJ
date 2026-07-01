import os
import sys
import json
import time
import requests
from pyspark.sql import SparkSession

def get_elasticsearch_url():
    es_user = os.getenv("ELASTICSEARCH_USER", "elastic")
    es_pass = os.getenv("ELASTICSEARCH_PASSWORD", "sdoqap_secure")
    es_host = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
    es_port = os.getenv("ELASTICSEARCH_PORT", "9200")
    if "ELASTICSEARCH_HOST" not in os.environ and "ELASTICSEARCH_URL" not in os.environ:
        es_host = "localhost"
    es_url = os.getenv("ELASTICSEARCH_URL")
    if not es_url:
        es_url = f"http://{es_user}:{es_pass}@{es_host}:{es_port}"
    return es_url

ELASTICSEARCH_URL = get_elasticsearch_url()
HDFS_URL = os.getenv("HDFS_URL", "hdfs://namenode:9000")

def get_es_auth():
    from urllib.parse import urlparse
    parsed = urlparse(ELASTICSEARCH_URL)
    if parsed.username and parsed.password:
        return (parsed.username, parsed.password)
    return None

def get_es_base_url():
    from urllib.parse import urlparse
    parsed = urlparse(ELASTICSEARCH_URL)
    return f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"

def get_spark():
    builder = SparkSession.builder \
        .appName("SDOQAP_UnitTest_Runner")
        
    if "SPARK_HOME" not in os.environ:
        builder = builder.master("local[*]")
        
    return builder.getOrCreate()

def setup_hdfs(spark):
    print("Clearing any stale locks in Elasticsearch for benchmark_test...")
    es_base = get_es_base_url()
    auth = get_es_auth()
    try:
        url = f"{es_base}/sdoqap_run_locks/_doc/benchmark_test"
        res = requests.delete(url, auth=auth, timeout=5)
        print(f"Stale lock clear status: {res.status_code}")
    except Exception as e:
        print(f"Failed to clear lock: {e}")

    print("Setting up HDFS directories and uploading benchmark dataset using Java HDFS API...")
    sc = spark.sparkContext
    conf = sc._jsc.hadoopConfiguration()
    URI = sc._gateway.jvm.java.net.URI
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    fs = FileSystem.get(URI(HDFS_URL), conf)
    
    # Create /data/raw/benchmark_test directory
    raw_dir = Path("/data/raw/benchmark_test")
    if not fs.exists(raw_dir):
        fs.mkdirs(raw_dir)
        
    # Upload local benchmark_dataset.csv to HDFS
    benchmark_local_path = "/opt/spark-apps/tests/benchmark_dataset.csv"
    if not os.path.exists(benchmark_local_path):
        benchmark_local_path = os.path.join(os.path.dirname(__file__), "benchmark_dataset.csv")
        
    local_path = Path(f"file://{os.path.abspath(benchmark_local_path)}")
    hdfs_path = Path("/data/raw/benchmark_test/benchmark_dataset.csv")
    
    print(f"Copying from local {local_path} to HDFS {hdfs_path}...")
    fs.copyFromLocalFile(False, True, local_path, hdfs_path)
    print("HDFS Setup completed successfully.")

def run_spark_job():
    print("Running Spark Quality Engine job for benchmark_test...")
    engine_path = "/opt/spark-apps/spark_quality_engine.py"
    if not os.path.exists(engine_path):
        engine_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "spark_quality_engine.py")

    # Run the quality engine using spark-submit pointing to the Spark master cluster
    result = os.system(f"spark-submit --master spark://spark-master:7077 --packages io.delta:delta-spark_2.12:3.0.0 {engine_path} benchmark_test")
    if result != 0:
        raise RuntimeError(f"spark-submit failed with exit code {result}")
    print("Spark Quality Engine run finished.")

def verify_results():
    print("Verifying pipeline results in Elasticsearch...")
    # Sleep briefly to ensure ES has indexed the document
    time.sleep(3)
    
    es_base = get_es_base_url()
    auth = get_es_auth()
    
    # Check if index exists first
    check_url = f"{es_base}/sdoqap_quality_runs"
    try:
        res = requests.head(check_url, auth=auth, timeout=5)
        if res.status_code == 404:
            raise AssertionError("Index 'sdoqap_quality_runs' does not exist in Elasticsearch. The Spark job may have failed to log metrics.")
    except Exception as e:
        print(f"Warning during index status check: {e}")
    
    url = f"{es_base}/sdoqap_quality_runs/_search"
    query = {
        "query": {
            "term": {
                "table_name.keyword": "benchmark_test"
            }
        },
        "sort": [{"timestamp": {"order": "desc"}}],
        "size": 1
    }
    
    headers = {"Content-Type": "application/json"}
    res = requests.post(url, headers=headers, auth=auth, data=json.dumps(query), timeout=10)
    res.raise_for_status()
    
    hits = res.json().get("hits", {}).get("hits", [])
    if not hits:
        raise AssertionError("No quality run record found in Elasticsearch for 'benchmark_test'.")
        
    doc = hits[0]["_source"]
    print(f"Latest Elasticsearch Quality Document: {json.dumps(doc, indent=2)}")
    
    # Assertions
    # 20 clean rows, 5 missing primary keys, 3 duplicate rows, 2 missing dates = 30 total rows
    expected_total = 30
    expected_clean = 20
    expected_quarantine = 10
    
    actual_total = doc.get("total_records")
    actual_clean = doc.get("clean_records")
    actual_quarantine = doc.get("quarantined_records")
    actual_score = doc.get("quality_score")
    
    print("--- Running Assertions ---")
    assert actual_total == expected_total, f"Expected total_records to be {expected_total}, got {actual_total}"
    assert actual_clean == expected_clean, f"Expected clean_records to be {expected_clean}, got {actual_clean}"
    assert actual_quarantine == expected_quarantine, f"Expected quarantined_records to be {expected_quarantine}, got {actual_quarantine}"
    
    expected_score = (expected_clean / expected_total) * 100.0
    assert abs(actual_score - expected_score) < 0.1, f"Expected quality_score to be {expected_score:.2f}%, got {actual_score:.2f}%"
    
    print("✅ All Assertions PASSED!")
    print(f"Total: {actual_total} | Clean: {actual_clean} | Quarantined: {actual_quarantine} | Quality Score: {actual_score:.2f}%")

def cleanup(spark):
    print("Cleaning up HDFS benchmark test data using Java HDFS API...")
    try:
        sc = spark.sparkContext
        conf = sc._jsc.hadoopConfiguration()
        URI = sc._gateway.jvm.java.net.URI
        FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        fs = FileSystem.get(URI(HDFS_URL), conf)
        
        fs.delete(Path("/data/raw/benchmark_test"), True)
        fs.delete(Path("/data/active/benchmark_test"), True)
        fs.delete(Path("/data/quarantine/benchmark_test"), True)
        print("Cleanup completed.")
    except Exception as e:
        print(f"Cleanup encountered warnings: {e}")

if __name__ == "__main__":
    print("=== STARTING DATA PIPELINE UNIT TEST ===")
    spark = None
    try:
        spark = get_spark()
        setup_hdfs(spark)
        run_spark_job()
        verify_results()
        print("=== UNIT TEST COMPLETED SUCCESSFULLY ===")
        sys.exit(0)
    except Exception as e:
        print(f"❌ UNIT TEST FAILED: {e}")
        sys.exit(1)
    finally:
        if spark:
            cleanup(spark)
            spark.stop()
