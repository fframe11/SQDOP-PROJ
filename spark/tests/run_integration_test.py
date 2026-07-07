import os
import sys
import json
import time
import subprocess
import requests
import io

# Force UTF-8 console output on Windows to prevent UnicodeEncodeError
if sys.platform.startswith("win"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Ensure project root is on PYTHONPATH for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
try:
    from api.app.api.config import get_required_env
except ModuleNotFoundError:
    # Fallback when api package is unavailable
    def get_required_env(name: str) -> str:
        """Retrieve required env var or raise clear error"""
        import os
        value = os.getenv(name)
        if value is None:
            raise RuntimeError(f"Missing required environment variable '{name}'. Set it in the environment.")
        return value

# Default HDFS URL for container execution if not provided
import os
os.environ.setdefault('HDFS_URL', 'hdfs://localhost:9002')
# Set default Elasticsearch env for test if not provided
os.environ.setdefault('ELASTICSEARCH_USER', 'elastic')
os.environ.setdefault('ELASTICSEARCH_PASSWORD', 'sdoqap_secure')
os.environ.setdefault('ELASTICSEARCH_HOST', 'localhost')
os.environ.setdefault('ELASTICSEARCH_PORT', '9200')

def get_elasticsearch_url():
    # Use strict env retrieval; raises if missing
    es_user = get_required_env("ELASTICSEARCH_USER")
    es_pass = get_required_env("ELASTICSEARCH_PASSWORD")
    # Use Docker service name if host not set (default for internal network)
    es_host = os.getenv("ELASTICSEARCH_HOST", "localhost")
    es_port = get_required_env("ELASTICSEARCH_PORT")
    # Allow optional ELASTICSEARCH_URL override
    es_url = os.getenv("ELASTICSEARCH_URL")
    if not es_url:
        es_url = f"http://{es_user}:{es_pass}@{es_host}:{es_port}"
    return es_url

ELASTICSEARCH_URL = get_elasticsearch_url()

def run_cmd(args):
    print(f"Executing host command: {' '.join(args)}")
    result = subprocess.run(args, capture_output=True, encoding="utf-8", shell=True)
    if result.returncode != 0:
        print(f"Error stdout: {result.stdout}")
        print(f"Error stderr: {result.stderr}")
        raise RuntimeError(f"Command failed with exit code {result.returncode}")
    return result.stdout.strip()

def setup_hdfs():
    print("Setting up HDFS benchmark dataset using docker HDFS commands...")
    # 1. Copy local file to namenode container
    run_cmd(["docker", "cp", "spark/tests/benchmark_dataset.csv", "sdoqap-namenode:/tmp/benchmark_dataset.csv"])
    
    # 2. Create directories on HDFS
    run_cmd(["docker", "exec", "-t", "-e", "HADOOP_USER_NAME=root", "sdoqap-namenode", "hdfs", "dfs", "-mkdir", "-p", "/data/raw/benchmark_test"])
    run_cmd(["docker", "exec", "-t", "-e", "HADOOP_USER_NAME=root", "sdoqap-namenode", "hdfs", "dfs", "-mkdir", "-p", "/data/active"])
    run_cmd(["docker", "exec", "-t", "-e", "HADOOP_USER_NAME=root", "sdoqap-namenode", "hdfs", "dfs", "-mkdir", "-p", "/data/quarantine"])
    
    # Ensure HDFS is ready before putting the file
    wait_for_hdfs()

    # 3. Put file onto HDFS
    run_cmd(["docker", "exec", "-t", "-e", "HADOOP_USER_NAME=root", "sdoqap-namenode", "hdfs", "dfs", "-put", "-f", "/tmp/benchmark_dataset.csv", "/data/raw/benchmark_test/benchmark_dataset.csv"])
    
    # Clean up host-copied file from Namenode's container /tmp directory to prevent disk bloating
    run_cmd(["docker", "exec", "-t", "sdoqap-namenode", "rm", "-f", "/tmp/benchmark_dataset.csv"])

    # 4. Set secure permissions: owner spark, group spark, mode 770 (no world write)
    run_cmd(["docker", "exec", "-t", "-e", "HADOOP_USER_NAME=root", "sdoqap-namenode", "hdfs", "dfs", "-chown", "-R", "spark:spark", "/data"])
    run_cmd(["docker", "exec", "-t", "-e", "HADOOP_USER_NAME=root", "sdoqap-namenode", "hdfs", "dfs", "-chmod", "-R", "770", "/data"])
    print("HDFS permissions set to spark:spark with mode 770.")

def clear_es_lock():
    print("Clearing any stale locks in Elasticsearch for benchmark_test...")
    try:
        url = f"{ELASTICSEARCH_URL}/sdoqap_run_locks/_doc/benchmark_test"
        res = requests.delete(url, timeout=5)
        print(f"Stale lock clear status: {res.status_code}")
    except Exception as e:
        print(f"Failed to clear lock: {e}")

def get_spark_master_container_name():
    try:
        import subprocess
        # Get list of running containers using docker ps
        res = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, encoding="utf-8", check=True)
        for line in res.stdout.split("\n"):
            name = line.strip()
            if "spark-master" in name:
                return name
    except Exception as e:
        print(f"Warning: Failed to locate spark-master container dynamically: {e}")
    return "sdoqap-spark-master"

def run_spark_job():
    print("Running Spark Quality Engine job for benchmark_test on cluster...")
    container_name = get_spark_master_container_name()
    # Run the quality engine using spark-submit pointing to the Spark master cluster
    run_cmd([
        "docker", "exec", "-t", "-e", "HADOOP_USER_NAME=spark", "-e", "HDFS_URL=hdfs://sdoqap-namenode:9000", container_name,
        "spark-submit",
        "--master", "spark://spark-master:7077",
        "--conf", "spark.executorEnv.HADOOP_USER_NAME=spark",
        "--conf", "spark.executor.extraJavaOptions=-DHADOOP_USER_NAME=spark",
        "--conf", "spark.driver.extraJavaOptions=-DHADOOP_USER_NAME=spark",
        "--packages", "io.delta:delta-core_2.12:2.4.0",
        "/opt/spark-apps/spark_quality_engine.py",
        "benchmark_test"
      ])
    print("Spark Quality Engine run finished.")

def verify_results():
    print("Verifying pipeline results in Elasticsearch...")
    time.sleep(3)
    
    # Check if index exists first
    check_url = f"{ELASTICSEARCH_URL}/sdoqap_quality_runs"
    res = requests.head(check_url, timeout=30)
    if res.status_code == 404:
        raise AssertionError("Index 'sdoqap_quality_runs' does not exist in Elasticsearch. The Spark job failed to log metrics.")
        
    url = f"{ELASTICSEARCH_URL}/sdoqap_quality_runs/_search"
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
    res = requests.post(url, headers=headers, data=json.dumps(query), timeout=30)
    res.raise_for_status()
    
    hits = res.json().get("hits", {}).get("hits", [])
    if not hits:
        raise AssertionError("No quality run record found in Elasticsearch for 'benchmark_test'.")
        
    doc = hits[0]["_source"]
    print(f"Latest Elasticsearch Quality Document: {json.dumps(doc, indent=2)}")
    
    expected_total = 27
    expected_clean = 20
    expected_quarantine = 7
    
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
    
    print("[PASS] All Assertions PASSED!")
    print(f"Total: {actual_total} | Clean: {actual_clean} | Quarantined: {actual_quarantine} | Quality Score: {actual_score:.2f}%")

def cleanup():
    print("Cleaning up HDFS benchmark test data...")
    try:
        run_cmd(["docker", "exec", "-t", "-e", "HADOOP_USER_NAME=root", "sdoqap-namenode", "hdfs", "dfs", "-rm", "-r", "-skipTrash", "/data/raw/benchmark_test"])
        run_cmd(["docker", "exec", "-t", "-e", "HADOOP_USER_NAME=root", "sdoqap-namenode", "hdfs", "dfs", "-rm", "-r", "-skipTrash", "/data/active/benchmark_test"])
        run_cmd(["docker", "exec", "-t", "-e", "HADOOP_USER_NAME=root", "sdoqap-namenode", "hdfs", "dfs", "-rm", "-r", "-skipTrash", "/data/quarantine/benchmark_test"])
        print("Cleanup completed.")
    except Exception as e:
        print(f"Cleanup encountered warnings: {e}")

def get_spark_session(app_name):
    from pyspark.sql import SparkSession
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.executor.memory", "3g")
        .config("spark.executor.cores", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "1")
        .config("spark.dynamicAllocation.maxExecutors", "4")
        .config("spark.hadoop.fs.defaultFS", HDFS_URL)
        .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    )
    if "SPARK_HOME" not in os.environ:
        builder = builder.master("local[*]")
        builder = builder.config("spark.driver.host", "127.0.0.1") \
                         .config("spark.driver.bindAddress", "127.0.0.1")
    return builder.getOrCreate()

def setup_spark_env():
    print("Ensuring Spark containers have 'requests' package installed...")
    print("Ensuring Spark containers have 'requests' and 'pyyaml' packages installed...")
    try:
        import subprocess
        res = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, encoding="utf-8", check=True)
        containers = [line.strip() for line in res.stdout.split("\n") if line.strip()]
        for container in containers:
            if "spark-master" in container or "spark-worker" in container:
                print(f"Installing dependencies in {container}...")
                subprocess.run(["docker", "exec", "-t", "-u", "root", container, "pip", "install", "requests", "pyyaml"], check=True)
        print("Spark dependencies check completed successfully.")
    except Exception as e:
        print(f"Warning: Spark dependency setup encountered an issue: {e}")

def wait_for_hdfs(retries=5, delay=5):
    print("Waiting for HDFS namenode to become reachable...")
    for i in range(retries):
        try:
            # Simple check using hdfs dfs -ls /
            run_cmd(["docker", "exec", "-t", "-e", "HADOOP_USER_NAME=root", "sdoqap-namenode", "hdfs", "dfs", "-ls", "/"])
            print("HDFS is reachable.")
            return True
        except Exception as e:
            print(f"HDFS not reachable yet (attempt {i+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("HDFS namenode not reachable after retries")

if __name__ == "__main__":
    print("=== STARTING HOST-BASED PIPELINE INTEGRATION TEST ===")
    try:
        setup_spark_env()
        setup_hdfs()
        wait_for_hdfs()
        clear_es_lock()
        run_spark_job()
        verify_results()
        print("=== INTEGRATION TEST COMPLETED SUCCESSFULLY ===")
        sys.exit(0)
    except Exception as e:
        print(f"[ERROR] INTEGRATION TEST FAILED: {e}")
        sys.exit(1)
    finally:
        cleanup()
