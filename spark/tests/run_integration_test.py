import os
import sys
import json
import time
import subprocess
import requests

ELASTICSEARCH_URL = "http://elastic:sdoqap_secure@localhost:9200"

def run_cmd(args):
    print(f"Executing host command: {' '.join(args)}")
    result = subprocess.run(args, capture_output=True, text=True, shell=True)
    if result.returncode != 0:
        print(f"Error stdout: {result.stdout}")
        print(f"Error stderr: {result.stderr}")
        raise RuntimeError(f"Command failed with exit code {result.returncode}")
    return result.stdout.strip()

def setup_hdfs():
    print("Setting up HDFS benchmark dataset using docker HDFS commands...")
    # 1. Copy local file to namenode container
    run_cmd(["docker", "cp", "spark/tests/benchmark_dataset.csv", "sdoqap-namenode:/tmp/benchmark_dataset.csv"])
    
    # 2. Create raw directory on HDFS
    run_cmd(["docker", "exec", "-t", "-e", "HADOOP_USER_NAME=root", "sdoqap-namenode", "hdfs", "dfs", "-mkdir", "-p", "/data/raw/benchmark_test"])
    
    # 3. Put file onto HDFS
    run_cmd(["docker", "exec", "-t", "-e", "HADOOP_USER_NAME=root", "sdoqap-namenode", "hdfs", "dfs", "-put", "-f", "/tmp/benchmark_dataset.csv", "/data/raw/benchmark_test/benchmark_dataset.csv"])
    print("HDFS Setup completed successfully.")

def clear_es_lock():
    print("Clearing any stale locks in Elasticsearch for benchmark_test...")
    try:
        url = f"{ELASTICSEARCH_URL}/sdoqap_run_locks/_doc/benchmark_test"
        res = requests.delete(url, timeout=5)
        print(f"Stale lock clear status: {res.status_code}")
    except Exception as e:
        print(f"Failed to clear lock: {e}")

def run_spark_job():
    print("Running Spark Quality Engine job for benchmark_test on cluster...")
    # Run the quality engine using spark-submit pointing to the Spark master cluster
    run_cmd([
        "docker", "exec", "-t", "-e", "HADOOP_USER_NAME=root", "sdoqap-spark-master",
        "spark-submit",
        "--master", "spark://spark-master:7077",
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

if __name__ == "__main__":
    print("=== STARTING HOST-BASED PIPELINE INTEGRATION TEST ===")
    try:
        setup_hdfs()
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
