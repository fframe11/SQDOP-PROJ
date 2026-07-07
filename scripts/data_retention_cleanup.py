#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
SDOQAP Automated Storage Retention & Cleanup System
===================================================
Cleans up old HDFS files and Elasticsearch documents older than 30 days
to prevent disk exhaustion in production.
"""

import os
import time
import requests
from datetime import datetime, timedelta, timezone

# Load environment variables
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://elastic:sdoqap_secure@elasticsearch:9200")
HDFS_HTTP_URL = os.getenv("HDFS_HTTP_URL", "http://namenode:9870")
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "30"))

# Hostname resolution fallback for local development outside Docker
if "elasticsearch" in ELASTICSEARCH_URL:
    try:
        import socket
        socket.gethostbyname("elasticsearch")
    except socket.gaierror:
        ELASTICSEARCH_URL = ELASTICSEARCH_URL.replace("elasticsearch", "localhost")
        print(f"[CLEANUP] Fallback: Elasticsearch redirected to localhost for host development.")

if "namenode" in HDFS_HTTP_URL:
    try:
        import socket
        socket.gethostbyname("namenode")
    except socket.gaierror:
        HDFS_HTTP_URL = HDFS_HTTP_URL.replace("namenode", "localhost")
        print(f"[CLEANUP] Fallback: WebHDFS redirected to localhost for host development.")

def clean_elasticsearch():
    """Delete documents older than RETENTION_DAYS in ES indices."""
    print("=== [CLEANUP] Starting Elasticsearch Retention Cleanup ===")
    limit_date = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).isoformat()
    print(f"Purging documents older than: {limit_date}")

    # Index configs: (Index Name, Timestamp Field Name)
    indices = [
        ("sdoqap_pipeline_runs", "timestamp"),
        ("sdoqap_quality_runs", "timestamp"),
        ("sdoqap_schema_drifts", "timestamp"),
        ("sdoqap_lineage_runs", "timestamp"),
        ("sdoqap_ai_rule_proposals", "timestamp"),
        ("sdoqap_rules_audit_log", "timestamp"),
        ("sdoqap_schema_proposals", "proposed_at")
    ]

    for index, ts_field in indices:
        query = {
            "query": {
                "range": {
                    ts_field: {
                        "lt": limit_date
                    }
                }
            }
        }
        url = f"{ELASTICSEARCH_URL}/{index}/_delete_by_query?conflicts=proceed"
        try:
            res = requests.post(url, json=query, headers={"Content-Type": "application/json"}, timeout=30)
            if res.status_code == 200:
                result = res.json()
                deleted = result.get("deleted", 0)
                print(f"  Index '{index}': Successfully purged {deleted} old documents.")
            else:
                print(f"  Index '{index}': Status {res.status_code} - {res.text[:200]}")
        except Exception as e:
            print(f"  Index '{index}': Failed to purge - {e}")

def clean_hdfs():
    """Delete files older than RETENTION_DAYS in HDFS raw/quarantine zones."""
    print("\n=== [CLEANUP] Starting HDFS Storage Retention Cleanup ===")
    now_ms = int(time.time() * 1000)
    retention_ms = RETENTION_DAYS * 24 * 60 * 60 * 1000
    cutoff_ms = now_ms - retention_ms
    limit_str = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Purging files modified before: {limit_str}")

    hdfs_paths = [
        "/data/raw",
        "/data/quarantine"
    ]

    for root_path in hdfs_paths:
        print(f"Scanning HDFS directory: {root_path}...")
        # 1. List tables
        list_url = f"{HDFS_HTTP_URL}/webhdfs/v1{root_path}?op=LISTSTATUS"
        try:
            res = requests.get(list_url, timeout=10)
            if res.status_code != 200:
                print(f"  Path '{root_path}' not accessible (HTTP {res.status_code}).")
                continue
            
            tables = res.json().get("FileStatuses", {}).get("FileStatus", [])
            for table_status in tables:
                table_name = table_status.get("pathSuffix")
                table_path = f"{root_path}/{table_name}"
                
                # 2. List items in table directory
                item_url = f"{HDFS_HTTP_URL}/webhdfs/v1{table_path}?op=LISTSTATUS"
                item_res = requests.get(item_url, timeout=10)
                if item_res.status_code != 200:
                    continue
                
                files = item_res.json().get("FileStatuses", {}).get("FileStatus", [])
                for file_status in files:
                    file_name = file_status.get("pathSuffix")
                    file_path = f"{table_path}/{file_name}"
                    mod_time = file_status.get("modificationTime", 0)
                    
                    if mod_time < cutoff_ms:
                        # 3. Delete file
                        del_url = f"{HDFS_HTTP_URL}/webhdfs/v1{file_path}?op=DELETE&recursive=true&user.name=root"
                        del_res = requests.delete(del_url, timeout=10)
                        if del_res.status_code == 200:
                            print(f"  HDFS: Deleted old file/directory '{file_path}' (Modified: {datetime.fromtimestamp(mod_time/1000, timezone.utc).isoformat()})")
                        else:
                            print(f"  HDFS: Failed to delete '{file_path}' (HTTP {del_res.status_code})")
        except Exception as e:
            print(f"  HDFS: Error cleaning path '{root_path}' - {e}")

if __name__ == "__main__":
    print("=========================================================")
    print(f"SDOQAP Retention Job Started at {datetime.now(timezone.utc).isoformat()}")
    print("=========================================================")
    clean_elasticsearch()
    clean_hdfs()
    print("=========================================================")
    print("SDOQAP Retention Job Completed successfully.")
    print("=========================================================")
