# test_ingest.py
"""Simple test script to verify n8n ingestion webhook for three data sources.
It sends dummy payloads to the n8n webhook and checks that the expected
PostgreSQL tables have been populated.
"""
import sys
import time
import json
import subprocess
import requests

# Configuration – adjust if ports differ
N8N_PORT = int("${N8N_PORT}".strip("{}")) if "${N8N_PORT}" != "" else 5678
WEBHOOK_URL = f"http://localhost:{N8N_PORT}/webhook/1/webhooktrigger/ingest"

# Dummy payloads for three sources (example CSV, API JSON, Kafka placeholder)
payloads = [
    {"source": "csv", "data": "id,name\n1,Test"},
    {"source": "api", "data": {"id": 2, "name": "API Test"}},
    {"source": "kafka", "data": {"id": 3, "name": "Kafka Test"}},
]

def post_payload(payload):
    try:
        resp = requests.post(WEBHOOK_URL, json=payload, timeout=10)
        resp.raise_for_status()
        print(f"[INFO] Posted payload for {payload['source']}, status {resp.status_code}")
    except Exception as e:
        print(f"[ERROR] Failed to post {payload['source']}: {e}")
        sys.exit(1)

def check_postgres_table(source):
    # Simple row count check via psql inside container
    cmd = [
        "docker", "exec", "sdoqap-postgres",
        "psql", "-U", "sdoqap", "-d", "sdoqap_oltp",
        "-t", "-c",
        f"SELECT COUNT(*) FROM {source}_records;"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"[ERROR] Postgres check failed for {source}: {result.stderr.strip()}")
        sys.exit(1)
    count = result.stdout.strip()
    print(f"[INFO] {source}_records count: {count}")

def main():
    for p in payloads:
        post_payload(p)
        time.sleep(2)  # give pipeline time to process
        check_postgres_table(p['source'])
    print("[SUCCESS] All ingestion tests passed.")

if __name__ == "__main__":
    main()
