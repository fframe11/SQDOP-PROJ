import os
import json
from datetime import datetime, timedelta, timezone
from elasticsearch import Elasticsearch

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://elastic:sdoqap_secure@localhost:9200")

def seed():
    print(f"Connecting to Elasticsearch at {ELASTICSEARCH_URL}...")
    es = Elasticsearch(ELASTICSEARCH_URL)
    
    # 1. Reset / recreate indices if they exist
    indices = ["sdoqap_quality_runs", "sdoqap_pipeline_runs", "sdoqap_schema_drifts", "sdoqap_lineage_runs"]
    for idx in indices:
        if es.indices.exists(index=idx):
            print(f"Deleting index {idx}...")
            es.indices.delete(index=idx)
        print(f"Creating index {idx}...")
        es.indices.create(index=idx)

    now = datetime.now(timezone.utc)
    
    # Helper to generate timestamps
    def get_ts(offset_mins):
        return (now - timedelta(minutes=offset_mins)).isoformat()

    print("Seeding sdoqap_pipeline_runs...")
    # Pipeline runs (success, failed, quarantined)
    pipeline_runs = [
        # Users Table (API Ingestion)
        {"run_id": "run_users_010", "table_name": "users", "state": "success", "duration_seconds": 12.4, "timestamp": get_ts(110)},
        {"run_id": "run_users_009", "table_name": "users", "state": "success", "duration_seconds": 13.1, "timestamp": get_ts(100)},
        {"run_id": "run_users_008", "table_name": "users", "state": "success", "duration_seconds": 12.8, "timestamp": get_ts(90)},
        {"run_id": "run_users_007", "table_name": "users", "state": "success", "duration_seconds": 14.2, "timestamp": get_ts(80)},
        {"run_id": "run_users_006", "table_name": "users", "state": "success", "duration_seconds": 11.9, "timestamp": get_ts(70)},
        {"run_id": "run_users_005", "table_name": "users", "state": "quarantined", "duration_seconds": 15.6, "timestamp": get_ts(60)},
        {"run_id": "run_users_004", "table_name": "users", "state": "success", "duration_seconds": 12.1, "timestamp": get_ts(50)},
        {"run_id": "run_users_003", "table_name": "users", "state": "failed", "error_msg": "Timeout connecting to API Gateway after 3 retries", "duration_seconds": 24.5, "timestamp": get_ts(40)},
        {"run_id": "run_users_002", "table_name": "users", "state": "success", "duration_seconds": 13.0, "timestamp": get_ts(20)},
        {"run_id": "run_users_001", "table_name": "users", "state": "quarantined", "duration_seconds": 14.8, "timestamp": get_ts(10)},

        # Products Table (Database Sync)
        {"run_id": "run_prod_005", "table_name": "products", "state": "success", "duration_seconds": 8.4, "timestamp": get_ts(105)},
        {"run_id": "run_prod_004", "table_name": "products", "state": "success", "duration_seconds": 7.9, "timestamp": get_ts(85)},
        {"run_id": "run_prod_003", "table_name": "products", "state": "success", "duration_seconds": 9.1, "timestamp": get_ts(65)},
        {"run_id": "run_prod_002", "table_name": "products", "state": "success", "duration_seconds": 8.8, "timestamp": get_ts(45)},
        {"run_id": "run_prod_001", "table_name": "products", "state": "success", "duration_seconds": 8.6, "timestamp": get_ts(25)},

        # MBTI Table (CSV Upload)
        {"run_id": "run_mbti_003", "table_name": "mbti", "state": "success", "duration_seconds": 18.2, "timestamp": get_ts(95)},
        {"run_id": "run_mbti_002", "table_name": "mbti", "state": "success", "duration_seconds": 19.5, "timestamp": get_ts(55)},
        {"run_id": "run_mbti_001", "table_name": "mbti", "state": "success", "duration_seconds": 17.8, "timestamp": get_ts(15)},
    ]
    for doc in pipeline_runs:
        es.index(index="sdoqap_pipeline_runs", document=doc)

    print("Seeding sdoqap_quality_runs...")
    # Quality audits (records count, clean vs quarantined, score)
    quality_runs = [
        # Users Table Ingestions
        {
            "run_id": "run_users_010", "table_name": "users", "total_records": 1000, "clean_records": 1000, "quarantined_records": 0, 
            "quality_score": 100.0, "freshness_lag_hours": 0.1, "timestamp": get_ts(110), "quarantine_breakdown": {}
        },
        {
            "run_id": "run_users_009", "table_name": "users", "total_records": 1200, "clean_records": 1200, "quarantined_records": 0, 
            "quality_score": 100.0, "freshness_lag_hours": 0.1, "timestamp": get_ts(100), "quarantine_breakdown": {}
        },
        {
            "run_id": "run_users_008", "table_name": "users", "total_records": 1150, "clean_records": 1130, "quarantined_records": 20, 
            "quality_score": 98.26, "freshness_lag_hours": 0.12, "timestamp": get_ts(90), "quarantine_breakdown": {"null_primary_key": 20}
        },
        {
            "run_id": "run_users_007", "table_name": "users", "total_records": 1300, "clean_records": 1300, "quarantined_records": 0, 
            "quality_score": 100.0, "freshness_lag_hours": 0.08, "timestamp": get_ts(80), "quarantine_breakdown": {}
        },
        {
            "run_id": "run_users_006", "table_name": "users", "total_records": 1250, "clean_records": 1250, "quarantined_records": 0, 
            "quality_score": 100.0, "freshness_lag_hours": 0.09, "timestamp": get_ts(70), "quarantine_breakdown": {}
        },
        {
            "run_id": "run_users_005", "table_name": "users", "total_records": 1400, "clean_records": 1200, "quarantined_records": 200, 
            "quality_score": 85.71, "freshness_lag_hours": 0.15, "timestamp": get_ts(60), "quarantine_breakdown": {"schema_drift": 200}
        },
        {
            "run_id": "run_users_004", "table_name": "users", "total_records": 1350, "clean_records": 1350, "quarantined_records": 0, 
            "quality_score": 100.0, "freshness_lag_hours": 0.11, "timestamp": get_ts(50), "quarantine_breakdown": {}
        },
        # (run_users_003 failed connection, no quality run)
        {
            "run_id": "run_users_002", "table_name": "users", "total_records": 1420, "clean_records": 1420, "quarantined_records": 0, 
            "quality_score": 100.0, "freshness_lag_hours": 0.1, "timestamp": get_ts(20), "quarantine_breakdown": {}
        },
        {
            "run_id": "run_users_001", "table_name": "users", "total_records": 1500, "clean_records": 975, "quarantined_records": 525, 
            "quality_score": 65.0, "freshness_lag_hours": 0.18, "timestamp": get_ts(10), "quarantine_breakdown": {"invalid_email_format": 300, "null_role": 225}
        },

        # Products Table Ingestions
        {
            "run_id": "run_prod_005", "table_name": "products", "total_records": 500, "clean_records": 500, "quarantined_records": 0, 
            "quality_score": 100.0, "freshness_lag_hours": 0.5, "timestamp": get_ts(105), "quarantine_breakdown": {}
        },
        {
            "run_id": "run_prod_004", "table_name": "products", "total_records": 510, "clean_records": 508, "quarantined_records": 2, 
            "quality_score": 99.61, "freshness_lag_hours": 0.48, "timestamp": get_ts(85), "quarantine_breakdown": {"negative_price": 2}
        },
        {
            "run_id": "run_prod_003", "table_name": "products", "total_records": 505, "clean_records": 505, "quarantined_records": 0, 
            "quality_score": 100.0, "freshness_lag_hours": 0.52, "timestamp": get_ts(65), "quarantine_breakdown": {}
        },
        {
            "run_id": "run_prod_002", "table_name": "products", "total_records": 520, "clean_records": 520, "quarantined_records": 0, 
            "quality_score": 100.0, "freshness_lag_hours": 0.45, "timestamp": get_ts(45), "quarantine_breakdown": {}
        },
        {
            "run_id": "run_prod_001", "table_name": "products", "total_records": 530, "clean_records": 524, "quarantined_records": 6, 
            "quality_score": 98.87, "freshness_lag_hours": 0.5, "timestamp": get_ts(25), "quarantine_breakdown": {"duplicate_product_id": 6}
        },

        # MBTI Table Ingestions
        {
            "run_id": "run_mbti_003", "table_name": "mbti", "total_records": 8000, "clean_records": 7920, "quarantined_records": 80, 
            "quality_score": 99.0, "freshness_lag_hours": 1.2, "timestamp": get_ts(95), "quarantine_breakdown": {"invalid_mbti_label": 80}
        },
        {
            "run_id": "run_mbti_002", "table_name": "mbti", "total_records": 8100, "clean_records": 8050, "quarantined_records": 50, 
            "quality_score": 99.38, "freshness_lag_hours": 1.15, "timestamp": get_ts(55), "quarantine_breakdown": {"invalid_mbti_label": 50}
        },
        {
            "run_id": "run_mbti_001", "table_name": "mbti", "total_records": 8200, "clean_records": 7790, "quarantined_records": 410, 
            "quality_score": 95.0, "freshness_lag_hours": 1.3, "timestamp": get_ts(15), "quarantine_breakdown": {"invalid_mbti_label": 320, "null_text": 90}
        },
    ]
    for doc in quality_runs:
        es.index(index="sdoqap_quality_runs", document=doc)

    print("Seeding sdoqap_schema_drifts...")
    # Schema drifts detected
    schema_drifts = [
        {
            "run_id": "run_users_005", "table_name": "users", "timestamp": get_ts(60),
            "drift_details": {
                "role": {"expected": "IntegerType", "actual": "StringType", "error": "type mismatch"},
                "status": {"expected": "IntegerType", "actual": "DoubleType", "error": "type mismatch"}
            }
        }
    ]
    for doc in schema_drifts:
        es.index(index="sdoqap_schema_drifts", document=doc)

    print("Seeding sdoqap_lineage_runs...")
    # Data lineages
    lineage_runs = [
        {
            "run_id": "run_users_001", "source_table": "raw-users", "target_table": "active-users",
            "source_path": "hdfs://namenode:9000/data/raw/users", "target_path": "hdfs://namenode:9000/data/active/users",
            "quarantine_path": "hdfs://namenode:9000/data/quarantine/users", "timestamp": get_ts(10)
        },
        {
            "run_id": "run_prod_001", "source_table": "raw-products", "target_table": "active-products",
            "source_path": "hdfs://namenode:9000/data/raw/products", "target_path": "hdfs://namenode:9000/data/active/products",
            "quarantine_path": "hdfs://namenode:9000/data/quarantine/products", "timestamp": get_ts(25)
        },
        {
            "run_id": "run_mbti_001", "source_table": "raw-mbti", "target_table": "active-mbti",
            "source_path": "hdfs://namenode:9000/data/raw/mbti", "target_path": "hdfs://namenode:9000/data/active/mbti",
            "quarantine_path": "hdfs://namenode:9000/data/quarantine/mbti", "timestamp": get_ts(15)
        }
    ]
    for doc in lineage_runs:
        es.index(index="sdoqap_lineage_runs", document=doc)

    print("Seeding completed successfully!")

if __name__ == "__main__":
    seed()
