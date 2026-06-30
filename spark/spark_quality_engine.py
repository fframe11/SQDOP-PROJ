import sys
import os
import json
from datetime import datetime
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
HDFS_URL = "hdfs://namenode:9000"

def get_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

def log_to_elasticsearch(index_name, doc):
    """Writes metadata document directly to Elasticsearch."""
    url = f"{ELASTICSEARCH_URL}/{index_name}/_doc"
    headers = {"Content-Type": "application/json"}
    try:
        res = requests.post(url, headers=headers, data=json.dumps(doc), timeout=10)
        res.raise_for_status()
        print(f"Metrics logged to Elasticsearch index '{index_name}' successfully.")
    except Exception as e:
        print(f"Error logging to Elasticsearch: {e}")

N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "http://sdoqap-n8n:5678/webhook/sdoqap-alerts")

def send_n8n_alert(title, message, severity="warning"):
    """Sends an alert to n8n webhook."""
    payload = {
        "title": title,
        "message": message,
        "severity": severity,
        "timestamp": datetime.utcnow().isoformat()
    }
    try:
        res = requests.post(N8N_WEBHOOK_URL, headers={"Content-Type": "application/json"}, json=payload, timeout=5)
        if res.status_code == 200:
            print(f"Alert sent to n8n successfully: {title}")
        else:
            print(f"Failed to send alert to n8n. Status: {res.status_code}")
    except Exception as e:
        print(f"Error sending alert to n8n: {e}")

def run_quality_check(table_name, primary_key, date_column, schema_spec):
    spark = get_spark_session(f"SDOQAP_QualityCheck_{table_name}")
    run_id = f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"

    raw_path = f"{HDFS_URL}/data/raw/{table_name}"
    active_path = f"{HDFS_URL}/data/active/{table_name}"
    quarantine_path = f"{HDFS_URL}/data/quarantine/{table_name}"

    print(f"Starting quality check for '{table_name}' in run {run_id}...")

    try:
        # Load raw data from HDFS as strings to avoid inference issues (Bug 6)
        print(f"Reading raw CSV data from {raw_path}")
        df = spark.read.option("header", "true").csv(raw_path)

        # Cast columns according to schema_spec
        from pyspark.sql.types import IntegerType, DoubleType, TimestampType, StringType
        for col_name, type_str in schema_spec.items():
            if col_name in df.columns:
                if type_str == "IntegerType":
                    df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))
                elif type_str == "DoubleType":
                    df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
                elif type_str == "TimestampType":
                    df = df.withColumn(col_name, F.col(col_name).cast(TimestampType()))

        # Partition data into smaller chunks to enable parallel processing in small batches
        df = df.repartition(10)

        # Adjust shuffle partitions dynamically
        spark.conf.set("spark.sql.shuffle.partitions", "2")
    except Exception as e:
        print(f"Error reading raw data path {raw_path}: {e}")
        # Log run failure
        log_to_elasticsearch("sdoqap_pipeline_runs", {
            "run_id": run_id,
            "table_name": table_name,
            "state": "failed",
            "error_msg": str(e),
            "timestamp": datetime.utcnow().isoformat()
        })
        spark.stop()
        sys.exit(1)

    # 1. AUTO SCHEMA EVOLUTION & DRIFT CHECK
    actual_columns = {field.name: field.dataType.__class__.__name__ for field in df.schema.fields}
    drift_detected = False
    drift_details = {}

    # 1.1 Check missing columns in DF (API didn't send them)
    for col_name, expected_type in schema_spec.items():
        if col_name not in actual_columns:
            drift_detected = True
            drift_details[col_name] = {"error": "missing_column", "action": "auto_filled_null"}
            # Auto fill with null casted to expected type
            spark_type_map = {
                "IntegerType": "integer",
                "DoubleType": "double",
                "TimestampType": "timestamp",
                "StringType": "string"
            }
            sql_type = spark_type_map.get(expected_type, "string")
            df = df.withColumn(col_name, F.lit(None).cast(sql_type))
            actual_columns[col_name] = expected_type
        elif actual_columns[col_name] != expected_type:
            drift_detected = True
            drift_details[col_name] = {"error": "type_mismatch", "expected": expected_type, "actual": actual_columns[col_name], "action": "coerced_to_string"}
            df = df.withColumn(col_name, F.col(col_name).cast("string"))
            schema_spec[col_name] = "StringType"
            actual_columns[col_name] = "StringType"

    # 1.2 Check new columns in DF (API sent extra fields)
    for col_name, actual_type in list(actual_columns.items()):
        if col_name not in schema_spec:
            drift_detected = True
            drift_details[col_name] = {"error": "new_column", "actual": actual_type, "action": "auto_added"}
            schema_spec[col_name] = actual_type

    if drift_detected:
        print(f"Schema drift detected and auto-evolved: {drift_details}")
        log_to_elasticsearch("sdoqap_schema_drifts", {
            "run_id": run_id,
            "table_name": table_name,
            "registered_schema": schema_spec,
            "detected_schema": actual_columns,
            "drift_details": drift_details,
            "timestamp": datetime.utcnow().isoformat()
        })

        # Save updated schema back to registry
        try:
            registry_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema_registry.json")
            if os.path.exists(registry_path):
                with open(registry_path, "r") as f:
                    registry = json.load(f)
                if table_name in registry:
                    registry[table_name]["schema_spec"] = schema_spec
                    with open(registry_path, "w") as f:
                        json.dump(registry, f, indent=4)
                    print(f"Schema registry updated dynamically for '{table_name}'.")
        except Exception as e:
            print(f"Failed to update schema registry: {e}")

        send_n8n_alert(
            title=f"⚠️ Schema Evolution Triggered: {table_name}",
            message=f"Run ID: {run_id}\nChanges: {json.dumps(drift_details)}",
            severity="warning"
        )

    # 2. DATA VALIDATION (Row-level Quality check)
    df_with_status = df.withColumn("is_invalid", F.col(primary_key).isNull()) \
                       .withColumn("reject_reason", F.when(F.col("is_invalid"), F.lit("missing_primary_key")).otherwise(F.lit("")))

    if date_column and date_column in df.columns:
        df_with_status = df_with_status.withColumn(
            "is_invalid",
            F.col("is_invalid") | F.col(date_column).isNull()
        ).withColumn(
            "reject_reason",
            F.when(F.col(date_column).isNull() & F.col("is_invalid"), F.concat(F.col("reject_reason"), F.lit("; missing_date")))
             .when(F.col(date_column).isNull(), F.lit("missing_date"))
             .otherwise(F.col("reject_reason"))
        )

    # Filter Valid vs Invalid records (handling NULLs in is_invalid)
    invalid_df = df_with_status.filter(F.col("is_invalid") | F.col("is_invalid").isNull())
    valid_df = df_with_status.filter(~F.col("is_invalid") & F.col("is_invalid").isNotNull())

    # Check duplicates on valid records (incremental deduplication)
    from pyspark.sql.window import Window
    pk_cols = [primary_key] if isinstance(primary_key, str) else primary_key

    # Bug 1: Deterministic order for deduplication
    order_cols = [F.col(date_column).desc()] if date_column and date_column in df.columns else [F.monotonically_increasing_id()]
    window_spec = Window.partitionBy(*pk_cols).orderBy(*order_cols)

    valid_dedup = valid_df.withColumn("row_num", F.row_number().over(window_spec))

    clean_df = valid_dedup.filter(F.col("row_num") == 1).drop("is_invalid", "reject_reason", "row_num")
    duplicate_df = valid_dedup.filter(F.col("row_num") > 1) \
                              .drop("row_num") \
                              .withColumn("reject_reason", F.lit("duplicate_records"))

    # Combine all quarantined records
    all_quarantined = invalid_df.unionByName(duplicate_df, allowMissingColumns=True)

    # Add run_id partition structure to quarantined parquet
    all_quarantined_write = all_quarantined.withColumn("run_id", F.lit(run_id)) \
                                           .withColumn("rejected_at", F.current_timestamp())

    # Cache and count before writing to avoid re-reading and partial failures
    clean_df.cache()
    all_quarantined_write.cache()

    clean_count = clean_df.count()
    quarantine_count = all_quarantined_write.count()
    total_records = clean_count + quarantine_count

    print("Writing validated datasets to HDFS...")
    # Use append to avoid partial overwrite issues, and because run_id is unique per run
    clean_df.write.mode("append").parquet(f"{active_path}/run_id={run_id}")
    all_quarantined_write.write.mode("append").parquet(f"{quarantine_path}/run_id={run_id}")

    # Class Balance / Data Distribution calculation
    class_balance = {}
    group_col = None
    if clean_count > 0:
        try:
            clean_run_df = spark.read.parquet(f"{active_path}/run_id={run_id}")
            # 1. Search for common categorical column names
            common_categorical_cols = ["label", "role", "category", "type", "status", "class", "gender", "country", "state", "sector", "transaction_type"]
            for col_name in common_categorical_cols:
                if col_name in clean_run_df.columns:
                    group_col = col_name
                    break

            # 2. Cardinality-based automatic String column fallback (detects 2 to 25 categories)
            if not group_col:
                string_cols = [f.name for f in clean_run_df.schema.fields if f.dataType.__class__.__name__ == "StringType"]
                for col_name in string_cols:
                    distinct_count = clean_run_df.select(col_name).distinct().count()
                    if 1 < distinct_count <= 25:
                        group_col = col_name
                        break

            if group_col:
                balance_df = clean_run_df.groupBy(group_col).count().collect()
                class_balance = {str(row[group_col] if row[group_col] is not None else "NULL"): row["count"] for row in balance_df}
                print(f"Data Distribution for {table_name} grouped by '{group_col}': {class_balance}")
        except Exception as e:
            print(f"Error computing data distribution: {e}")

    # Quarantine Reasons Breakdown
    quarantine_breakdown = {}
    if quarantine_count > 0:
        try:
            quar_run_df = spark.read.parquet(f"{quarantine_path}/run_id={run_id}")
            if "reject_reason" in quar_run_df.columns:
                breakdown_df = quar_run_df.groupBy("reject_reason").count().collect()
                for row in breakdown_df:
                    reason_str = row["reject_reason"] or "unknown"
                    count = row["count"]
                    if reason_str == "":
                        reason_str = "unknown"
                    reasons = [r.strip() for r in reason_str.split(";") if r.strip()]
                    if not reasons:
                        reasons = ["unknown"]
                    for r in reasons:
                        quarantine_breakdown[r] = quarantine_breakdown.get(r, 0) + count
                print(f"Quarantine Breakdown: {quarantine_breakdown}")
        except Exception as e:
            print(f"Error computing quarantine breakdown: {e}")

    # 3. FRESHNESS LAG
    max_lag_hours = 0.0
    if date_column and clean_count > 0:
        try:
            clean_run_df = spark.read.parquet(f"{active_path}/run_id={run_id}")
            if date_column in clean_run_df.columns:
                max_ts = clean_run_df.select(F.max(date_column)).collect()[0][0]
                if max_ts:
                    if isinstance(max_ts, str):
                        try:
                            max_ts = datetime.fromisoformat(max_ts.replace("Z", "+00:00"))
                        except ValueError:
                            parsed = False
                            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y/%m/%d %H:%M:%S", "%d-%m-%Y %H:%M:%S"):
                                try:
                                    max_ts = datetime.strptime(max_ts, fmt)
                                    parsed = True
                                    break
                                except ValueError:
                                    continue
                            if not parsed:
                                raise ValueError(f"Unsupported timestamp format: {max_ts}")
                    from datetime import timezone
                    now_utc = datetime.now(timezone.utc)
                    if max_ts.tzinfo is None:
                        max_ts = max_ts.replace(tzinfo=timezone.utc)
                    lag_seconds = (now_utc - max_ts).total_seconds()
                    max_lag_hours = max(0.0, lag_seconds / 3600.0)
        except Exception as e:
            print(f"Error computing freshness: {e}")

    # 4. QUALITY SCORE
    passed_tests = clean_count
    total_tests = total_records
    quality_score = 100.0 if total_tests == 0 else (passed_tests / total_tests) * 100.0

    if quality_score < 90.0:
        send_n8n_alert(
            title=f"🚨 Critical Data Quality Drop: {table_name}",
            message=f"Run ID: {run_id}\nQuality Score: {quality_score:.1f}%\nQuarantined: {quarantine_count} rows\nClean: {clean_count} rows",
            severity="critical"
        )

    # Log operational run metrics to Elasticsearch
    quality_run_doc = {
        "run_id": run_id,
        "table_name": table_name,
        "total_records": total_records,
        "clean_records": clean_count,
        "quarantined_records": quarantine_count,
        "quality_score": quality_score,
        "freshness_lag_hours": max_lag_hours,
        "timestamp": datetime.utcnow().isoformat()
    }
    if class_balance:
        quality_run_doc["class_balance"] = class_balance
        quality_run_doc["class_balance_column"] = group_col
    if quarantine_breakdown:
        quality_run_doc["quarantine_breakdown"] = quarantine_breakdown

    log_to_elasticsearch("sdoqap_quality_runs", quality_run_doc)

    log_to_elasticsearch("sdoqap_lineage_runs", {
        "run_id": run_id,
        "source_table": f"raw-{table_name}",
        "target_table": f"active-{table_name}",
        "source_path": raw_path,
        "target_path": active_path,
        "quarantine_path": quarantine_path,
        "timestamp": datetime.utcnow().isoformat()
    })

    log_to_elasticsearch("sdoqap_pipeline_runs", {
        "run_id": run_id,
        "table_name": table_name,
        "state": "success" if quality_score >= 90 else "warnings",
        "timestamp": datetime.utcnow().isoformat()
    })

    print(f"Quality validation completed. Quality Score: {quality_score:.2f}%")
    spark.stop()

if __name__ == "__main__":
    # If table argument is passed via spark-submit, run that table, else run default
    target_table = "users"
    if len(sys.argv) > 1:
        target_table = sys.argv[1]

    registry_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema_registry.json")

    # Standard built-in registry configurations
    default_registry = {
        "mbti": {
            "primary_key": ["author", "text"],
            "date_column": None,
            "schema_spec": {
                "author": "StringType",
                "text": "StringType",
                "label": "StringType",
                "EI": "StringType",
                "NS": "StringType",
                "TF": "StringType",
                "JP": "StringType"
            }
        },
        "users": {
            "primary_key": "id",
            "date_column": "updated_at",
            "schema_spec": {
                "id": "IntegerType",
                "username": "StringType",
                "email": "StringType",
                "role": "StringType",
                "created_at": "TimestampType",
                "updated_at": "TimestampType"
            }
        }
    }

    # Load registry if exists, otherwise create it with defaults
    registry = default_registry.copy()
    if os.path.exists(registry_path):
        try:
            with open(registry_path, "r") as f:
                loaded = json.load(f)
                registry.update(loaded)
        except Exception as e:
            print(f"Error loading schema registry: {e}")

    # If the target table is in the registry, run it
    if target_table in registry:
        spec = registry[target_table]
        run_quality_check(target_table, spec["primary_key"], spec["date_column"], spec["schema_spec"])
    else:
        print(f"Table '{target_table}' not found in registry. Inferring configuration dynamically...")
        # Resolve configuration dynamically using a temporary Spark session
        temp_spark = SparkSession.builder \
            .appName(f"SDOQAP_Infer_{target_table}") \
            .master("local[*]") \
            .getOrCreate()

        raw_path = f"hdfs://namenode:9000/data/raw/{target_table}"
        try:
            # Read sample to infer schema
            df_temp = temp_spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)

            # Map column names and types
            schema_spec = {field.name: field.dataType.__class__.__name__ for field in df_temp.schema.fields}
            columns = list(schema_spec.keys())

            # 1. Infer Primary Key
            primary_key = None
            # Look for exact match first
            for col in columns:
                if col.lower() == "id" or col.lower() == f"{target_table}_id" or col.lower() == f"{target_table}id":
                    primary_key = col
                    break
            # Look for sub-string match
            if not primary_key:
                for col in columns:
                    if "id" in col.lower():
                        primary_key = col
                        break
            # Default to the first column
            if not primary_key and columns:
                primary_key = columns[0]

            # 2. Infer Date Column
            date_column = None
            for col in columns:
                if col.lower() in ["updated_at", "created_at", "timestamp", "date", "time"]:
                    date_column = col
                    break
            if not date_column:
                for col in columns:
                    if "date" in col.lower() or "time" in col.lower() or "timestamp" in col.lower():
                        date_column = col
                        break

            print(f"Inferred configuration for '{target_table}':")
            print(f"  Primary Key: {primary_key}")
            print(f"  Date Column: {date_column}")
            print(f"  Schema Spec: {schema_spec}")

            # Update registry dictionary
            registry[target_table] = {
                "primary_key": primary_key,
                "date_column": date_column,
                "schema_spec": schema_spec
            }

            # Write updated registry back to local file
            try:
                with open(registry_path, "w") as f:
                    json.dump(registry, f, indent=4)
                print(f"Saved inferred configuration for '{target_table}' to schema registry.")
            except Exception as w_err:
                print(f"Could not save registry file: {w_err}")

            # Do NOT stop temp_spark here, as get_spark_session will reuse the existing active session in local mode
            # temp_spark.stop()

            # Now execute quality check with inferred schema
            run_quality_check(target_table, primary_key, date_column, schema_spec)

        except Exception as infer_err:
            print(f"Failed to infer schema dynamically for '{target_table}': {infer_err}")
            # Write a failed run document to ES
            import requests
            run_id = f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            url = f"http://elasticsearch:9200/sdoqap_pipeline_runs/_doc"
            doc = {
                "run_id": run_id,
                "table_name": target_table,
                "state": "failed",
                "error_msg": f"Failed to infer schema: {str(infer_err)}",
                "timestamp": datetime.utcnow().isoformat()
            }
            try:
                requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(doc), timeout=10)
            except Exception:
                pass
            temp_spark.stop()
            sys.exit(1)
