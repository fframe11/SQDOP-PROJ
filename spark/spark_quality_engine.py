import sys
import argparse
import os

def load_env_file():
    # Dynamically search and load .env from project root if available
    current_dir = os.path.dirname(os.path.abspath(__file__))
    for _ in range(3):
        env_path = os.path.join(current_dir, ".env")
        if os.path.exists(env_path):
            try:
                with open(env_path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            k, v = line.split("=", 1)
                            os.environ.setdefault(k.strip(), v.strip())
            except Exception:
                pass
            break
        current_dir = os.path.dirname(current_dir)

load_env_file()

try:
    from api.app.api.config import get_required_env
except ModuleNotFoundError:
    # Fallback implementation when the 'api' package is unavailable (e.g., inside Docker container)
    def get_required_env(name: str) -> str:
        """Retrieve required environment variable or raise a clear error."""
        import os
        value = os.getenv(name)
        if value is None:
            raise RuntimeError(f"Missing required environment variable '{name}'. Set it in the environment.")
        return value

# Set default env values if not provided by OS environment or loaded .env file
os.environ.setdefault('ELASTICSEARCH_USER', 'elastic')
os.environ.setdefault('ELASTICSEARCH_PASSWORD', 'sdoqap_secure')
os.environ.setdefault('ELASTICSEARCH_HOST', 'localhost')
os.environ.setdefault('ELASTICSEARCH_PORT', '9200')
os.environ.setdefault('HDFS_URL', 'hdfs://namenode:9000')
os.environ.setdefault('N8N_WEBHOOK_URL', 'http://localhost:5678')
import json
import yaml
import subprocess

# Load HDFS configuration from yaml if available
_config_path = os.path.join(os.path.dirname(__file__), "config", "hdfs_config.yaml")
if os.path.isfile(_config_path):
    try:
        with open(_config_path) as f:
            _cfg = yaml.safe_load(f)
        HDFS_URL = _cfg.get("hdfs", {}).get("url", HDFS_URL)
    except Exception as e:
        print(f"Failed to load HDFS config: {e}")
from datetime import datetime, timedelta, timezone
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_elasticsearch_url():
    # Prefer full URL if provided via environment
    es_url = os.getenv("ELASTICSEARCH_URL")
    if es_url:
        return es_url
    # Otherwise construct from components, using defaults where appropriate
    es_user = get_required_env("ELASTICSEARCH_USER")
    es_pass = get_required_env("ELASTICSEARCH_PASSWORD")
    es_host = os.getenv("ELASTICSEARCH_HOST", "localhost")
    es_port = os.getenv("ELASTICSEARCH_PORT", "9200")
    return f"http://{es_user}:{es_pass}@{es_host}:{es_port}"

ELASTICSEARCH_URL = get_elasticsearch_url()
HDFS_URL = get_required_env("HDFS_URL")

def normalize_name(name):
    import re
    if not name:
        return ""
    return re.sub(r'[\s\-_]', '', name).lower()

def clean_column_name(name):
    import re
    if not name:
        return ""
    cleaned = re.sub(r'[ ,;{}()\n\t=]', '_', name)
    cleaned = re.sub(r'_{2,}', '_', cleaned)
    return cleaned.strip('_')

def get_spark_session(app_name):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "1")
        .config("spark.dynamicAllocation.maxExecutors", "4")
        .config("spark.hadoop.fs.defaultFS", HDFS_URL)
        .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    if "SPARK_HOME" not in os.environ:
        builder = builder.master("local[*]")
        builder = builder.config("spark.driver.host", "127.0.0.1") \
                         .config("spark.driver.bindAddress", "127.0.0.1")
                         
    return builder.getOrCreate()

def log_to_elasticsearch(index_name, doc):
    """Writes metadata document directly to Elasticsearch."""
    url = f"{ELASTICSEARCH_URL}/{index_name}/_doc"
    headers = {"Content-Type": "application/json"}
    
    # Parse basic auth from URL if present (e.g., http://user:pass@host:port)
    auth = None
    from urllib.parse import urlparse
    parsed = urlparse(ELASTICSEARCH_URL)
    if parsed.username and parsed.password:
        auth = (parsed.username, parsed.password)
        # Remove credentials from the URL used for the request to avoid exposure
        base_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
        url = f"{base_url}/{index_name}/_doc"

    try:
        res = requests.post(url, headers=headers, auth=auth, data=json.dumps(doc), timeout=10)
        res.raise_for_status()
        print(f"Metrics logged to Elasticsearch index '{index_name}' successfully.")
    except Exception as e:
        print(f"Error logging to Elasticsearch: {e}")

N8N_WEBHOOK_URL = get_required_env("N8N_WEBHOOK_URL")

# ─── FIX 2A: Distributed Lock via Elasticsearch with Self-Healing ────────────────
def acquire_lock(table_name: str, run_id: str, force: bool = False) -> bool:
    """Atomically lock a table before Spark starts. Uses ES op_type=create.
    If lock exists (409 Conflict), checks expires_at. If expired, force overwrites it
    using Optimistic Concurrency Control (seq_no & primary_term) to ensure atomicity."""
    from urllib.parse import urlparse
    from datetime import timezone
    parsed = urlparse(ELASTICSEARCH_URL)
    auth = (parsed.username, parsed.password) if parsed.username else None
    base_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
    lock_doc = {
        "table_name": table_name,
        "run_id": run_id,
        "status": "RUNNING",
        "locked_at": datetime.now(timezone.utc).isoformat(),
        "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat()
    }
    try:
        url = f"{base_url}/sdoqap_run_locks/_doc/{table_name}?op_type=create"
        res = requests.put(url, json=lock_doc, auth=auth, timeout=5)
        if res.status_code == 201:
            print(f"[LOCK] Acquired lock for '{table_name}' (run_id={run_id})")
            return True
        elif res.status_code == 409:
            # Lock already exists
            if force:
                # Force overwrite the existing lock
                print(f"[LOCK] Force flag enabled. Overwriting existing lock for '{table_name}'.")
                lock_url = f"{base_url}/sdoqap_run_locks/_doc/{table_name}"
                get_res = requests.get(lock_url, auth=auth, timeout=5)
                if get_res.status_code == 200:
                    existing = get_res.json()
                    seq_no = existing.get("_seq_no")
                    primary_term = existing.get("_primary_term")
                    occ_url = f"{lock_url}?if_seq_no={seq_no}&if_primary_term={primary_term}"
                    res = requests.put(occ_url, json=lock_doc, auth=auth, timeout=5)
                    if res.status_code in [200, 201]:
                        print(f"[LOCK] Forced lock acquisition for '{table_name}'.")
                        return True
                # If unable to force, fall through to abort
                print(f"[LOCK] Failed to force lock for '{table_name}'. Aborting.")
                return False
            else:
                # Normal behavior: check expiration
                lock_url = f"{base_url}/sdoqap_run_locks/_doc/{table_name}"
                get_res = requests.get(lock_url, auth=auth, timeout=5)
                if get_res.status_code == 200:
                    existing = get_res.json()
                    expires_at_str = existing.get("_source", {}).get("expires_at")
                    if expires_at_str:
                        expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
                        if datetime.now(timezone.utc) > expires_at:
                            print(f"[LOCK] Previous lock for '{table_name}' expired. Overwriting...")
                            seq_no = existing.get("_seq_no")
                            primary_term = existing.get("_primary_term")
                            occ_url = f"{lock_url}?if_seq_no={seq_no}&if_primary_term={primary_term}"
                            res = requests.put(occ_url, json=lock_doc, auth=auth, timeout=5)
                            if res.status_code in [200, 201]:
                                print(f"[LOCK] Re-acquired expired lock for '{table_name}'.")
                                return True
                print(f"[LOCK] Table '{table_name}' already locked. Aborting duplicate run.")
                return False
        return False
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as conn_err:
        print(f"[LOCK] Elasticsearch is offline ({conn_err}). Bypassing lock as a fail-safe to prevent pipeline disruption.")
        return True
    except Exception as e:
        print(f"[LOCK] Lock acquisition failed: {e}. Aborting (fail-safe).")
        return False

def release_lock(table_name: str):
    """Release the distributed lock for a table after job completes or fails."""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(ELASTICSEARCH_URL)
        auth = (parsed.username, parsed.password) if parsed.username else None
        base_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
        requests.delete(f"{base_url}/sdoqap_run_locks/_doc/{table_name}", auth=auth, timeout=5)
        print(f"[LOCK] Released lock for '{table_name}'.")
    except Exception as e:
        print(f"[LOCK] Failed to release lock: {e}")

# ─── FIX 3B: Database-Backed Schema Registry via Elasticsearch ───────────────
def load_expected_schema(table_name: str) -> dict:
    """Loads schema spec, primary key, and date column for a table from Elasticsearch index sdoqap_schema_registry.
    Falls back to default_registry if Elasticsearch doesn't have it."""
    from urllib.parse import urlparse
    parsed = urlparse(ELASTICSEARCH_URL)
    auth = (parsed.username, parsed.password) if parsed.username else None
    base_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
    
    url = f"{base_url}/sdoqap_schema_registry/_doc/{table_name}"
    try:
        res = requests.get(url, auth=auth, timeout=5)
        if res.status_code == 200:
            doc = res.json().get("_source", {})
            print(f"[REGISTRY] Loaded schema spec for '{table_name}' from Elasticsearch sdoqap_schema_registry.")
            return doc
    except Exception as e:
        print(f"[REGISTRY] Failed to read from Elasticsearch: {e}. Falling back to default registry.")
        
    # Fallback to default in-memory registry if not in ES
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
        },
        "benchmark_test": {
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
    
    normalized_target = normalize_name(table_name)
    for tbl_name, spec in default_registry.items():
        if normalize_name(tbl_name) == normalized_target:
            print(f"[REGISTRY] Found fallback matching registry spec for '{tbl_name}'")
            return spec
    return None

def save_registry_to_es(table_name: str, spec: dict) -> bool:
    """Saves schema specification to Elasticsearch index sdoqap_schema_registry."""
    from urllib.parse import urlparse
    parsed = urlparse(ELASTICSEARCH_URL)
    auth = (parsed.username, parsed.password) if parsed.username else None
    base_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
    url = f"{base_url}/sdoqap_schema_registry/_doc/{table_name}"
    try:
        res = requests.put(url, json=spec, auth=auth, headers={"Content-Type": "application/json"}, timeout=5)
        if res.status_code in [200, 201]:
            print(f"[REGISTRY] Saved schema spec for '{table_name}' to Elasticsearch sdoqap_schema_registry.")
            return True
    except Exception as e:
        print(f"[REGISTRY] Failed to save schema spec to Elasticsearch: {e}")
    return False

def auto_evolve_schema_registry(table_name: str, proposed_schema: dict) -> bool:
    """Auto-evolves the schema registry in ES and local schema_registry.json.
    Called when drift is determined to be safe (only new columns added).
    """
    # 1. Fetch current registry doc from ES (or construct standard fallback)
    try:
        from urllib.parse import urlparse
        parsed = urlparse(ELASTICSEARCH_URL)
        auth = (parsed.username, parsed.password) if parsed.username else None
        base_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
        
        url = f"{base_url}/sdoqap_schema_registry/_doc/{table_name}"
        res = requests.get(url, auth=auth, timeout=5)
        if res.status_code == 200:
            reg_doc = res.json().get("_source", {})
        else:
            reg_doc = {
                "primary_key": "id",
                "date_column": None,
                "schema_spec": {}
            }
        
        # Merge proposed schema spec
        reg_doc["schema_spec"] = proposed_schema
        
        # Write back to ES
        requests.put(url, json=reg_doc, auth=auth, headers={"Content-Type": "application/json"}, timeout=5)
        print(f"[AUTO-EVOLVE] Successfully auto-evolved sdoqap_schema_registry in ES for '{table_name}'.")
        
        # 2. Update local schema_registry.json
        local_registry_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema_registry.json")
        if os.path.exists(local_registry_path):
            with open(local_registry_path, "r", encoding="utf-8") as f:
                local_reg = json.load(f)
            
            table_entry = local_reg.setdefault(table_name, {
                "primary_key": reg_doc.get("primary_key", "id"),
                "date_column": reg_doc.get("date_column"),
                "schema_spec": {}
            })
            table_entry["schema_spec"] = proposed_schema
            
            # Save backup
            with open(local_registry_path + ".bak", "w", encoding="utf-8") as f:
                json.dump(local_reg, f, indent=4)
                
            with open(local_registry_path, "w", encoding="utf-8") as f:
                json.dump(local_reg, f, indent=4)
            print(f"[AUTO-EVOLVE] Successfully updated local schema_registry.json for '{table_name}'.")
            return True
            
    except Exception as e:
        print(f"[AUTO-EVOLVE] Failed to auto-evolve schema: {e}")
    return False

# ─── FIX 3A: Config-Driven Rules Engine ───────────────────────────────────────
def load_rules_config(table_name: str) -> dict:
    """Load validation rules from Elasticsearch index sdoqap_rules_registry first.
    Falls back to rules_config.json on disk if ES is unreachable or index does not exist.
    """
    from urllib.parse import urlparse
    parsed = urlparse(ELASTICSEARCH_URL)
    auth = (parsed.username, parsed.password) if parsed.username else None
    base_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
    
    # 1. Try reading from ES
    es_default = {}
    es_table = {}
    es_read_success = False
    
    try:
        # Read default config
        url_default = f"{base_url}/sdoqap_rules_registry/_doc/_default"
        res_default = requests.get(url_default, auth=auth, timeout=3)
        if res_default.status_code == 200:
            es_default = res_default.json().get("_source", {})
            es_read_success = True
            
        # Read table-specific overrides
        url_table = f"{base_url}/sdoqap_rules_registry/_doc/{table_name}"
        res_table = requests.get(url_table, auth=auth, timeout=3)
        if res_table.status_code == 200:
            es_table = res_table.json().get("_source", {})
            es_read_success = True
            
        if es_read_success:
            print(f"[RULES] Successfully loaded configuration for '{table_name}' from Elasticsearch sdoqap_rules_registry.")
            return _merge_configs_dict(es_default, es_table)
    except Exception as e:
        print(f"[RULES] Failed to read rules from Elasticsearch: {e}. Falling back to rules_config.json on disk.")

    # 2. Fallback to local file config
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rules_config.json")
    if not os.path.exists(config_path):
        return {"quality_score_threshold": 90.0, "freshness_threshold_hours": 48}
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            all_rules = json.load(f)
        default = all_rules.get("_default", {})
        table_rules = all_rules.get(table_name, {})
        return _merge_configs_dict(default, table_rules)
    except Exception as e:
        print(f"[RULES] Failed to load rules_config.json: {e}. Using defaults.")
        return {"quality_score_threshold": 90.0, "freshness_threshold_hours": 48}

def _merge_configs_dict(default: dict, table_rules: dict) -> dict:
    """Helper to deep merge table-specific configs override default configs."""
    merged = {}
    for key in set(list(default.keys()) + list(table_rules.keys())):
        default_val = default.get(key)
        table_val = table_rules.get(key)
        if table_val is not None:
            if isinstance(default_val, dict) and isinstance(table_val, dict):
                merged[key] = {**default_val, **table_val}
            else:
                merged[key] = table_val
        elif default_val is not None:
            merged[key] = default_val
    return merged


def resolve_rule_value(rule_entry, fallback):
    """Extract the effective scalar value from a rule entry.
    Supports both v1 flat format (e.g., 90.0) and v2 nested format (e.g., {'mode': 'adaptive', 'base_value': 90.0}).
    Returns the base_value from nested format, or the raw value from flat format."""
    if isinstance(rule_entry, dict):
        return rule_entry.get("base_value", fallback)
    if rule_entry is not None:
        return rule_entry
    return fallback

FAST_TRACK_MB_LIMIT = 50

def detect_track(spark, table_name: str) -> str:
    """Detect whether a table should use Fast Track or Batch Track
    based on the raw data size on HDFS. Defaults to 'batch' on any error."""
    try:
        sc = spark.sparkContext
        conf = sc._jsc.hadoopConfiguration()
        URI = sc._gateway.jvm.java.net.URI
        FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        fs = FileSystem.get(URI(HDFS_URL), conf)
        
        raw_dir = Path(f"/data/raw/{table_name}")
        if fs.exists(raw_dir):
            size_bytes = fs.getContentSummary(raw_dir).getLength()
            track = "fast" if size_bytes < FAST_TRACK_MB_LIMIT * 1024 * 1024 else "batch"
            print(f"[TRACK] Table '{table_name}' size={size_bytes//1024}KB -> {track.upper()} track")
            return track
    except Exception as e:
        print(f"[TRACK] Size detection failed: {e}. Defaulting to batch track.")
    return "batch"

def send_n8n_alert(title, message, severity="warning"):
    """Sends an alert to n8n webhook and routes to active channels."""
    # 1. Route to external platforms (Slack / LINE Notify)
    try:
        from alert_router import route_alert
        route_alert(title, message, severity)
    except Exception as e:
        print(f"Failed to route alert locally: {e}")

    # 2. Original n8n webhook alert
    payload = {
        "title": title,
        "message": message,
        "severity": severity,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    def _send():
        try:
            res = requests.post(N8N_WEBHOOK_URL, headers={"Content-Type": "application/json"}, json=payload, timeout=5)
            if res.status_code == 200:
                print(f"Alert sent to n8n successfully: {title}")
            else:
                print(f"Failed to send alert to n8n. Status: {res.status_code}")
        except Exception as e:
            print(f"Error sending alert to n8n: {e}")
            
    import threading
    t = threading.Thread(target=_send)
    t.daemon = True
    t.start()

def get_historical_stats(table_name):
    """Fetches historical quarantine rates for z-score anomaly detection."""
    url = f"{ELASTICSEARCH_URL}/sdoqap_quality_runs/_search"
    query = {
        "query": {
            "term": {
                "table_name.keyword": table_name
            }
        },
        "sort": [{"timestamp": {"order": "desc"}}],
        "size": 15
    }
    try:
        # Parse basic auth from URL if present
        auth = None
        from urllib.parse import urlparse
        parsed = urlparse(ELASTICSEARCH_URL)
        if parsed.username and parsed.password:
            auth = (parsed.username, parsed.password)
            base_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
            url = f"{base_url}/sdoqap_quality_runs/_search"

        res = requests.post(url, json=query, auth=auth, headers={"Content-Type": "application/json"}, timeout=5)
        if res.status_code == 200:
            hits = res.json().get("hits", {}).get("hits", [])
            rates = []
            for hit in hits:
                source = hit.get("_source", {})
                total = source.get("total_records", 0)
                quar = source.get("quarantined_records", 0)
                if total > 0:
                    rates.append(float(quar) / float(total))
            return rates
        return []
    except Exception as e:
        print(f"[STAT] Failed to fetch historical stats: {e}")
    return []

def lock_protector(func):
    import functools
    @functools.wraps(func)
    def wrapper(table_name, *args, **kwargs):
        try:
            return func(table_name, *args, **kwargs)
        except BaseException as e:
            print(f"[LOCK-PROTECTOR] Run failed or terminated for '{table_name}': {e}")
            try:
                release_lock(table_name)
            except Exception as le:
                print(f"[LOCK-PROTECTOR] Redundant lock release failed: {le}")
            raise e
    return wrapper

@lock_protector
def run_quality_check(table_name, primary_key, date_column, schema_spec, input_table_name=None):
    if not input_table_name:
        input_table_name = table_name

    schema_spec = {clean_column_name(k): v for k, v in schema_spec.items()}
    if isinstance(primary_key, list):
        primary_key = [clean_column_name(pk) for pk in primary_key]
    elif isinstance(primary_key, str):
        primary_key = clean_column_name(primary_key)
    if date_column:
        date_column = clean_column_name(date_column)

    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}"

    # ─── FIX 2A: Acquire distributed lock BEFORE starting Spark ───────────────
    if not acquire_lock(table_name, run_id, force=FORCE_LOCK):
        print(f"[ABORT] Duplicate run blocked for '{table_name}'. Exiting cleanly.")
        return None

    # ─── FIX 1B: Create Spark session, detect track, and configure Spark resources accordingly ────────
    spark = get_spark_session(f"SDOQAP_QualityCheck_{table_name}")
    track = detect_track(spark, input_table_name)
    if track == "fast":
        spark.conf.set("spark.sql.shuffle.partitions", "2")
    else:
        spark.conf.set("spark.sql.shuffle.partitions", "10")

    # ─── DYNAMIC RULES ENGINE: Load and adapt per-table validation rules ─────
    rules = load_rules_config(table_name)
    
    # Try to apply dynamic adaptive rules (Layer 2: Statistical Engine)
    # Falls back gracefully to base values if dynamic_rules_engine is unavailable
    try:
        from dynamic_rules_engine import apply_adaptive_rules
        rules = apply_adaptive_rules(rules, table_name, df=None, spark=None)
        print(f"[DYNAMIC RULES] Adaptive rules applied for '{table_name}'")
    except ImportError:
        print(f"[DYNAMIC RULES] dynamic_rules_engine not available, using base config")
    except Exception as dre:
        print(f"[DYNAMIC RULES] Failed to apply adaptive rules: {dre}. Using base config.")
    
    quality_threshold = resolve_rule_value(rules.get("quality_score_threshold"), 90.0)
    freshness_limit_hours = resolve_rule_value(rules.get("freshness_threshold_hours"), 48)

    # Determine raw HDFS path using FileSystem API check
    raw_path = f"{HDFS_URL}/data/raw/{table_name}"
    try:
        sc = spark.sparkContext
        conf = sc._jsc.hadoopConfiguration()
        URI = sc._gateway.jvm.java.net.URI
        FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        fs = FileSystem.get(URI(HDFS_URL), conf)
        if fs.exists(Path(f"/data/raw/{input_table_name}")):
            raw_path = f"{HDFS_URL}/data/raw/{input_table_name}"
            print(f"[PATH] Resolved raw HDFS path to input folder: {raw_path}")
        else:
            print(f"[PATH] Input folder not found. Using canonical raw HDFS path: {raw_path}")
    except Exception as e:
        print(f"[PATH] HDFS check failed: {e}. Falling back to default: {raw_path}")

    active_path = f"{HDFS_URL}/data/active/{table_name}"
    staging_path = f"{HDFS_URL}/data/staging/{table_name}/run_id={run_id}"
    quarantine_path = f"{HDFS_URL}/data/quarantine/{table_name}"

    print(f"Starting quality check for '{table_name}' in run {run_id} [{track.upper()} track]...")

    try:
        # Load raw data from HDFS as strings to avoid inference issues (Bug 6)
        print(f"Reading raw CSV data from {raw_path}")
        df = spark.read.option("header", "true").csv(raw_path)
        for col_name in df.columns:
            cleaned_col = clean_column_name(col_name)
            if col_name != cleaned_col:
                df = df.withColumnRenamed(col_name, cleaned_col)

        # Column Name Standardization (Fuzzy/Alias Mapping)
        normalized_spec = {normalize_name(k): k for k in schema_spec.keys()}
        for col_name in df.columns:
            norm_col = normalize_name(col_name)
            if norm_col in normalized_spec:
                canonical_name = normalized_spec[norm_col]
                if col_name != canonical_name:
                    print(f"[RENAME] Standardizing column name: '{col_name}' -> '{canonical_name}'")
                    df = df.withColumnRenamed(col_name, canonical_name)

        # Cast columns according to schema_spec with Smart Parser / Normalization / Type Promotion
        from pyspark.sql.types import IntegerType, DoubleType, TimestampType, StringType
        # Safe Type Promotion: IntegerType to DoubleType if non-zero decimals exist
        integer_promotion_candidates = [col_name for col_name, t_str in schema_spec.items() if t_str == "IntegerType" and col_name in df.columns]
        promoted_columns = set()
        if integer_promotion_candidates:
            agg_exprs = []
            for col_name in integer_promotion_candidates:
                non_null_col = F.coalesce(F.col(col_name), F.lit(""))
                agg_exprs.append(F.max(F.when(non_null_col.rlike(r"\.[0-9]*[1-9]+"), 1).otherwise(0)).alias(col_name))
            if agg_exprs:
                try:
                    res_row = df.agg(*agg_exprs).first()
                    if res_row:
                        for col_name in integer_promotion_candidates:
                            if res_row[col_name] == 1:
                                promoted_columns.add(col_name)
                except Exception as e:
                    print(f"[PROMOTION] Warning: Failed to scan decimals in parallel: {e}")

        for col_name, type_str in list(schema_spec.items()):
            if col_name in df.columns:
                if type_str == "IntegerType" and col_name in promoted_columns:
                    print(f"[PROMOTION] Column '{col_name}' promoted from IntegerType to DoubleType to preserve decimal precision.")
                    type_str = "DoubleType"
                    schema_spec[col_name] = "DoubleType"

                if type_str == "IntegerType":
                    # Remove currency, spaces, and commas
                    clean_col = F.regexp_replace(F.col(col_name), r"[\$,\s]", "")
                    # Cast to double first, then to integer, so float strings like "12.00" don't become null
                    df = df.withColumn(col_name, clean_col.cast("double").cast(IntegerType()))
                elif type_str == "DoubleType":
                    clean_col = F.regexp_replace(F.col(col_name), r"[\$,\s]", "")
                    df = df.withColumn(col_name, clean_col.cast(DoubleType()))
                elif type_str == "TimestampType":
                    # Support multiple common date/timestamp formats
                    date_formats = [
                        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
                        "yyyy-MM-dd'T'HH:mm:ss",
                        "yyyy-MM-dd HH:mm:ss",
                        "yyyy-MM-dd",
                        "dd/MM/yyyy",
                        "MM/dd/yyyy"
                    ]
                    parsed_ts = None
                    for fmt in date_formats:
                        ts_attempt = F.to_timestamp(F.col(col_name), fmt)
                        parsed_ts = ts_attempt if parsed_ts is None else F.coalesce(parsed_ts, ts_attempt)
                    
                    # Epoch unix timestamp support (if input is numerical string)
                    epoch_ts_ms = F.to_timestamp(F.col(col_name).cast("double") / 1000.0)
                    epoch_ts_sec = F.to_timestamp(F.col(col_name).cast("double"))
                    parsed_ts = F.coalesce(parsed_ts, epoch_ts_ms, epoch_ts_sec)
                    
                    df = df.withColumn(col_name, parsed_ts)

        # Partition data into smaller chunks to enable parallel processing in small batches
        df = df.repartition(10)
    except Exception as e:
        print(f"Error reading raw data path {raw_path}: {e}")
        log_to_elasticsearch("sdoqap_pipeline_runs", {
            "run_id": run_id,
            "table_name": table_name,
            "state": "failed",
            "error_msg": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        release_lock(table_name)  # FIX 2A: Always release lock on failure
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
            send_n8n_alert(
                title=f"🚨 CRITICAL Schema Drift: Missing Column in {table_name}",
                message=f"Column '{col_name}' is missing from source data. Auto-healed with NULLs.",
                severity="critical"
            )
        elif actual_columns[col_name] != expected_type:
            drift_detected = True
            drift_details[col_name] = {"error": "type_mismatch", "expected": expected_type, "actual": actual_columns[col_name], "action": "coerced_to_string"}
            df = df.withColumn(col_name, F.col(col_name).cast("string"))
            schema_spec[col_name] = "StringType"
            actual_columns[col_name] = "StringType"
            send_n8n_alert(
                title=f"🚨 CRITICAL Schema Drift: Type Mismatch in {table_name}",
                message=f"Column '{col_name}' changed from {expected_type} to {actual_columns[col_name]}.",
                severity="critical"
            )

    # 1.2 Check new columns in DF (API sent extra fields)
    for col_name, actual_type in list(actual_columns.items()):
        if col_name not in schema_spec:
            drift_detected = True
            drift_details[col_name] = {"error": "new_column", "actual": actual_type, "action": "auto_added"}
            schema_spec[col_name] = actual_type

    if drift_detected:
        print(f"Schema drift detected and auto-evolved: {drift_details}")
        
        # Calculate overall drift severity weight (Root Cause Fix for Binary Drift - Point 26)
        total_drift_severity = 0
        for col, details in drift_details.items():
            if details["error"] == "new_column":
                total_drift_severity += 1  # Low risk
            elif details["error"] == "missing_column":
                total_drift_severity += 5  # High risk
            elif details["error"] == "type_mismatch":
                total_drift_severity += 5  # High risk

        log_to_elasticsearch("sdoqap_schema_drifts", {
            "run_id": run_id,
            "table_name": table_name,
            "registered_schema": schema_spec,
            "detected_schema": actual_columns,
            "drift_details": drift_details,
            "drift_severity": total_drift_severity,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

        # FIX 2B: Self-Healing Schema Evolution Gate
        is_safe_drift = all(details["error"] == "new_column" for details in drift_details.values())
        if is_safe_drift:
            print(f"[SELF-HEALING] Safe schema drift detected (only new columns). Automatically evolving schema...")
            auto_evolve_schema_registry(table_name, actual_columns)
            log_to_elasticsearch("sdoqap_schema_proposals", {
                "run_id": run_id,
                "table_name": table_name,
                "current_schema": {k: v for k, v in schema_spec.items()},
                "proposed_schema": actual_columns,
                "drift_details": drift_details,
                "drift_severity": total_drift_severity,
                "status": "APPROVED",
                "proposed_at": datetime.now(timezone.utc).isoformat(),
                "proposed_by": f"spark_engine/{run_id}",
                "resolved_at": datetime.now(timezone.utc).isoformat()
            })
            print(f"[SELF-HEALING] Auto-evolved schema proposal logged as APPROVED.")
        else:
            # Dangerous drift (missing columns, type mismatches) requires manual Data Engineer approval
            log_to_elasticsearch("sdoqap_schema_proposals", {
                "run_id": run_id,
                "table_name": table_name,
                "current_schema": {k: v for k, v in schema_spec.items()},
                "proposed_schema": actual_columns,
                "drift_details": drift_details,
                "drift_severity": total_drift_severity,
                "status": "PENDING",
                "proposed_at": datetime.now(timezone.utc).isoformat(),
                "proposed_by": f"spark_engine/{run_id}"
            })
            print(f"[APPROVAL GATE] Schema drift proposal written as PENDING. Awaiting Data Engineer approval.")
            print(f"[APPROVAL GATE] schema_registry.json NOT modified. Review at /api/v1/schema/proposals")

            send_n8n_alert(
                title=f"⚠️ Schema Change PENDING Approval: {table_name}",
                message=f"Run ID: {run_id}\nChanges: {json.dumps(drift_details)}\nSeverity: {total_drift_severity}\nAction Required: Review at /api/v1/schema/proposals",
                severity="warning"
            )

    # 2. AUTO-CLEANSING & HEALING (Self-Healing Data Pipeline - Correct Enterprise Logic)
    # We do NOT generate fake values (UUIDs/Timestamps) for structural keys (PKs/Dates) as it violates data integrity.
    # We only perform safe deduplication and format-level normalization.
    auto_clean = rules.get("auto_clean", True)
    remediation_logs = []
    
    if auto_clean:
        print("[AUTO-CLEAN] Starting self-healing preprocessing...")
        # 2.1 Safe Deduplication (Resolve duplicates on valid PKs)
        pk_cols = [primary_key] if isinstance(primary_key, str) else primary_key
        
        # Filter rows with non-null PKs for deduplication, leaving null PKs to be quarantined
        non_null_pk_cond = F.col(primary_key).isNotNull() if isinstance(primary_key, str) else F.col(pk_cols[0]).isNotNull()
        df_non_null = df.filter(non_null_pk_cond)
        df_null_pk = df.filter(~non_null_pk_cond)
        
        df_count_before = df_non_null.count()
        if date_column and date_column in df.columns:
            df_non_null = df_non_null.orderBy(F.col(date_column).desc())
            
        df_non_null_dedup = df_non_null.dropDuplicates(subset=pk_cols)
        df_count_after = df_non_null_dedup.count()
        
        dup_resolved = df_count_before - df_count_after
        if dup_resolved > 0:
            print(f"[AUTO-CLEAN] Deduplicated and resolved {dup_resolved} duplicate records.")
            remediation_logs.append(f"resolved_{dup_resolved}_duplicates")
            
        # Re-combine non-null deduped rows with null PK rows to preserve data integrity
        df = df_non_null_dedup.unionByName(df_null_pk, allowMissingColumns=True)

    # 3. DATA VALIDATION (Row-level Quality check)
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
    # Bug 1 & 2 Fix: OOM Window Function -> Optimized dropDuplicates and Anti-Join
    pk_cols = [primary_key] if isinstance(primary_key, str) else primary_key
    valid_df_with_id = valid_df.withColumn("__row_id", F.monotonically_increasing_id())

    if date_column and date_column in df.columns:
        valid_df_with_id = valid_df_with_id.orderBy(F.col(date_column).desc())
    
    # Efficiently keep only the latest unique records
    valid_dedup_with_id = valid_df_with_id.dropDuplicates(subset=pk_cols)
    clean_df = valid_dedup_with_id.drop("__row_id", "is_invalid", "reject_reason")
    
    # Find the dropped duplicates by Anti-Join to send to quarantine
    duplicate_df = valid_df_with_id.join(valid_dedup_with_id.select("__row_id"), on="__row_id", how="left_anti") \
                                   .drop("__row_id") \
                                   .withColumn("reject_reason", F.lit("duplicate_records"))

    # ─── DYNAMIC RULES Layer 2: IQR Value Range Outlier Detection ─────────────
    outlier_df = None
    value_range_profile = {}
    try:
        from dynamic_rules_engine import compute_value_range_rules, flag_outlier_rows
        vr_config = rules.get("value_range", {})
        vr_mode = vr_config.get("mode", "off") if isinstance(vr_config, dict) else "off"
        
        if vr_mode in ("auto", "adaptive"):
            # Identify numeric columns from schema_spec
            numeric_cols = [col for col, t in schema_spec.items() 
                           if t in ("IntegerType", "DoubleType") and col in clean_df.columns]
            
            if numeric_cols:
                iqr_mult = vr_config.get("iqr_multiplier", 1.5) if isinstance(vr_config, dict) else 1.5
                value_range_profile = compute_value_range_rules(clean_df, numeric_cols, multiplier=iqr_mult)
                
                if value_range_profile:
                    flagged_df = flag_outlier_rows(clean_df, value_range_profile)
                    outlier_df = flagged_df.filter(F.col("_outlier_flag") == True) \
                                          .withColumn("is_invalid", F.lit(True)) \
                                          .withColumn("reject_reason", F.col("_outlier_details")) \
                                          .drop("_outlier_flag", "_outlier_details")
                    clean_df = flagged_df.filter((F.col("_outlier_flag") == False) | F.col("_outlier_flag").isNull()) \
                                        .drop("_outlier_flag", "_outlier_details")
                    
                    outlier_count = outlier_df.count()
                    if outlier_count > 0:
                        print(f"[DYNAMIC RULES] IQR outlier detection flagged {outlier_count} rows as value outliers")
                        remediation_logs.append(f"iqr_outliers_flagged_{outlier_count}")
    except ImportError:
        pass  # dynamic_rules_engine not available, skip outlier detection
    except Exception as ore:
        print(f"[DYNAMIC RULES] IQR outlier detection failed: {ore}. Continuing without outlier flagging.")

    # ─── DYNAMIC RULES: Unsupervised Anomaly Detection (Z-score) ──────────────
    unsupervised_outlier_df = None
    try:
        from dynamic_rules_engine import detect_unsupervised_anomalies
        # Find numeric columns
        numeric_cols = [col for col, t in schema_spec.items() 
                       if t in ("IntegerType", "DoubleType") and col in clean_df.columns]
        if numeric_cols:
            flagged_unsupervised = detect_unsupervised_anomalies(clean_df, numeric_cols, threshold=3.0)
            unsupervised_outlier_df = flagged_unsupervised.filter(F.col("_unsupervised_anomaly") == True) \
                                                          .withColumn("is_invalid", F.lit(True)) \
                                                          .withColumn("reject_reason", F.col("_unsupervised_anomaly_details")) \
                                                          .drop("_unsupervised_anomaly", "_unsupervised_anomaly_details")
            clean_df = flagged_unsupervised.filter((F.col("_unsupervised_anomaly") == False) | F.col("_unsupervised_anomaly").isNull()) \
                                           .drop("_unsupervised_anomaly", "_unsupervised_anomaly_details")
            
            unsupervised_count = unsupervised_outlier_df.count()
            if unsupervised_count > 0:
                print(f"[DYNAMIC RULES] Unsupervised Z-score anomaly detection flagged {unsupervised_count} rows")
                remediation_logs.append(f"unsupervised_anomalies_flagged_{unsupervised_count}")
    except ImportError:
        pass
    except Exception as uae:
        print(f"[DYNAMIC RULES] Unsupervised anomaly detection failed: {uae}. Continuing.")

    # ─── DYNAMIC RULES: Induced Tree Rules ────────────────────────────────────
    induced_outlier_df = None
    try:
        induced_config = rules.get("induced", {})
        if induced_config:
            # We will build a combined SQL filter expression from all induced rules
            conditions = []
            for rule_name, rule_data in induced_config.items():
                cond = rule_data.get("condition")
                if cond:
                    conditions.append(f"({cond})")
            
            if conditions:
                combined_sql_cond = " OR ".join(conditions)
                print(f"[DYNAMIC ENGINE] Applying induced rules filter: {combined_sql_cond}")
                
                # Flag rows matching the induced conditions
                flagged_induced = clean_df.withColumn(
                    "_induced_anomaly", 
                    F.expr(combined_sql_cond)
                )
                
                induced_outlier_df = flagged_induced.filter(F.col("_induced_anomaly") == True) \
                    .withColumn("is_invalid", F.lit(True)) \
                    .withColumn("reject_reason", F.lit("induced_tree_rule_match")) \
                    .drop("_induced_anomaly")
                
                clean_df = flagged_induced.filter((F.col("_induced_anomaly") == False) | F.col("_induced_anomaly").isNull()) \
                    .drop("_induced_anomaly")
                
                induced_count = induced_outlier_df.count()
                if induced_count > 0:
                    print(f"[DYNAMIC ENGINE] Induced ML rules flagged {induced_count} rows")
                    remediation_logs.append(f"induced_rules_flagged_{induced_count}")
    except Exception as ie:
        print(f"[DYNAMIC ENGINE] Induced rules execution failed: {ie}. Continuing.")

    # Combine all quarantined records (null PKs + duplicates + outliers + unsupervised anomalies + induced anomalies)
    all_quarantined = invalid_df.unionByName(duplicate_df, allowMissingColumns=True)
    if unsupervised_outlier_df is not None:
        try:
            if unsupervised_outlier_df.count() > 0:
                all_quarantined = all_quarantined.unionByName(unsupervised_outlier_df, allowMissingColumns=True)
        except Exception:
            pass
    if outlier_df is not None:
        try:
            outlier_count_check = outlier_df.count()
            if outlier_count_check > 0:
                all_quarantined = all_quarantined.unionByName(outlier_df, allowMissingColumns=True)
        except Exception:
            pass
    if induced_outlier_df is not None:
        try:
            if induced_outlier_df.count() > 0:
                all_quarantined = all_quarantined.unionByName(induced_outlier_df, allowMissingColumns=True)
        except Exception:
            pass

    # Add run_id partition structure to quarantined parquet
    all_quarantined_write = all_quarantined.withColumn("run_id", F.lit(run_id)) \
                                           .withColumn("rejected_at", F.current_timestamp())

    # Cache and count before writing to avoid re-reading and partial failures
    clean_df.cache()
    all_quarantined_write.cache()

    clean_count = clean_df.count()
    quarantine_count = all_quarantined_write.count()
    total_records = clean_count + quarantine_count

    print("Writing validated datasets to HDFS using Delta Lake...")
    
    # Delta Lake MERGE (True Upsert)
    clean_df_for_upsert = clean_df.withColumn("run_id", F.lit(run_id))
    
    try:
        from delta.tables import DeltaTable
        if DeltaTable.isDeltaTable(spark, active_path):
            print("Existing Delta Table found. Performing MERGE INTO...")
            delta_table = DeltaTable.forPath(spark, active_path)
            if isinstance(pk_cols, list) and len(pk_cols) > 0:
                merge_cond = " AND ".join([f"old.{col} = new.{col}" for col in pk_cols])
            else:
                merge_cond = f"old.{pk_cols} = new.{pk_cols}"
            delta_table.alias("old").merge(
                clean_df_for_upsert.alias("new"),
                merge_cond
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            print("Delta Lake Upsert completed successfully.")
        else:
            print(f"No existing Delta table. Creating new Delta table at: {active_path}")
            clean_df_for_upsert.write \
                .format("delta") \
                .option("delta.columnMapping.mode", "name") \
                .option("delta.minReaderVersion", "2") \
                .option("delta.minWriterVersion", "5") \
                .mode("overwrite") \
                .option("txnVersion", run_id) \
                .save(active_path)
            print("Delta Lake table created successfully with Column Mapping enabled.")
    except Exception as e:
        print(f"Error during Delta write: {e}. Attempting direct fallback write.")
        clean_df_for_upsert.write \
            .format("delta") \
            .option("delta.columnMapping.mode", "name") \
            .option("delta.minReaderVersion", "2") \
            .option("delta.minWriterVersion", "5") \
            .mode("overwrite") \
            .save(active_path)

    # Quarantined data written with run_id partition for traceability
    all_quarantined_write.write.format("delta").mode("append").partitionBy("run_id").save(quarantine_path)

    # Release cached DataFrames from memory to prevent memory leaks and OOM
    clean_df.unpersist()
    all_quarantined_write.unpersist()

    # Class Balance / Data Distribution calculation
    class_balance = {}
    group_col = None
    if clean_count > 0:
        try:
            clean_run_df = spark.read.format("delta").load(active_path)
            # 1. Search for common categorical column names (Case Insensitive)
            common_categorical_cols = ["label", "role", "category", "type", "status", "class", "gender", "country", "state", "sector", "transaction_type"]
            df_cols_lower = {c.lower(): c for c in clean_run_df.columns}
            
            for col_name in common_categorical_cols:
                if col_name in df_cols_lower:
                    group_col = df_cols_lower[col_name]
                    break

            # 2. Cardinality-based automatic String column fallback (detects 2 to 25 categories)
            if not group_col:
                string_cols = [f.name for f in clean_run_df.schema.fields if f.dataType.__class__.__name__ == "StringType"]
                if string_cols:
                    agg_exprs = [F.countDistinct(col_name).alias(col_name) for col_name in string_cols]
                    try:
                        counts_row = clean_run_df.agg(*agg_exprs).first()
                        if counts_row:
                            for col_name in string_cols:
                                distinct_count = counts_row[col_name]
                                if 1 < distinct_count <= 25:
                                    group_col = col_name
                                    break
                    except Exception as e:
                        print(f"[DISTRIBUTION] Warning: Failed to scan distinct count in parallel: {e}")

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
            quar_run_df = spark.read.format("delta").load(quarantine_path).filter(F.col("run_id") == run_id)
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

    # Calculate Dynamic Financial COPDQ (Root Cause Fix for hardcoded math)
    quarantined_financial_value = 0.0
    if quarantine_count > 0:
        try:
            fin_cols = ["total_sales", "sales", "revenue", "profit", "price", "amount", "total"]
            fin_col_actual = None
            df_cols_lower = {c.lower(): c for c in quar_run_df.columns}
            for c in fin_cols:
                if c in df_cols_lower:
                    fin_col_actual = df_cols_lower[c]
                    break
            
            if fin_col_actual:
                sum_val = quar_run_df.select(F.sum(F.col(fin_col_actual).cast("double"))).collect()[0][0]
                quarantined_financial_value = float(sum_val) if sum_val is not None else 0.0
                print(f"Dynamic COPDQ: Calculated ${quarantined_financial_value:.2f} lost from '{fin_col_actual}'")
        except Exception as e:
            print(f"Error computing financial loss: {e}")

    # 3. FRESHNESS LAG
    max_lag_hours = 0.0
    # Root Cause Fix (Point 27): Skip Freshness if table is known to be historical or static
    is_historical = table_name.endswith("_historical") or table_name == "global_ecommerce_sales"

    if date_column and clean_count > 0 and not is_historical:
        try:
            clean_run_df = spark.read.format("delta").load(active_path).filter(F.col("run_id") == run_id)
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
                    now_utc = datetime.now(timezone.utc)
                    if max_ts.tzinfo is None:
                        max_ts = max_ts.replace(tzinfo=timezone.utc)
                    lag_seconds = (now_utc - max_ts).total_seconds()
                    max_lag_hours = max(0.0, lag_seconds / 3600.0)
        except Exception as e:
            print(f"Error computing freshness: {e}")
    elif is_historical:
        print(f"Skipping freshness check for historical dataset '{table_name}'.")

    # 4. QUALITY SCORE
    passed_tests = clean_count
    total_tests = total_records
    if total_tests == 0:
        quality_score = 0.0
        remediation_logs.append("Warning: Empty source file ingested. Quality score defaulted to 0.0% to prevent masking upstream ingestion failure.")
    else:
        quality_score = (passed_tests / total_tests) * 100.0

    # FIX 3A: Use per-table threshold from rules_config instead of hardcoded 90.0
    if quality_score < quality_threshold:
        send_n8n_alert(
            title=f"🚨 Critical Data Quality Drop: {table_name}",
            message=f"Run ID: {run_id}\nQuality Score: {quality_score:.1f}% (threshold={quality_threshold}%)\nQuarantined: {quarantine_count} rows\nClean: {clean_count} rows",
            severity="critical"
        )

    # 4.1 Z-Score Anomaly Detection on Quarantine Rate (Task 3 with Bug 6 Fix)
    current_quarantine_rate = 0.0 if total_records == 0 else float(quarantine_count) / float(total_records)
    historical_rates = get_historical_stats(table_name)
    z_score = 0.0
    is_anomaly = False
    
    # We need at least 3 historical runs to compute standard deviation
    if len(historical_rates) >= 3:
        avg_rate = sum(historical_rates) / len(historical_rates)
        variance = sum((x - avg_rate) ** 2 for x in historical_rates) / len(historical_rates)
        std_dev = variance ** 0.5
        # Robust Z-Score: minimum standard deviation floor of 0.05 (5%) to prevent scaling blowup
        std_dev = max(std_dev, 0.05)
        # Prevent false alarms on tiny, insignificant fluctuations by ignoring changes under 2%
        if abs(current_quarantine_rate - avg_rate) < 0.02:
            z_score = 0.0
        else:
            z_score = abs(current_quarantine_rate - avg_rate) / std_dev
        
        if z_score > 3.0:
            is_anomaly = True
            print(f"[ANOMALY] Statistical Anomaly Detected! Quarantine Rate: {current_quarantine_rate*100:.2f}% vs Avg: {avg_rate*100:.2f}%, Z-Score: {z_score:.2f}")
            send_n8n_alert(
                title=f"🚨 CRITICAL ANOMALY: {table_name} Data Anomaly",
                message=f"Run ID: {run_id}\nQuarantine Rate: {current_quarantine_rate*100:.2f}% (Historical Avg: {avg_rate*100:.2f}%)\nZ-Score: {z_score:.2f} (exceeds threshold 3.0)",
                severity="critical"
            )

    # ─── DYNAMIC RULES Layer 3: Dynamic Decision Engine ─────────────────────────
    ai_config = rules.get("ai_advisor", {})
    ai_enabled = ai_config.get("enabled", False) if isinstance(ai_config, dict) else False
    profile_report = None
    
    # ── Component A: Data Profile Cycle (runs every time when enabled) ─────────
    if ai_enabled:
        try:
            from data_profile_store import run_profile_cycle
            # Use clean_df snapshot to build profiles (read from Delta since clean_df is unpersisted)
            try:
                profile_df = spark.read.format("delta").load(active_path)
                profile_report = run_profile_cycle(profile_df, table_name)
                
                if profile_report.get("total_drifted_columns", 0) > 0:
                    remediation_logs.append(f"profile_drift_detected_{profile_report['total_drifted_columns']}_columns")
                    # Drift counts as an anomaly signal for triggering deeper analysis
                    if not is_anomaly:
                        print("[DYNAMIC ENGINE] Distribution drift detected — escalating to AI analysis.")
            except Exception as profile_err:
                print(f"[DYNAMIC ENGINE] Profile cycle failed (non-fatal): {profile_err}")
                profile_report = None
        except ImportError:
            print("[DYNAMIC ENGINE] data_profile_store module not available. Skipping profile cycle.")
    
    # ── Components B+C: AI Analysis + Rule Induction (on anomaly/drift) ────────
    trigger_always = ai_config.get("trigger", "on_anomaly") == "always"
    if ai_enabled and (trigger_always or is_anomaly or quality_score < (quality_threshold - 15) or
                       (profile_report and profile_report.get("total_drifted_columns", 0) > 0)):
        try:
            from ai_rule_advisor import get_ai_advisor
            advisor = get_ai_advisor()
            if advisor:
                quality_context = {
                    "is_anomaly": is_anomaly,
                    "quality_score": quality_score,
                    "current_threshold": quality_threshold,
                    "historical_avg": 100.0 - (sum(historical_rates) / len(historical_rates) * 100) if historical_rates else 90.0,
                    "z_score": z_score,
                    "schema_drift_detected": drift_detected,
                    "quarantine_rate": current_quarantine_rate,
                    "table_name": table_name,
                    "trigger_always": trigger_always
                }
                
                if trigger_always or advisor.should_trigger(quality_context) or \
                   (profile_report and profile_report.get("total_drifted_columns", 0) > 0):
                    max_ai_rows = ai_config.get("max_rows_to_analyze", 50)
                    # Sample quarantined rows from persisted Delta Lake
                    try:
                        sample_rows = [
                            row.asDict() for row in spark.read.format("delta").load(quarantine_path)
                            .filter(F.col("run_id") == run_id)
                            .limit(max_ai_rows).collect()
                        ]
                    except Exception as sample_err:
                        print(f"[DYNAMIC ENGINE] Failed to read quarantine sample from Delta: {sample_err}")
                        sample_rows = []
                    
                    # Gather column statistics
                    col_stats = {}
                    if value_range_profile:
                        col_stats["value_ranges"] = value_range_profile
                    col_stats["quarantine_breakdown"] = quarantine_breakdown
                    
                    historical_ctx = {
                        "avg_quality": quality_context["historical_avg"],
                        "current_quality": quality_score,
                        "current_threshold": quality_threshold,
                        "total_records": total_records,
                        "quarantined_records": quarantine_count,
                        "primary_key": primary_key if isinstance(primary_key, str) else ",".join(pk_cols) if pk_cols else "unknown",
                        "date_column": date_column or "unknown"
                    }
                    
                    # ── Component B: Profile-Based Analysis (enhanced heuristic + drift) ──
                    if profile_report and not profile_report.get("is_first_run"):
                        analysis = advisor.run_profile_based_analysis(
                            table_name, sample_rows, col_stats, historical_ctx,
                            profile_report=profile_report
                        )
                    else:
                        analysis = advisor.ai_analyze_quarantined_sample(
                            table_name, sample_rows, col_stats, historical_ctx
                        )
                    
                    min_confidence = ai_config.get("confidence_threshold", 0.7)
                    if analysis and analysis.get("confidence", 0) >= min_confidence:
                        # Auto-promote if confidence is extremely high (e.g. >= 0.90)
                        if analysis.get("confidence", 0) >= 0.90:
                            print(f"[DYNAMIC ENGINE] Auto-promoting analysis rules (confidence={analysis['confidence']})")
                            analysis["status"] = "APPROVED"
                            advisor.promote_rules_to_config(table_name, analysis.get("suggested_rules", []))
                        
                        advisor.log_proposal_to_es(table_name, run_id, analysis)
                        method = analysis.get("analysis_metadata", {}).get("method", "unknown")
                        print(f"[DYNAMIC ENGINE] Analysis complete ({method}). Root cause: {analysis.get('root_cause', 'N/A')}")
                        print(f"[DYNAMIC ENGINE] Suggested {len(analysis.get('suggested_rules', []))} rules. Confidence: {analysis.get('confidence', 0):.0%}")
                        remediation_logs.append(f"ai_advisor_triggered_confidence_{analysis.get('confidence', 0):.2f}")
                    else:
                        print(f"[DYNAMIC ENGINE] Analysis returned low confidence. No proposal created.")
                    
                    # ── Component C: Decision Tree Rule Induction (only on anomaly) ──
                    if is_anomaly and quarantine_count >= 10:
                        try:
                            # Get numeric columns for features
                            numeric_features = profile_report.get("numeric_columns", []) if profile_report else []
                            if not numeric_features:
                                from pyspark.sql.types import IntegerType, LongType, FloatType, DoubleType, ShortType, DecimalType
                                numeric_types = (IntegerType, LongType, FloatType, DoubleType, ShortType, DecimalType)
                                try:
                                    active_df_check = spark.read.format("delta").load(active_path)
                                    numeric_features = [f.name for f in active_df_check.schema.fields
                                                        if isinstance(f.dataType, numeric_types)]
                                except Exception:
                                    numeric_features = []
                            
                            if len(numeric_features) >= 2:
                                clean_for_tree = spark.read.format("delta").load(active_path)
                                quarantine_for_tree = spark.read.format("delta").load(quarantine_path) \
                                    .filter(F.col("run_id") == run_id)
                                
                                induction_result = advisor.induce_rules_from_data(
                                    spark, clean_for_tree, quarantine_for_tree,
                                    numeric_features, table_name, run_id
                                )
                                
                                n_induced = len(induction_result.get("induced_rules", []))
                                if n_induced > 0:
                                    auc = induction_result.get("model_accuracy", 0)
                                    if auc >= 0.90:
                                        print(f"[DYNAMIC ENGINE] Auto-promoting induced rules (AUC={auc:.4f})")
                                        # Set status as APPROVED inside each rule
                                        for r in induction_result.get("induced_rules", []):
                                            r["status"] = "APPROVED"
                                        advisor.promote_rules_to_config(table_name, induction_result.get("induced_rules", []))
                                    print(f"[DYNAMIC ENGINE] Decision Tree induced {n_induced} rules (AUC={auc:.4f})")
                                    remediation_logs.append(f"decision_tree_induced_{n_induced}_rules_auc_{auc:.2f}")
                        except Exception as dt_err:
                            print(f"[DYNAMIC ENGINE] Decision Tree induction failed (non-fatal): {dt_err}")
                    
        except ImportError:
            print(f"[DYNAMIC ENGINE] ai_rule_advisor module not available. Skipping AI analysis.")
        except Exception as ai_err:
            print(f"[DYNAMIC ENGINE] AI analysis failed (non-fatal): {ai_err}")


    # 4.2 Weighted Operational COPDQ Score (Task 4 with Bug 5 Row-Level Max Weight Fix)
    column_weights = rules.get("column_weights", {})
    pk_weight = column_weights.get(primary_key if isinstance(primary_key, str) else pk_cols[0], 1.0)
    date_weight = column_weights.get(date_column, 0.5) if date_column else 0.5
    
    operational_impact_score = 0.0
    if total_records > 0 and quarantine_count > 0:
        try:
            # Build Spark expression to calculate max failure weight per row
            weight_col = F.lit(0.0)
            weight_col = F.when(
                F.col("reject_reason").contains("primary") | F.col("reject_reason").contains("duplicate"),
                F.lit(pk_weight)
            ).otherwise(weight_col)
            
            weight_col = F.when(
                F.col("reject_reason").contains("date"),
                F.greatest(weight_col, F.lit(date_weight))
            ).otherwise(weight_col)
            
            for col, w in column_weights.items():
                if col != primary_key and col != date_column:
                    weight_col = F.when(
                        F.col("reject_reason").contains(col),
                        F.greatest(weight_col, F.lit(w))
                    ).otherwise(weight_col)
            
            weight_col = F.when(
                (F.col("reject_reason") != "") & (F.col("reject_reason").isNotNull()),
                F.greatest(weight_col, F.lit(0.2))
            ).otherwise(weight_col)
            
            sum_val = all_quarantined.select(F.sum(weight_col)).collect()[0][0]
            sum_of_max_row_weights = float(sum_val) if sum_val is not None else 0.0
            operational_impact_score = (sum_of_max_row_weights / total_records) * 100.0
        except Exception as e:
            print(f"Error calculating row-level weighted score: {e}")
            operational_impact_score = (float(quarantine_count) / float(total_records)) * 100.0
    print(f"Weighted Operational Impact Score: {operational_impact_score:.2f}%")

    # Log operational run metrics to Elasticsearch
    quality_run_doc = {
        "run_id": run_id,
        "table_name": table_name,
        "total_records": total_records,
        "clean_records": clean_count,
        "quarantined_records": quarantine_count,
        "quality_score": quality_score,
        "freshness_lag_hours": max_lag_hours,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "quarantined_financial_value": quarantined_financial_value,
        "operational_impact_score": operational_impact_score,
        "z_score": z_score,
        "is_anomaly": is_anomaly,
        "remediation_logs": remediation_logs,
        "auto_cleaned": auto_clean
    }
    if class_balance:
        quality_run_doc["class_balance"] = class_balance
        quality_run_doc["class_balance_column"] = group_col
    if quarantine_breakdown:
        quality_run_doc["quarantine_breakdown"] = quarantine_breakdown
    # Dynamic Rules metadata: log which mode was used and computed thresholds
    quality_run_doc["rules_mode"] = "adaptive" if isinstance(rules.get("quality_score_threshold"), dict) else "static"
    quality_run_doc["effective_quality_threshold"] = quality_threshold
    quality_run_doc["effective_freshness_threshold"] = freshness_limit_hours
    if value_range_profile:
        quality_run_doc["value_range_profile"] = value_range_profile

    log_to_elasticsearch("sdoqap_quality_runs", quality_run_doc)

    log_to_elasticsearch("sdoqap_lineage_runs", {
        "run_id": run_id,
        "source_table": f"raw-{table_name}",
        "target_table": f"active-{table_name}",
        "source_path": raw_path,
        "target_path": active_path,
        "quarantine_path": quarantine_path,
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

    log_to_elasticsearch("sdoqap_pipeline_runs", {
        "run_id": run_id,
        "table_name": table_name,
        "state": "success" if quality_score >= quality_threshold else "warnings",
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

    print(f"Quality validation completed. Quality Score: {quality_score:.2f}% (threshold={quality_threshold}%)")

    # ─── Track 3: Downstream Event-Driven Trigger ─────────────────────────────
    if quality_score >= quality_threshold:
        try:
            print("[DYNAMIC ENGINE] Automatically triggering downstream Gold Layer rebuild locally...")
            import subprocess
            gold_script = "/opt/spark-apps/spark_gold_layer.py"
            if not os.path.exists(gold_script):
                gold_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "spark_gold_layer.py")
            subprocess.Popen(["python", gold_script])
            print("[DYNAMIC ENGINE] Gold Layer rebuild triggered successfully in background.")
        except Exception as gold_err:
            print(f"[DYNAMIC ENGINE] Local downstream trigger failed (non-fatal): {gold_err}")

    # FIX 2A: Release the distributed lock after all work is done
    release_lock(table_name)
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Quality Engine")
    parser.add_argument("table", nargs='?', default="users", help="Target table name")
    parser.add_argument("--force", action="store_true", help="Force lock acquisition, overriding existing locks")
    args = parser.parse_args()
    target_table = args.table
    FORCE_LOCK = args.force

    # Load canonical schema registry config from ES or default fallbacks
    spec = load_expected_schema(target_table)

    if spec:
        # Resolve table name from spec if it was fuzzy matched
        normalized_target = normalize_name(target_table)
        matched_table_name = target_table
        default_registry_tables = ["mbti", "users", "benchmark_test"]
        for tbl in default_registry_tables:
            if normalize_name(tbl) == normalized_target:
                matched_table_name = tbl
                break
        
        run_quality_check(matched_table_name, spec["primary_key"], spec["date_column"], spec["schema_spec"], input_table_name=target_table)
    else:
        print(f"Table '{target_table}' not found in registry. Inferring configuration dynamically...")
        # Resolve configuration dynamically using the standard Spark session
        temp_spark = get_spark_session(f"SDOQAP_Infer_{target_table}")

        raw_path = f"hdfs://namenode:9000/data/raw/{target_table}"
        try:
            # 1. Read raw strings to preserve exact values
            df_raw = temp_spark.read.option("header", "true").csv(raw_path)
            for col_name in df_raw.columns:
                cleaned_col = clean_column_name(col_name)
                if col_name != cleaned_col:
                    df_raw = df_raw.withColumnRenamed(col_name, cleaned_col)

            # 2. Read with inferSchema to get Spark's baseline guesses
            df_infer = temp_spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)
            for col_name in df_infer.columns:
                cleaned_col = clean_column_name(col_name)
                if col_name != cleaned_col:
                    df_infer = df_infer.withColumnRenamed(col_name, cleaned_col)
            
            # 3. Deterministic Profiling (Full 100% Dataset Pass)
            # We check EVERY row to see if ANY row contains a leading zero (e.g. '01234')
            agg_exprs = []
            for col in df_raw.columns:
                agg_exprs.append(
                    F.max(F.when(F.col(col).rlike("^0[0-9]+$"), 1).otherwise(0)).alias(f"{col}_has_leading_zero")
                )
            
            # Execute full dataset scan
            profile_row = df_raw.agg(*agg_exprs).collect()[0]
            
            schema_spec = {}
            for field in df_infer.schema.fields:
                col = field.name
                inferred_type = field.dataType.__class__.__name__
                has_leading_zero = profile_row[f"{col}_has_leading_zero"] == 1
                
                if has_leading_zero:
                    # Root Cause Fix: 100% Guarantee no leading zeros are lost
                    schema_spec[col] = "StringType"
                else:
                    # Safe to use Spark's inferred type (Integer, Double, Timestamp, etc.)
                    schema_spec[col] = inferred_type

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

            inferred_spec = {
                "primary_key": primary_key,
                "date_column": date_column,
                "schema_spec": schema_spec
            }

            # Save inferred registry config to ES sdoqap_schema_registry
            save_registry_to_es(target_table, inferred_spec)

            # Auto-generate dynamic rules configuration for new table
            try:
                from dynamic_rules_engine import generate_rules_from_schema
                generate_rules_from_schema(target_table, schema_spec, primary_key, date_column)
            except Exception as ar_err:
                print(f"[DYNAMIC_RULES] Failed to generate default rules for '{target_table}': {ar_err}")

            # Stop the temporary Spark session so that the main job can run on the cluster
            temp_spark.stop()

            # Now execute quality check with inferred schema
            run_quality_check(target_table, primary_key, date_column, schema_spec)

        except Exception as infer_err:
            print(f"Failed to infer schema dynamically for '{target_table}': {infer_err}")
            # Write a failed run document to ES
            run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
            doc = {
                "run_id": run_id,
                "table_name": target_table,
                "state": "failed",
                "error_msg": f"Failed to infer schema: {str(infer_err)}",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            try:
                log_to_elasticsearch("sdoqap_pipeline_runs", doc)
            except Exception:
                pass
            temp_spark.stop()
            sys.exit(1)
