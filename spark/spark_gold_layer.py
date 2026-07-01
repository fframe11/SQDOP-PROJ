"""
SDOQAP Gold Layer Aggregation Engine
=====================================
Reads from Elasticsearch (Silver-layer quality run logs) and writes
pre-aggregated Gold-layer summaries back to Elasticsearch indices:

  sdoqap_gold_daily_quality    — daily quality KPI per table
  sdoqap_gold_error_patterns   — top error reasons with trend
  sdoqap_gold_financial_impact — cumulative cost-of-poor-data per day
  sdoqap_gold_schema_drift     — schema drift event history

Run via spark-submit or directly: python spark_gold_layer.py
"""

import os
import json
import requests
import re
from datetime import datetime, timezone, timedelta
from collections import defaultdict

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://elastic:sdoqap_secure@elasticsearch:9200")
HEADERS = {"Content-Type": "application/json"}

# Financial model constants (IBM / Gartner COPDQ benchmark)
# Cost per quarantined record: $1 detection + $10 remediation (1-10-100 rule simplified)
COST_PER_QUARANTINED_RECORD_USD = 11.0


# ─────────────────────────────────────────────
# Helper: ES query + write
# ─────────────────────────────────────────────

def sanitize_id(raw_id):
    return re.sub(r'[^a-zA-Z0-9_-]', '_', raw_id)

def es_search(index, body, size=1000):
    # Using Scroll API to fetch all results without truncation (Bug 12, 13)
    url = f"{ELASTICSEARCH_URL}/{index}/_search?scroll=2m"
    try:
        res = requests.post(url, headers=HEADERS, data=json.dumps({**body, "size": size}), timeout=15)
        res.raise_for_status()
        data = res.json()
        hits = data.get("hits", {}).get("hits", [])
        scroll_id = data.get("_scroll_id")
        all_hits = list(hits)

        while hits and scroll_id:
            scroll_res = requests.post(
                f"{ELASTICSEARCH_URL}/_search/scroll",
                headers=HEADERS,
                data=json.dumps({"scroll": "2m", "scroll_id": scroll_id}),
                timeout=15
            )
            scroll_data = scroll_res.json()
            hits = scroll_data.get("hits", {}).get("hits", [])
            scroll_id = scroll_data.get("_scroll_id")
            if hits:
                all_hits.extend(hits)

        return all_hits
    except Exception as e:
        print(f"[ES SEARCH ERROR] index={index} : {e}")
        return []


def es_index_exists(index):
    try:
        res = requests.head(f"{ELASTICSEARCH_URL}/{index}", timeout=5)
        return res.status_code == 200
    except Exception:
        return False


def es_write(index, doc_id, doc):
    url = f"{ELASTICSEARCH_URL}/{index}/_doc/{doc_id}"
    try:
        res = requests.put(url, headers=HEADERS, data=json.dumps(doc), timeout=10)
        res.raise_for_status()
        print(f"  [GOLD WRITE] {index}/{doc_id} ✓")
    except Exception as e:
        print(f"  [GOLD WRITE ERROR] {index}/{doc_id} : {e}")


def es_create_index_if_missing(index, mappings):
    if not es_index_exists(index):
        try:
            res = requests.put(
                f"{ELASTICSEARCH_URL}/{index}",
                headers=HEADERS,
                data=json.dumps({"mappings": mappings}),
                timeout=10
            )
            res.raise_for_status()
            print(f"  [INDEX CREATED] {index}")
        except Exception as e:
            print(f"  [INDEX CREATE ERROR] {index} : {e}")


# ─────────────────────────────────────────────
# Setup Gold indices with proper mappings
# ─────────────────────────────────────────────

def setup_gold_indices():
    print("\n[SETUP] Creating Gold Layer Elasticsearch indices...")

    es_create_index_if_missing("sdoqap_gold_daily_quality", {
        "properties": {
            "date": {"type": "date", "format": "yyyy-MM-dd"},
            "table_name": {"type": "keyword"},
            "avg_quality_score": {"type": "float"},
            "min_quality_score": {"type": "float"},
            "max_quality_score": {"type": "float"},
            "total_records": {"type": "long"},
            "total_clean": {"type": "long"},
            "total_quarantined": {"type": "long"},
            "quarantine_rate_pct": {"type": "float"},
            "avg_freshness_lag_hours": {"type": "float"},
            "run_count": {"type": "integer"},
            "computed_at": {"type": "date"}
        }
    })

    es_create_index_if_missing("sdoqap_gold_error_patterns", {
        "properties": {
            "date": {"type": "date", "format": "yyyy-MM-dd"},
            "error_type": {"type": "keyword"},
            "source_table": {"type": "keyword"},
            "count": {"type": "long"},
            "percentage": {"type": "float"},
            "computed_at": {"type": "date"}
        }
    })

    es_create_index_if_missing("sdoqap_gold_financial_impact", {
        "properties": {
            "date": {"type": "date", "format": "yyyy-MM-dd"},
            "total_quarantined_records": {"type": "long"},
            "estimated_cost_usd": {"type": "float"},
            "cumulative_cost_usd": {"type": "float"},
            "most_impacted_table": {"type": "keyword"},
            "computed_at": {"type": "date"}
        }
    })

    es_create_index_if_missing("sdoqap_gold_schema_drift", {
        "properties": {
            "date": {"type": "date", "format": "yyyy-MM-dd"},
            "table_name": {"type": "keyword"},
            "drift_count": {"type": "integer"},
            "affected_columns": {"type": "keyword"},
            "computed_at": {"type": "date"}
        }
    })

    print("[SETUP] Gold indices ready.\n")


# ─────────────────────────────────────────────
# GOLD TABLE 1: Daily Quality Summary per Table
# ─────────────────────────────────────────────

def build_gold_daily_quality():
    print("[GOLD 1/4] Building daily_quality summary...")

    hits = es_search(
        "sdoqap_quality_runs",
        {"query": {"match_all": {}}, "sort": [{"timestamp": {"order": "asc"}}]},
        size=1000
    )

    if not hits:
        print("  No quality_runs data found. Skipping.")
        return

    # Group by (date, table_name)
    groups = defaultdict(lambda: {
        "scores": [], "total": 0, "clean": 0,
        "quarantined": 0, "freshness": [], "runs": 0
    })

    for hit in hits:
        src = hit.get("_source", {})
        ts = src.get("timestamp", "")
        date_key = ts[:10] if ts else "unknown"
        table = src.get("table_name", "unknown")
        key = (date_key, table)

        g = groups[key]
        g["scores"].append(src.get("quality_score", 100.0))
        g["total"] += src.get("total_records", 0)
        g["clean"] += src.get("clean_records", 0)
        g["quarantined"] += src.get("quarantined_records", 0)
        g["runs"] += 1
        lag = src.get("freshness_lag_hours")
        if lag is not None:
            g["freshness"].append(lag)

    now_iso = datetime.now(timezone.utc).isoformat()

    for (date_key, table), g in groups.items():
        scores = g["scores"]
        avg_score = round(sum(scores) / len(scores), 2) if scores else 100.0
        min_score = round(min(scores), 2) if scores else 100.0
        max_score = round(max(scores), 2) if scores else 100.0
        total = g["total"]
        quarantined = g["quarantined"]
        q_rate = round((quarantined / total * 100), 2) if total > 0 else 0.0
        avg_fresh = round(sum(g["freshness"]) / len(g["freshness"]), 2) if g["freshness"] else 0.0

        doc_id = sanitize_id(f"{date_key}_{table}")
        doc = {
            "date": date_key,
            "table_name": table,
            "avg_quality_score": avg_score,
            "min_quality_score": min_score,
            "max_quality_score": max_score,
            "total_records": total,
            "total_clean": g["clean"],
            "total_quarantined": quarantined,
            "quarantine_rate_pct": q_rate,
            "avg_freshness_lag_hours": avg_fresh,
            "run_count": g["runs"],
            "computed_at": now_iso
        }
        es_write("sdoqap_gold_daily_quality", doc_id, doc)

    print(f"  Done. {len(groups)} daily-table records written.")


# ─────────────────────────────────────────────
# GOLD TABLE 2: Error Pattern Aggregation
# ─────────────────────────────────────────────

def build_gold_error_patterns():
    print("[GOLD 2/4] Building error_patterns summary...")

    hits = es_search(
        "sdoqap_quality_runs",
        {"query": {"bool": {"should": [
            {"range": {"quarantined_records": {"gt": 0}}},
            {"exists": {"field": "quarantine_breakdown"}}
        ]}}},
        size=1000
    )

    if not hits:
        print("  No quarantine data found. Skipping.")
        return

    # Group by (date, error_type, source_table)
    groups = defaultdict(int)

    for hit in hits:
        src = hit.get("_source", {})
        ts = src.get("timestamp", "")
        date_key = ts[:10] if ts else "unknown"
        table = src.get("table_name", "unknown")
        breakdown = src.get("quarantine_breakdown", {})

        if breakdown:
            for reason, count in breakdown.items():
                groups[(date_key, reason, table)] += count
        else:
            q_count = src.get("quarantined_records", 0)
            groups[(date_key, "unclassified", table)] += q_count

    # Compute daily totals for percentage
    daily_totals = defaultdict(int)
    for (date_key, reason, table), count in groups.items():
        daily_totals[date_key] += count

    now_iso = datetime.now(timezone.utc).isoformat()

    for (date_key, reason, table), count in groups.items():
        total_day = daily_totals[date_key]
        pct = round((count / total_day * 100), 1) if total_day > 0 else 0.0
        doc_id = sanitize_id(f"{date_key}_{reason}_{table}")
        doc = {
            "date": date_key,
            "error_type": reason,
            "source_table": table,
            "count": count,
            "percentage": pct,
            "computed_at": now_iso
        }
        es_write("sdoqap_gold_error_patterns", doc_id, doc)

    print(f"  Done. {len(groups)} error pattern records written.")


# ─────────────────────────────────────────────
# GOLD TABLE 3: Financial Impact per Day
# ─────────────────────────────────────────────

def build_gold_financial_impact():
    print("[GOLD 3/4] Building financial_impact summary...")

    hits = es_search(
        "sdoqap_quality_runs",
        {"query": {"match_all": {}}, "sort": [{"timestamp": {"order": "asc"}}]},
        size=1000
    )

    if not hits:
        print("  No data found. Skipping.")
        return

    # Group by date
    daily = defaultdict(lambda: {"quarantined": 0, "tables": defaultdict(int)})

    for hit in hits:
        src = hit.get("_source", {})
        ts = src.get("timestamp", "")
        date_key = ts[:10] if ts else "unknown"
        table = src.get("table_name", "unknown")
        q = src.get("quarantined_records", 0)
        daily[date_key]["quarantined"] += q
        daily[date_key]["tables"][table] += q

    now_iso = datetime.now(timezone.utc).isoformat()
    cumulative = 0.0
    sorted_dates = sorted(daily.keys())

    for date_key in sorted_dates:
        d = daily[date_key]
        q_records = d["quarantined"]
        daily_cost = round(q_records * COST_PER_QUARANTINED_RECORD_USD, 2)
        cumulative += daily_cost
        most_impacted = max(d["tables"], key=d["tables"].get) if d["tables"] else "none"

        doc_id = sanitize_id(date_key)
        doc = {
            "date": date_key,
            "total_quarantined_records": q_records,
            "estimated_cost_usd": daily_cost,
            "cumulative_cost_usd": round(cumulative, 2),
            "most_impacted_table": most_impacted,
            "computed_at": now_iso
        }
        es_write("sdoqap_gold_financial_impact", doc_id, doc)

    print(f"  Done. {len(daily)} daily financial records written. Total COPDQ: ${cumulative:,.2f}")


# ─────────────────────────────────────────────
# GOLD TABLE 4: Schema Drift History
# ─────────────────────────────────────────────

def build_gold_schema_drift():
    print("[GOLD 4/4] Building schema_drift history...")

    if not es_index_exists("sdoqap_schema_drifts"):
        print("  sdoqap_schema_drifts index not found. Skipping.")
        return

    hits = es_search(
        "sdoqap_schema_drifts",
        {"query": {"match_all": {}}, "sort": [{"timestamp": {"order": "asc"}}]},
        size=1000
    )

    if not hits:
        print("  No schema drift events found. Skipping.")
        return

    # Group by (date, table_name)
    groups = defaultdict(lambda: {"count": 0, "columns": set()})

    for hit in hits:
        src = hit.get("_source", {})
        ts = src.get("timestamp", "")
        date_key = ts[:10] if ts else "unknown"
        table = src.get("table_name", "unknown")
        drift_details = src.get("drift_details", {})

        key = (date_key, table)
        groups[key]["count"] += 1
        for col in drift_details.keys():
            groups[key]["columns"].add(col)

    now_iso = datetime.now(timezone.utc).isoformat()

    for (date_key, table), g in groups.items():
        doc_id = sanitize_id(f"{date_key}_{table}")
        doc = {
            "date": date_key,
            "table_name": table,
            "drift_count": g["count"],
            "affected_columns": list(g["columns"]),
            "computed_at": now_iso
        }
        es_write("sdoqap_gold_schema_drift", doc_id, doc)

    print(f"  Done. {len(groups)} schema drift records written.")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  SDOQAP Gold Layer Aggregation Engine")
    print(f"  Started at: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 55)

    setup_gold_indices()
    build_gold_daily_quality()
    build_gold_error_patterns()
    build_gold_financial_impact()
    build_gold_schema_drift()

    print("\n" + "=" * 55)
    print("  Gold Layer build complete!")
    print("=" * 55)
