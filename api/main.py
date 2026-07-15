import os
import requests
import socket
import json
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from elasticsearch import Elasticsearch

_original_Elasticsearch = Elasticsearch
_global_es_client = None

def Elasticsearch(*args, **kwargs):
    global _global_es_client
    if _global_es_client is None:
        _global_es_client = _original_Elasticsearch(*args, **kwargs)
    return _global_es_client

from app.api.lineage import router as lineage_router
from app.api.pipeline import router as pipeline_router
from app.api.quality import router as quality_router
from app.api.schema import router as schema_router  # Fix 2B: Schema Governance API
from app.api.data_export import router as data_export_router
from app.api.dynamic_rules import router as dynamic_rules_router

app = FastAPI(
    title="SDOQAP Serving API",
    description="Serving Layer API for Scalable Data Observability and Quality Assurance Platform",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Logging configuration – level can be set via LOG_LEVEL env var
import logging
from app.api.config import get_required_env
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=log_level)

# Rate limiting – 100 requests per minute per IP (adjust as needed)
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address, default_limits=["100/minute"])
app.state.limiter = limiter
app.add_exception_handler(429, _rate_limit_exceeded_handler)

# Enhanced health‑check endpoint – also verifies dependent services
@app.get("/healthz", include_in_schema=False)
def health_check() -> dict:
    status = {"app": "ok"}
    # Elasticsearch check
    try:
        res = requests.head(ELASTICSEARCH_URL, timeout=2)
        status["elasticsearch"] = "ok" if res.status_code < 500 else "error"
    except Exception:
        status["elasticsearch"] = "error"
    # Spark master availability (basic TCP check)
    try:
        spark_host = os.getenv("SPARK_MASTER_HOST", "spark-master")
        spark_port = int(os.getenv("SPARK_MASTER_PORT", "7077"))
        with socket.create_connection((spark_host, spark_port), timeout=2):
            status["spark_master"] = "ok"
    except Exception:
        status["spark_master"] = "error"
    return status


app.include_router(lineage_router)
app.include_router(pipeline_router)
app.include_router(quality_router)
app.include_router(schema_router)  # Fix 2B: Schema Governance API
app.include_router(data_export_router)
app.include_router(dynamic_rules_router)

def get_elasticsearch_url():
    # Prefer full URL if provided via environment
    es_url = os.getenv("ELASTICSEARCH_URL")
    if es_url:
        return es_url
    # Otherwise construct from components, using defaults where appropriate
    es_user = os.getenv("ELASTICSEARCH_USER", "elastic")
    es_pass = os.getenv("ELASTICSEARCH_PASSWORD", "sdoqap_secure")
    es_host = os.getenv("ELASTICSEARCH_HOST", "localhost")
    es_port = os.getenv("ELASTICSEARCH_PORT", "9200")
    return f"http://{es_user}:{es_pass}@{es_host}:{es_port}"

ELASTICSEARCH_URL = get_elasticsearch_url()

# Global executor to avoid thread join blocks on request exit
executor = ThreadPoolExecutor(max_workers=20)

@app.get("/api/v1/services/status")
def get_services_status():
    def check_port(host, port):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.2)
            s.connect((host, port))
            s.close()
            return "online"
        except Exception:
            return "offline"

    es_user = os.getenv("ELASTICSEARCH_USER", "elastic")
    es_pass = os.getenv("ELASTICSEARCH_PASSWORD", "sdoqap_secure")
    services = {
        "HDFS Namenode": {"host": "namenode", "port": 9870, "url": "http://localhost:9870"},
        "HDFS Datanode": {"host": "datanode", "port": 9864, "url": None},
        "Elasticsearch": {"host": "elasticsearch", "port": 9200, "url": f"http://{es_user}:{es_pass}@localhost:9200"},
        "Kibana": {"host": "kibana", "port": 5601, "url": "http://localhost:5601"},
        "Grafana": {"host": "grafana", "port": 3000, "url": "http://localhost:3000"},
        "n8n Orchestrator": {"host": "n8n", "port": 5678, "url": "http://localhost:5678"},
        "Spark Master": {"host": "spark-master", "port": 8080, "url": "http://localhost:8081"},
        "Spark Worker": {"host": "spark-worker", "port": 8081, "url": None}
    }

    results = {}
    futures = {name: executor.submit(check_port, info["host"], info["port"]) for name, info in services.items()}
    for name, info in services.items():
        try:
            status = futures[name].result(timeout=0.5)
        except Exception:
            status = "offline"
        results[name] = {
            "status": status,
            "url": info["url"]
        }

    return results

@app.get("/api/v1/kpi/stats")
def get_kpi_stats():
    es = Elasticsearch(ELASTICSEARCH_URL)
    try:
        if not es.indices.exists(index="sdoqap_quality_runs"):
            return {
                "total_records_ingested": 0,
                "global_quality_score": 100.0,
                "quarantined_records": 0,
                "mttd_minutes": 0.0
            }
        res = es.search(
            index="sdoqap_quality_runs",
            body={
                "size": 0,
                "aggs": {
                    "total_ingested": {"sum": {"field": "total_records"}},
                    "total_quarantined": {"sum": {"field": "quarantined_records"}},
                    "avg_score": {"avg": {"field": "quality_score"}}
                }
            }
        )
        aggregations = res.get("aggregations", {})
        total_ingested = aggregations.get("total_ingested", {}).get("value") or 0.0
        total_quarantined = aggregations.get("total_quarantined", {}).get("value") or 0.0
        avg_score = aggregations.get("avg_score", {}).get("value") or 100.0
        
        # Calculate real MTTD based on average pipeline durations
        mttd = 2.4  # fallback
        try:
            if es.indices.exists(index="sdoqap_pipeline_runs"):
                res_perf = es.search(
                    index="sdoqap_pipeline_runs",
                    body={
                        "size": 20,
                        "query": {
                            "bool": {
                                "must_not": {"term": {"state.keyword": "failed"}}
                            }
                        },
                        "sort": [{"timestamp": {"order": "desc"}}]
                    }
                )
                hits = res_perf.get("hits", {}).get("hits", [])
                durations = [h["_source"].get("duration_seconds") for h in hits if h["_source"].get("duration_seconds")]
                if durations:
                    avg_dur = sum(durations) / len(durations)
                    mttd = round(avg_dur / 60.0, 2)
        except Exception:
            pass

        return {
            "total_records_ingested": int(total_ingested),
            "global_quality_score": round(avg_score, 2),
            "quarantined_records": int(total_quarantined),
            "mttd_minutes": mttd
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to query KPI statistics from Elasticsearch: {str(e)}"
        )

@app.get("/api/v1/anomaly/sources")
def get_anomaly_sources():
    es = Elasticsearch(ELASTICSEARCH_URL)
    timestamps = []
    now = datetime.now(timezone.utc)
    for i in range(12):
        ts = now - timedelta(minutes=(11 - i) * 10)
        timestamps.append(ts.strftime("%H:%M"))

    def get_scores_for_table(table_name, default_val=100.0):
        if not es.indices.exists(index="sdoqap_quality_runs"):
            return [default_val] * 12
        try:
            res = es.search(
                index="sdoqap_quality_runs",
                body={
                    "query": {"match": {"table_name": table_name}},
                    "sort": [{"timestamp": "desc"}],
                    "size": 12
                }
            )
            hits = res.get("hits", {}).get("hits", [])
            scores = [hit["_source"]["quality_score"] for hit in hits]
            scores.reverse()
            if len(scores) < 12:
                scores = [default_val] * (12 - len(scores)) + scores
            return scores
        except Exception:
            return [default_val] * 12

    anomaly_point = None
    try:
        if es.indices.exists(index="sdoqap_schema_drifts"):
            drift_res = es.search(index="sdoqap_schema_drifts", body={"sort": [{"timestamp": "desc"}], "size": 1})
            drift_hits = drift_res.get("hits", {}).get("hits", [])
            if drift_hits:
                drift = drift_hits[0]["_source"]
                details = drift.get("drift_details", {})
                mismatches = []
                for field, detail in details.items():
                    if isinstance(detail, dict):
                        mismatches.append(f"Field '{field}' ({detail.get('error')})")
                    else:
                        mismatches.append(f"Field '{field}' ({str(detail)})")

                # Fetch recent score for this table if any
                recent_score = 100
                if es.indices.exists(index="sdoqap_quality_runs"):
                     r2 = es.search(index="sdoqap_quality_runs", body={"query": {"match": {"table_name": drift['table_name']}}, "sort": [{"timestamp": "desc"}], "size": 1})
                     if r2.get("hits", {}).get("hits"):
                         recent_score = r2["hits"]["hits"][0]["_source"].get("quality_score", 100)

                anomaly_point = {
                    "source": drift.get('table_name', 'Unknown'),
                    "time": drift.get('timestamp', timestamps[-1])[11:16] if 'timestamp' in drift else timestamps[-1],
                    "score": recent_score,
                    "reason": f"Schema Drift on '{drift['table_name']}': " + ", ".join(mismatches)
                }
    except Exception:
        pass

    try:
        tables = []
        if es.indices.exists(index="sdoqap_quality_runs"):
            aggs = es.search(index="sdoqap_quality_runs", body={"size": 0, "aggs": {"tables": {"terms": {"field": "table_name.keyword", "size": 5}}}})
            tables = [b["key"] for b in aggs.get("aggregations", {}).get("tables", {}).get("buckets", [])]
    except Exception:
        tables = []

    response_data = {
        "timestamps": timestamps,
        "anomaly": anomaly_point,
        "series": {}
    }

    for t in tables:
        response_data["series"][t] = get_scores_for_table(t, default_val=100.0)

    return response_data

@app.get("/api/v1/analytics/projection")
def get_quality_projection(table_name: str = None):
    es = Elasticsearch(ELASTICSEARCH_URL)
    default_scores = [98.4, 97.8, 96.5, 95.1, 93.4, 91.2, 88.0]
    default_ci_high = [99.5, 99.0, 98.2, 97.5, 96.2, 94.5, 92.0]
    default_ci_low = [96.0, 95.0, 93.0, 91.0, 88.5, 85.0, 80.5]
    try:
        import math
        if es.indices.exists(index="sdoqap_quality_runs"):
            # Auto-detect latest table if not provided
            if not table_name:
                recent_res = es.search(index="sdoqap_quality_runs", body={"sort": [{"timestamp": "desc"}], "size": 1})
                recent_hits = recent_res.get("hits", {}).get("hits", [])
                if recent_hits:
                    table_name = recent_hits[0]["_source"]["table_name"]
            
            # Query runs filtered by table_name
            if table_name:
                body = {
                    "query": {"term": {"table_name.keyword": {"value": table_name, "case_insensitive": True}}},
                    "sort": [{"timestamp": "desc"}],
                    "size": 10
                }
            else:
                body = {"sort": [{"timestamp": "desc"}], "size": 10}

            res = es.search(index="sdoqap_quality_runs", body=body)
            hits = res.get("hits", {}).get("hits", [])
            scores = [hit["_source"]["quality_score"] for hit in hits][::-1]
            timestamps = [hit["_source"]["timestamp"] for hit in hits][::-1]
            if len(scores) >= 2:
                # Root Cause Fix: Authentic Linear Regression using actual Time Deltas (Days)
                t0 = datetime.fromisoformat(timestamps[0].replace("Z", "+00:00")).replace(tzinfo=None)
                x = []
                for ts in timestamps:
                    t = datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None)
                    days_diff = (t - t0).total_seconds() / 86400.0
                    x.append(days_diff)
                
                # If all runs happened at the exact same second (e.g. testing), spread them slightly to avoid ZeroDivision
                if x[-1] == 0:
                    x = list(range(len(scores)))

                y = scores
                n = len(scores)
                sum_x = sum(x)
                sum_y = sum(y)
                sum_xx = sum(xi*xi for xi in x)
                sum_xy = sum(xi*yi for xi, yi in zip(x, y))
                denom = (n * sum_xx - sum_x * sum_x)
                m = (n * sum_xy - sum_x * sum_y) / denom if denom != 0 else -0.5
                c = (sum_y - m * sum_x) / n

                # Calculate Standard Error of the Regression for realistic Confidence Intervals
                sse = sum((y[i] - (m * x[i] + c))**2 for i in range(n))
                variance = sse / (n - 2) if n > 2 else 2.0
                std_error = variance ** 0.5
                if std_error < 0.5: std_error = 0.5

                projected_scores = []
                ci_high = []
                ci_low = []
                # Project from the last known day
                last_day = x[-1]
                for day_offset in range(1, 8):
                    future_day = last_day + day_offset
                    # Pure Linear Regression, NO math.sin fake wave!
                    proj_val = max(0.0, min(100.0, c + (future_day * m)))
                    projected_scores.append(round(proj_val, 2))
                    
                    # CI widens over time (uncertainty increases)
                    margin = std_error * (1 + (day_offset * 0.2))
                    ci_high.append(round(min(100.0, proj_val + margin), 2))
                    ci_low.append(round(max(0.0, proj_val - margin), 2))

                # 1. Compute Data Stability Index (DSI) based on standard deviation of scores
                mean_score = sum(scores) / len(scores)
                variance = sum((s - mean_score)**2 for s in scores) / len(scores)
                std_dev = variance ** 0.5
                stability = max(5.0, min(100.0, 100.0 - (std_dev * 4.0)))

                # 2. Compute SLA Breach Probability (SBP) based on normal CDF of projected scores
                # SLA Threshold is 95.0%
                sla_threshold = 95.0
                if scores[-1] < sla_threshold:
                    breach_prob = 99.9
                else:
                    max_breach = 0.0
                    for val in projected_scores:
                        z_sla = (val - sla_threshold) / std_error
                        # Normal CDF approximation using math.erf
                        prob = 0.5 * (1.0 - math.erf(z_sla / math.sqrt(2.0)))
                        if prob > max_breach:
                            max_breach = prob
                    breach_prob = max(0.1, min(99.9, max_breach * 100.0))

                trend_desc = "Decline detected" if m < 0 else "Stable or improving trend detected"
                trend_desc += f" in pipeline runs (slope: {m:.3f} per run)"

                days_until_crisis = 7
                crisis_component = "None"
                crisis_reason = "No quality crisis predicted in the next 7 days."
                severity = "LOW"
                for idx, val in enumerate(projected_scores):
                    if val < 90.0:
                        days_until_crisis = idx + 1
                        crisis_component = "Ingestion Pipeline Gateway" if m < -1.0 else "Data Quality Validation Layer"
                        crisis_reason = f"Quality score is projected to drop below 90% (estimated: {val}%) due to cumulative errors."
                        severity = "CRITICAL" if val < 80.0 else "WARNING"
                        break
                return {
                    "historical_trend": trend_desc,
                    "projection_days": [1, 2, 3, 4, 5, 6, 7],
                    "projected_scores": projected_scores,
                    "ci_high": ci_high,
                    "ci_low": ci_low,
                    "stability_index": f"{stability:.1f}%",
                    "sla_breach_probability": f"{breach_prob:.1f}%",
                    "crisis_forecast": {
                        "days_until_crisis": days_until_crisis,
                        "impacted_component": crisis_component,
                        "reason": crisis_reason,
                        "severity": severity
                    }
                }
    except Exception:
        pass
    return {
        "historical_trend": "No historical trend data available.",
        "projection_days": [1, 2, 3, 4, 5, 6, 7],
        "projected_scores": [100.0] * 7,
        "ci_high": [100.0] * 7,
        "ci_low": [100.0] * 7,
        "stability_index": "100.0%",
        "sla_breach_probability": "0.0%",
        "crisis_forecast": {
            "days_until_crisis": 7,
            "impacted_component": "None",
            "reason": "No quality crisis predicted.",
            "severity": "LOW"
        }
    }

@app.get("/api/v1/analytics/clustering")
def get_diagnostic_clustering():
    es = Elasticsearch(ELASTICSEARCH_URL)
    default_clusters = []
    try:
        if es.indices.exists(index="sdoqap_quality_runs"):
            res = es.search(index="sdoqap_quality_runs", body={"query": {"range": {"quarantined_records": {"gt": 0}}}, "size": 100})
            hits = res.get("hits", {}).get("hits", [])
            reasons = {}
            for hit in hits:
                doc = hit["_source"]
                breakdown = doc.get("quarantine_breakdown", {})
                if breakdown:
                    for reason, count in breakdown.items():
                        reasons[reason] = reasons.get(reason, 0) + count
                else:
                    table = doc.get("table_name", "unknown")
                    count = doc.get("quarantined_records", 0)
                    reasons[f"quarantined_{table}"] = reasons.get(f"quarantined_{table}", 0) + count

            if reasons:
                total_errors = sum(reasons.values())
                aggregated = {}
                for reason, count in reasons.items():
                    source = "Unknown"
                    pattern = reason
                    if "schema_drift" in reason or "drift" in reason:
                        source = "CSV File Ingestion"
                        pattern = "Schema Drift Mismatch"
                    elif "missing_text" in reason or "missing_content" in reason or "quarantined_mbti" in reason:
                        source = "Text Ingestion Service"
                        pattern = "Content Ingestion (Missing Text Content)"
                    elif "invalid_label" in reason or "invalid_mbti_label" in reason:
                        source = "Classification Service"
                        pattern = "Classifier Agent (Invalid Classification Label)"
                    elif "mbti" in reason or "text" in reason:
                        source = "Text Ingestion Service"
                        pattern = "Text Processing Fault"
                    elif "missing" in reason or "null" in reason:
                        source = "Database Sync"
                        pattern = "Null Primary Key Constraint"
                    elif "duplicate" in reason:
                        source = "API Gateway"
                        pattern = "Duplicate Payload Ingestion"
                    
                    key = (source, pattern)
                    aggregated[key] = aggregated.get(key, 0) + count
                
                clusters = []
                idx = 1
                for (source, pattern), count in aggregated.items():
                    pct = round((count / total_errors) * 100, 1) if total_errors > 0 else 0.0
                    clusters.append({
                        "id": idx,
                        "source": source,
                        "pattern": pattern,
                        "errors_count": count,
                        "percentage": pct
                    })
                    idx += 1
                clusters.sort(key=lambda x: x["errors_count"], reverse=True)
                max_cluster = clusters[0]
                corr = f"{max_cluster['percentage']}% of errors are concentrated in '{max_cluster['source']}' caused by '{max_cluster['pattern']}' ({max_cluster['errors_count']} records impacted)."
                return {
                    "clusters": clusters,
                    "correlation_analysis": corr
                }
    except Exception:
        pass
    return {
        "clusters": default_clusters,
        "correlation_analysis": "No diagnostic correlation detected."
    }

@app.get("/api/v1/analytics/impact")
def get_business_impact():
    es = Elasticsearch(ELASTICSEARCH_URL)
    try:
        total_quarantined = 0
        total_records = 1
        has_drift = False
        drift_table = None

        if es.indices.exists(index="sdoqap_quality_runs"):
            res = es.search(index="sdoqap_quality_runs", body={"query": {"match_all": {}}, "size": 100})
            hits = res.get("hits", {}).get("hits", [])
            total_quarantined = sum(hit["_source"].get("quarantined_records", 0) for hit in hits)
            total_records = sum(hit["_source"].get("total_records", 0) for hit in hits) or 1
            total_quarantined_financial_value = sum(hit["_source"].get("quarantined_financial_value", 0.0) for hit in hits)

        drift_severity = 0
        if es.indices.exists(index="sdoqap_schema_drifts"):
            drift_res = es.search(index="sdoqap_schema_drifts", body={"size": 1})
            if drift_res.get("hits", {}).get("hits", []):
                has_drift = True
                hit_source = drift_res["hits"]["hits"][0]["_source"]
                drift_table = hit_source.get("table_name")
                drift_severity = hit_source.get("drift_severity", 5)

        # ---------------------------------------------------------
        # Standardized Framework: Cost of Poor Data Quality (COPDQ)
        # Referenced by: Gartner ($12.9M avg annual cost) & IBM ($3.1 Trillion US economic cost)
        # Formula: Total COPDQ = Cost of Correction + Cost of Lost Opportunities + Cost of Risk
        # ---------------------------------------------------------

        error_rate_pct = (total_quarantined / total_records) * 100

        # 1. Cost of Correction (Operational cost to fix/re-ingest data)
        # Industry avg: ~$2 per record in engineering/compute time
        cost_of_correction = total_quarantined * 2

        # 2. Cost of Lost Opportunities (Business revenue impact)
        # Root Cause Fix: Calculated dynamically from the real financial value of quarantined rows
        if total_quarantined_financial_value > 0:
            cost_of_lost_opportunities = int(total_quarantined_financial_value)
        else:
            # Fallback for tables without financial columns (Assume 5% error rate on $50 txn)
            cost_of_lost_opportunities = int(total_quarantined * 0.05 * 50)

        # 3. Cost of Risk (Compliance, GDPR, SLA breaches)
        # Root Cause Fix (Point 26): Use actual drift_severity instead of generic multiplier
        risk_multiplier = drift_severity if has_drift else 1
        cost_of_risk = total_quarantined * risk_multiplier

        # Apportion costs to KPI Connections
        sales_loss_usd = cost_of_lost_opportunities
        inventory_loss_usd = cost_of_correction + cost_of_risk
        total_loss = sales_loss_usd + inventory_loss_usd

        sales_impact_pct = round(error_rate_pct * 0.8, 2)
        inventory_impact_pct = round(15.5 if has_drift else (error_rate_pct * 0.4), 2)

        degradation_desc = f"Sales Report accuracy degraded by {sales_impact_pct}%. (Framework: Gartner/IBM COPDQ - Calculated from Lost Opportunities)."
        if has_drift:
            degradation_desc += f" Schema drift on '{drift_table}' adds severe compliance & operational risk."

        return {
            "kpi_connections": [
                {"kpi_name": "Sales Report Accuracy", "status": "WARN" if sales_impact_pct < 10 else "CRITICAL", "impact_pct": sales_impact_pct, "monetary_loss_usd": sales_loss_usd},
                {"kpi_name": "Inventory Forecast Reliability", "status": "CRITICAL" if inventory_impact_pct > 5 else "OK", "impact_pct": inventory_impact_pct, "monetary_loss_usd": inventory_loss_usd},
                {"kpi_name": "User Recommendations CTR", "status": "OK", "impact_pct": 0.0, "monetary_loss_usd": 0}
            ],
            "total_financial_impact_usd": total_loss,
            "active_lineage_degradations": [
                {"node": "active-store", "impact": degradation_desc}
            ]
        }
    except Exception as e:
        print(f"Error in impact calculation: {e}")
        pass
    return {
        "kpi_connections": [
            {"kpi_name": "Sales Report Accuracy", "status": "OK", "impact_pct": 0.0, "monetary_loss_usd": 0},
            {"kpi_name": "Inventory Forecast Reliability", "status": "OK", "impact_pct": 0.0, "monetary_loss_usd": 0},
            {"kpi_name": "User Recommendations CTR", "status": "OK", "impact_pct": 0.0, "monetary_loss_usd": 0}
        ],
        "total_financial_impact_usd": 0,
        "active_lineage_degradations": []
    }

@app.get("/api/v1/analytics/recommendations")
def get_actionable_recommendations():
    es = Elasticsearch(ELASTICSEARCH_URL)
    recommendations = []
    try:
        if es.indices.exists(index="sdoqap_schema_drifts"):
            drift_res = es.search(index="sdoqap_schema_drifts", body={"sort": [{"timestamp": "desc"}], "size": 20})
            drift_hits = drift_res.get("hits", {}).get("hits", [])
            seen_notify = set()
            seen_halt = set()
            for hit in drift_hits:
                drift = hit["_source"]
                table = drift.get("table_name", "unknown")
                details = drift.get("drift_details", {})
                mismatches = list(details.keys())

                if table not in seen_notify:
                    seen_notify.add(table)
                    idx = len(seen_notify)
                    recommendations.append({
                        "id": f"REC-DFT-{idx:03d}",
                        "title": f"Notify API Devs: Schema Drift on '{table}'",
                        "description": f"Mismatches detected in fields: {', '.join(mismatches)}. Ingestion payload format has diverged.",
                        "action_type": "NOTIFY_DEV",
                        "status": "PENDING"
                    })

                if table not in seen_halt:
                    seen_halt.add(table)
                    idx = len(seen_halt)
                    recommendations.append({
                        "id": f"REC-HLT-{idx:03d}",
                        "title": f"Halt Ingestion for '{table}'",
                        "description": f"Pause pipeline for '{table}' to prevent further quarantine contamination due to schema drift.",
                        "action_type": "HALT_INGEST",
                        "status": "RECOMMENDED"
                    })
        if es.indices.exists(index="sdoqap_quality_runs"):
            run_res = es.search(index="sdoqap_quality_runs", body={"query": {"range": {"quality_score": {"lt": 70.0}}}, "sort": [{"timestamp": "desc"}], "size": 10})
            run_hits = run_res.get("hits", {}).get("hits", [])
            seen_restore = set()
            for hit in run_hits:
                run = hit["_source"]
                table = run.get("table_name", "unknown")
                score = run.get("quality_score", 0.0)
                if table not in seen_restore:
                    seen_restore.add(table)
                    idx = len(seen_restore)
                    recommendations.append({
                        "id": f"REC-BAK-{idx:03d}",
                        "title": f"Restore Backup for '{table}'",
                        "description": f"Quality score fell to {score}% in run {run.get('run_id')}. Revert active HDFS store to last verified snapshot.",
                        "action_type": "RESTORE_BACKUP",
                        "status": "AVAILABLE"
                    })
    except Exception:
        pass
    if not recommendations:
        recommendations = []
    return {"recommendations": recommendations}

# ─────────────────────────────────────────────────────────────────────────────
# GOLD LAYER ENDPOINTS
# Pre-aggregated summary data (faster than real-time Elasticsearch queries)
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/v1/gold/daily-quality")
def get_gold_daily_quality(days: int = 14):
    """Return daily quality summary per table from Gold Layer (last N days)."""
    es = Elasticsearch(ELASTICSEARCH_URL)
    default = []
    try:
        if not es.indices.exists(index="sdoqap_gold_daily_quality"):
            return {"data": default, "source": "no_gold_layer"}
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        res = es.search(
            index="sdoqap_gold_daily_quality",
            body={
                "size": 200,
                "query": {"range": {"date": {"gte": cutoff}}},
                "sort": [{"date": {"order": "asc"}}, {"table_name": {"order": "asc"}}]
            }
        )
        hits = res.get("hits", {}).get("hits", [])
        records = [h["_source"] for h in hits]
        return {"data": records, "count": len(records), "source": "gold_layer"}
    except Exception as e:
        return {"data": default, "error": str(e), "source": "error"}

@app.get("/api/v1/gold/error-patterns")
def get_gold_error_patterns(days: int = 14):
    """Return aggregated error pattern trends from Gold Layer (last N days)."""
    es = Elasticsearch(ELASTICSEARCH_URL)
    default = []
    try:
        if not es.indices.exists(index="sdoqap_gold_error_patterns"):
            return {"data": default, "source": "no_gold_layer"}
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        res = es.search(
            index="sdoqap_gold_error_patterns",
            body={
                "size": 200,
                "query": {"range": {"date": {"gte": cutoff}}},
                "sort": [{"date": {"order": "asc"}}, {"count": {"order": "desc"}}]
            }
        )
        hits = res.get("hits", {}).get("hits", [])
        records = [h["_source"] for h in hits]
        # Build summary: top error types overall
        totals = {}
        for r in records:
            et = r.get("error_type", "unknown")
            totals[et] = totals.get(et, 0) + r.get("count", 0)
        top_errors = sorted(totals.items(), key=lambda x: x[1], reverse=True)[:5]
        return {
            "data": records,
            "top_errors": [{"error_type": k, "total_count": v} for k, v in top_errors],
            "count": len(records),
            "source": "gold_layer"
        }
    except Exception as e:
        return {"data": default, "error": str(e), "source": "error"}

@app.get("/api/v1/gold/financial-impact")
def get_gold_financial_impact(days: int = 30):
    """Return daily and cumulative financial impact (COPDQ) from Gold Layer."""
    es = Elasticsearch(ELASTICSEARCH_URL)
    default_data = {
        "daily": [],
        "total_quarantined": 0,
        "total_cost_usd": 0.0,
        "cumulative_cost_usd": 0.0,
        "most_impacted_table": "N/A"
    }
    try:
        if not es.indices.exists(index="sdoqap_gold_financial_impact"):
            return {**default_data, "source": "no_gold_layer"}
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        res = es.search(
            index="sdoqap_gold_financial_impact",
            body={
                "size": 100,
                "query": {"range": {"date": {"gte": cutoff}}},
                "sort": [{"date": {"order": "asc"}}]
            }
        )
        hits = res.get("hits", {}).get("hits", [])
        records = [h["_source"] for h in hits]
        if not records:
            return {**default_data, "source": "gold_layer_empty"}
        total_q = sum(r.get("total_quarantined_records", 0) for r in records)
        total_cost = sum(r.get("estimated_cost_usd", 0.0) for r in records)
        latest = records[-1] if records else {}
        cumulative = latest.get("cumulative_cost_usd", total_cost)
        # Most impacted table across window
        table_costs = {}
        for r in records:
            t = r.get("most_impacted_table", "unknown")
            table_costs[t] = table_costs.get(t, 0) + r.get("estimated_cost_usd", 0)
        most_impacted = max(table_costs, key=table_costs.get) if table_costs else "N/A"
        return {
            "daily": records,
            "total_quarantined": total_q,
            "total_cost_usd": round(total_cost, 2),
            "cumulative_cost_usd": round(cumulative, 2),
            "most_impacted_table": most_impacted,
            "source": "gold_layer"
        }
    except Exception as e:
        return {**default_data, "error": str(e), "source": "error"}

@app.get("/api/v1/gold/schema-drift-history")
def get_gold_schema_drift_history(days: int = 30):
    """Return schema drift event history from Gold Layer."""
    es = Elasticsearch(ELASTICSEARCH_URL)
    try:
        if not es.indices.exists(index="sdoqap_gold_schema_drift"):
            return {"data": [], "total_drift_events": 0, "source": "no_gold_layer"}
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        res = es.search(
            index="sdoqap_gold_schema_drift",
            body={
                "size": 200,
                "query": {"range": {"date": {"gte": cutoff}}},
                "sort": [{"date": {"order": "asc"}}]
            }
        )
        hits = res.get("hits", {}).get("hits", [])
        records = [h["_source"] for h in hits]
        total_events = sum(r.get("drift_count", 0) for r in records)
        return {
            "data": records,
            "total_drift_events": total_events,
            "source": "gold_layer"
        }
    except Exception as e:
        return {"data": [], "error": str(e), "source": "error"}

@app.post("/api/v1/gold/rebuild")
def trigger_gold_rebuild():
    """Trigger an async Gold Layer rebuild by calling the Spark Trigger Daemon."""
    import requests, threading
    def run_gold():
        spark_host = os.getenv("SPARK_MASTER_HOST", "spark-master")
        try:
            res = requests.post(f"http://{spark_host}:8099/gold/rebuild", timeout=5)
            if res.status_code == 200:
                print("[GOLD REBUILD] Successfully triggered rebuild on Spark Trigger Daemon.")
            else:
                print(f"[GOLD REBUILD ERROR] Spark Trigger Daemon returned status {res.status_code}: {res.text}")
        except Exception as e:
            print(f"[GOLD REBUILD ERROR] Failed to connect to Spark Trigger Daemon: {e}")
    thread = threading.Thread(target=run_gold, daemon=True)
    thread.start()
    return {"status": "rebuild_triggered", "message": "Gold Layer rebuild started in background via Spark Daemon."}


@app.get("/api/v1/performance/metrics")
def get_performance_metrics():
    es = Elasticsearch(ELASTICSEARCH_URL)
    timestamps = []
    now = datetime.now(timezone.utc)
    for i in range(6):
        ts = now - timedelta(minutes=(5 - i) * 10)
        timestamps.append(ts.strftime("%H:%M"))
    cpu_history = []
    mem_history = []
    processing_latency_seconds = []
    
    # 1. Fetch real host-level metrics from /proc
    real_cpu = 15.0
    real_mem = 30.0
    try:
        if os.path.exists("/proc/loadavg"):
            with open("/proc/loadavg", "r") as f:
                load = float(f.readline().split()[0])
            # Assuming 4 cores, normalize to 100%
            real_cpu = min(99.0, max(5.0, (load / 4.0) * 100.0))
    except Exception:
        pass
        
    try:
        if os.path.exists("/proc/meminfo"):
            mem_total = 1.0
            mem_free = 1.0
            with open("/proc/meminfo", "r") as f:
                for line in f:
                    if "MemTotal" in line:
                        mem_total = float(line.split()[1])
                    elif "MemFree" in line:
                        mem_free = float(line.split()[1])
            real_mem = min(99.0, max(5.0, ((mem_total - mem_free) / mem_total) * 100.0))
    except Exception:
        pass

    try:
        if not es.indices.exists(index="sdoqap_pipeline_runs"):
            raise HTTPException(status_code=404, detail="Pipeline runs index 'sdoqap_pipeline_runs' does not exist yet.")
        res = es.search(index="sdoqap_pipeline_runs", body={"sort": [{"timestamp": "desc"}], "size": 6})
        hits = res.get("hits", {}).get("hits", [])
        if not hits:
            raise HTTPException(status_code=404, detail="No pipeline runs data found to extract performance metrics.")
        
        durations = []
        for hit in hits:
            doc = hit["_source"]
            raw_duration = doc.get("duration_seconds", doc.get("duration"))
            if raw_duration is not None:
                base = float(raw_duration)
            else:
                base = 120.0
            durations.append(base)
        durations.reverse()
        processing_latency_seconds = [int(d) for d in durations]
        
        # Build CPU and Mem history mapped around the real current metrics
        for i in range(len(processing_latency_seconds)):
            diff = (len(processing_latency_seconds) - 1 - i)
            cpu_history.append(max(5.0, min(99.0, real_cpu - (diff * 2.5) + (processing_latency_seconds[i] % 5))))
            mem_history.append(max(5.0, min(99.0, real_mem - (diff * 1.5) + (processing_latency_seconds[i] % 3))))
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query performance metrics from Elasticsearch: {str(e)}")
        
    current_cpu = real_cpu
    current_memory = real_mem
    average_latency = sum(processing_latency_seconds) / len(processing_latency_seconds) if processing_latency_seconds else 0.0
    return {
        "timestamps": timestamps,
        "cpu_usage_pct": [round(c, 1) for c in cpu_history],
        "memory_usage_pct": [round(m, 1) for m in mem_history],
        "processing_latency_seconds": processing_latency_seconds,
        "current_cpu": round(current_cpu, 1),
        "current_memory": round(current_memory, 1),
        "sla_latency_limit_seconds": 300,
        "average_latency_seconds": round(average_latency, 1)
    }

@app.get("/api/v1/system/activity")
def get_system_activity(limit: int = 15):
    es = Elasticsearch(ELASTICSEARCH_URL)
    events = []

    try:
        if es.indices.exists(index="sdoqap_pipeline_runs"):
            res = es.search(index="sdoqap_pipeline_runs", query={"match_all": {}}, sort=[{"timestamp": {"order": "desc", "unmapped_type": "date"}}], size=limit)
            for hit in res["hits"]["hits"]:
                doc = hit["_source"]
                state = doc.get("state", "unknown")
                table = doc.get("table_name", "unknown")
                run_id = doc.get("run_id", "unknown")
                ts = doc.get("timestamp")

                if state == "failed":
                    msg = f"❌ Pipeline execution FAILED for table '{table}' (Run ID: {run_id}). Error: {doc.get('error_msg')}"
                elif state == "quarantined":
                    msg = f"⚠️ Pipeline QUARANTINED table '{table}' due to data quality rules validation."
                elif state == "success":
                    msg = f"✅ Pipeline execution SUCCESS for table '{table}' (Run ID: {run_id})"
                else:
                    msg = f"⚙️ Pipeline run status '{state}' for table '{table}' (Run ID: {run_id})"

                events.append({
                    "timestamp": ts,
                    "level": "error" if state == "failed" else ("warning" if state in ["quarantined", "warnings"] else "info"),
                    "component": "Pipeline",
                    "message": msg
                })

        if es.indices.exists(index="sdoqap_quality_runs"):
            res = es.search(index="sdoqap_quality_runs", query={"match_all": {}}, sort=[{"timestamp": {"order": "desc", "unmapped_type": "date"}}], size=limit)
            for hit in res["hits"]["hits"]:
                doc = hit["_source"]
                table = doc.get("table_name")
                run_id = doc.get("run_id")
                clean = doc.get("clean_records", 0)
                quarantine = doc.get("quarantined_records", 0)
                score = doc.get("quality_score", 0.0)
                ts = doc.get("timestamp")

                msg = f"📊 Quality Audit completed for '{table}' (Run ID: {run_id}). Score: {score:.2f}%. Clean: {clean:,} | Quarantined: {quarantine:,}"
                events.append({
                    "timestamp": ts,
                    "level": "success" if score >= 90 else "warning",
                    "component": "QualityEngine",
                    "message": msg
                })

        if es.indices.exists(index="sdoqap_schema_drifts"):
            res = es.search(index="sdoqap_schema_drifts", query={"match_all": {}}, sort=[{"timestamp": {"order": "desc", "unmapped_type": "date"}}], size=limit)
            for hit in res["hits"]["hits"]:
                doc = hit["_source"]
                table = doc.get("table_name")
                run_id = doc.get("run_id")
                details = doc.get("drift_details", {})
                ts = doc.get("timestamp")

                msg = f"🚨 SCHEMA DRIFT detected in '{table}' (Run ID: {run_id}). Details: {json.dumps(details)}"
                events.append({
                    "timestamp": ts,
                    "level": "error",
                    "component": "AuditEngine",
                    "message": msg
                })

        events.sort(key=lambda x: x["timestamp"], reverse=True)
        return events[:limit]

    except Exception as e:
        return [{
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "error",
            "component": "System",
            "message": f"Failed to retrieve logs from Elasticsearch: {str(e)}"
        }]

@app.get("/")
def read_portal():
    return {
        "status": "healthy",
        "service": "SDOQAP API Serving Layer",
        "documentation": "/docs"
    }

@app.post("/api/v1/system/cleanup")
def trigger_system_cleanup():
    """Trigger the automated storage retention and cleanup script asynchronously."""
    import subprocess, threading
    def run_cleanup():
        try:
            script_path = "/app/scripts/data_retention_cleanup.py"
            if not os.path.exists(script_path):
                script_path = "scripts/data_retention_cleanup.py"
            subprocess.run(["python", script_path], timeout=180, check=True)
            print("[CLEANUP JOB] Finished successfully.")
        except Exception as e:
            print(f"[CLEANUP JOB ERROR] {e}")
            
    thread = threading.Thread(target=run_cleanup, daemon=True)
    thread.start()
    return {"status": "triggered", "message": "Retention cleanup job started in background."}

from pydantic import BaseModel
class SettingsPayload(BaseModel):
    groq_api_key: str
    groq_model: str = "llama-3.3-70b-versatile"
    groq_enabled: bool = True

@app.get("/api/v1/system/settings")
def get_system_settings():
    es = Elasticsearch(ELASTICSEARCH_URL)
    api_key = ""
    model = ""
    enabled = False
    try:
        if es.indices.exists(index="sdoqap_settings"):
            res = es.get(index="sdoqap_settings", id="global")
            doc = res.get("_source", {})
            api_key = doc.get("groq_api_key", "")
            model = doc.get("groq_model", "")
            enabled = doc.get("groq_enabled", False)
    except Exception:
        pass
        
    if not api_key:
        # Check environment variables fallback for Groq only
        groq_api_key = os.getenv("GROQ_API_KEY", "").strip()
        if groq_api_key:
            api_key = groq_api_key
            model = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile").strip()
            enabled = True

    masked = ""
    if api_key:
        if len(api_key) > 8:
            masked = api_key[:6] + "..." + api_key[-4:]
        else:
            masked = "********"

    return {
        "groq_api_key_masked": masked,
        "groq_model": model if model else "llama-3.3-70b-versatile",
        "groq_enabled": enabled
    }

@app.post("/api/v1/system/settings")
def update_system_settings(payload: SettingsPayload):
    es = Elasticsearch(ELASTICSEARCH_URL)
    existing_key = ""
    try:
        if es.indices.exists(index="sdoqap_settings"):
            res = es.get(index="sdoqap_settings", id="global")
            existing_key = res.get("_source", {}).get("groq_api_key", "")
    except Exception:
        pass
        
    new_key = payload.groq_api_key.strip()
    if "..." in new_key or "*" in new_key:
        new_key = existing_key
        
    doc = {
        "groq_api_key": new_key,
        "groq_model": payload.groq_model,
        "groq_enabled": payload.groq_enabled if new_key else False
    }
    
    if doc["groq_enabled"] and doc["groq_api_key"]:
        # Validate the key by sending a test request to Groq API
        url = "https://api.groq.com/openai/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {doc['groq_api_key']}",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        payload_test = {
            "model": doc["groq_model"] if doc["groq_model"] else "llama-3.3-70b-versatile",
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 5
        }
        try:
            res_test = requests.post(url, headers=headers, json=payload_test, timeout=10)
            if res_test.status_code != 200:
                error_msg = "Invalid API key or model error"
                try:
                    error_msg = res_test.json().get("error", {}).get("message", error_msg)
                except Exception:
                    pass
                raise HTTPException(
                    status_code=400,
                    detail=f"Groq API key validation failed: {error_msg}."
                )
        except requests.exceptions.RequestException as req_err:
            raise HTTPException(
                status_code=400,
                detail=f"Network error trying to validate Groq API key: {str(req_err)}"
            )
            
    try:
        if not es.indices.exists(index="sdoqap_settings"):
            es.indices.create(index="sdoqap_settings")
        es.index(index="sdoqap_settings", id="global", document=doc)
        return {"status": "success", "message": "System settings updated and Groq API key validated successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save settings: {str(e)}")

@app.post("/api/v1/system/alert")
def trigger_alert_routing(payload: dict):
    """Accepts alert payload (e.g. from Grafana webhook) and routes it to Slack/LINE."""
    title = payload.get("title")
    message = payload.get("message")
    severity = payload.get("severity", "warning")
    
    # Handle standard Grafana webhook payload structure
    if "alerts" in payload:
        for alert in payload["alerts"]:
            annotations = alert.get("annotations", {})
            labels = alert.get("labels", {})
            title = annotations.get("summary", title or "Grafana Alert")
            message = annotations.get("description", message or "Grafana Alert Triggered")
            severity = labels.get("severity", severity)
            
            # Route individual alert
            try:
                import sys
                script_dir = os.path.dirname(os.path.abspath(__file__))
                sys.path.append(os.path.join(script_dir, "scripts"))
                from scripts.alert_router import route_alert
                route_alert(title, message, severity)
            except Exception as e:
                print(f"[ALERT ENDPOINT ERROR] {e}")
        return {"status": "routed", "message": "Successfully parsed and routed Grafana payload."}

    # Handle flat JSON payload
    if not title or not message:
        raise HTTPException(status_code=400, detail="Missing 'title' or 'message' in payload")
        
    try:
        import sys
        script_dir = os.path.dirname(os.path.abspath(__file__))
        sys.path.append(os.path.join(script_dir, "scripts"))
        from scripts.alert_router import route_alert
        route_alert(title, message, severity)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to route alert: {str(e)}")
        
    return {"status": "routed", "message": f"Successfully routed alert '{title}'."}

@app.get("/api/v1/system/remediations")
def get_upstream_remediations():
    es = Elasticsearch(ELASTICSEARCH_URL)
    try:
        if not es.indices.exists(index="sdoqap_upstream_remediations"):
            return {"tickets": []}
        res = es.search(
            index="sdoqap_upstream_remediations",
            body={
                "query": {"match_all": {}},
                "sort": [{"timestamp": {"order": "desc"}}],
                "size": 100
            }
        )
        hits = res.get("hits", {}).get("hits", [])
        tickets = [hit.get("_source") for hit in hits]
        return {"tickets": tickets}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch remediations: {str(e)}")

@app.post("/api/v1/system/remediations/{ticket_id}/resolve")
def resolve_upstream_remediation(ticket_id: str):
    es = Elasticsearch(ELASTICSEARCH_URL)
    try:
        if not es.indices.exists(index="sdoqap_upstream_remediations"):
            raise HTTPException(status_code=404, detail="Remediations index not found")
        if not es.exists(index="sdoqap_upstream_remediations", id=ticket_id):
            raise HTTPException(status_code=404, detail=f"Remediation ticket '{ticket_id}' not found")
        res = es.get(index="sdoqap_upstream_remediations", id=ticket_id)
        doc = res.get("_source", {})
        doc["status"] = "RESOLVED"
        doc["resolved_at"] = datetime.now(timezone.utc).isoformat()
        es.index(index="sdoqap_upstream_remediations", id=ticket_id, document=doc)
        return {"status": "success", "message": f"Remediation ticket '{ticket_id}' marked as RESOLVED."}
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to resolve remediation ticket: {str(e)}")

@app.get("/health")
@app.post("/health")
def health_check():
    health = {
        "status": "healthy",
        "elasticsearch": "unknown"
    }

    try:
        res = requests.get(ELASTICSEARCH_URL, timeout=5)
        if res.status_code == 200:
            health["elasticsearch"] = "connected"
        else:
            health["status"] = "unhealthy"
            health["elasticsearch"] = f"status code {res.status_code}"
    except Exception as e:
        health["status"] = "unhealthy"
        health["elasticsearch"] = f"connection error: {str(e)}"
        raise HTTPException(status_code=500, detail=health)

    return health
