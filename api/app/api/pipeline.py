import os
from datetime import datetime
import requests
from fastapi import APIRouter, HTTPException
from elasticsearch import Elasticsearch

router = APIRouter(
    prefix="/api/v1/pipeline",
    tags=["pipeline"]
)

def get_elasticsearch_url():
    from .config import get_required_env
    es_user = get_required_env("ELASTICSEARCH_USER")
    es_pass = get_required_env("ELASTICSEARCH_PASSWORD")
    es_host = get_required_env("ELASTICSEARCH_HOST")
    es_port = get_required_env("ELASTICSEARCH_PORT")
    if "ELASTICSEARCH_HOST" not in os.environ and "ELASTICSEARCH_URL" not in os.environ:
        es_host = "localhost"
    es_url = get_required_env("ELASTICSEARCH_URL")
    if not es_url:
        es_url = f"http://{es_user}:{es_pass}@{es_host}:{es_port}"
    return es_url

acknowledged_runs = set()

_es_client = None

def get_es_client():
    global _es_client
    if _es_client is None:
        try:
            _es_client = Elasticsearch(get_elasticsearch_url())
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to connect to Elasticsearch: {str(e)}")
    return _es_client

@router.get("")
def list_pipeline_runs(page: int = 1, size: int = 50, limit: int = 50, paginated: bool = False):
    """
    Retrieves the execution runs history from Elasticsearch with pagination.
    """
    es = get_es_client()
    try:
        if not es.indices.exists(index="sdoqap_pipeline_runs"):
            return {"data": [], "total": 0, "page": page, "size": size} if paginated else []
            
        actual_size = limit if limit != 50 else size
        from_idx = (page - 1) * actual_size
        
        # Prevent ES Deep Pagination memory blowup (> 10000 window limit)
        if from_idx + actual_size > 10000:
            raise HTTPException(
                status_code=400,
                detail="Deep pagination limit exceeded. Elasticsearch restricts offsets above 10,000 to prevent memory exhaustion."
            )
            
        res = es.search(
            index="sdoqap_pipeline_runs",
            query={"match_all": {}},
            sort=[{"timestamp": {"order": "desc"}}],
            from_=from_idx,
            size=actual_size
        )
        data = [hit["_source"] for hit in res["hits"]["hits"]]
        if paginated:
            total = res["hits"]["total"]["value"] if isinstance(res["hits"]["total"], dict) else res["hits"]["total"]
            return {"data": data, "total": total, "page": page, "size": actual_size}
        else:
            return data
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query failed: {str(e)}")

@router.get("/{run_id}")
def get_pipeline_run_detail(run_id: str):
    """
    Retrieves detailed validation logs and metadata for a specific execution run.
    """
    es = get_es_client()
    try:
        if not es.indices.exists(index="sdoqap_pipeline_runs"):
            raise HTTPException(status_code=404, detail="Pipeline runs index 'sdoqap_pipeline_runs' not found.")
            
        # Get run details
        res_run = es.search(
            index="sdoqap_pipeline_runs",
            query={"term": {"run_id.keyword": run_id}}
        )
        hits_run = res_run["hits"]["hits"]
        if not hits_run:
            raise HTTPException(status_code=404, detail=f"Pipeline run '{run_id}' not found.")
        run_detail = hits_run[0]["_source"]
        
        # Get associated quality checks
        quality = []
        if es.indices.exists(index="sdoqap_quality_runs"):
            res_q = es.search(
                index="sdoqap_quality_runs",
                query={"term": {"run_id.keyword": run_id}}
            )
            quality = [hit["_source"] for hit in res_q["hits"]["hits"]]
            
        # Get schema drift logs
        drifts = []
        if es.indices.exists(index="sdoqap_schema_drifts"):
            res_d = es.search(
                index="sdoqap_schema_drifts",
                query={"term": {"run_id.keyword": run_id}}
            )
            drifts = [hit["_source"] for hit in res_d["hits"]["hits"]]
            
        return {
            "run_details": run_detail,
            "quality_audits": quality,
            "schema_drift_alerts": [] if run_id in acknowledged_runs else drifts,
            "is_acknowledged": run_id in acknowledged_runs
        }
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query failed: {str(e)}")

@router.post("/acknowledge/{run_id}")
def acknowledge_run_drift(run_id: str):
    """
    Acknowledges the schema drift for a specific execution run.
    """
    acknowledged_runs.add(run_id)
    return {"status": "success", "run_id": run_id, "message": "Schema drift acknowledged."}


@router.post("/retry/{run_id}")
def retry_pipeline_run(run_id: str):
    """
    Retries/reruns ingestion and audit pipeline for a table.
    """
    es = get_es_client()
    try:
        # Get current run details to know the table name
        if not es.indices.exists(index="sdoqap_pipeline_runs"):
            raise HTTPException(status_code=404, detail="Index 'sdoqap_pipeline_runs' not found.")
        res = es.search(index="sdoqap_pipeline_runs", query={"term": {"run_id.keyword": run_id}})
        hits = res["hits"]["hits"]
        if not hits:
            raise HTTPException(status_code=404, detail=f"Pipeline run '{run_id}' not found.")
        run_doc = hits[0]["_source"]
        table_name = run_doc.get("table_name")
        if not table_name:
            raise HTTPException(status_code=400, detail="Cannot retry: Pipeline run document does not contain 'table_name'.")
        
        # Trigger actual spark job asynchronously via Spark Trigger Daemon HTTP API!
        triggered = False
        try:
            spark_host = os.getenv("SPARK_MASTER_HOST", "spark-master")
            res_trigger = requests.post(
                f"http://{spark_host}:8099/retry",
                json={"table": table_name},
                timeout=5
            )
            if res_trigger.status_code == 200:
                print(f"[HTTP TRIGGER] Successfully triggered rerun for table '{table_name}' via daemon.")
                triggered = True
            else:
                print(f"[HTTP TRIGGER] Failed with status {res_trigger.status_code}: {res_trigger.text}")
        except Exception as e:
            print(f"[HTTP TRIGGER] Error calling Spark trigger daemon: {e}")
        
        if not triggered:
            # Fallback to local subprocess if daemon is not reachable (for local testing environments)
            try:
                import subprocess
                subprocess.Popen(["python", "spark/spark_quality_engine.py", table_name])
                print("[HTTP TRIGGER] Fallback to local python process triggered.")
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Failed to trigger rerun via daemon or local process: {e}")
        
        return {
            "status": "success",
            "message": f"Pipeline rerun successfully triggered for table '{table_name}'."
        }
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
