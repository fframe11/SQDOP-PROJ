import os
from datetime import datetime
import requests
from typing import Optional
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from elasticsearch import Elasticsearch

router = APIRouter(
    prefix="/api/v1/pipeline",
    tags=["pipeline"]
)

from .config import get_elasticsearch_url

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
            
        # Check ES for acknowledgement
        is_ack = False
        try:
            if es.indices.exists(index="sdoqap_acknowledged_runs") and es.exists(index="sdoqap_acknowledged_runs", id=run_id):
                is_ack = True
        except Exception:
            pass

        return {
            "run_details": run_detail,
            "quality_audits": quality,
            "schema_drift_alerts": [] if is_ack else drifts,
            "is_acknowledged": is_ack
        }
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query failed: {str(e)}")

@router.post("/acknowledge/{run_id}")
def acknowledge_run_drift(run_id: str):
    """
    Acknowledges the schema drift for a specific execution run. Stored in ES for persistence.
    """
    es = get_es_client()
    try:
        from datetime import timezone
        es.index(
            index="sdoqap_acknowledged_runs",
            id=run_id,
            document={
                "run_id": run_id,
                "acknowledged_at": datetime.now(timezone.utc).isoformat()
            }
        )
        return {"status": "success", "run_id": run_id, "message": "Schema drift acknowledged and saved to Elasticsearch."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to persist acknowledgement in Elasticsearch: {str(e)}")


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


class ApiIngestPayload(BaseModel):
    table_name: str
    url: str
    headers: Optional[dict] = None
    api_key: Optional[str] = None

class RedditIngestPayload(BaseModel):
    subreddits: str = "python"
    duration: int = 40

async def upload_to_webhdfs(table_name: str, content: bytes):
    try:
        # Step 1: PUT without data to initialize
        webhdfs_url = f"http://namenode:9870/webhdfs/v1/data/raw/{table_name}/{table_name}.csv?op=CREATE&overwrite=true&user.name=spark"
        r1 = requests.put(webhdfs_url, allow_redirects=False, timeout=5)
        if r1.status_code != 307:
            raise HTTPException(status_code=500, detail=f"WebHDFS create handshake failed: HTTP {r1.status_code}")
        
        redirect_url = r1.headers["Location"]
        # Replace localhost/127.0.0.1 with datanode if Namenode returns host redirection
        redirect_url = redirect_url.replace("localhost:", "datanode:").replace("127.0.0.1:", "datanode:")
        
        # Step 2: PUT with data
        r2 = requests.put(redirect_url, data=content, timeout=10)
        if r2.status_code not in (200, 201):
            raise HTTPException(status_code=500, detail=f"WebHDFS write failed: HTTP {r2.status_code} - {r2.text}")
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"WebHDFS upload exception: {str(e)}")

def trigger_spark_job(table_name: str):
    spark_host = os.getenv("SPARK_MASTER_HOST", "spark-master")
    try:
        res = requests.post(
            f"http://{spark_host}:8099/retry",
            json={"table": table_name},
            timeout=5
        )
        if res.status_code == 200:
            return True
    except Exception:
        pass
    
    # Fallback to local subprocess execution
    try:
        import subprocess
        subprocess.Popen(["python", "spark/spark_quality_engine.py", table_name])
        return True
    except Exception:
        return False

@router.post("/ingest/csv")
async def ingest_csv(table_name: str = Form(...), file: UploadFile = File(...)):
    """
    Ingests an uploaded CSV file into HDFS raw store and triggers Spark quality check.
    """
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")
        
    await upload_to_webhdfs(table_name, content)
    triggered = trigger_spark_job(table_name)
    
    return {
        "status": "success",
        "message": f"CSV dataset successfully uploaded and quality check triggered for '{table_name}'.",
        "spark_triggered": triggered
    }

@router.post("/ingest/api")
async def ingest_api(payload: ApiIngestPayload):
    """
    Downloads JSON data from an API, converts it to CSV, writes it to HDFS, and triggers Spark.
    """
    table_name = payload.table_name
    url = payload.url
    req_headers = payload.headers or {}
    
    # Auto-inject api_key if provided
    if payload.api_key:
        req_headers["api-key"] = payload.api_key
        req_headers["Authorization"] = f"Bearer {payload.api_key}"
        
    meta_headers = {}
    if payload.api_key:
        meta_headers["api-key"] = payload.api_key
        meta_headers["Authorization"] = f"Bearer {payload.api_key}"
    
    # Smart data.go.th URL & Resource ID Resolver
    import re
    resolved_url = url.strip()
    uuid_pattern = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
    
    # Check if raw Resource UUID was supplied
    resource_id = None
    if uuid_pattern.match(resolved_url):
        resource_id = resolved_url
    elif "data.go.th/dataset/" in resolved_url and "/resource/" in resolved_url:
        try:
            parts = resolved_url.split("/resource/")
            if len(parts) > 1:
                res_id = parts[1].split("/")[0].split("?")[0]
                if uuid_pattern.match(res_id):
                    resource_id = res_id
        except Exception:
            pass
            
    # If we have a resource ID (directly or from URL):
    if resource_id:
        # First try to query datastore_search
        test_url = f"https://data.go.th/api/3/action/datastore_search?resource_id={resource_id}"
        print(f"[SMART INGEST] Trying CKAN Datastore API: {test_url}")
        res = requests.get(test_url, headers=meta_headers, timeout=10)
        if res.status_code == 200:
            resolved_url = test_url
        else:
            # Datastore search failed/404, query resource_show to find direct download link
            print(f"[SMART INGEST] Datastore 404, querying resource_show for download link...")
            show_url = f"https://data.go.th/api/3/action/resource_show?id={resource_id}"
            show_res = requests.get(show_url, headers=meta_headers, timeout=10)
            if show_res.status_code == 200:
                show_json = show_res.json()
                if show_json.get("success"):
                    direct_url = show_json.get("result", {}).get("url")
                    if direct_url:
                        # Check file format of direct url
                        if direct_url.lower().endswith(".xlsx") or direct_url.lower().endswith(".xls"):
                            raise HTTPException(
                                status_code=400,
                                detail="This data.go.th resource is an Excel (.xlsx) file. SDOQAP normalizer currently supports JSON APIs and CSV formats. Please download the CSV version, or upload it via Local CSV."
                            )
                        resolved_url = direct_url
                        print(f"[SMART INGEST] Auto-resolved to direct download link: {resolved_url}")
            
    # Case 3: data.go.th Dataset page URL (without resource id in path)
    elif "data.go.th/dataset/" in resolved_url:
        try:
            parts = resolved_url.split("data.go.th/dataset/")
            if len(parts) > 1:
                dataset_name = parts[1].split("/")[0].split("?")[0]
                meta_url = f"https://data.go.th/api/3/action/package_show?id={dataset_name}"
                meta_res = requests.get(meta_url, headers=meta_headers, timeout=10)
                if meta_res.status_code == 200:
                    meta_json = meta_res.json()
                    if meta_json.get("success"):
                        resources = meta_json.get("result", {}).get("resources", [])
                        if resources:
                            # 1. Prefer resources with datastore_active
                            datastore_res = next((r for r in resources if r.get("datastore_active")), None)
                            if datastore_res:
                                resource_id = datastore_res.get("id")
                                resolved_url = f"https://data.go.th/api/3/action/datastore_search?resource_id={resource_id}"
                            else:
                                # 2. Prefer CSV format
                                csv_res = next((r for r in resources if r.get("format", "").lower() == "csv"), None)
                                if csv_res:
                                    resolved_url = csv_res.get("url")
                                else:
                                    # check if xlsx
                                    first_url = resources[0].get("url")
                                    if first_url.lower().endswith(".xlsx") or first_url.lower().endswith(".xls"):
                                        raise HTTPException(
                                            status_code=400,
                                            detail="This data.go.th package contains Excel (.xlsx) files. SDOQAP normalizer currently supports JSON APIs and CSV formats."
                                        )
                                    resolved_url = first_url
                            print(f"[SMART INGEST] Auto-resolved data.go.th package to: {resolved_url}")
        except HTTPException as he:
            raise he
        except Exception as resolver_err:
            print(f"[SMART INGEST] Failed to resolve data.go.th package: {resolver_err}")
            
    try:
        api_res = requests.get(resolved_url, headers=req_headers, timeout=15)
        if api_res.status_code != 200:
            raise HTTPException(status_code=400, detail=f"API returned status code {api_res.status_code}")
            
        records = []
        # Try parsing JSON first
        try:
            json_data = api_res.json()
            if isinstance(json_data, list):
                records = json_data
            elif isinstance(json_data, dict):
                if "result" in json_data and isinstance(json_data["result"], dict) and "records" in json_data["result"]:
                    records = json_data["result"]["records"]
                elif "records" in json_data and isinstance(json_data["records"], list):
                    records = json_data["records"]
                elif "data" in json_data and isinstance(json_data["data"], list):
                    records = json_data["data"]
                elif "items" in json_data and isinstance(json_data["items"], list):
                    records = json_data["items"]
                else:
                    records = [json_data]
            else:
                raise ValueError("Unsupported JSON type")
        except Exception:
            # Fallback to CSV parsing (in case we resolved to a direct CSV download link!)
            import io, csv
            text_content = api_res.text
            csv_file = io.StringIO(text_content)
            reader = csv.DictReader(csv_file)
            records = list(reader)
            if not records or len(reader.fieldnames or []) < 2:
                raise HTTPException(
                    status_code=400, 
                    detail="Failed to parse response. Source is not valid JSON, and CSV parsing failed (too few fields or empty content)."
                )
            print(f"[SMART INGEST] Successfully resolved and parsed direct CSV link ({len(records)} records)")
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to fetch API or parse dataset: {str(e)}")
        
    if not records:
        raise HTTPException(status_code=400, detail="No valid records found in the API response.")
        
    import io, csv
    headers = set()
    for r in records:
        if isinstance(r, dict):
            headers.update(r.keys())
    headers = sorted(list(headers))
    if not headers:
        headers = ["value"]
        
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()
    for r in records:
        if isinstance(r, dict):
            row = {}
            for k, v in r.items():
                if isinstance(v, (dict, list)):
                    import json
                    row[k] = json.dumps(v)
                else:
                    row[k] = v
            writer.writerow(row)
        else:
            writer.writerow({"value": str(r)})
            
    csv_bytes = output.getvalue().encode('utf-8')
    await upload_to_webhdfs(table_name, csv_bytes)
    triggered = trigger_spark_job(table_name)
    
    return {
        "status": "success",
        "message": f"API data ingested successfully and quality check triggered for '{table_name}'.",
        "spark_triggered": triggered
    }

@router.post("/ingest/reddit")
def ingest_reddit(payload: RedditIngestPayload):
    """
    Triggers Reddit live streaming pipeline via the Spark Trigger Daemon.
    """
    spark_host = os.getenv("SPARK_MASTER_HOST", "spark-master")
    try:
        res = requests.post(
            f"http://{spark_host}:8099/stream/start",
            json={"subreddits": payload.subreddits, "duration": payload.duration},
            timeout=5
        )
        if res.status_code == 200:
            return res.json()
        else:
            raise HTTPException(status_code=res.status_code, detail=res.text)
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to communicate with Spark Trigger Daemon: {str(e)}")

@router.get("/ingest/reddit/status")
def ingest_reddit_status():
    """
    Fetches the status and logs of the active Reddit streaming job.
    """
    spark_host = os.getenv("SPARK_MASTER_HOST", "spark-master")
    try:
        res = requests.get(f"http://{spark_host}:8099/stream/status", timeout=5)
        if res.status_code == 200:
            return res.json()
        else:
            raise HTTPException(status_code=res.status_code, detail=res.text)
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch streaming status: {str(e)}")

@router.post("/ingest/reddit/stop")
def ingest_reddit_stop():
    """
    Forcibly terminates the active Reddit streaming job.
    """
    spark_host = os.getenv("SPARK_MASTER_HOST", "spark-master")
    try:
        res = requests.post(f"http://{spark_host}:8099/stream/stop", timeout=5)
        if res.status_code == 200:
            return res.json()
        else:
            raise HTTPException(status_code=res.status_code, detail=res.text)
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop streaming job: {str(e)}")
