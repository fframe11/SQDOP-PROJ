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
    es_user = os.getenv("ELASTICSEARCH_USER", "elastic")
    es_pass = os.getenv("ELASTICSEARCH_PASSWORD", "sdoqap_secure")
    es_host = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
    es_port = os.getenv("ELASTICSEARCH_PORT", "9200")
    if "ELASTICSEARCH_HOST" not in os.environ and "ELASTICSEARCH_URL" not in os.environ:
        es_host = "localhost"
    es_url = os.getenv("ELASTICSEARCH_URL")
    if not es_url:
        es_url = f"http://{es_user}:{es_pass}@{es_host}:{es_port}"
    return es_url

acknowledged_runs = set()

def get_es_client():
    try:
        return Elasticsearch(get_elasticsearch_url())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to Elasticsearch: {str(e)}")

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

def exec_docker_cmd(container_name: str, cmd_args: list):
    """Executes a command inside a docker container via the docker unix socket."""
    import socket
    import json
    import re
    
    socket_path = "/var/run/docker.sock"
    if not os.path.exists(socket_path):
        print(f"[DOCKER EXEC] Warning: socket {socket_path} does not exist.")
        return False
        
    try:
        # Dynamically query containers to find the exact spark-master container name
        s_list = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s_list.connect(socket_path)
        s_list.sendall(b"GET /containers/json HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        response_list = b''.join(iter(lambda: s_list.recv(65536), b''))
        s_list.close()
        
        response_list_str = response_list.decode('utf-8', errors='ignore')
        m_list = re.search(r'\[.*\]', response_list_str, re.DOTALL)
        if m_list:
            containers = json.loads(m_list.group(0))
            for c in containers:
                names = c.get("Names", [])
                if any("spark-master" in name for name in names):
                    matched_name = names[0].lstrip("/")
                    print(f"[DOCKER EXEC] Dynamically located spark-master container: {matched_name}")
                    container_name = matched_name
                    break
    except Exception as ex:
        print(f"[DOCKER EXEC] Warning: Dynamic container resolution failed, fallback to default name: {ex}")
        
    payload = {
        "AttachStdout": False,
        "AttachStderr": False,
        "Tty": False,
        "Cmd": cmd_args
    }
    
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        s.connect(socket_path)
        body = json.dumps(payload)
        req = (
            f"POST /containers/{container_name}/exec HTTP/1.1\r\n"
            f"Host: localhost\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n\r\n"
            f"{body}"
        )
        s.sendall(req.encode('utf-8'))
        response = s.recv(4096).decode('utf-8')
        s.close()
        
        # Parse response using regex to find JSON Id
        m = re.search(r'\{.*\}', response, re.DOTALL)
        if not m:
            print(f"[DOCKER EXEC] Could not find JSON block in response: {response}")
            return False
            
        exec_info = json.loads(m.group(0))
        exec_id = exec_info.get("Id")
        if not exec_id:
            print(f"[DOCKER EXEC] Failed to locate exec Id: {m.group(0)}")
            return False
            
        # 2. Start the exec instance
        s_start = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s_start.connect(socket_path)
        start_payload = {
            "Detach": True,
            "Tty": False
        }
        start_body = json.dumps(start_payload)
        start_req = (
            f"POST /exec/{exec_id}/start HTTP/1.1\r\n"
            f"Host: localhost\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(start_body)}\r\n\r\n"
            f"{start_body}"
        )
        s_start.sendall(start_req.encode('utf-8'))
        s_start.recv(1024)
        s_start.close()
        print(f"[DOCKER EXEC] Triggered command asynchronously: {cmd_args}")
        return True
    except Exception as e:
        print(f"[DOCKER EXEC] Error calling docker socket: {e}")
        return False

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
        
        # Trigger actual spark job asynchronously via Docker Unix Socket!
        cmd = [
            "spark-submit",
            "--master", "spark://spark-master:7077",
            "--packages", "io.delta:delta-core_2.12:2.4.0",
            "/opt/spark-apps/spark_quality_engine.py",
            table_name
        ]
        triggered = exec_docker_cmd("sdoqap-spark-master", cmd)
        
        if not triggered:
            # Fallback to local subprocess if Docker socket is not available (for local testing environments)
            try:
                import subprocess
                subprocess.Popen(["python", "spark/spark_quality_engine.py", table_name])
                print("[DOCKER EXEC] Fallback to local python process triggered.")
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Failed to trigger rerun via Docker socket or local process: {e}")
        
        return {
            "status": "success",
            "message": f"Pipeline rerun successfully triggered for table '{table_name}'."
        }
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
