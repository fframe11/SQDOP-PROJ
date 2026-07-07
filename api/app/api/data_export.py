import os
import io
import requests
import pandas as pd
from fastapi import APIRouter, HTTPException, Response
from fastapi.responses import StreamingResponse
from elasticsearch import Elasticsearch

router = APIRouter(prefix="/api/v1/export", tags=["Data Export"])

from .config import get_elasticsearch_url
ELASTICSEARCH_URL = get_elasticsearch_url()

_es_client = None

def get_es():
    global _es_client
    if _es_client is None:
        try:
            _es_client = Elasticsearch(ELASTICSEARCH_URL)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to connect to Elasticsearch: {str(e)}")
    return _es_client


from urllib.parse import urlparse, urlunparse

def replace_hdfs_redirect_host(redirect_url: str) -> str:
    """Robustly parse redirect URL and replace container ID/localhost with 'datanode' service name."""
    parsed = urlparse(redirect_url)
    port = parsed.port or 9864
    new_parsed = parsed._replace(netloc=f"datanode:{port}")
    return urlunparse(new_parsed)


def read_hdfs_file(path: str) -> bytes:
    """Read full file content from WebHDFS (follows redirection to datanode)."""
    webhdfs_url = f"http://namenode:9870/webhdfs/v1{path}?op=OPEN&user.name=spark"
    try:
        r = requests.get(webhdfs_url, allow_redirects=False, timeout=10)
        if r.status_code == 307:
            redirect_url = replace_hdfs_redirect_host(r.headers["Location"])
            r2 = requests.get(redirect_url, timeout=20)
            if r2.status_code == 200:
                return r2.content
            raise HTTPException(status_code=500, detail=f"Failed to fetch from datanode: HTTP {r2.status_code}")
        elif r.status_code == 200:
            return r.content
        raise HTTPException(status_code=r.status_code, detail=f"WebHDFS read failed: {r.text}")
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"HDFS read exception: {str(e)}")


def stream_hdfs_file_raw(path: str):
    """Stream large raw files from WebHDFS in chunks."""
    webhdfs_url = f"http://namenode:9870/webhdfs/v1{path}?op=OPEN&user.name=spark"
    try:
        r = requests.get(webhdfs_url, allow_redirects=False, timeout=10)
        if r.status_code == 307:
            redirect_url = replace_hdfs_redirect_host(r.headers["Location"])
            r2 = requests.get(redirect_url, stream=True, timeout=30)
            r2.raise_for_status()
            for chunk in r2.iter_content(chunk_size=16384):
                yield chunk
        elif r.status_code == 200:
            for chunk in r.iter_content(chunk_size=16384):
                yield chunk
        else:
            raise HTTPException(status_code=r.status_code, detail=f"WebHDFS stream failed: {r.text}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"HDFS stream exception: {str(e)}")


def read_parquet_folder_to_df(hdfs_folder: str) -> pd.DataFrame:
    """List and read all Parquet files inside a folder and return a single concatenated DataFrame."""
    list_url = f"http://namenode:9870/webhdfs/v1{hdfs_folder}?op=LISTSTATUS&user.name=spark"
    try:
        r = requests.get(list_url, timeout=10)
        if r.status_code == 404:
            raise HTTPException(status_code=404, detail=f"HDFS path {hdfs_folder} not found")
        r.raise_for_status()
        
        files = r.json().get("FileStatuses", {}).get("FileStatus", [])
        parquet_files = [f["pathSuffix"] for f in files if f["pathSuffix"].endswith(".parquet")]
        
        if not parquet_files:
            raise HTTPException(status_code=404, detail=f"No parquet data files found in {hdfs_folder}")
            
        dfs = []
        for file_name in parquet_files:
            file_path = f"{hdfs_folder}/{file_name}"
            content = read_hdfs_file(file_path)
            df = pd.read_parquet(io.BytesIO(content))
            dfs.append(df)
            
        if not dfs:
            raise HTTPException(status_code=404, detail="No datasets could be loaded")
            
        return pd.concat(dfs, ignore_index=True)
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading Parquet dataset: {str(e)}")


@router.get("/tables")
def list_export_tables():
    """List all available tables across active, raw, and quarantine layers."""
    tables_map = {}
    
    # helper to check paths
    def check_layer(path, layer_name):
        url = f"http://namenode:9870/webhdfs/v1{path}?op=LISTSTATUS&user.name=spark"
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                files = r.json().get("FileStatuses", {}).get("FileStatus", [])
                for f in files:
                    if f["type"] == "DIRECTORY":
                        t_name = f["pathSuffix"]
                        if t_name not in tables_map:
                            tables_map[t_name] = []
                        tables_map[t_name].append(layer_name)
        except Exception:
            pass

    check_layer("/data/raw", "raw")
    check_layer("/data/active", "active")
    check_layer("/data/quarantine", "quarantine")
    
    # Reddit streaming dataset check
    has_reddit = False
    try:
        r = requests.get("http://namenode:9870/webhdfs/v1/data/reddit/parquet?op=LISTSTATUS&user.name=spark", timeout=5)
        if r.status_code == 200:
            has_reddit = True
    except Exception:
        pass

    results = []
    for name, layers in tables_map.items():
        results.append({"name": name, "layers": layers})
        
    return {
        "tables": results,
        "reddit_available": has_reddit
    }


@router.get("/preview/{layer}/{table_name}")
def get_dataset_preview(layer: str, table_name: str):
    """Get a 10-row JSON preview of the dataset from HDFS raw, active, or quarantine layers."""
    try:
        if layer == "raw":
            # Read first few lines of CSV
            file_path = f"/data/raw/{table_name}/{table_name}.csv"
            webhdfs_url = f"http://namenode:9870/webhdfs/v1{file_path}?op=OPEN&user.name=spark"
            r = requests.get(webhdfs_url, allow_redirects=False, timeout=10)
            if r.status_code == 307:
                redirect_url = r.headers["Location"]
                redirect_url = redirect_url.replace("localhost:", "datanode:").replace("127.0.0.1:", "datanode:")
                r2 = requests.get(redirect_url, timeout=10)
                if r2.status_code == 200:
                    df = pd.read_csv(io.StringIO(r2.text), nrows=10)
                    return {"columns": list(df.columns), "rows": df.to_dict(orient="records")}
            elif r.status_code == 200:
                df = pd.read_csv(io.StringIO(r.text), nrows=10)
                return {"columns": list(df.columns), "rows": df.to_dict(orient="records")}
            raise HTTPException(status_code=404, detail="Raw CSV file not found")
            
        elif layer in ("active", "quarantine"):
            folder_path = f"/data/{layer}/{table_name}"
            df = read_parquet_folder_to_df(folder_path)
            preview_df = df.head(10)
            return {"columns": list(preview_df.columns), "rows": preview_df.to_dict(orient="records")}
            
        elif layer == "reddit":
            folder_path = f"/data/reddit/parquet/subreddit={table_name}"
            df = read_parquet_folder_to_df(folder_path)
            df["subreddit"] = table_name
            preview_df = df.head(10)
            return {"columns": list(preview_df.columns), "rows": preview_df.to_dict(orient="records")}
            
        else:
            raise HTTPException(status_code=400, detail="Invalid layer name")
            
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Preview generation error: {str(e)}")


@router.get("/raw/{table_name}")
def export_raw_data(table_name: str):
    """Download the raw CSV file directly from HDFS raw storage."""
    file_path = f"/data/raw/{table_name}/{table_name}.csv"
    
    # Fast check if file exists
    check_url = f"http://namenode:9870/webhdfs/v1{file_path}?op=GETFILESTATUS&user.name=spark"
    try:
        r = requests.get(check_url, timeout=5)
        if r.status_code != 200:
            raise HTTPException(status_code=404, detail=f"Raw dataset file not found for table '{table_name}'")
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return StreamingResponse(
        stream_hdfs_file_raw(file_path),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={table_name}_raw.csv"}
    )


@router.get("/active/{table_name}")
def export_active_data(table_name: str, limit: int = None):
    """Download clean Silver active dataset as CSV."""
    df = read_parquet_folder_to_df(f"/data/active/{table_name}")
    
    if limit:
        df = df.head(limit)
        
    csv_data = df.to_csv(index=False)
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={table_name}_active.csv"}
    )


@router.get("/quarantine/{table_name}")
def export_quarantine_data(table_name: str, limit: int = None):
    """Download quarantined records dataset as CSV."""
    df = read_parquet_folder_to_df(f"/data/quarantine/{table_name}")
    
    if limit:
        df = df.head(limit)
        
    csv_data = df.to_csv(index=False)
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={table_name}_quarantine.csv"}
    )


@router.get("/reddit")
def export_reddit_data(subreddit: str = "python", limit: int = None):
    """Download parsed Reddit streaming data from HDFS parquet files as CSV."""
    folder_path = f"/data/reddit/parquet/subreddit={subreddit}"
    df = read_parquet_folder_to_df(folder_path)
    
    # Inject subreddit name column since it is partitioned out in HDFS path
    df["subreddit"] = subreddit
    
    if limit:
        df = df.head(limit)
        
    csv_data = df.to_csv(index=False)
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=reddit_{subreddit}.csv"}
    )


@router.get("/gold/{metric}")
def export_gold_metric(metric: str, days: int = 14):
    """Download aggregated Gold metrics from Elasticsearch as a clean CSV report."""
    es = get_es()
    index_name = f"sdoqap_gold_{metric.replace('-', '_')}"
    
    if not es.indices.exists(index=index_name):
        raise HTTPException(status_code=404, detail=f"Gold metric index '{index_name}' does not exist")
        
    try:
        res = es.search(
            index=index_name,
            body={
                "query": {
                    "range": {
                        "date": {
                            "gte": f"now-{days}d/d",
                            "lte": "now/d"
                        }
                    }
                },
                "size": 10000,
                "sort": [{"date": {"order": "desc"}}]
            }
        )
        hits = res.get("hits", {}).get("hits", [])
        if not hits:
            raise HTTPException(status_code=404, detail=f"No gold metric records found in the last {days} days")
            
        data = [h["_source"] for h in hits]
        df = pd.DataFrame(data)
        
        # Sort column names for standard presentation
        df = df.reindex(sorted(df.columns), axis=1)
        
        csv_data = df.to_csv(index=False)
        return Response(
            content=csv_data,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=gold_{metric}_{days}d.csv"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch metrics export error: {str(e)}")
