import os
from fastapi import APIRouter, HTTPException
from elasticsearch import Elasticsearch

router = APIRouter(
    prefix="/api/v1/quality",
    tags=["quality"]
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
def list_quality_runs(page: int = 1, size: int = 50, limit: int = 50, paginated: bool = False):
    """
    Retrieves history logs of all data quality validations from Elasticsearch with pagination.
    """
    es = get_es_client()
    try:
        if not es.indices.exists(index="sdoqap_quality_runs"):
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
            index="sdoqap_quality_runs",
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

@router.get("/{table_name}")
def get_table_quality_history(table_name: str, page: int = 1, size: int = 20, limit: int = 20, paginated: bool = False):
    """
    Retrieves the quality scorecard history for a specific table from Elasticsearch with pagination.
    """
    es = get_es_client()
    try:
        if not es.indices.exists(index="sdoqap_quality_runs"):
            raise HTTPException(status_code=404, detail="Quality runs index 'sdoqap_quality_runs' not found.")
            
        actual_size = limit if limit != 20 else size
        from_idx = (page - 1) * actual_size
        
        # Prevent ES Deep Pagination memory blowup (> 10000 window limit)
        if from_idx + actual_size > 10000:
            raise HTTPException(
                status_code=400,
                detail="Deep pagination limit exceeded. Elasticsearch restricts offsets above 10,000 to prevent memory exhaustion."
            )
            
        res = es.search(
            index="sdoqap_quality_runs",
            query={"term": {"table_name.keyword": table_name}},
            sort=[{"timestamp": {"order": "desc"}}],
            from_=from_idx,
            size=actual_size
        )
        hits = res["hits"]["hits"]
        if not hits and not paginated:
            raise HTTPException(
                status_code=404, 
                detail=f"No quality logs found for table query '{table_name}'."
            )
        data = [hit["_source"] for hit in hits]
        if paginated:
            total = res["hits"]["total"]["value"] if isinstance(res["hits"]["total"], dict) else res["hits"]["total"]
            return {"data": data, "total": total, "page": page, "size": actual_size}
        else:
            return data
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query failed: {str(e)}")
