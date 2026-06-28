import os
from fastapi import APIRouter, HTTPException
from elasticsearch import Elasticsearch

router = APIRouter(
    prefix="/api/v1/quality",
    tags=["quality"]
)

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")

def get_es_client():
    try:
        return Elasticsearch(ELASTICSEARCH_URL)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to Elasticsearch: {str(e)}")

@router.get("")
def list_quality_runs(limit: int = 50):
    """
    Retrieves history logs of all data quality validations from Elasticsearch.
    """
    es = get_es_client()
    try:
        if not es.indices.exists(index="sdoqap_quality_runs"):
            return []
            
        res = es.search(
            index="sdoqap_quality_runs",
            query={"match_all": {}},
            sort=[{"timestamp": {"order": "desc"}}],
            size=limit
        )
        return [hit["_source"] for hit in res["hits"]["hits"]]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query failed: {str(e)}")

@router.get("/{table_name}")
def get_table_quality_history(table_name: str, limit: int = 20):
    """
    Retrieves the quality scorecard history for a specific table from Elasticsearch.
    """
    es = get_es_client()
    try:
        if not es.indices.exists(index="sdoqap_quality_runs"):
            raise HTTPException(status_code=404, detail="Quality runs index 'sdoqap_quality_runs' not found.")
            
        res = es.search(
            index="sdoqap_quality_runs",
            query={"term": {"table_name.keyword": table_name}},
            sort=[{"timestamp": {"order": "desc"}}],
            size=limit
        )
        hits = res["hits"]["hits"]
        if not hits:
            raise HTTPException(
                status_code=404, 
                detail=f"No quality logs found for table query '{table_name}'."
            )
        return [hit["_source"] for hit in hits]
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query failed: {str(e)}")
