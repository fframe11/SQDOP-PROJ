"""
Fix 2B: Schema Proposals API — Approval Gate for Schema Registry
Data Engineers can review PENDING schema drift proposals and approve/reject them.
Approved proposals update schema_registry.json. Rejected proposals are discarded.
"""
import os
import json
from fastapi import APIRouter, HTTPException
from elasticsearch import Elasticsearch

router = APIRouter(prefix="/api/v1/schema", tags=["Schema Governance"])

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
SCHEMA_REGISTRY_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "..", "spark", "schema_registry.json"
)

def get_es():
    return Elasticsearch(ELASTICSEARCH_URL)


@router.get("/proposals")
def list_proposals(status: str = "PENDING"):
    """List schema drift proposals filtered by status (PENDING / APPROVED / REJECTED)."""
    es = get_es()
    if not es.indices.exists(index="sdoqap_schema_proposals"):
        return {"proposals": [], "total": 0}
    try:
        res = es.search(
            index="sdoqap_schema_proposals",
            body={
                "query": {"term": {"status.keyword": status}},
                "sort": [{"proposed_at": {"order": "desc"}}],
                "size": 50
            }
        )
        hits = res.get("hits", {}).get("hits", [])
        proposals = [{"id": h["_id"], **h["_source"]} for h in hits]
        return {"proposals": proposals, "total": len(proposals), "status_filter": status}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/proposals/{proposal_id}/approve")
def approve_proposal(proposal_id: str):
    """
    Approve a PENDING schema proposal.
    This writes the proposed schema into schema_registry.json permanently.
    """
    es = get_es()
    try:
        doc = es.get(index="sdoqap_schema_proposals", id=proposal_id)
        proposal = doc["_source"]
    except Exception:
        raise HTTPException(status_code=404, detail=f"Proposal '{proposal_id}' not found.")

    if proposal.get("status") != "PENDING":
        raise HTTPException(
            status_code=400,
            detail=f"Proposal is already '{proposal.get('status')}'. Only PENDING proposals can be approved."
        )

    table_name = proposal["table_name"]
    proposed_schema = proposal["proposed_schema"]

    # Apply to schema_registry.json
    registry_path = os.path.normpath(SCHEMA_REGISTRY_PATH)
    try:
        if os.path.exists(registry_path):
            with open(registry_path, "r") as f:
                registry = json.load(f)
            if table_name in registry:
                registry[table_name]["schema_spec"] = proposed_schema
                with open(registry_path, "w") as f:
                    json.dump(registry, f, indent=4)
                print(f"[SCHEMA APPROVED] schema_registry.json updated for '{table_name}'.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update schema_registry.json: {e}")

    # Update proposal status in ES
    es.update(
        index="sdoqap_schema_proposals",
        id=proposal_id,
        body={"doc": {"status": "APPROVED", "resolved_at": __import__("datetime").datetime.utcnow().isoformat()}}
    )

    return {
        "message": f"Schema proposal '{proposal_id}' APPROVED.",
        "table_name": table_name,
        "schema_applied": proposed_schema
    }


@router.post("/proposals/{proposal_id}/reject")
def reject_proposal(proposal_id: str):
    """
    Reject a PENDING schema proposal.
    The current schema_registry.json remains unchanged.
    """
    es = get_es()
    try:
        es.get(index="sdoqap_schema_proposals", id=proposal_id)
    except Exception:
        raise HTTPException(status_code=404, detail=f"Proposal '{proposal_id}' not found.")

    es.update(
        index="sdoqap_schema_proposals",
        id=proposal_id,
        body={"doc": {"status": "REJECTED", "resolved_at": __import__("datetime").datetime.utcnow().isoformat()}}
    )

    return {
        "message": f"Schema proposal '{proposal_id}' REJECTED. schema_registry.json unchanged."
    }
