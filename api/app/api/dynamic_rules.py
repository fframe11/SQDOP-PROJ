"""
Dynamic Rules API Router
=========================
Provides endpoints for managing data quality rules dynamically:
- Read/write merged rules per table from rules_config.json
- Query column profiles (null rates, value ranges) from Elasticsearch
- Review, approve, or reject AI-generated rule proposals

Design notes
------------
* rules_config.json may live at different paths depending on the runtime
  environment (host dev, API container, Spark container). The helper
  ``_resolve_rules_path()`` walks a fallback chain so the router works
  everywhere without hard-coding a single path.
* All Elasticsearch access follows the same ``get_elasticsearch_url`` /
  ``Elasticsearch`` client pattern used by the rest of the SDOQAP API.
"""

import os
import json
import logging
import copy
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query
from elasticsearch import Elasticsearch

from .config import get_elasticsearch_url

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/rules", tags=["Dynamic Rules"])

ELASTICSEARCH_URL = get_elasticsearch_url()

# ---------------------------------------------------------------------------
# ES indices used by this router
# ---------------------------------------------------------------------------
ES_INDEX_DYNAMIC_RULES_LOG = "sdoqap_dynamic_rules_log"
ES_INDEX_AI_PROPOSALS = "sdoqap_ai_rule_proposals"

# ---------------------------------------------------------------------------
# Rules config file resolution
# ---------------------------------------------------------------------------
# Ordered list of candidate paths.  The first one that exists wins.
_RULES_CONFIG_CANDIDATES = [
    # 1. Mounted Spark volume (available when docker-compose mounts ./spark)
    "/opt/spark-apps/rules_config.json",
    # 2. Relative path from *this* file (works during local development)
    os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     "..", "..", "..", "spark", "rules_config.json")
    ),
    # 3. Project-root fallback (if CWD is the project root)
    os.path.join(os.getcwd(), "spark", "rules_config.json"),
]


def _resolve_rules_path() -> str:
    """Return the first existing rules_config.json path from the candidate list.

    Raises:
        HTTPException 404: when no candidate path exists on disk.
    """
    for candidate in _RULES_CONFIG_CANDIDATES:
        if os.path.isfile(candidate):
            return candidate
    raise HTTPException(
        status_code=404,
        detail=(
            "rules_config.json not found. Searched paths: "
            + ", ".join(_RULES_CONFIG_CANDIDATES)
        ),
    )


def _load_rules_config() -> dict:
    """Load and parse the rules configuration, syncing with Elasticsearch sdoqap_rules_registry."""
    config = {}
    path = None
    try:
        path = _resolve_rules_path()
        with open(path, "r", encoding="utf-8") as fh:
            config = json.load(fh)
    except Exception as exc:
        logger.warning("Failed to read local rules_config.json: %s", exc)

    # Sync from ES sdoqap_rules_registry
    try:
        es = _get_es()
        if es.indices.exists(index="sdoqap_rules_registry"):
            res = es.search(index="sdoqap_rules_registry", body={"query": {"match_all": {}}, "size": 100})
            hits = res.get("hits", {}).get("hits", [])
            for h in hits:
                tbl = h["_id"]
                config[tbl] = h["_source"]
            logger.info("Successfully loaded and synced rules config from Elasticsearch sdoqap_rules_registry.")
    except Exception as exc:
        logger.warning("Failed to sync rules config from ES: %s. Using disk fallback.", exc)
        
    return config


def _save_rules_config(config: dict) -> None:
    """Atomically write *config* back to rules_config.json with file locking and sync to Elasticsearch sdoqap_rules_registry."""
    path = None
    try:
        import time
        path = _resolve_rules_path()
        lock_path = path + ".lock"
        
        # Acquire atomic file-based lock
        acquired = False
        for _ in range(30):
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                acquired = True
                break
            except FileExistsError:
                time.sleep(0.1)
                
        try:
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(config, fh, indent=2, ensure_ascii=False)
                fh.write("\n")
        finally:
            if acquired:
                try:
                    os.remove(lock_path)
                except Exception:
                    pass
    except Exception as exc:
        logger.warning("Failed to write local rules_config.json: %s", exc)

    # Sync to ES sdoqap_rules_registry index
    try:
        es = _get_es()
        if not es.indices.exists(index="sdoqap_rules_registry"):
            es.indices.create(index="sdoqap_rules_registry")
            
        for table_name, table_rules in config.items():
            if table_name == "_comment":
                continue
            es.index(index="sdoqap_rules_registry", id=table_name, document=table_rules)
        logger.info("Successfully synced rules config to Elasticsearch sdoqap_rules_registry.")
    except Exception as exc:
        logger.warning("Failed to sync rules config to ES: %s", exc)
        # Raise HTTP 500 only if saving locally failed as well
        if not path or not os.path.exists(path):
            raise HTTPException(
                status_code=500,
                detail=f"Failed to write rules_config.json: {exc}",
            )


def _merge_rules(default: dict, table_specific: dict) -> dict:
    """Deep-merge *table_specific* overrides onto a copy of *default*.

    For each key present in *table_specific*, the table value replaces
    the default value.  Keys present only in *default* are preserved.
    """
    merged = copy.deepcopy(default)
    for key, value in table_specific.items():
        if (
            isinstance(value, dict)
            and isinstance(merged.get(key), dict)
        ):
            merged[key].update(value)
        else:
            merged[key] = value
    return merged


# ---------------------------------------------------------------------------
# Singleton Elasticsearch client
# ---------------------------------------------------------------------------
_es_client: Optional[Elasticsearch] = None


def _get_es() -> Elasticsearch:
    """Return a lazily-initialised Elasticsearch client."""
    global _es_client
    if _es_client is None:
        try:
            _es_client = Elasticsearch(ELASTICSEARCH_URL)
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to connect to Elasticsearch: {exc}",
            )
    return _es_client


# ═══════════════════════════════════════════════════════════════════════════
#  RULES CRUD
# ═══════════════════════════════════════════════════════════════════════════

# Wildcard routes are moved to the end of the file to prevent FastAPI route conflicts


# ═══════════════════════════════════════════════════════════════════════════
#  COLUMN PROFILES (from Dynamic Rules Engine logs)
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/profiles/{table_name}", summary="Get column profiles for a table")
def get_column_profiles(table_name: str) -> dict:
    """Query Elasticsearch for the latest dynamic-rule computation for
    ``table_name`` and return the null-rate and value-range profiles.

    Source index: ``sdoqap_dynamic_rules_log``
    """
    es = _get_es()

    if not es.indices.exists(index=ES_INDEX_DYNAMIC_RULES_LOG):
        return {
            "table": table_name,
            "profiles": [],
            "source": "no_index",
            "message": f"Index '{ES_INDEX_DYNAMIC_RULES_LOG}' does not exist yet.",
        }

    try:
        res = es.search(
            index=ES_INDEX_DYNAMIC_RULES_LOG,
            body={
                "query": {
                    "term": {
                        "table_name.keyword": {
                            "value": table_name,
                            "case_insensitive": True,
                        }
                    }
                },
                "sort": [{"timestamp": {"order": "desc"}}],
                "size": 1,
            },
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Elasticsearch query failed: {exc}",
        )

    hits = res.get("hits", {}).get("hits", [])
    if not hits:
        return {
            "table": table_name,
            "profiles": [],
            "source": "empty",
            "message": f"No dynamic-rule log entries found for table '{table_name}'.",
        }

    doc = hits[0]["_source"]
    return {
        "table": table_name,
        "timestamp": doc.get("timestamp"),
        "null_profiles": doc.get("null_profiles", []),
        "value_range_profiles": doc.get("value_range_profiles", []),
        "suggested_rules": doc.get("suggested_rules", {}),
        "source": ES_INDEX_DYNAMIC_RULES_LOG,
    }


# ═══════════════════════════════════════════════════════════════════════════
#  AI RULE PROPOSALS
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/ai-proposals", summary="List pending AI rule proposals")
def list_ai_proposals(table: Optional[str] = Query(None, description="Filter by table name")) -> dict:
    """Return all AI-generated rule proposals with ``status='PROPOSED'``,
    sorted by timestamp descending.

    Source index: ``sdoqap_ai_rule_proposals``
    """
    es = _get_es()

    if not es.indices.exists(index=ES_INDEX_AI_PROPOSALS):
        return {
            "proposals": [],
            "count": 0,
            "source": "no_index",
            "message": f"Index '{ES_INDEX_AI_PROPOSALS}' does not exist yet.",
        }

    # Build query: status=PROPOSED, optionally filtered by table
    must_clauses: list = [{"term": {"status.keyword": "PROPOSED"}}]
    if table:
        must_clauses.append(
            {"term": {"table_name.keyword": {"value": table, "case_insensitive": True}}}
        )

    try:
        res = es.search(
            index=ES_INDEX_AI_PROPOSALS,
            body={
                "query": {"bool": {"must": must_clauses}},
                "sort": [{"timestamp": {"order": "desc"}}],
                "size": 100,
            },
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Elasticsearch query failed: {exc}",
        )

    hits = res.get("hits", {}).get("hits", [])
    proposals = []
    for hit in hits:
        proposal = hit["_source"]
        proposal["_id"] = hit["_id"]
        proposals.append(proposal)

    return {
        "proposals": proposals,
        "count": len(proposals),
        "source": ES_INDEX_AI_PROPOSALS,
    }


@router.post("/ai-proposals/{proposal_id}/approve", summary="Approve an AI rule proposal")
def approve_proposal(proposal_id: str) -> dict:
    """Mark an AI proposal as ``APPROVED`` and merge its ``suggested_rules``
    into ``rules_config.json`` for the relevant table.

    This implements the **Upstream Remediation** principle: accepted rule
    changes are persisted at the configuration source so every subsequent
    pipeline run benefits automatically.
    """
    es = _get_es()

    if not es.indices.exists(index=ES_INDEX_AI_PROPOSALS):
        raise HTTPException(
            status_code=404,
            detail=f"Index '{ES_INDEX_AI_PROPOSALS}' does not exist.",
        )

    # Fetch the proposal document
    try:
        doc = es.get(index=ES_INDEX_AI_PROPOSALS, id=proposal_id)
    except Exception:
        raise HTTPException(
            status_code=404,
            detail=f"Proposal '{proposal_id}' not found.",
        )

    source = doc.get("_source", {})
    current_status = source.get("status", "")

    if current_status != "PROPOSED":
        raise HTTPException(
            status_code=409,
            detail=f"Proposal '{proposal_id}' is already '{current_status}' and cannot be approved.",
        )

    # 1. Update status in Elasticsearch
    try:
        es.update(
            index=ES_INDEX_AI_PROPOSALS,
            id=proposal_id,
            body={
                "doc": {
                    "status": "APPROVED",
                    "approved_at": datetime.now(timezone.utc).isoformat(),
                }
            },
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update proposal status: {exc}",
        )

    # 2. If the proposal contains suggested_rules, merge them into rules_config.json
    analysis = source.get("analysis_result", {})
    suggested = analysis.get("suggested_rules", [])
    target_table = source.get("table_name")
 
    if suggested and target_table:
        try:
            config = _load_rules_config()
            table_config = config.setdefault(target_table, {})
            
            # Support both list of dicts (from advisor) and raw dict override format
            rules_list = suggested if isinstance(suggested, list) else [suggested]
            
            promoted_count = 0
            for rule in rules_list:
                if not isinstance(rule, dict):
                    continue
                rule_path = rule.get("rule_path", "")
                val = rule.get("value")
                action = rule.get("action", "update")
                condition = rule.get("condition")

                # Handle normal overrides
                if rule_path and val is not None:
                    if action == "escalate":
                        continue
                    parts = rule_path.split(".")
                    d = table_config
                    for part in parts[:-1]:
                        d = d.setdefault(part, {})
                    if "tolerance" in parts[-1] and isinstance(val, (int, float)):
                        val = min(val, 0.30)
                    d[parts[-1]] = val
                    promoted_count += 1

                # Handle induced tree rules
                elif rule_path and condition:
                    parts = rule_path.split(".")
                    induced_sec = table_config.setdefault("induced", {})
                    rule_name = parts[-1]
                    induced_sec[rule_name] = {
                        "condition": condition,
                        "action": "quarantine",
                        "origin": rule.get("origin", "decision_tree_induction"),
                        "reason": rule.get("reason", "Auto-induced rule")
                    }
                    promoted_count += 1

            if promoted_count > 0:
                _save_rules_config(config)
                logger.info(
                    "AI proposal '%s' approved — successfully merged %d rules for table '%s'.",
                    proposal_id,
                    promoted_count,
                    target_table,
                )
        except Exception as exc:
            logger.warning(
                "Proposal '%s' approved in ES but rules config merge failed: %s",
                proposal_id,
                exc,
            )

    return {"status": "approved", "proposal_id": proposal_id}


@router.post("/ai-proposals/{proposal_id}/reject", summary="Reject an AI rule proposal")
def reject_proposal(proposal_id: str) -> dict:
    """Mark an AI proposal as ``REJECTED``.

    No changes are made to ``rules_config.json``.
    """
    es = _get_es()

    if not es.indices.exists(index=ES_INDEX_AI_PROPOSALS):
        raise HTTPException(
            status_code=404,
            detail=f"Index '{ES_INDEX_AI_PROPOSALS}' does not exist.",
        )

    # Fetch the proposal document to verify it exists and is in PROPOSED state
    try:
        doc = es.get(index=ES_INDEX_AI_PROPOSALS, id=proposal_id)
    except Exception:
        raise HTTPException(
            status_code=404,
            detail=f"Proposal '{proposal_id}' not found.",
        )

    source = doc.get("_source", {})
    current_status = source.get("status", "")

    if current_status != "PROPOSED":
        raise HTTPException(
            status_code=409,
            detail=f"Proposal '{proposal_id}' is already '{current_status}' and cannot be rejected.",
        )

    try:
        es.update(
            index=ES_INDEX_AI_PROPOSALS,
            id=proposal_id,
            body={
                "doc": {
                    "status": "REJECTED",
                    "rejected_at": datetime.now(timezone.utc).isoformat(),
                }
            },
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update proposal status: {exc}",
        )

    logger.info("AI proposal '%s' rejected.", proposal_id)

    return {"status": "rejected", "proposal_id": proposal_id}


# ═══════════════════════════════════════════════════════════════════════════
#  WILDCARD RULES CRUD (Moved to the end to prevent route conflicts)
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/{table_name}", summary="Get effective rules for a table")
def get_rules_for_table(table_name: str) -> dict:
    """Return the *effective* rules for ``table_name``.

    The effective rules are computed by merging the ``_default`` section
    with the table-specific overrides (if any) stored in
    ``rules_config.json``.
    """
    config = _load_rules_config()

    defaults = config.get("_default", {})
    table_overrides = config.get(table_name, {})

    effective = _merge_rules(defaults, table_overrides)

    return {
        "table": table_name,
        "effective_rules": effective,
        "has_overrides": bool(table_overrides),
        "default_rules": defaults,
        "table_overrides": table_overrides,
    }


@router.put("/{table_name}", summary="Update rule overrides for a table")
def update_rules_for_table(table_name: str, body: Dict[str, Any]) -> dict:
    """Merge *body* into the table-specific section of ``rules_config.json``.

    This endpoint does **not** touch the ``_default`` block; it only
    creates or updates the ``table_name`` key.

    Upstream-First note: rule changes made here are persisted so that
    the next Spark quality-engine run picks them up automatically.
    """
    if table_name == "_default":
        raise HTTPException(
            status_code=400,
            detail="Use a specific table name. Editing '_default' directly is not allowed via this endpoint.",
        )
    if table_name == "_comment":
        raise HTTPException(
            status_code=400,
            detail="'_comment' is a reserved key.",
        )

    config = _load_rules_config()

    # Merge new values into the existing table section (create if absent)
    existing = config.get(table_name, {})
    existing.update(body)
    config[table_name] = existing

    _save_rules_config(config)

    logger.info("Rules updated for table '%s': %s", table_name, body)

    return {"status": "updated", "table": table_name}
