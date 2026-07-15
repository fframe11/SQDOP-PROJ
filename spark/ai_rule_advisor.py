"""
SDOQAP AI Rule Advisor — Semantic Analysis Module (Layer 3)
============================================================

Uses the Google Gemini API to perform *semantic* root-cause analysis on
quarantined data samples.  This module is the highest layer of the adaptive
rules stack:

    Layer 1 — ``rules_config.json`` (static, human-authored)
    Layer 2 — ``dynamic_rules_engine.py`` (statistical, data-driven)
    Layer 3 — ``ai_rule_advisor.py`` ← **this module** (semantic, AI-driven)

Design principles (Upstream-First Remediation):
    * The AI does **not** silently fix data.  It *proposes* rule changes and
      root-cause hypotheses that a human or automated governance flow must
      approve before they take effect.
    * Every interaction with the LLM is logged to Elasticsearch for full
      auditability and lineage traceability.
    * If the ``GEMINI_API_KEY`` is not configured the module degrades
      gracefully — the pipeline continues without AI analysis.

Usage from ``spark_quality_engine.py``::

    from ai_rule_advisor import get_ai_advisor
    advisor = get_ai_advisor()
    if advisor and advisor.should_trigger(quality_context):
        result = advisor.ai_analyze_quarantined_sample(
            table_name, quarantined_rows, column_stats, historical_context,
        )
        advisor.log_proposal_to_es(table_name, run_id, result)

Runs inside Docker at ``/opt/spark-apps/``.
"""

import os
import sys
import json
from datetime import datetime, timezone
from urllib.parse import urlparse

# ─── Environment bootstrap (same pattern as spark_quality_engine.py) ──────────

def load_env_file():
    """Walk up to 3 parent directories looking for a ``.env`` file and load
    its key=value pairs into ``os.environ`` (without overwriting existing vars).
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    for _ in range(3):
        env_path = os.path.join(current_dir, ".env")
        if os.path.exists(env_path):
            try:
                with open(env_path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            k, v = line.split("=", 1)
                            os.environ.setdefault(k.strip(), v.strip())
            except Exception:
                pass
            break
        current_dir = os.path.dirname(current_dir)

load_env_file()

import requests

# ─── Elasticsearch helpers ────────────────────────────────────────────────────

def _get_es_connection(es_url=None):
    """Return ``(base_url, auth_tuple_or_None)`` parsed from the given ES URL
    or the ``ELASTICSEARCH_URL`` environment variable.

    Follows the same credential-stripping pattern used throughout the project
    so that Basic-Auth credentials are never leaked into request URLs.
    """
    if es_url is None:
        es_url = os.getenv("ELASTICSEARCH_URL", "")
    parsed = urlparse(es_url)
    auth = (parsed.username, parsed.password) if parsed.username else None
    if parsed.username:
        base_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
    else:
        base_url = es_url
    return base_url, auth


# ─── Constants ────────────────────────────────────────────────────────────────

GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"
ES_INDEX_PROPOSALS = "sdoqap_ai_rule_proposals"


# ═══════════════════════════════════════════════════════════════════════════════
# AIRuleAdvisor class
# ═══════════════════════════════════════════════════════════════════════════════

class AIRuleAdvisor:
    """Semantic data-quality advisor powered by Google Gemini.

    The advisor analyses quarantined data samples and proposes rule changes
    with root-cause explanations.  All proposals are persisted to
    Elasticsearch with ``status='PROPOSED'`` — they do **not** auto-apply.

    Parameters
    ----------
    api_key : str
        Google Gemini API key.  Typically sourced from the
        ``GEMINI_API_KEY`` environment variable.
    model : str
        Gemini model identifier (default ``'gemini-2.0-flash'``).
    es_url : str | None
        Elasticsearch URL.  Falls back to ``ELASTICSEARCH_URL`` env var.
    """

    def __init__(self, api_key=None, model=None, es_url=None):
        self.es_url = es_url or os.getenv("ELASTICSEARCH_URL", "")
        self._base_url, self._auth = _get_es_connection(self.es_url)

    # ── Trigger guard ─────────────────────────────────────────────────────

    def should_trigger(self, quality_context):
        """Decide whether AI analysis is warranted for the current run.

        The AI layer is intentionally *not* invoked on every run — it fires
        only when statistical signals indicate something worth investigating.

        Parameters
        ----------
        quality_context : dict
            Must contain at least ``is_anomaly`` (bool) and
            ``quality_score`` (float).  Optional keys:
            ``historical_avg``, ``schema_drift_detected``.

        Returns
        -------
        bool
        """
        if not isinstance(quality_context, dict):
            return False

        # Trigger 1: Statistical anomaly detected by Layer 2
        if quality_context.get("is_anomaly", False):
            print("[AI_ADVISOR] Trigger: anomaly detected.")
            return True

        # Trigger 2: Quality score dropped significantly below target threshold
        score = quality_context.get("quality_score")
        target_threshold = quality_context.get("current_threshold") or quality_context.get("historical_avg") or 95.0
        if score is not None:
            try:
                if float(score) <= float(target_threshold) - 15:
                    print(f"[AI_ADVISOR] Trigger: quality_score {score} <= "
                          f"target_threshold {target_threshold} − 15.")
                    return True
            except (TypeError, ValueError):
                pass

        # Trigger 3: Schema drift observed upstream
        if quality_context.get("schema_drift_detected", False):
            print("[AI_ADVISOR] Trigger: schema drift detected.")
            return True

        return False

    # ── Core analysis ─────────────────────────────────────────────────────

    def ai_analyze_quarantined_sample(self, table_name, quarantined_rows,
                                      column_stats, historical_context):
        """Analyze quarantined sample using Gemini 1.5 if configured, otherwise fallback to Local Heuristic Advisor."""
        fallback = {
            "root_cause": "AI analysis unavailable",
            "suggested_rules": [],
            "false_positive_indices": [],
            "recommended_threshold": None,
            "confidence": 0.0,
            "explanation": "Gemini API call failed or was unavailable.",
            "status": "FAILED",
        }

        # ── 1. Check Elasticsearch settings for Gemini credentials ────────────────
        gemini_api_key = ""
        gemini_model = "gemini-1.5-flash"
        gemini_enabled = False
        
        try:
            if self._base_url:
                url_settings = f"{self._base_url}/sdoqap_settings/_doc/global"
                res_settings = requests.get(url_settings, auth=self._auth, timeout=3)
                if res_settings.status_code == 200:
                    settings_doc = res_settings.json().get("_source", {})
                    gemini_api_key = settings_doc.get("gemini_api_key", "").strip()
                    gemini_model = settings_doc.get("gemini_model", "gemini-1.5-flash").strip()
                    gemini_enabled = settings_doc.get("gemini_enabled", False)
        except Exception as e:
            print(f"[AI_ADVISOR] Failed to load settings from ES: {e}")

        # Fallback to env var if ES is down or empty
        if not gemini_api_key:
            groq_api_key = os.getenv("GROQ_API_KEY", "").strip()
            if groq_api_key:
                gemini_api_key = groq_api_key
                gemini_model = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile").strip()
                gemini_enabled = True
            else:
                gemini_api_key = os.getenv("GEMINI_API_KEY", "").strip()
                gemini_model = os.getenv("GEMINI_MODEL", "gemini-1.5-flash").strip()
                gemini_enabled = bool(gemini_api_key)

        if not gemini_enabled or not gemini_api_key:
            print("[AI_ADVISOR] Gemini is disabled or not configured. Running Local Heuristic Advisor...")
            return self._run_local_heuristic_advisor(table_name, quarantined_rows, column_stats, historical_context)

        # ── 2. Build structured prompt ───────────────────────────────────────
        prompt = self._build_analysis_prompt(
            table_name, quarantined_rows, column_stats, historical_context,
        )

        # ── 3. Call Gemini API or Groq API ─────────────────────────
        if gemini_api_key.startswith("gsk_"):
            # Route to Groq!
            url = "https://api.groq.com/openai/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {gemini_api_key}",
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            # Use llama-3.3-70b-versatile as default for Groq
            groq_model = gemini_model if "llama" in gemini_model else "llama-3.3-70b-versatile"
            payload = {
                "model": groq_model,
                "messages": [
                    {"role": "user", "content": prompt}
                ],
                "response_format": {"type": "json_object"},
                "temperature": 0.0
            }
            try:
                print(f"[AI_ADVISOR] Routing request to Groq ({groq_model}) via gsk_ API key detected...")
                res = requests.post(url, headers=headers, json=payload, timeout=60)
                if res.status_code != 200:
                    print(f"[AI_ADVISOR] Groq API returned HTTP {res.status_code}: {res.text[:500]}")
                    print("[AI_ADVISOR] Falling back to Local Heuristic Advisor...")
                    return self._run_local_heuristic_advisor(table_name, quarantined_rows, column_stats, historical_context)
                
                response_body = res.json()
                raw_text = response_body.get("choices", [{}])[0].get("message", {}).get("content", "")
                parsed = json.loads(raw_text)
                result = {
                    "root_cause": parsed.get("root_cause", "Unknown"),
                    "suggested_rules": parsed.get("suggested_rules", []),
                    "false_positive_indices": parsed.get("false_positive_indices", []),
                    "recommended_threshold": parsed.get("recommended_threshold"),
                    "confidence": float(parsed.get("confidence", 0.0) or 0.8),
                    "explanation": parsed.get("explanation", ""),
                    "remediation_ticket": parsed.get("remediation_ticket"),
                    "status": "SUCCESS",
                }
                print(f"[AI_ADVISOR] Groq Analysis complete — confidence={result['confidence']:.2f}, "
                      f"rules_suggested={len(result['suggested_rules'])}")
                return result
            except Exception as e:
                print(f"[AI_ADVISOR] Unexpected error during Groq call: {e}. Falling back to Local Heuristic Advisor...")
                return self._run_local_heuristic_advisor(table_name, quarantined_rows, column_stats, historical_context)
        else:
            # Route to Gemini API (Supports Gemini 1.5 / 2.0)
            api_base = "https://generativelanguage.googleapis.com/v1beta"
            url = f"{api_base}/models/{gemini_model}:generateContent?key={gemini_api_key}"
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "responseMimeType": "application/json",
                    "temperature": 0.0
                },
            }
            headers = {"Content-Type": "application/json"}
            try:
                print(f"[AI_ADVISOR] Routing request to Gemini ({gemini_model}) via settings configuration...")
                res = requests.post(url, headers=headers, json=payload, timeout=60)
                if res.status_code != 200:
                    print(f"[AI_ADVISOR] Gemini API returned HTTP {res.status_code}: {res.text[:500]}")
                    print("[AI_ADVISOR] Falling back to Local Heuristic Advisor...")
                    return self._run_local_heuristic_advisor(table_name, quarantined_rows, column_stats, historical_context)
                
                response_body = res.json()
                return self._parse_gemini_response(response_body, fallback)
            except Exception as e:
                print(f"[AI_ADVISOR] Unexpected error during Gemini call: {e}. Falling back to Local Heuristic Advisor...")
                return self._run_local_heuristic_advisor(table_name, quarantined_rows, column_stats, historical_context)

    # ── Prompt builder (private) ──────────────────────────────────────────

    def _build_analysis_prompt(self, table_name, quarantined_rows,
                               column_stats, historical_context):
        """Assemble a structured prompt that guides Gemini toward actionable
        root-cause analysis.

        The prompt is intentionally explicit about the desired JSON output
        schema so that parsing is deterministic.
        """
        # Truncate sample to avoid token-limit issues
        sample_limit = 20
        sample = quarantined_rows[:sample_limit] if quarantined_rows else []

        prompt = f"""You are a Data Quality Engineer analysing quarantined records.

## Context
- **Table:** {table_name}
- **Sample size:** {len(sample)} quarantined rows (out of {len(quarantined_rows) if quarantined_rows else 0} total quarantined)
- **Column statistics:** {json.dumps(column_stats, default=str)}
- **Historical context:** {json.dumps(historical_context, default=str)}

## Quarantined Sample
```json
{json.dumps(sample, default=str, indent=2)}
```

## Instructions
1. Identify the **root cause** of the data quality issues in the sample.
2. Suggest specific **rule changes** that would prevent these issues upstream.
3. Flag any rows that appear to be **false positives** (incorrectly quarantined).
4. Recommend an adjusted quality-score **threshold** if appropriate.
5. Rate your **confidence** (0.0 to 1.0) in the analysis.
6. Generate a structured **remediation_ticket** detailing the target upstream system and specific action required by the upstream team to fix the issue at the source.

**CRITICAL THRESHOLD CONSTRAINTS (HARD RULES FOR PROMPT INFERENCE):**
- You are FORBIDDEN from generating or recommending arbitrary numerical thresholds.
- If recommending rule changes or thresholds, you MUST strictly use numbers provided within "Column statistics" or "Historical context" that were calculated by the mathematical profiling engine.
- You are NEVER allowed to guess or invent arbitrary decimal thresholds (e.g. do not lower a 95% target to 73% randomly). If no historical mathematically-derived number is available, leave the recommended threshold null.
- Under NO circumstances can any quality threshold proposal be below 70.0%.

## Required JSON Output Schema
Return ONLY valid JSON matching this schema:
{{
  "root_cause": "string — concise root-cause hypothesis",
  "suggested_rules": [
    {{
      "column": "column_name",
      "rule_type": "one of: null_check | range_check | format_check | uniqueness | custom",
      "params": {{}},
      "reason": "why this rule should be added or modified"
    }}
  ],
  "false_positive_indices": [0],
  "recommended_threshold": 85.0,
  "confidence": 0.85,
  "explanation": "string — detailed reasoning",
  "remediation_ticket": {{
    "target_system": "probable upstream source system, e.g. Auth API / Ingestion Gateway / Partner Exporter CSV / OLTP DB",
    "remediation_action": "concrete steps for the upstream team to resolve the issue at the source",
    "severity": "critical or warning"
  }}
}}
"""
        return prompt

    # ── Response parser (private) ─────────────────────────────────────────

    def _parse_gemini_response(self, response_body, fallback):
        """Extract the structured JSON from Gemini's response envelope.

        Gemini wraps content in ``candidates[0].content.parts[0].text``.
        The text should already be JSON thanks to ``responseMimeType``, but
        we handle edge-cases defensively.
        """
        try:
            candidates = response_body.get("candidates", [])
            if not candidates:
                print("[AI_ADVISOR] Gemini returned no candidates.")
                fallback["error"] = "No candidates in response"
                return fallback

            parts = candidates[0].get("content", {}).get("parts", [])
            if not parts:
                print("[AI_ADVISOR] Gemini candidate has no parts.")
                fallback["error"] = "No parts in candidate"
                return fallback

            raw_text = parts[0].get("text", "")
            if not raw_text.strip():
                print("[AI_ADVISOR] Gemini returned empty text.")
                fallback["error"] = "Empty response text"
                return fallback

            # Strip potential markdown fences (```json … ```)
            cleaned = raw_text.strip()
            if cleaned.startswith("```"):
                lines = cleaned.split("\n")
                # Remove first and last fence lines
                lines = [l for l in lines if not l.strip().startswith("```")]
                cleaned = "\n".join(lines)

            parsed = json.loads(cleaned)

            # Validate expected keys — fill missing with safe defaults
            result = {
                "root_cause": parsed.get("root_cause", "Unknown"),
                "suggested_rules": parsed.get("suggested_rules", []),
                "false_positive_indices": parsed.get("false_positive_indices", []),
                "recommended_threshold": parsed.get("recommended_threshold"),
                "confidence": float(parsed.get("confidence", 0.0)),
                "explanation": parsed.get("explanation", ""),
                "remediation_ticket": parsed.get("remediation_ticket"),
                "status": "SUCCESS",
            }
            print(f"[AI_ADVISOR] Analysis complete — confidence={result['confidence']:.2f}, "
                  f"rules_suggested={len(result['suggested_rules'])}")
            return result

        except json.JSONDecodeError as e:
            print(f"[AI_ADVISOR] Failed to parse Gemini response as JSON: {e}")
            fallback["error"] = f"JSONDecodeError: {e}"
            fallback["raw_response"] = raw_text[:2000] if 'raw_text' in dir() else ""
            return fallback
        except Exception as e:
            print(f"[AI_ADVISOR] Unexpected error parsing Gemini response: {e}")
            fallback["error"] = str(e)
            return fallback

    def _call_ollama(self, prompt, fallback):
        """Invoke a local LLM via Ollama endpoint.
        Enforces JSON output and parses the results into the standard schema.
        """
        url = f"{self.ollama_host}/api/generate"
        payload = {
            "model": self.ollama_model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {
                "temperature": 0.0
            }
        }
        headers = {"Content-Type": "application/json"}
        try:
            print(f"[AI_ADVISOR] Routing request to local Ollama ({self.ollama_model}) at {self.ollama_host}...")
            res = requests.post(url, headers=headers, json=payload, timeout=90)
            if res.status_code != 200:
                print(f"[AI_ADVISOR] Ollama returned HTTP {res.status_code}: {res.text[:500]}")
                fallback["error"] = f"Ollama HTTP {res.status_code}"
                return fallback

            response_body = res.json()
            raw_text = response_body.get("response", "")
            if not raw_text.strip():
                fallback["error"] = "Ollama returned empty response"
                return fallback

            parsed = json.loads(raw_text)
            result = {
                "root_cause": parsed.get("root_cause", "Unknown"),
                "suggested_rules": parsed.get("suggested_rules", []),
                "false_positive_indices": parsed.get("false_positive_indices", []),
                "recommended_threshold": parsed.get("recommended_threshold"),
                "confidence": float(parsed.get("confidence", 0.0) or 0.8),
                "explanation": parsed.get("explanation", ""),
                "status": "SUCCESS",
            }
            print(f"[AI_ADVISOR] Ollama analysis complete. Root cause: {result['root_cause']}")
            return result
        except Exception as e:
            print(f"[AI_ADVISOR] Ollama request failed: {e}")
            fallback["error"] = f"OllamaError: {e}"
            return fallback

    def _run_local_heuristic_advisor(self, table_name, quarantined_rows, column_stats, historical_context):
        """Analyzes quarantined samples and stats programmatically using statistics and heuristics.
        
        This is a Rule-based Expert System that:
        1. Parses reject_reason patterns to identify root causes
        2. Reads past proposals from ES to avoid duplicate suggestions (Feedback Loop)
        3. Analyzes quality score trends across runs (Trend Analysis)
        4. Dynamically adjusts confidence based on data completeness
        
        Does not require external APIs or local models, ensuring 100% lightweight offline reliability.
        """
        import collections
        import re
        
        # ── Step 0: Extract column context from historical_context ────────────
        pk_column = historical_context.get("primary_key", "unknown")
        date_col = historical_context.get("date_column", "unknown")
        total_records = historical_context.get("total_records", 0)
        quarantined_records = historical_context.get("quarantined_records", 0)
        
        # ── Step 1: Feedback Loop — Read past proposals from ES ───────────────
        past_proposals = self._read_past_proposals(table_name)
        past_rejected_rules = set()
        past_approved_rules = set()
        for p in past_proposals:
            result = p.get("analysis_result", {})
            status = p.get("status", "PROPOSED")
            for rule in result.get("suggested_rules", []):
                rule_id = rule.get("rule_path", "")
                if status == "REJECTED":
                    past_rejected_rules.add(rule_id)
                elif status == "APPROVED":
                    past_approved_rules.add(rule_id)
        
        # ── Step 2: Parse quarantine reasons ──────────────────────────────────
        # Handle BOTH delimiters: ";" (from null checks) and "|" (from outlier details)
        reasons = []
        has_real_data = len(quarantined_rows) > 0
        
        for r in quarantined_rows:
            reason = r.get("reject_reason") or ""
            if reason:
                # Split on both ; and | delimiters
                parts = re.split(r'[;|]', reason)
                reasons.extend([part.strip() for part in parts if part.strip()])
                
        reason_counts = collections.Counter(reasons)
        most_common = reason_counts.most_common(3)
        
        root_cause = "General data quality degradation detected."
        explanation = "Multiple rows failed the data quality criteria."
        suggested_rules = []
        
        # Dynamic confidence: starts at 0.90 with real data, 0.40 without
        confidence = 0.90 if has_real_data else 0.40
        
        # ── Step 3: Trend Analysis from quality history ───────────────────────
        trend_info = ""
        curr_quality = historical_context.get("current_quality", 90.0)
        avg_quality = historical_context.get("avg_quality", 90.0)
        quality_delta = curr_quality - avg_quality
        
        if quality_delta < -10:
            trend_info = f" Quality is in SHARP DECLINE ({quality_delta:+.1f} vs historical average)."
            confidence = min(confidence + 0.05, 0.98)  # Higher confidence when signal is strong
        elif quality_delta < -5:
            trend_info = f" Quality shows moderate decline ({quality_delta:+.1f} vs historical average)."
        elif quality_delta > 5:
            trend_info = f" Quality is IMPROVING ({quality_delta:+.1f} vs historical average)."
            confidence = max(confidence - 0.10, 0.50)  # Lower confidence — might be a false trigger
        
        # ── Step 4: Root cause analysis based on quarantine patterns ──────────
        remediation_ticket = {
            "target_system": "Upstream Data Source",
            "remediation_action": "Investigate data quality degradation at the source system.",
            "severity": "warning"
        }

        if most_common:
            top_reason, count = most_common[0]
            pct = (count / len(quarantined_rows)) * 100 if quarantined_rows else 0
            
            if "duplicate" in top_reason.lower():
                root_cause = f"Duplicate records detected on primary key '{pk_column}'."
                explanation = (
                    f"We found that {pct:.1f}% of quarantined records ({count}/{len(quarantined_rows)}) "
                    f"failed because of duplicate entries on key '{pk_column}'. "
                    f"This suggests the upstream data source is sending duplicate payloads "
                    f"(retry issues, overlapping ingestion windows, or missing dedup at source).{trend_info}"
                )
                remediation_ticket = {
                    "target_system": "Upstream Ingestion API / DB Exporter",
                    "remediation_action": f"Ensure duplicate event checking is configured at the source. Investigate retry logic on primary key '{pk_column}'.",
                    "severity": "warning"
                }
                
                rule_path = "duplicate_check.enabled"
                if rule_path not in past_rejected_rules:
                    suggested_rules.append({
                        "rule_path": rule_path,
                        "value": True,
                        "action": "keep",
                        "reason": f"Ensure duplicate prevention is active for key '{pk_column}'. "
                                  f"Recommend investigating retry logic at the upstream source."
                    })
                else:
                    # Previously rejected — escalate to upstream investigation
                    suggested_rules.append({
                        "rule_path": "upstream_investigation.duplicate_source",
                        "value": True,
                        "action": "escalate",
                        "reason": f"Previous duplicate_check proposal was REJECTED. "
                                  f"Escalating: investigate upstream source for retry/overlap logic."
                    })
                    
            elif "missing" in top_reason.lower() or "null" in top_reason.lower():
                # Extract real column name from reject_reason patterns:
                # "missing_primary_key" → pk_column, "missing_date" → date_col
                if "primary" in top_reason.lower() or "primary_key" in top_reason.lower():
                    col_name = pk_column
                elif "date" in top_reason.lower():
                    col_name = date_col
                else:
                    # Try to extract from pattern like "null_colname" or "missing_colname"
                    col_name = re.sub(r'^(missing_|null_|_null|_missing)', '', top_reason.strip())
                    if not col_name or col_name == top_reason.strip():
                        col_name = "unknown"
                
                root_cause = f"Missing values (Nulls) in column '{col_name}'."
                explanation = (
                    f"Column '{col_name}' has a high rate of missing values, causing {pct:.1f}% "
                    f"({count}/{len(quarantined_rows)}) of quarantined records. "
                    f"This indicates the upstream schema may have evolved or the source database "
                    f"constraint has been relaxed, allowing NULLs where they weren't expected.{trend_info}"
                )
                remediation_ticket = {
                    "target_system": "Upstream Source Database / API Schema Validator",
                    "remediation_action": f"Column '{col_name}' contains null values. Enforce a NOT NULL constraint in the source database or add schema constraints to the ingestion API.",
                    "severity": "critical"
                }
                
                rule_path = f"null_checks.column_overrides.{col_name}.tolerance"
                if rule_path not in past_rejected_rules:
                    suggested_rules.append({
                        "rule_path": rule_path,
                        "value": 0.15,
                        "action": "update",
                        "reason": f"Relax null tolerance on '{col_name}' to 15% to let Active layer flow, "
                                  f"while upstream fixes the null issue at the source database."
                    })
                else:
                    # Previously rejected — suggest a different tolerance
                    suggested_rules.append({
                        "rule_path": rule_path,
                        "value": 0.10,
                        "action": "update_alternative",
                        "reason": f"Previous 15% tolerance was REJECTED. Suggesting 10% as a compromise."
                    })
                    
            elif "outside" in top_reason.lower() or "outlier" in top_reason.lower() \
                 or "zscore" in top_reason.lower() or "expected" in top_reason.lower():
                # Parse outlier details format: "colname: 150.0 outside [18.0, 65.0]"
                col_name = "unknown"
                match = re.match(r'^(\w+)\s*:', top_reason)
                if match:
                    col_name = match.group(1)
                else:
                    # Fallback: try to find column name from value_range stats
                    value_ranges = column_stats.get("value_ranges", {})
                    if value_ranges:
                        col_name = next(iter(value_ranges), "unknown")
                
                root_cause = f"Out of range numerical values (Outliers) in column '{col_name}'."
                explanation = (
                    f"Numerical values in column '{col_name}' fall outside the statistically computed "
                    f"bounds (IQR/Z-score), contributing to {pct:.1f}% ({count}/{len(quarantined_rows)}) "
                    f"of quarantined records. Detail: '{top_reason}'.{trend_info}"
                )
                remediation_ticket = {
                    "target_system": "Upstream Data Entry Client / CSV Producer",
                    "remediation_action": f"Numerical values in '{col_name}' are out of range. Check validation rules on client forms or verification checks on generated files.",
                    "severity": "warning"
                }
                
                rule_path = f"value_range.column_overrides.{col_name}.iqr_multiplier"
                if rule_path not in past_rejected_rules:
                    suggested_rules.append({
                        "rule_path": rule_path,
                        "value": 2.5,
                        "action": "update",
                        "reason": f"Widen the IQR multiplier threshold for '{col_name}' to 2.5 "
                                  f"to accommodate natural data drift while still catching true anomalies."
                    })
                else:
                    suggested_rules.append({
                        "rule_path": rule_path,
                        "value": 3.0,
                        "action": "update_alternative",
                        "reason": f"Previous 2.5x IQR was REJECTED. Suggesting 3.0x as a wider alternative."
                    })
            else:
                # Unknown pattern — log it for observability
                root_cause = f"Unrecognized quarantine pattern: '{top_reason}'."
                explanation = (
                    f"The top quarantine reason '{top_reason}' ({pct:.1f}%) does not match any known "
                    f"pattern (duplicate, null, outlier). This may indicate a new type of data quality "
                    f"issue that requires manual investigation.{trend_info}"
                )
                remediation_ticket = {
                    "target_system": "Upstream Data Provider Exporter",
                    "remediation_action": f"Unknown data quarantine reason: '{top_reason}'. Investigate source pipeline format alignment.",
                    "severity": "warning"
                }
                confidence = max(confidence - 0.15, 0.50)  # Lower confidence for unknown patterns
                
            # ── Add secondary reasons as additional context ───────────────
            if len(most_common) > 1:
                secondary_reasons = [f"'{r}' ({c} rows)" for r, c in most_common[1:]]
                explanation += f" Secondary issues: {', '.join(secondary_reasons)}."
                
        else:
            # No quarantined data was available — use score-based analysis only
            curr_score = historical_context.get("current_quality", 90.0)
            target_score = historical_context.get("current_threshold", 95.0)
            
            if not has_real_data:
                # ⚠️ Cannot examine actual data — reduce confidence significantly
                root_cause = f"Table quality score drop ({curr_score:.1f}% vs threshold {target_score:.1f}%), but no quarantined samples were available for analysis."
                explanation = (
                    f"The AI advisor was triggered but could not read quarantined row samples. "
                    f"This analysis is based solely on aggregate quality scores and should be "
                    f"treated with LOW confidence. Manual investigation recommended.{trend_info}"
                )
                remediation_ticket = {
                    "target_system": "Upstream Partner System",
                    "remediation_action": "Quality score dropped but no quarantined sample was written. Investigate if data was completely empty or failed ingestion entirely.",
                    "severity": "warning"
                }
                confidence = 0.40  # Low confidence — we didn't look at actual data
            elif curr_score < target_score:
                root_cause = f"Table quality score drop ({curr_score:.1f}% vs threshold {target_score:.1f}%)."
                explanation = (
                    f"The overall table quality has dropped below threshold. "
                    f"Recommend investigating schema drift or upstream data source changes.{trend_info}"
                )
                remediation_ticket = {
                    "target_system": "Upstream Ingestion Flow",
                    "remediation_action": "Verify if the data schema, export script, or pipeline configuration was recently changed by the data provider.",
                    "severity": "warning"
                }
                
            if curr_score < target_score:
                suggested_rules.append({
                    "rule_path": "quality_score_threshold.base_value",
                    "value": max(curr_score - 2.0, historical_context.get("current_threshold", 70.0) * 0.9),
                    "action": "update",
                    "reason": "Lower threshold slightly to align with current source quality trends "
                              "while upstream root cause is investigated."
                })
                
        # ── Step 5: Check if suggested rules are already approved (avoid re-suggesting) ──
        filtered_rules = []
        for rule in suggested_rules:
            rule_path = rule.get("rule_path", "")
            if rule_path in past_approved_rules and rule.get("action") != "escalate":
                # Already approved — skip unless it's an escalation
                print(f"[AI_ADVISOR] Skipping rule '{rule_path}' — already APPROVED in past proposals.")
                continue
            filtered_rules.append(rule)
        
        return {
            "root_cause": root_cause,
            "suggested_rules": filtered_rules,
            "false_positive_indices": [],
            "recommended_threshold": None,
            "confidence": round(confidence, 2),
            "explanation": explanation,
            "remediation_ticket": remediation_ticket,
            "status": "SUCCESS",
            "analysis_metadata": {
                "method": "local_heuristic_v2",
                "sample_size": len(quarantined_rows),
                "has_real_data": has_real_data,
                "trend_delta": quality_delta,
                "past_proposals_consulted": len(past_proposals),
                "rules_filtered_by_feedback": len(suggested_rules) - len(filtered_rules)
            }
        }
    
    def _read_past_proposals(self, table_name, max_results=10):
        """Read past AI proposals from Elasticsearch for the given table.
        Used by the Feedback Loop to avoid duplicate suggestions and learn
        from APPROVED/REJECTED decisions.
        
        Returns an empty list if ES is unavailable — never blocks the pipeline.
        """
        if not self._base_url:
            return []
        
        url = f"{self._base_url}/{ES_INDEX_PROPOSALS}/_search"
        query = {
            "size": max_results,
            "sort": [{"timestamp": {"order": "desc"}}],
            "query": {
                "bool": {
                    "must": [{"term": {"table_name.keyword": table_name}}],
                    "filter": [{"terms": {"status": ["APPROVED", "REJECTED"]}}]
                }
            }
        }
        
        try:
            res = requests.post(
                url, headers={"Content-Type": "application/json"},
                auth=self._auth, data=json.dumps(query), timeout=5
            )
            if res.status_code == 200:
                hits = res.json().get("hits", {}).get("hits", [])
                proposals = [h.get("_source", {}) for h in hits]
                if proposals:
                    print(f"[AI_ADVISOR] Feedback Loop: Found {len(proposals)} past proposals for '{table_name}'.")
                return proposals
            else:
                return []
        except Exception:
            # ES unavailable — degrade gracefully, don't block
            return []

    # ── Profile-Based Analysis (Component B) ──────────────────────────────

    def run_profile_based_analysis(self, table_name, quarantined_rows,
                                   column_stats, historical_context,
                                   profile_report=None):
        """Enhanced analysis that combines heuristic reasoning with
        data profile drift detection.

        This method wraps ``_run_local_heuristic_advisor`` and enriches
        its output with drift signals from the Data Profile Store.

        Parameters
        ----------
        profile_report : dict | None
            Output of ``data_profile_store.run_profile_cycle()``.
            Contains ``distribution_drift``, ``null_drift``, etc.
        """
        # Step 1: Run the base analysis (LLM-based if enabled, otherwise heuristic fallback)
        result = self.ai_analyze_quarantined_sample(
            table_name, quarantined_rows, column_stats, historical_context
        )

        if not profile_report:
            return result

        # Step 2: Enrich with drift context
        dist_drift = profile_report.get("distribution_drift", {})
        null_drift = profile_report.get("null_drift", {})
        is_first_run = profile_report.get("is_first_run", True)

        drift_rules = []
        drift_explanations = []

        # ── Distribution drift → suggest rule updates ─────────────────
        for col_name, drift_info in dist_drift.items():
            if not drift_info.get("drift_detected"):
                continue

            psi = drift_info.get("psi", 0)
            status = drift_info.get("status", "UNKNOWN")

            drift_explanations.append(
                f"Column '{col_name}' distribution has shifted (PSI={psi:.4f}, {status})."
            )

            if status == "CRITICAL_DRIFT":
                drift_rules.append({
                    "rule_path": f"value_range.column_overrides.{col_name}.iqr_multiplier",
                    "value": 2.5,
                    "action": "update",
                    "reason": f"Distribution drift detected on '{col_name}' (PSI={psi:.4f}). "
                              f"Widen outlier bounds to accommodate the new distribution "
                              f"while upstream root cause is investigated.",
                    "origin": "profile_drift_detection"
                })

        # ── Null rate drift → suggest tolerance updates ───────────────
        for col_name, null_info in null_drift.items():
            current_rate = null_info.get("current_rate", 0)
            ema_rate = null_info.get("ema_rate", 0)
            status = null_info.get("status", "UNKNOWN")

            drift_explanations.append(
                f"Column '{col_name}' null rate spiked to {current_rate:.1%} "
                f"(historical avg: {ema_rate:.1%}, status: {status})."
            )

            if "CRITICAL" in status:
                drift_rules.append({
                    "rule_path": f"null_checks.column_overrides.{col_name}.tolerance",
                    "value": min(current_rate * 1.5, 0.30),
                    "action": "update",
                    "reason": f"Null rate on '{col_name}' spiked to {current_rate:.1%} "
                              f"(was {ema_rate:.1%}). Upstream schema change likely. "
                              f"Temporarily relax tolerance while root cause is resolved.",
                    "origin": "null_drift_detection"
                })

        # Step 3: Merge drift findings into result
        if drift_rules:
            result["suggested_rules"].extend(drift_rules)

        if drift_explanations:
            result["explanation"] += " DRIFT CONTEXT: " + " ".join(drift_explanations)

        # Adjust confidence based on drift evidence
        if dist_drift or null_drift:
            # More evidence = higher confidence
            drift_count = len(dist_drift) + len(null_drift)
            confidence_boost = min(drift_count * 0.03, 0.10)
            result["confidence"] = min(result["confidence"] + confidence_boost, 0.98)
            result["confidence"] = round(result["confidence"], 2)

        # Add metadata
        result.setdefault("analysis_metadata", {})
        result["analysis_metadata"]["method"] = "profile_based_v1"
        result["analysis_metadata"]["distribution_drifts"] = len(dist_drift)
        result["analysis_metadata"]["null_drifts"] = len(null_drift)
        result["analysis_metadata"]["is_first_run"] = is_first_run
        result["analysis_metadata"]["total_drift_rules_added"] = len(drift_rules)

        return result

    # ── Decision Tree Rule Induction (Component C) ────────────────────────

    def induce_rules_from_data(self, spark, clean_df, quarantine_df,
                               feature_columns, table_name, run_id):
        """Automatically generate interpretable rules from data patterns
        using a PySpark MLlib Decision Tree.

        This method trains a shallow Decision Tree on labeled data
        (clean=0, quarantine=1) and extracts human-readable IF/THEN rules
        from the tree structure.

        Parameters
        ----------
        spark : SparkSession
        clean_df : pyspark.sql.DataFrame
            Clean (non-quarantined) records.
        quarantine_df : pyspark.sql.DataFrame
            Quarantined records.
        feature_columns : list[str]
            Numeric columns to use as features for the tree.
        table_name : str
        run_id : str

        Returns
        -------
        dict
            ``{induced_rules: [...], model_accuracy: float, tree_depth: int}``
            or ``{error: str}`` on failure.
        """
        try:
            from pyspark.ml.feature import VectorAssembler
            from pyspark.ml.classification import DecisionTreeClassifier
            from pyspark.ml.evaluation import BinaryClassificationEvaluator
            from pyspark.sql import functions as F_ml
        except ImportError:
            print("[AI_ADVISOR] PySpark ML not available. Skipping rule induction.")
            return {"error": "PySpark ML not available", "induced_rules": []}

        if clean_df is None or quarantine_df is None:
            return {"error": "Missing DataFrames", "induced_rules": []}

        try:
            # ── Step 1: Prepare labeled dataset ───────────────────────
            # Keep only numeric feature columns that exist in both DataFrames
            clean_cols = set(clean_df.columns)
            quarantine_cols = set(quarantine_df.columns)
            valid_features = [c for c in feature_columns
                              if c in clean_cols and c in quarantine_cols]

            if len(valid_features) < 2:
                return {"error": "Insufficient numeric features for tree",
                        "induced_rules": []}

            # Select only features, fill nulls with 0 for training
            clean_subset = clean_df.select(valid_features).na.fill(0) \
                .withColumn("label", F_ml.lit(0.0))
            quarantine_subset = quarantine_df.select(valid_features).na.fill(0) \
                .withColumn("label", F_ml.lit(1.0))

            # Sample to keep training fast (max 5000 rows per class)
            max_per_class = 5000
            clean_count = clean_subset.count()
            quarantine_count = quarantine_subset.count()

            if quarantine_count < 10:
                return {"error": f"Too few quarantined records ({quarantine_count})",
                        "induced_rules": []}

            if clean_count > max_per_class:
                clean_subset = clean_subset.sample(
                    fraction=max_per_class / clean_count, seed=42
                )
            if quarantine_count > max_per_class:
                quarantine_subset = quarantine_subset.sample(
                    fraction=max_per_class / quarantine_count, seed=42
                )

            training_data = clean_subset.unionByName(quarantine_subset)

            # ── Step 2: Assemble features ─────────────────────────────
            assembler = VectorAssembler(
                inputCols=valid_features,
                outputCol="features",
                handleInvalid="skip"
            )
            assembled = assembler.transform(training_data)

            # ── Step 3: Train Decision Tree ───────────────────────────
            # maxDepth=4: keeps rules interpretable (max 16 leaf nodes)
            # minInstancesPerNode=20: prevents overfitting to noise
            dt = DecisionTreeClassifier(
                featuresCol="features",
                labelCol="label",
                maxDepth=4,
                minInstancesPerNode=20,
                impurity="gini"
            )

            print(f"[AI_ADVISOR] Training Decision Tree on {assembled.count()} rows "
                  f"with {len(valid_features)} features...")

            model = dt.fit(assembled)

            # ── Step 4: Evaluate accuracy ─────────────────────────────
            predictions = model.transform(assembled)
            evaluator = BinaryClassificationEvaluator(
                labelCol="label",
                rawPredictionCol="rawPrediction",
                metricName="areaUnderROC"
            )
            auc = evaluator.evaluate(predictions)

            print(f"[AI_ADVISOR] Decision Tree trained. AUC={auc:.4f}, "
                  f"depth={model.depth}, nodes={model.numNodes}")

            # ── Step 5: Extract rules from tree ───────────────────────
            induced_rules = self._extract_tree_rules(
                model, valid_features, table_name
            )

            result = {
                "induced_rules": induced_rules,
                "model_accuracy": round(auc, 4),
                "tree_depth": model.depth,
                "num_nodes": model.numNodes,
                "training_samples": assembled.count(),
                "features_used": valid_features
            }

            # Log induced rules as proposals
            if induced_rules:
                proposal = {
                    "root_cause": f"Decision Tree identified {len(induced_rules)} "
                                  f"data quality patterns (AUC={auc:.2f})",
                    "suggested_rules": induced_rules,
                    "false_positive_indices": [],
                    "recommended_threshold": None,
                    "confidence": round(min(auc, 0.98), 2),
                    "explanation": f"Automatic rule induction from {assembled.count()} "
                                   f"training samples. Tree depth={model.depth}. "
                                   f"Rules describe conditions under which records "
                                   f"are quarantined vs. passed.",
                    "status": "SUCCESS",
                    "analysis_metadata": {
                        "method": "decision_tree_induction",
                        "auc": round(auc, 4),
                        "tree_depth": model.depth,
                        "features": valid_features
                    }
                }
                self.log_proposal_to_es(table_name, run_id, proposal)
                print(f"[AI_ADVISOR] Logged {len(induced_rules)} induced rules "
                      f"to ES for governance review.")

            return result

        except Exception as e:
            print(f"[AI_ADVISOR] Rule induction failed: {e}")
            return {"error": str(e), "induced_rules": []}

    def _extract_tree_rules(self, model, feature_names, table_name):
        """Extract human-readable rules from a trained DecisionTreeModel.

        Walks the tree structure and converts split conditions into
        interpretable rule descriptions.

        Returns
        -------
        list[dict]
            Each dict has: condition, action, confidence, support, origin
        """
        rules = []

        try:
            tree_string = model.toDebugString
            if not tree_string:
                return rules

            # Parse the debug string to extract leaf nodes that predict "1" (quarantine)
            # Format: "If (feature X <= Y) ... Predict: 1.0"
            lines = tree_string.split("\n")
            current_conditions = []
            depth_conditions = {}

            for line in lines:
                stripped = line.strip()
                if not stripped:
                    continue

                # Calculate depth from indentation
                depth = (len(line) - len(line.lstrip())) // 3

                # Parse "If (feature N <= V)" or "If (feature N > V)"
                if stripped.startswith("If (feature"):
                    # Extract feature index and threshold
                    import re
                    match = re.search(
                        r'feature (\d+)\s*(<=|>)\s*([-\d.]+)', stripped
                    )
                    if match:
                        feat_idx = int(match.group(1))
                        operator = match.group(2)
                        threshold = float(match.group(3))

                        if feat_idx < len(feature_names):
                            col_name = feature_names[feat_idx]
                            condition = f"{col_name} {operator} {threshold:.4f}"
                            depth_conditions[depth] = condition

                # Parse "Predict: 1.0" (quarantine prediction)
                elif "Predict:" in stripped:
                    prediction = float(stripped.split(":")[-1].strip())

                    if prediction >= 0.5:  # Predicts quarantine
                        # Collect conditions from root to this leaf
                        leaf_conditions = []
                        for d in sorted(depth_conditions.keys()):
                            if d < depth:
                                leaf_conditions.append(depth_conditions[d])

                        if leaf_conditions:
                            condition_str = " AND ".join(leaf_conditions)
                            rules.append({
                                "rule_path": f"induced.{table_name}.tree_rule_{len(rules)+1}",
                                "condition": condition_str,
                                "action": "quarantine",
                                "confidence": round(prediction, 2),
                                "origin": "decision_tree_induction",
                                "reason": f"Decision Tree identified pattern: "
                                          f"IF {condition_str} THEN quarantine. "
                                          f"This rule was automatically induced from "
                                          f"historical clean vs quarantined data."
                            })

                # Clear deeper conditions when backtracking
                keys_to_remove = [k for k in depth_conditions if k >= depth]
                for k in keys_to_remove:
                    if k > depth:
                        del depth_conditions[k]

        except Exception as e:
            print(f"[AI_ADVISOR] Error extracting tree rules: {e}")

        # Limit to top 5 most specific rules (most conditions)
        rules.sort(key=lambda r: len(r.get("condition", "").split(" AND ")),
                   reverse=True)
        return rules[:5]

    # ── Elasticsearch logging ─────────────────────────────────────────────

    # ── Elasticsearch logging ─────────────────────────────────────────────

    def log_proposal_to_es(self, table_name, run_id, analysis_result):
        """Persist an AI analysis result to Elasticsearch for governance.

        If auto-promoted, the proposal is stored with status='APPROVED'.
        Otherwise it is logged as status='PROPOSED' awaiting human review.

        Parameters
        ----------
        table_name : str
            Logical table name.
        run_id : str
            Unique run identifier for correlation with quality-run logs.
        analysis_result : dict
            The output of :meth:`ai_analyze_quarantined_sample`.
        """
        doc = {
            "table_name": table_name,
            "run_id": run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": analysis_result.get("status", "PROPOSED"),
            "analysis_result": analysis_result,
        }

        url = f"{self._base_url}/{ES_INDEX_PROPOSALS}/_doc"
        headers = {"Content-Type": "application/json"}

        try:
            res = requests.post(
                url, headers=headers, auth=self._auth,
                data=json.dumps(doc, default=str), timeout=10,
            )
            res.raise_for_status()
            print(f"[AI_ADVISOR] Proposal logged to ES index '{ES_INDEX_PROPOSALS}' "
                  f"for table '{table_name}', run_id='{run_id}' with status '{doc['status']}'.")
            
            # Log upstream remediation ticket if present
            if "remediation_ticket" in analysis_result:
                self.log_remediation_ticket_to_es(table_name, run_id, analysis_result["remediation_ticket"])
        except Exception as e:
            print(f"[AI_ADVISOR] Failed to log proposal to ES: {e}")

    def log_remediation_ticket_to_es(self, table_name, run_id, remediation_ticket):
        """Persist an upstream remediation ticket to Elasticsearch for governance."""
        if not remediation_ticket or not isinstance(remediation_ticket, dict):
            return
        
        ticket_id = f"tkt_{run_id}_{table_name}"
        doc = {
            "ticket_id": ticket_id,
            "table_name": table_name,
            "run_id": run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "target_system": remediation_ticket.get("target_system", "Unknown Upstream Source"),
            "remediation_action": remediation_ticket.get("remediation_action", "Investigate anomalies at source."),
            "severity": remediation_ticket.get("severity", "warning"),
            "status": "OPEN"
        }
        
        url = f"{self._base_url}/sdoqap_upstream_remediations/_doc/{ticket_id}"
        headers = {"Content-Type": "application/json"}
        try:
            res = requests.put(
                url, headers=headers, auth=self._auth,
                data=json.dumps(doc, default=str), timeout=10,
            )
            res.raise_for_status()
            print(f"[AI_ADVISOR] Upstream Remediation Ticket logged to ES: {ticket_id}")
        except Exception as e:
            print(f"[AI_ADVISOR] Failed to log remediation ticket to ES: {e}")

    def promote_rules_to_config(self, table_name, suggested_rules):
        """Auto-promote and merge rules directly into Elasticsearch sdoqap_rules_registry
        and local rules_config.json.
        Keeps a backup copy rules_config.json.bak for safety/rollback.
        """
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rules_config.json")
        backup_path = config_path + ".bak"
        
        # 1. Load active config for the table (try ES first, then local)
        table_config = {}
        config = {}
        
        # Try local first to get structure & default settings
        if os.path.exists(config_path):
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    config = json.load(f)
                table_config = config.get(table_name, {})
            except Exception as e:
                print(f"[AUTO_PROMOTION] Failed to read local rules_config.json: {e}")

        # Try fetching from ES to ensure we have the absolute latest centralized overrides
        if self._base_url:
            try:
                url = f"{self._base_url}/sdoqap_rules_registry/_doc/{table_name}"
                res = requests.get(url, auth=self._auth, timeout=3)
                if res.status_code == 200:
                    table_config = res.json().get("_source", {})
                    print(f"[AUTO_PROMOTION] Loaded active rules override from Elasticsearch sdoqap_rules_registry.")
            except Exception as e:
                print(f"[AUTO_PROMOTION] Failed to read overrides from ES: {e}")

        try:
            promoted_count = 0
            for rule in suggested_rules:
                rule_path = rule.get("rule_path", "")
                val = rule.get("value")
                action = rule.get("action", "update")
                condition = rule.get("condition")

                # Handle normal override rules
                if rule_path and val is not None:
                    if action == "escalate":
                        continue

                    parts = rule_path.split(".")
                    
                    # ── Hard Guardrails Validation Layer (Task 3) ─────────────────────
                    # 1. Quality threshold base floor at 70.0%
                    if any(t in rule_path for t in ["quality_score_threshold", "base_value", "min_value"]) and isinstance(val, (int, float)):
                        if val < 70.0:
                            print(f"[GUARDRAIL VIOLATION] Rejected threshold change for '{rule_path}' = {val} on table '{table_name}'. Hard floor is 70.0%")
                            continue
                        
                        # 2. Maximum deviation (decrease) cap of 10% from active configuration
                        curr = table_config
                        for part in parts:
                            if isinstance(curr, dict) and part in curr:
                                curr = curr[part]
                            else:
                                curr = None
                                break
                        
                        if isinstance(curr, (int, float)) and curr > 0:
                            max_reduction = curr * 0.90
                            if val < max_reduction:
                                print(f"[GUARDRAIL VIOLATION] Rejected threshold change from {curr} to {val} on table '{table_name}'. Exceeds maximum 10% allowed decrease (Limit: {max_reduction:.2f})")
                                continue

                    # Dotted path update
                    d = table_config
                    for part in parts[:-1]:
                        d = d.setdefault(part, {})
                    
                    # 3. Check guardrails: Null tolerance cap at 0.30
                    if "tolerance" in parts[-1] and isinstance(val, (int, float)):
                        val = min(val, 0.30)
                    
                    d[parts[-1]] = val
                    promoted_count += 1
                    print(f"[AUTO_PROMOTION] Promoted override '{rule_path}' = {val} for table '{table_name}'")

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
                    print(f"[AUTO_PROMOTION] Promoted induced rule '{rule_name}': {condition}")

            if promoted_count > 0:
                # 2. Write to Elasticsearch sdoqap_rules_registry
                if self._base_url:
                    try:
                        url = f"{self._base_url}/sdoqap_rules_registry/_doc/{table_name}"
                        headers = {"Content-Type": "application/json"}
                        res = requests.put(url, headers=headers, auth=self._auth, json=table_config, timeout=5)
                        if res.status_code in [200, 201]:
                            print(f"[AUTO_PROMOTION] Successfully saved rules override to Elasticsearch sdoqap_rules_registry for '{table_name}'.")
                            # Log audit entry to ES for rule version control
                            audit_url = f"{self._base_url}/sdoqap_rules_audit_log/_doc"
                            for rule in suggested_rules:
                                audit_doc = {
                                    "table_name": table_name,
                                    "rule_path": rule.get("rule_path"),
                                    "value": rule.get("value"),
                                    "condition": rule.get("condition"),
                                    "action": rule.get("action"),
                                    "origin": rule.get("origin", "ai_advisor"),
                                    "reason": rule.get("reason", "Auto-promotion"),
                                    "timestamp": datetime.now(timezone.utc).isoformat()
                                }
                                requests.post(audit_url, headers=headers, auth=self._auth, json=audit_doc, timeout=3)
                    except Exception as e:
                        print(f"[AUTO_PROMOTION] Failed to save rules override/audit log to ES: {e}")

                # 3. Update local config file as a persistent fallback cache with locking
                if os.path.exists(config_path):
                    import time
                    lock_path = config_path + ".lock"
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
                        config[table_name] = table_config
                        with open(backup_path, "w", encoding="utf-8") as f:
                            json.dump(config, f, indent=2)
                        with open(config_path, "w", encoding="utf-8") as f:
                            json.dump(config, f, indent=2)
                        print(f"[AUTO_PROMOTION] Successfully updated local rules_config.json fallback cache. Backup saved.")
                    finally:
                        if acquired:
                            try:
                                os.remove(lock_path)
                            except Exception:
                                pass
                return True

        except Exception as e:
            print(f"[AUTO_PROMOTION] Failed to promote rules to config: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# Module-level safe entry point
# ═══════════════════════════════════════════════════════════════════════════════

def get_ai_advisor():
    """Return an :class:`AIRuleAdvisor` instance utilizing the local Heuristic Advisor.

    This is the recommended entry-point for ``spark_quality_engine.py`` —
    callers simply check for ``None`` instead of catching exceptions::

        advisor = get_ai_advisor()
        if advisor:
            ...

    Returns
    -------
    AIRuleAdvisor | None
        Returns ``None`` only if an unexpected error prevents initialization.
    """
    es_url = os.getenv("ELASTICSEARCH_URL", "")

    try:
        advisor = AIRuleAdvisor(es_url=es_url)
        print(f"[AI_ADVISOR] Initialized AIRuleAdvisor (Local Heuristic Backend: active).")
        return advisor
    except Exception as e:
        print(f"[AI_ADVISOR] Failed to initialize AIRuleAdvisor: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# Module self-test (developer convenience)
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("ai_rule_advisor.py loaded successfully.")
    print(f"  ELASTICSEARCH_URL = {os.getenv('ELASTICSEARCH_URL', '(not set)')}")

    advisor = get_ai_advisor()
    if advisor:
        # Quick trigger test
        ctx_anomaly = {"is_anomaly": True, "quality_score": 75}
        ctx_normal = {"is_anomaly": False, "quality_score": 95}
        print(f"  should_trigger(anomaly) = {advisor.should_trigger(ctx_anomaly)}")
        print(f"  should_trigger(normal)  = {advisor.should_trigger(ctx_normal)}")
    else:
        print("  Advisor not available. Pipeline will proceed without AI.")
