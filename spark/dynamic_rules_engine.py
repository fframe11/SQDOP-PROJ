"""
SDOQAP Dynamic Rules Engine — Statistical Adaptive Rules Engine (Layer 2)
==========================================================================

Dynamically computes rule thresholds from historical data distributions and
current DataFrame statistics.  This module sits between the static
``rules_config.json`` (Layer 1) and the AI Rule Advisor (Layer 3), providing
data-driven thresholds that adapt to evolving data profiles without requiring
manual tuning.

Design principles (Upstream-First Remediation):
    * Thresholds are derived from the data itself so that rule drift is
      automatically corrected at the *source* of truth (the data profile).
    * Every computed rule is logged to Elasticsearch for full auditability
      and lineage traceability.
    * Outlier flagging enriches records with context rather than silently
      dropping them — downstream consumers can decide how to act.

Usage from ``spark_quality_engine.py``::

    from dynamic_rules_engine import apply_adaptive_rules
    rules = apply_adaptive_rules(rules_config, table_name, df, spark)

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

try:
    from pyspark.sql import functions as F
    from pyspark.sql import DataFrame
except ImportError:
    # Allow module to be imported for unit-testing outside of a Spark env
    F = None  # type: ignore[assignment]
    DataFrame = None  # type: ignore[assignment,misc]

# ─── Elasticsearch helpers ────────────────────────────────────────────────────

def _get_es_connection():
    """Return ``(base_url, auth_tuple_or_None)`` parsed from the
    ``ELASTICSEARCH_URL`` environment variable.

    Follows the same credential-stripping pattern used throughout the project
    so that Basic-Auth credentials are never leaked into request URLs.
    """
    es_url = os.getenv("ELASTICSEARCH_URL", "")
    parsed = urlparse(es_url)
    auth = (parsed.username, parsed.password) if parsed.username else None
    if parsed.username:
        base_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
    else:
        base_url = es_url
    return base_url, auth


def _log_to_es(index_name, doc):
    """Post a JSON document to a given Elasticsearch index.

    Parameters
    ----------
    index_name : str
        Target ES index (e.g. ``sdoqap_dynamic_rules_log``).
    doc : dict
        Payload to index.
    """
    base_url, auth = _get_es_connection()
    url = f"{base_url}/{index_name}/_doc"
    headers = {"Content-Type": "application/json"}
    try:
        res = requests.post(url, headers=headers, auth=auth,
                            data=json.dumps(doc, default=str), timeout=10)
        res.raise_for_status()
        print(f"[DYNAMIC_RULES] Logged to ES index '{index_name}' successfully.")
    except Exception as e:
        print(f"[DYNAMIC_RULES] Error logging to ES index '{index_name}': {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Null-Profile Computation
# ═══════════════════════════════════════════════════════════════════════════════

def compute_null_profile(df, columns, historical_profiles=None):
    """Compute per-column null-rate profile and derive adaptive tolerances.

    For each requested column the function calculates the current null rate
    (``null_count / total_count``).  When *historical_profiles* are supplied,
    the current rate is blended with the historical average using an
    Exponential Moving Average (α = 0.3) so that one-off spikes do not
    immediately shift the threshold.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The DataFrame to profile.
    columns : list[str]
        Column names to inspect.  Columns not present in *df* are silently
        skipped (with a warning).
    historical_profiles : dict | None
        Optional mapping ``{column_name: avg_null_rate}`` from previous runs.
        Used to smooth the current rate via EMA.

    Returns
    -------
    dict
        ``{ column_name: { 'null_rate': float, 'tolerance': float,
        'is_required': bool } }``

        * **null_rate** — blended (or raw) null ratio in [0, 1].
        * **tolerance** — ``max(null_rate × 1.5, 0.01)`` capped at 0.50.
          Serves as the adaptive upper bound for acceptable nulls.
        * **is_required** — ``True`` when the historical null rate is below
          2 %, signalling a field that *should* always be populated.
    """
    if df is None or F is None:
        print("[DYNAMIC_RULES] Spark not available. Returning empty null profile.")
        return {}

    alpha = 0.3  # EMA smoothing factor — weight given to current observation
    total_count = df.count()
    if total_count == 0:
        print("[DYNAMIC_RULES] DataFrame is empty. Returning empty null profile.")
        return {}

    available_columns = set(df.columns)
    profiles = {}

    for col_name in columns:
        if col_name not in available_columns:
            print(f"[DYNAMIC_RULES] Column '{col_name}' not in DataFrame — skipped.")
            continue

        try:
            null_count = df.filter(F.col(col_name).isNull() | F.isnan(col_name)).count()
        except Exception:
            # isnan() fails for non-numeric types; fall back to null-only check
            null_count = df.filter(F.col(col_name).isNull()).count()

        current_rate = null_count / total_count

        # Blend with history if available
        if historical_profiles and col_name in historical_profiles:
            hist_rate = historical_profiles[col_name]
            blended_rate = alpha * current_rate + (1 - alpha) * hist_rate
        else:
            blended_rate = current_rate

        # Derive tolerance: generous enough for natural variance, capped to
        # prevent meaningless thresholds on heavily-null columns.
        tolerance = min(max(blended_rate * 1.5, 0.01), 0.50)

        # A column is "required" if its historical footprint shows it is
        # almost never null (< 2 %).
        hist_rate_for_required = (
            historical_profiles.get(col_name, blended_rate)
            if historical_profiles
            else blended_rate
        )
        is_required = hist_rate_for_required < 0.02

        profiles[col_name] = {
            "null_rate": round(blended_rate, 6),
            "tolerance": round(tolerance, 6),
            "is_required": is_required,
        }

    print(f"[DYNAMIC_RULES] Null profile computed for {len(profiles)} columns.")
    return profiles


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Value-Range Rule Computation
# ═══════════════════════════════════════════════════════════════════════════════

def compute_value_range_rules(df, numeric_columns, method="iqr", multiplier=1.5):
    """Compute outlier fences for numeric columns using the IQR method.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Source DataFrame.
    numeric_columns : list[str]
        Numeric column names to analyse.
    method : str
        Currently only ``'iqr'`` is implemented.  Included for future
        extensibility (e.g. ``'zscore'``, ``'mad'``).
    multiplier : float
        Tukey fence multiplier (default 1.5 for standard outliers; use 3.0
        for "far-out" detection).

    Returns
    -------
    dict
        ``{ column_name: { 'q1', 'q3', 'iqr', 'lower_bound',
        'upper_bound', 'method' } }``
    """
    if df is None or F is None:
        print("[DYNAMIC_RULES] Spark not available. Returning empty value ranges.")
        return {}

    available_columns = set(df.columns)
    value_ranges = {}

    for col_name in numeric_columns:
        if col_name not in available_columns:
            print(f"[DYNAMIC_RULES] Column '{col_name}' not in DataFrame — skipped.")
            continue
        try:
            # approxQuantile is preferred for large datasets: O(n) single pass.
            quantiles = df.stat.approxQuantile(col_name, [0.25, 0.75], 0.01)
            if len(quantiles) < 2 or quantiles[0] is None or quantiles[1] is None:
                print(f"[DYNAMIC_RULES] Could not compute quantiles for '{col_name}'.")
                continue

            q1, q3 = quantiles[0], quantiles[1]
            iqr = q3 - q1
            lower_bound = q1 - (multiplier * iqr)
            upper_bound = q3 + (multiplier * iqr)

            value_ranges[col_name] = {
                "q1": round(q1, 6),
                "q3": round(q3, 6),
                "iqr": round(iqr, 6),
                "lower_bound": round(lower_bound, 6),
                "upper_bound": round(upper_bound, 6),
                "method": method,
            }
        except Exception as e:
            print(f"[DYNAMIC_RULES] Error computing range for '{col_name}': {e}")

    print(f"[DYNAMIC_RULES] Value-range rules computed for {len(value_ranges)} columns.")
    return value_ranges


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Adaptive Quality-Score Threshold
# ═══════════════════════════════════════════════════════════════════════════════

def compute_adaptive_threshold(table_name, base_value, metric_key="quality_score",
                               window=15, min_value=70.0):
    """Derive a quality-score threshold that adapts to the table's own history.

    The function queries the last *window* runs from the
    ``sdoqap_quality_runs`` Elasticsearch index, computes the moving average
    and standard deviation of *metric_key*, then sets:

        adaptive_threshold = max(moving_avg − std_dev, min_value)

    This prevents a single bad run from permanently relaxing the threshold
    while still accommodating genuine distribution shifts.

    Parameters
    ----------
    table_name : str
        Logical table name as stored in ES (e.g. ``'users'``).
    base_value : float
        Static threshold from ``rules_config.json``.  Used as fallback when
        no history is available.
    metric_key : str
        Field inside the ES ``_source`` document to aggregate (default
        ``'quality_score'``).
    window : int
        Number of most-recent runs to consider.
    min_value : float
        Hard floor — the threshold will never drop below this value.

    Returns
    -------
    float
        The adaptive threshold.
    """
    base_url, auth = _get_es_connection()

    if not base_url:
        print("[DYNAMIC_RULES] ELASTICSEARCH_URL not set. Using base_value as threshold.")
        return float(base_value)

    index = "sdoqap_quality_runs"
    url = f"{base_url}/{index}/_search"
    query = {
        "query": {
            "term": {"table_name.keyword": table_name}
        },
        "sort": [{"timestamp": {"order": "desc"}}],
        "size": window,
        "_source": [metric_key, "table_name", "timestamp"],
    }

    try:
        res = requests.post(url, json=query, auth=auth,
                            headers={"Content-Type": "application/json"}, timeout=10)
        if res.status_code != 200:
            print(f"[DYNAMIC_RULES] ES query failed (HTTP {res.status_code}). "
                  f"Using base_value={base_value}.")
            return float(base_value)

        hits = res.json().get("hits", {}).get("hits", [])
        values = []
        for hit in hits:
            v = hit.get("_source", {}).get(metric_key)
            if v is not None:
                try:
                    values.append(float(v))
                except (TypeError, ValueError):
                    pass

        if len(values) < 2:
            print(f"[DYNAMIC_RULES] Insufficient history ({len(values)} runs). "
                  f"Using base_value={base_value}.")
            return float(base_value)

        import statistics
        moving_avg = statistics.mean(values)
        std_dev = statistics.stdev(values)
        adaptive = max(moving_avg - std_dev, min_value)

        print(f"[DYNAMIC_RULES] Adaptive threshold for '{table_name}': "
              f"avg={moving_avg:.2f}, std={std_dev:.2f}, "
              f"threshold={adaptive:.2f} (base={base_value})")
        return round(adaptive, 4)

    except Exception as e:
        print(f"[DYNAMIC_RULES] Error computing adaptive threshold: {e}. "
              f"Falling back to base_value={base_value}.")
        return float(base_value)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Orchestrator — apply_adaptive_rules
# ═══════════════════════════════════════════════════════════════════════════════

def apply_adaptive_rules(rules_config, table_name, df, spark=None):
    """Enhance a static *rules_config* dict with dynamically computed values.

    This is the main entry-point called from ``spark_quality_engine.py``.
    For every rule entry whose ``mode`` is ``'adaptive'`` or ``'auto'``, the
    corresponding compute function is invoked and the result merged back into
    the config dict.

    Supported adaptive rule types
    -----------------------------
    * ``quality_score_threshold`` with ``mode: 'adaptive'`` — delegates to
      :func:`compute_adaptive_threshold`.
    * ``null_checks`` with ``mode: 'adaptive'`` — delegates to
      :func:`compute_null_profile`.
    * ``value_range`` with ``mode: 'auto'`` — delegates to
      :func:`compute_value_range_rules` for numeric columns.

    Parameters
    ----------
    rules_config : dict
        The loaded rules dictionary (from ``rules_config.json`` via
        ``load_rules_config()``).
    table_name : str
        Logical table name.
    df : pyspark.sql.DataFrame
        The DataFrame being validated.
    spark : SparkSession | None
        Optional Spark session (currently unused but reserved for future
        extensions such as reading historical profiles from Delta tables).

    Returns
    -------
    dict
        A *new* dict with all original keys plus computed dynamic values
        under the ``_dynamic`` namespace.
    """
    if rules_config is None:
        rules_config = {}

    enhanced = dict(rules_config)  # shallow copy — original is untouched
    dynamic_log = {
        "table_name": table_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "computed_rules": {},
    }

    # ── 4a. Adaptive quality-score threshold ──────────────────────────────
    qst = rules_config.get("quality_score_threshold")
    qst_mode = rules_config.get("quality_score_threshold_mode", "static")

    if qst_mode in ("adaptive", "auto") and qst is not None:
        try:
            adaptive_threshold = compute_adaptive_threshold(
                table_name, base_value=qst,
            )
            enhanced["quality_score_threshold"] = adaptive_threshold
            dynamic_log["computed_rules"]["quality_score_threshold"] = {
                "base": qst,
                "adaptive": adaptive_threshold,
                "mode": qst_mode,
            }
        except Exception as e:
            print(f"[DYNAMIC_RULES] Adaptive threshold computation failed: {e}. "
                  f"Keeping static value={qst}.")

    # ── 4b. Adaptive null checks ──────────────────────────────────────────
    null_checks = rules_config.get("null_checks", {})
    null_mode = null_checks.get("mode", "static") if isinstance(null_checks, dict) else "static"

    if null_mode in ("adaptive", "auto") and df is not None:
        try:
            columns = null_checks.get("columns", df.columns) if isinstance(null_checks, dict) else df.columns
            historical = null_checks.get("historical_profiles") if isinstance(null_checks, dict) else None
            null_profiles = compute_null_profile(df, columns, historical)
            enhanced["_dynamic_null_profiles"] = null_profiles
            dynamic_log["computed_rules"]["null_profiles"] = null_profiles
        except Exception as e:
            print(f"[DYNAMIC_RULES] Null-profile computation failed: {e}.")

    # ── 4c. Auto value-range rules ────────────────────────────────────────
    vr_config = rules_config.get("value_range", {})
    vr_mode = vr_config.get("mode", "static") if isinstance(vr_config, dict) else "static"

    if vr_mode in ("adaptive", "auto") and df is not None and F is not None:
        try:
            # Detect numeric columns automatically unless explicitly listed
            explicit_cols = vr_config.get("columns") if isinstance(vr_config, dict) else None
            if explicit_cols:
                numeric_cols = explicit_cols
            else:
                from pyspark.sql.types import (
                    IntegerType, LongType, FloatType, DoubleType, ShortType,
                    DecimalType,
                )
                numeric_types = (IntegerType, LongType, FloatType, DoubleType,
                                 ShortType, DecimalType)
                numeric_cols = [
                    field.name for field in df.schema.fields
                    if isinstance(field.dataType, numeric_types)
                ]

            multiplier = vr_config.get("multiplier", 1.5) if isinstance(vr_config, dict) else 1.5
            value_ranges = compute_value_range_rules(
                df, numeric_cols, multiplier=multiplier,
            )
            enhanced["_dynamic_value_ranges"] = value_ranges
            dynamic_log["computed_rules"]["value_ranges"] = value_ranges
        except Exception as e:
            print(f"[DYNAMIC_RULES] Value-range computation failed: {e}.")

    # ── 4d. Persist the dynamic log ───────────────────────────────────────
    if dynamic_log["computed_rules"]:
        _log_to_es("sdoqap_dynamic_rules_log", dynamic_log)
    else:
        print("[DYNAMIC_RULES] No adaptive rules computed — all rules are static.")

    return enhanced


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Outlier Row Flagging
# ═══════════════════════════════════════════════════════════════════════════════

def flag_outlier_rows(df, value_ranges):
    """Annotate each row with outlier flags based on precomputed value ranges.

    Rather than silently discarding suspicious rows, this function enriches
    them with context so that downstream consumers (and the AI Advisor) can
    perform root-cause diagnosis on the *original* data.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame.
    value_ranges : dict
        Output of :func:`compute_value_range_rules`.

    Returns
    -------
    pyspark.sql.DataFrame
        The original DataFrame plus two new columns:

        * ``_outlier_flag`` (BooleanType) — ``True`` when at least one
          numeric value falls outside its expected range.
        * ``_outlier_details`` (StringType) — pipe-delimited description of
          which columns are out-of-range and by how much, useful for
          quarantine-log inspection.
    """
    if df is None or F is None:
        print("[DYNAMIC_RULES] Spark not available. Returning DataFrame unchanged.")
        return df

    if not value_ranges:
        print("[DYNAMIC_RULES] No value ranges provided. Adding clean flag columns.")
        return (
            df
            .withColumn("_outlier_flag", F.lit(False))
            .withColumn("_outlier_details", F.lit(""))
        )

    available_columns = set(df.columns)
    condition_parts = []
    detail_parts = []

    for col_name, bounds in value_ranges.items():
        if col_name not in available_columns:
            continue

        lower = bounds["lower_bound"]
        upper = bounds["upper_bound"]

        # Boolean condition: value outside [lower, upper]
        is_outlier = (F.col(col_name) < F.lit(lower)) | (F.col(col_name) > F.lit(upper))
        condition_parts.append(is_outlier)

        # Human-readable detail fragment
        detail_expr = F.when(
            is_outlier,
            F.concat(
                F.lit(f"{col_name}="),
                F.coalesce(F.col(col_name).cast("string"), F.lit("NULL")),
                F.lit(f" (expected [{lower:.4f}, {upper:.4f}])"),
            ),
        ).otherwise(F.lit(""))
        detail_parts.append(detail_expr)

    if not condition_parts:
        return (
            df
            .withColumn("_outlier_flag", F.lit(False))
            .withColumn("_outlier_details", F.lit(""))
        )

    # Combine: any single column outlier ⇒ row is flagged
    combined_flag = condition_parts[0]
    for cond in condition_parts[1:]:
        combined_flag = combined_flag | cond

    # Concatenate non-empty detail fragments with pipe separator
    if len(detail_parts) == 1:
        combined_details = detail_parts[0]
    else:
        combined_details = F.concat_ws(" | ", *detail_parts)
        # Trim leading/trailing separators from empty fragments
        combined_details = F.regexp_replace(combined_details, r"^\s*\|\s*|\s*\|\s*$", "")
        combined_details = F.regexp_replace(combined_details, r"\s*\|\s*\|\s*", " | ")

    result_df = (
        df
        .withColumn("_outlier_flag", combined_flag)
        .withColumn("_outlier_details", F.trim(combined_details))
    )

    print("[DYNAMIC_RULES] Outlier flags applied to DataFrame.")
    return result_df



def detect_unsupervised_anomalies(df, numeric_columns, threshold=3.0):
    """Performs unsupervised anomaly detection across numeric columns.
    Uses native Spark SQL functions to calculate Z-scores dynamically for each row.
    Rows with values deviating by more than `threshold` standard deviations
    from the mean are flagged.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame
    numeric_columns : list of str
        List of numeric column names to analyze
    threshold : float, default 3.0
        The Z-score threshold beyond which a row is flagged as an anomaly

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame with `_unsupervised_anomaly` (boolean) and `_unsupervised_anomaly_details` (string)
    """
    if not numeric_columns or df is None:
        return df.withColumn("_unsupervised_anomaly", F.lit(False)).withColumn("_unsupervised_anomaly_details", F.lit(""))

    # Compute mean and stddev for all numeric columns in a single aggregation
    stats_exprs = []
    for col in numeric_columns:
        stats_exprs.append(F.mean(col).alias(f"{col}_mean"))
        stats_exprs.append(F.stddev(col).alias(f"{col}_stddev"))

    try:
        stats = df.select(*stats_exprs).first()
        if not stats:
            return df.withColumn("_unsupervised_anomaly", F.lit(False)).withColumn("_unsupervised_anomaly_details", F.lit(""))
    except Exception as e:
        print(f"[DYNAMIC_RULES] Failed to compute column statistics for unsupervised check: {e}")
        return df.withColumn("_unsupervised_anomaly", F.lit(False)).withColumn("_unsupervised_anomaly_details", F.lit(""))

    anomaly_conditions = []
    detail_exprs = []

    for col in numeric_columns:
        mean_val = stats[f"{col}_mean"]
        std_val = stats[f"{col}_stddev"]

        # Handle zero variance or single-value columns
        if std_val is None or std_val == 0.0:
            continue

        # Z-score condition: abs(val - mean) / stddev > threshold
        z_score_expr = F.abs(F.col(col) - F.lit(mean_val)) / F.lit(std_val)
        is_anomaly = z_score_expr > F.lit(threshold)
        anomaly_conditions.append(is_anomaly)

        detail_expr = F.when(
            is_anomaly,
            F.concat(
                F.lit(f"{col}_zscore="),
                F.round(z_score_expr, 2).cast("string"),
                F.lit(f" (val={col} deviates > {threshold}σ)")
            )
        ).otherwise(F.lit(""))
        detail_exprs.append(detail_expr)

    if not anomaly_conditions:
        return df.withColumn("_unsupervised_anomaly", F.lit(False)).withColumn("_unsupervised_anomaly_details", F.lit(""))

    # Combine anomaly flags: True if ANY column is anomalous
    combined_anomaly = anomaly_conditions[0]
    for cond in anomaly_conditions[1:]:
        combined_anomaly = combined_anomaly | cond

    # Concatenate anomaly details
    if len(detail_exprs) == 1:
        combined_details = detail_exprs[0]
    else:
        combined_details = F.concat_ws(" | ", *detail_exprs)
        combined_details = F.regexp_replace(combined_details, r"^\s*\|\s*|\s*\|\s*$", "")
        combined_details = F.regexp_replace(combined_details, r"\s*\|\s*\|\s*", " | ")

    return (
        df
        .withColumn("_unsupervised_anomaly", combined_anomaly)
        .withColumn("_unsupervised_anomaly_details", F.trim(combined_details))
    )


def generate_rules_from_schema(table_name, schema_spec, primary_key, date_column):
    """Automatically generates and registers a v2 rules configuration
    for a newly encountered table, ensuring out-of-the-box system coverage.

    Parameters
    ----------
    table_name : str
        Name of the table
    schema_spec : dict
        Inferred schema specification
    primary_key : str or list of str
        The primary key column name(s)
    date_column : str
        The date column name
    """
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rules_config.json")
    if not os.path.exists(config_path):
        return

    try:
        with open(config_path, "r") as f:
            config = json.load(f)

        if table_name in config:
            # Rules already exist, skip auto-generation to prevent overwriting custom config
            return

        # Build default dynamic rules layout
        new_rules = {
            "null_primary_key": { "enabled": True, "severity": "critical", "mode": "strict" },
            "null_date_column": { "enabled": bool(date_column), "severity": "warning", "mode": "strict" },
            "duplicate_check": { "enabled": True, "severity": "critical", "mode": "strict" },
            "null_checks": {
                "mode": "adaptive",
                "default_tolerance": 0.05,
                "learn_from_history": True,
                "column_overrides": {}
            },
            "value_range": {
                "mode": "auto",
                "method": "iqr",
                "iqr_multiplier": 1.5,
                "column_overrides": {}
            },
            "freshness_threshold_hours": {
                "mode": "adaptive" if date_column else "strict",
                "base_value": 48 if date_column else None,
                "learn_from_history": bool(date_column)
            },
            "quality_score_threshold": {
                "mode": "adaptive",
                "base_value": 90.0,
                "min_value": 70.0,
                "adjustment_window_runs": 15
            },
            "ai_advisor": {
                "enabled": True,
                "trigger": "on_anomaly",
                "model": "gemini-2.0-flash",
                "max_rows_to_analyze": 50,
                "confidence_threshold": 0.7
            }
        }

        config[table_name] = new_rules

        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)

        print(f"[DYNAMIC_RULES] Automatically generated rules config for new table '{table_name}'")
    except Exception as e:
        print(f"[DYNAMIC_RULES] Failed to auto-generate rules configuration: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# Module self-test (developer convenience)
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("dynamic_rules_engine.py loaded successfully.")
    print(f"  ELASTICSEARCH_URL = {os.getenv('ELASTICSEARCH_URL', '(not set)')}")
    print("  Available functions: compute_null_profile, compute_value_range_rules,")
    print("                       compute_adaptive_threshold, apply_adaptive_rules,")
    print("                       flag_outlier_rows")
