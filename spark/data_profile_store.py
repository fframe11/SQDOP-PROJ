"""
SDOQAP Data Profile Store — Stateful Data Intelligence (Component A+B)
======================================================================

Provides the "memory" for the Dynamic Decision Engine. Instead of treating
each quality run as independent, this module maintains evolving statistical
profiles for every table+column combination, enabling:

    * **Distribution drift detection** via Population Stability Index (PSI)
    * **Anomaly detection without rules** by comparing current batch stats
      against learned baselines
    * **Cross-column anomaly scoring** via correlation-aware distance metrics

Profiles are persisted to Elasticsearch index ``sdoqap_data_profiles`` and
updated via Exponential Moving Average (EMA) to smoothly adapt to genuine
data evolution while resisting one-off outlier spikes.

Design principles (Upstream-First Remediation):
    * Drift is detected at the *data distribution* level — the true upstream
      signal — rather than at the rule-violation level (which is downstream).
    * Every profile update is logged for full auditability.
    * The module degrades gracefully when ES is unavailable.

Runs inside Docker at ``/opt/spark-apps/``.
"""

import os
import json
import math
from datetime import datetime, timezone
from urllib.parse import urlparse

# ─── Environment bootstrap ───────────────────────────────────────────────────

def _load_env_file():
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

_load_env_file()

import requests

try:
    from pyspark.sql import functions as F
    from pyspark.sql import DataFrame
    from pyspark.sql.types import DoubleType, IntegerType, LongType, FloatType, ShortType, DecimalType
    HAS_SPARK = True
except ImportError:
    F = None
    DataFrame = None
    HAS_SPARK = False

# ─── Constants ────────────────────────────────────────────────────────────────

ES_INDEX_PROFILES = "sdoqap_data_profiles"
EMA_ALPHA = 0.3          # Weight for current observation in EMA
PSI_WARN_THRESHOLD = 0.1
PSI_CRITICAL_THRESHOLD = 0.25
PSI_NUM_BUCKETS = 10      # Number of quantile buckets for PSI calculation


# ─── Elasticsearch helpers ────────────────────────────────────────────────────

def _get_es_connection():
    es_url = os.getenv("ELASTICSEARCH_URL", "")
    parsed = urlparse(es_url)
    auth = (parsed.username, parsed.password) if parsed.username else None
    if parsed.username:
        base_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
    else:
        base_url = es_url
    return base_url, auth


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Profile Read/Write
# ═══════════════════════════════════════════════════════════════════════════════

def read_profile(table_name):
    """Read the stored data profile for a table from Elasticsearch.

    Returns
    -------
    dict
        Mapping ``{column_name: profile_dict}`` or empty dict if not found.
    """
    base_url, auth = _get_es_connection()
    if not base_url:
        return {}

    url = f"{base_url}/{ES_INDEX_PROFILES}/_search"
    query = {
        "size": 200,
        "query": {"term": {"table_name.keyword": table_name}},
        "sort": [{"updated_at": {"order": "desc"}}]
    }

    try:
        res = requests.post(url, json=query, auth=auth,
                            headers={"Content-Type": "application/json"}, timeout=10)
        if res.status_code != 200:
            return {}

        hits = res.json().get("hits", {}).get("hits", [])
        profiles = {}
        seen_columns = set()
        for hit in hits:
            src = hit.get("_source", {})
            col = src.get("column_name", "")
            if col and col not in seen_columns:
                seen_columns.add(col)
                profiles[col] = src
        
        if profiles:
            print(f"[PROFILE_STORE] Loaded {len(profiles)} column profiles for '{table_name}'.")
        return profiles
    except Exception as e:
        print(f"[PROFILE_STORE] Failed to read profiles from ES: {e}")
        return {}


def write_profile(table_name, column_name, profile_data):
    """Write or update a column profile to Elasticsearch.

    Uses upsert semantics: if a profile for this table+column exists, it is
    updated; otherwise a new document is created.
    """
    base_url, auth = _get_es_connection()
    if not base_url:
        return

    doc_id = f"{table_name}__{column_name}"
    url = f"{base_url}/{ES_INDEX_PROFILES}/_doc/{doc_id}"

    profile_data["table_name"] = table_name
    profile_data["column_name"] = column_name
    profile_data["updated_at"] = datetime.now(timezone.utc).isoformat()

    try:
        res = requests.put(
            url, auth=auth, headers={"Content-Type": "application/json"},
            data=json.dumps(profile_data, default=str), timeout=10
        )
        res.raise_for_status()
    except Exception as e:
        print(f"[PROFILE_STORE] Failed to write profile for '{table_name}.{column_name}': {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Profile Computation from DataFrame
# ═══════════════════════════════════════════════════════════════════════════════

def compute_current_profile(df, numeric_columns, all_columns):
    """Compute a fresh statistical profile from the current DataFrame batch.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
    numeric_columns : list[str]
        Columns to compute distribution stats for.
    all_columns : list[str]
        All columns to compute null rates for.

    Returns
    -------
    dict
        ``{column_name: {distribution: {...}, null_profile: {...}, cardinality: {...}}}``
    """
    if not HAS_SPARK or df is None:
        return {}

    total_count = df.count()
    if total_count == 0:
        return {}

    profiles = {}

    # ── Null rates for ALL columns ────────────────────────────────────────
    for col_name in all_columns:
        if col_name not in df.columns:
            continue

        try:
            null_count = df.filter(F.col(col_name).isNull()).count()
        except Exception:
            null_count = 0

        null_rate = null_count / total_count if total_count > 0 else 0.0

        profiles.setdefault(col_name, {})
        profiles[col_name]["null_profile"] = {
            "current_null_rate": round(null_rate, 6),
            "null_count": null_count,
            "total_count": total_count
        }

    # ── Distribution stats for NUMERIC columns ───────────────────────────
    for col_name in numeric_columns:
        if col_name not in df.columns:
            continue

        try:
            # Compute key statistics in a single aggregation pass
            stats_row = df.select(
                F.mean(col_name).alias("mean"),
                F.stddev(col_name).alias("stddev"),
                F.min(col_name).alias("min_val"),
                F.max(col_name).alias("max_val"),
                F.count(col_name).alias("non_null_count")
            ).first()

            if stats_row is None or stats_row["non_null_count"] == 0:
                continue

            # Compute percentiles (single pass via approxQuantile)
            percentiles = df.stat.approxQuantile(
                col_name, [0.05, 0.25, 0.50, 0.75, 0.95], 0.01
            )

            # Compute skewness and kurtosis
            try:
                sk_row = df.select(
                    F.skewness(col_name).alias("skew"),
                    F.kurtosis(col_name).alias("kurt")
                ).first()
                skewness = float(sk_row["skew"]) if sk_row["skew"] is not None else 0.0
                kurtosis = float(sk_row["kurt"]) if sk_row["kurt"] is not None else 0.0
            except Exception:
                skewness = 0.0
                kurtosis = 0.0

            # Compute distinct count for cardinality
            distinct_count = df.select(col_name).distinct().count()

            profiles.setdefault(col_name, {})
            profiles[col_name]["distribution"] = {
                "mean": round(float(stats_row["mean"] or 0), 6),
                "stddev": round(float(stats_row["stddev"] or 0), 6),
                "min": round(float(stats_row["min_val"] or 0), 6),
                "max": round(float(stats_row["max_val"] or 0), 6),
                "median": round(float(percentiles[2]) if len(percentiles) > 2 else 0, 6),
                "p5": round(float(percentiles[0]) if len(percentiles) > 0 else 0, 6),
                "p25": round(float(percentiles[1]) if len(percentiles) > 1 else 0, 6),
                "p75": round(float(percentiles[3]) if len(percentiles) > 3 else 0, 6),
                "p95": round(float(percentiles[4]) if len(percentiles) > 4 else 0, 6),
                "skewness": round(skewness, 6),
                "kurtosis": round(kurtosis, 6),
                "non_null_count": int(stats_row["non_null_count"])
            }
            profiles[col_name]["cardinality"] = {
                "distinct_count": distinct_count,
                "distinct_ratio": round(distinct_count / total_count, 6) if total_count > 0 else 0
            }

            # Compute histogram bucket boundaries for PSI calculation
            try:
                bucket_boundaries = df.stat.approxQuantile(
                    col_name,
                    [i / PSI_NUM_BUCKETS for i in range(PSI_NUM_BUCKETS + 1)],
                    0.02
                )
                profiles[col_name]["histogram_boundaries"] = [round(b, 6) for b in bucket_boundaries]
            except Exception:
                profiles[col_name]["histogram_boundaries"] = []

        except Exception as e:
            print(f"[PROFILE_STORE] Error profiling column '{col_name}': {e}")

    print(f"[PROFILE_STORE] Computed fresh profile for {len(profiles)} columns.")
    return profiles


# ═══════════════════════════════════════════════════════════════════════════════
# 3. EMA-Based Profile Update
# ═══════════════════════════════════════════════════════════════════════════════

def update_profiles_ema(stored_profiles, current_profiles):
    """Merge current batch statistics into stored profiles using EMA smoothing.

    For each numeric stat, the update formula is:
        new_value = α × current_value + (1 − α) × stored_value

    This ensures profiles adapt gradually to genuine data evolution while
    resisting one-off outlier spikes.

    Parameters
    ----------
    stored_profiles : dict
        Previously stored profiles from ES (or empty dict for first run).
    current_profiles : dict
        Fresh profiles computed from the current batch.

    Returns
    -------
    dict
        Updated profiles ready to be written back to ES.
    """
    alpha = EMA_ALPHA
    updated = {}

    for col_name, current in current_profiles.items():
        stored = stored_profiles.get(col_name, {})
        merged = {}

        # ── Distribution: EMA update ──────────────────────────────────────
        curr_dist = current.get("distribution", {})
        stored_dist = stored.get("distribution", {})

        if curr_dist:
            if stored_dist:
                merged_dist = {}
                for key in ["mean", "stddev", "median", "p5", "p25", "p75", "p95",
                            "skewness", "kurtosis"]:
                    curr_val = curr_dist.get(key, 0.0)
                    stored_val = stored_dist.get(key, curr_val)
                    merged_dist[key] = round(alpha * curr_val + (1 - alpha) * stored_val, 6)
                
                # min/max: keep absolute extremes
                merged_dist["min"] = min(curr_dist.get("min", 0), stored_dist.get("min", float('inf')))
                merged_dist["max"] = max(curr_dist.get("max", 0), stored_dist.get("max", float('-inf')))
                merged_dist["non_null_count"] = curr_dist.get("non_null_count", 0)
                merged["distribution"] = merged_dist
            else:
                # First run: use current as baseline
                merged["distribution"] = curr_dist

        # ── Null profile: EMA update + history ────────────────────────────
        curr_null = current.get("null_profile", {})
        stored_null = stored.get("null_profile", {})

        if curr_null:
            current_rate = curr_null.get("current_null_rate", 0.0)
            stored_rate = stored_null.get("null_rate_ema", current_rate)
            
            ema_rate = round(alpha * current_rate + (1 - alpha) * stored_rate, 6)
            
            # Keep a sliding window of last 10 null rates
            history = list(stored_null.get("null_rate_history", []))
            history.append(current_rate)
            if len(history) > 10:
                history = history[-10:]

            merged["null_profile"] = {
                "null_rate_ema": ema_rate,
                "current_null_rate": current_rate,
                "null_rate_history": history,
                "is_required": ema_rate < 0.02,
                "max_observed_null_rate": max(
                    current_rate,
                    stored_null.get("max_observed_null_rate", 0.0)
                )
            }

        # ── Cardinality: latest ───────────────────────────────────────────
        if "cardinality" in current:
            merged["cardinality"] = current["cardinality"]

        # ── Histogram boundaries: use current (for PSI against next batch) ─
        if "histogram_boundaries" in current:
            merged["histogram_boundaries"] = current["histogram_boundaries"]
        elif "histogram_boundaries" in stored:
            merged["histogram_boundaries"] = stored["histogram_boundaries"]

        # ── Profile version counter ───────────────────────────────────────
        merged["profile_version"] = stored.get("profile_version", 0) + 1

        updated[col_name] = merged

    return updated


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Population Stability Index (PSI) — Distribution Drift Detection
# ═══════════════════════════════════════════════════════════════════════════════

def compute_psi(df, col_name, baseline_boundaries):
    """Compute the Population Stability Index between a baseline distribution
    and the current batch for a single column.

    PSI measures how much the distribution has shifted:
        PSI = Σ (P_i − Q_i) × ln(P_i / Q_i)

    Where P_i = proportion in bucket i of baseline, Q_i = proportion in bucket i
    of current batch.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Current batch DataFrame.
    col_name : str
        Column to compute PSI for.
    baseline_boundaries : list[float]
        Quantile boundaries from the stored profile (e.g., [0, 10, 20, ..., 100]).

    Returns
    -------
    float
        PSI score. < 0.1 = stable, 0.1-0.25 = moderate drift, > 0.25 = significant drift.
        Returns -1.0 on error.
    """
    if not HAS_SPARK or df is None or not baseline_boundaries:
        return -1.0

    if len(baseline_boundaries) < 3:
        return -1.0

    try:
        total_count = df.filter(F.col(col_name).isNotNull()).count()
        if total_count == 0:
            return -1.0

        n_buckets = len(baseline_boundaries) - 1
        # Expected proportion per bucket (uniform from baseline quantiles)
        expected_prop = 1.0 / n_buckets

        psi = 0.0
        epsilon = 1e-6  # Prevent log(0) and division by zero

        for i in range(n_buckets):
            lower = baseline_boundaries[i]
            upper = baseline_boundaries[i + 1]

            if i == 0:
                bucket_count = df.filter(F.col(col_name) <= upper).count()
            elif i == n_buckets - 1:
                bucket_count = df.filter(F.col(col_name) > lower).count()
            else:
                bucket_count = df.filter(
                    (F.col(col_name) > lower) & (F.col(col_name) <= upper)
                ).count()

            actual_prop = max(bucket_count / total_count, epsilon)
            expected = max(expected_prop, epsilon)

            psi += (actual_prop - expected) * math.log(actual_prop / expected)

        return round(abs(psi), 6)

    except Exception as e:
        print(f"[PROFILE_STORE] Error computing PSI for '{col_name}': {e}")
        return -1.0


def detect_distribution_drift(df, numeric_columns, stored_profiles):
    """Run PSI-based drift detection across all profiled numeric columns.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
    numeric_columns : list[str]
    stored_profiles : dict
        Previously stored profiles from ES.

    Returns
    -------
    dict
        ``{column_name: {psi: float, status: str, drift_detected: bool}}``
    """
    drift_report = {}

    for col_name in numeric_columns:
        if col_name not in stored_profiles:
            continue

        boundaries = stored_profiles[col_name].get("histogram_boundaries", [])
        if not boundaries or len(boundaries) < 3:
            continue

        psi = compute_psi(df, col_name, boundaries)
        if psi < 0:
            continue

        if psi > PSI_CRITICAL_THRESHOLD:
            status = "CRITICAL_DRIFT"
            drift_detected = True
        elif psi > PSI_WARN_THRESHOLD:
            status = "MODERATE_DRIFT"
            drift_detected = True
        else:
            status = "STABLE"
            drift_detected = False

        drift_report[col_name] = {
            "psi": psi,
            "status": status,
            "drift_detected": drift_detected
        }

        if drift_detected:
            print(f"[PROFILE_STORE] Distribution drift detected on '{col_name}': "
                  f"PSI={psi:.4f} ({status})")

    return drift_report


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Null Rate Drift Detection
# ═══════════════════════════════════════════════════════════════════════════════

def detect_null_rate_drift(current_profiles, stored_profiles):
    """Detect columns where null rates have changed significantly compared
    to the stored EMA baseline.

    A column is flagged if:
        current_rate > ema_rate × 3.0  (3× the historical average)
        AND current_rate > 0.05        (ignore tiny absolute rates)

    Returns
    -------
    dict
        ``{column_name: {current_rate, ema_rate, ratio, status}}``
    """
    drift_report = {}

    for col_name, curr in current_profiles.items():
        curr_null = curr.get("null_profile", {})
        stored = stored_profiles.get(col_name, {})
        stored_null = stored.get("null_profile", {})

        current_rate = curr_null.get("current_null_rate", 0.0)
        ema_rate = stored_null.get("null_rate_ema", current_rate)

        if ema_rate <= 0:
            if current_rate > 0.05:
                drift_report[col_name] = {
                    "current_rate": current_rate,
                    "ema_rate": 0.0,
                    "ratio": float('inf'),
                    "status": "NEW_NULLS_DETECTED"
                }
            continue

        ratio = current_rate / ema_rate if ema_rate > 0 else 0

        if current_rate > ema_rate * 3.0 and current_rate > 0.05:
            status = "CRITICAL_NULL_DRIFT"
        elif current_rate > ema_rate * 2.0 and current_rate > 0.03:
            status = "MODERATE_NULL_DRIFT"
        else:
            status = "STABLE"
            continue  # Don't report stable columns

        drift_report[col_name] = {
            "current_rate": round(current_rate, 6),
            "ema_rate": round(ema_rate, 6),
            "ratio": round(ratio, 2),
            "status": status
        }

        print(f"[PROFILE_STORE] Null rate drift on '{col_name}': "
              f"current={current_rate:.2%} vs EMA={ema_rate:.2%} "
              f"(ratio={ratio:.1f}x, {status})")

    return drift_report


# ═══════════════════════════════════════════════════════════════════════════════
# 6. Orchestrator — Full Profile Update Cycle
# ═══════════════════════════════════════════════════════════════════════════════

def run_profile_cycle(df, table_name, numeric_columns=None):
    """Execute a complete profile update cycle:

    1. Read stored profiles from ES
    2. Compute fresh profiles from current batch
    3. Detect distribution drift (PSI) and null rate drift
    4. Update profiles via EMA
    5. Write updated profiles back to ES

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The DataFrame being validated (typically clean_df or full df).
    table_name : str
        Logical table name.
    numeric_columns : list[str] | None
        If None, auto-detects numeric columns from schema.

    Returns
    -------
    dict
        ``{
            "stored_profiles": {...},
            "current_profiles": {...},
            "distribution_drift": {...},
            "null_drift": {...},
            "is_first_run": bool,
            "total_drifted_columns": int
        }``
    """
    if not HAS_SPARK or df is None:
        return {"error": "Spark not available", "is_first_run": True,
                "distribution_drift": {}, "null_drift": {},
                "total_drifted_columns": 0}

    # Auto-detect numeric columns if not provided
    if numeric_columns is None:
        numeric_types = (DoubleType, IntegerType, LongType, FloatType, ShortType, DecimalType)
        numeric_columns = [
            field.name for field in df.schema.fields
            if isinstance(field.dataType, numeric_types)
        ]

    all_columns = df.columns

    # Step 1: Read stored profiles
    stored_profiles = read_profile(table_name)
    is_first_run = len(stored_profiles) == 0

    if is_first_run:
        print(f"[PROFILE_STORE] First run for '{table_name}' — building initial profile.")

    # Step 2: Compute current profiles
    current_profiles = compute_current_profile(df, numeric_columns, all_columns)

    # Step 3: Detect drift (only if we have stored profiles)
    distribution_drift = {}
    null_drift = {}

    if not is_first_run:
        distribution_drift = detect_distribution_drift(df, numeric_columns, stored_profiles)
        null_drift = detect_null_rate_drift(current_profiles, stored_profiles)

    # Step 4: Update profiles via EMA
    updated_profiles = update_profiles_ema(stored_profiles, current_profiles)

    # Step 5: Write updated profiles to ES
    for col_name, profile in updated_profiles.items():
        write_profile(table_name, col_name, profile)

    total_drifted = sum(1 for d in distribution_drift.values() if d.get("drift_detected")) + \
                    len(null_drift)

    if total_drifted > 0:
        print(f"[PROFILE_STORE] ⚠️ Drift detected in {total_drifted} columns for '{table_name}'.")
    else:
        print(f"[PROFILE_STORE] ✅ No significant drift detected for '{table_name}'.")

    return {
        "stored_profiles": stored_profiles,
        "current_profiles": current_profiles,
        "updated_profiles": updated_profiles,
        "distribution_drift": distribution_drift,
        "null_drift": null_drift,
        "is_first_run": is_first_run,
        "total_drifted_columns": total_drifted,
        "numeric_columns": numeric_columns
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Module self-test
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("data_profile_store.py loaded successfully.")
    print(f"  ELASTICSEARCH_URL = {os.getenv('ELASTICSEARCH_URL', '(not set)')}")
    print(f"  EMA_ALPHA = {EMA_ALPHA}")
    print(f"  PSI_WARN = {PSI_WARN_THRESHOLD}, PSI_CRITICAL = {PSI_CRITICAL_THRESHOLD}")
    print(f"  PySpark available = {HAS_SPARK}")
