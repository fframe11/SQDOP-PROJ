# Data Flow Design

This document maps out the path data takes through the Adaptive Data Reliability & Lineage Platform (ADRLP).

---

## 1. Step-by-Step Data Flow

```
[ Sources ] -> ( Postgres / MySQL / CSV / Excel / REST API )
    │
    ▼
[ Ingestion & Pre-Check ] 
    │  ├─ Compare source schema with registered schema in `metadata.schema_registry`
    │  ├─ IF Schema Drift Detected ──> Log to `metadata.schema_drift_logs` & Alert
    │  └─ Load raw data into `raw` schema (PostgreSQL)
    │
    ▼
[ Raw Quality Check (Great Expectations) ]
    │  ├─ Execute quality rules (missing, duplicate, null, type validation)
    │  ├─ Write validation metrics & Quality Score to `metadata.quality_runs`
    │  └─ IF Critical Failures ──> Move/Split data into `quarantine` schema (Skip ETL)
    │
    ▼
[ Transformation & ETL (dbt) ]
    │  ├─ Execute modular SQL transformations
    │  └─ Materialize clean tables in `analytics` schema (PostgreSQL)
    │
    ▼
[ Post-ETL Quality Check ]
    │  ├─ Validate schema completeness and business rules on transformed tables
    │  └─ Write results and final Quality Score to `metadata.quality_runs`
    │
    ▼
[ Lineage Extraction ]
    │  ├─ Parse dbt artifacts (`manifest.json` / `run_results.json`) & execution state
    │  └─ Populate mapping tables in `metadata.lineage_runs` (Source Table -> Dest Table)
    │
    ▼
[ Serving & Observability ]
       ├─ FastAPI endpoints serve data from `analytics` and metadata from `metadata`
       └─ Grafana queries `metadata` schema to plot pipeline health & quality trends
```

---

## 2. Ingestion Handling Details

### 2.1 Sources to Raw DB
- **Database (PostgreSQL/MySQL)**: Airflow uses `SqlToPySparkOperator` or simple Python scripts querying databases via `psycopg2` / `mysql-connector` to pull batches and insert them into the `raw` schema.
- **REST API**: Python requests fetch JSON payloads, flat-map them, and load them into `raw` tables.
- **CSV/Excel**: Files placed in an `/incoming` directory are parsed using `pandas`, verified, and loaded into `raw` tables.

---

## 3. Quality Validation & Quarantine Routing

1. **Assertion Execution**: When a batch finishes loading into the `raw` schema, Airflow triggers Great Expectations.
2. **Quarantine Logic**:
   - For non-critical failures (e.g., slight deviation in a value range), the pipeline logs the failure in `metadata.quality_runs` but continues.
   - For critical failures (e.g., primary key missing, high percentage of NULLs, datatype mismatch), the ingestion task copies the bad records into the corresponding table in the `quarantine` schema and deletes them or skips processing them in the `raw` schema.
   - An Alert is triggered immediately.

---

## 4. Metadata and Lineage Capture

- **Pipeline Lineage**: Captured at run-time. Airflow DAG run IDs are associated with the processed files and SQL statements executed.
- **Table & Column Lineage**: Extracted from dbt's internal graph compilation. After dbt runs, a python script parses `manifest.json` to insert lineage records into the `metadata` database, mapping inputs to outputs.
