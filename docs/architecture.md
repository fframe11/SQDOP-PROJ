# Adaptive Data Reliability & Lineage Platform (ADRLP) - Architecture Design

This document details the technical architecture of the Adaptive Data Reliability & Lineage Platform (ADRLP).

---

## 1. System Overview

ADRLP is designed as a modular, local, containerized data platform. It automates data ingestion, transformation, quality check, lineage tracking, and recovery.

```
+------------+      +------------------+      +-------------------+
|  Sources   | ---> | Ingestion Engine | ---> |  Raw Database     |
| (DB, APIs, |      |    (Airflow)     |      | (Postgres: raw)   |
| CSV, Excel)|      +------------------+      +-------------------+
+------------+                                          |
                                                        v
+------------+      +------------------+      +-------------------+
| Data API   | <--- |  Serving Layer   | <--- | Transformed DB    |
| (FastAPI)  |      | (dbt, Postgres)  |      | (Postgres: analytics)
+------------+      +------------------+      +-------------------+
      |                      ^                          |
      v                      |                          v
+------------+      +------------------+      +-------------------+
| Check /    | <--- | Lineage/Metadata | <--- | Quality & Audit   |
| Grafana    |      | (Postgres: meta) |      | (Great Expectation)
+------------+      +------------------+      +-------------------+
```

---

## 2. Component Design & Service Boundaries

The platform is split into distinct logical boundaries:

### 2.1 Ingestion & Orchestration Layer (Apache Airflow)
- **Role**: Schedules and triggers batch jobs.
- **Boundaries**: Reads from external configurations (Postgres, MySQL, API, filesystem for CSV/Excel) and loads into PostgreSQL `raw` schema.
- **Recovery Integration**: Handles retry logic and moves malformed ingestion files/records into quarantine.

### 2.2 Transformation Layer (dbt)
- **Role**: Executes T (Transform) of ETL inside PostgreSQL.
- **Boundaries**: Reads from `raw` schema and writes to `analytics`/`transformed` schema. Uses modular SQL models.

### 2.3 Data Quality & Reliability Layer (Great Expectations)
- **Role**: Validates data quality.
- **Boundaries**: Runs assertions against loaded tables in `raw` and `transformed` schemas. Generates validation reports.
- **Metrics**: Computes Quality Scores (0-100) based on test successes and logs results into `metadata.quality_runs`.

### 2.4 Metadata & Lineage Layer (Custom + PostgreSQL)
- **Role**: Tracks operational metadata.
- **Boundaries**: Stores pipeline run statuses, quality scores, schema drift events, and data lineage paths.
- **Data model**: `metadata` schema in PostgreSQL containing tables for:
  - `pipeline_runs`
  - `quality_runs`
  - `lineage_runs`
  - `schema_drift_logs`

### 2.5 Serving Layer (FastAPI)
- **Role**: Exposes endpoints for consuming clean data, query lineage, and fetching quality reports.
- **Boundaries**: Restricts access to read-only queries from PostgreSQL `analytics` and `metadata` schemas.

### 2.6 Observability Layer (Grafana)
- **Role**: Visualizes the state of the data platform.
- **Boundaries**: Queries the `metadata` schema directly to display Dashboards.

---

## 3. Database Strategy

The central PostgreSQL instance (University Infrastructure / Local Docker) will be partitioned using PostgreSQL schemas to ensure separation of concerns:

| Schema Name | Purpose | Access Control |
| :--- | :--- | :--- |
| `raw` | Landing zone for ingested raw data. | Write: Airflow Ingestion; Read: dbt, Great Expectations |
| `analytics` | Production-ready transformed tables. | Write: dbt; Read: FastAPI, Grafana |
| `metadata` | Lineage logs, quality metrics, pipeline execution statistics. | Write: Airflow, Great Expectations, dbt; Read: FastAPI, Grafana |
| `quarantine` | Quarantine zone for records failing critical quality checks. | Write: Airflow, Great Expectations |

### Database Migration & Initialization
- A shell or SQL script `init.sql` will initialize these schemas.
- Tables in `raw` will be managed by ingestion processes.
- Tables in `analytics` will be managed and versioned by dbt.
- Metadata tables will be initialized with DDL scripts.

---

## 4. Observability Strategy

To satisfy the KPI of detecting issue detection under 10 minutes, the following design is implemented:

### 4.1 Schema Drift Monitoring
- During ingestion, a pre-check step compares the incoming schema (columns, data types) with the registered schema in `metadata.schema_registry`.
- If a drift is detected:
  - Log details to `metadata.schema_drift_logs`.
  - Trigger an immediate alert flag.
  - Depending on severity, route data to `quarantine` instead of `raw`.

### 4.2 Quality Score Calculation
- `Quality Score = (Passed Expectations / Total Expectations) * 100`
- Results are stored in `metadata.quality_runs`. Grafana tracks this over time. Alert is triggered if score falls below 90.

### 4.3 Pipeline Monitoring
- Airflow callback handlers log status changes (started, success, failed) to `metadata.pipeline_runs` upon task execution.
