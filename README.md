# ETL-Project

This repository contains the **Adaptive Data Reliability & Lineage Platform (ADRLP)** implementation, a modular data‑engineering pipeline that ingests data from three sources (CSV files, an API, and Kafka), stores raw data in HDFS, runs quality checks with Spark, transforms data using dbt, and serves the results via a FastAPI + Grafana observability portal.

## Overview
- **Docker‑Compose** orchestrates all services (Postgres, Airflow, n8n, HDFS, Elasticsearch, Spark, Grafana, FastAPI).
- **`scripts/maintenance/start_and_test.bat`** is the single entry‑point for a fresh machine. Running this script boots the containers, loads sample data, triggers the ingestion workflow, runs Spark quality checks, and opens the UI portals.
- The project follows the folder layout described in `docs/folder_structure.md`.

## Quick Start (Windows)
```cmd
cd c:\DataEngProj
scripts\maintenance\start_and_test.bat
```
The script will:
1. Start all Docker containers.
2. Import n8n workflow and credentials.
3. Load sample CSV data into Postgres.
4. Wait for services to become healthy.
5. Trigger the n8n ingestion webhook (CSV, API, Kafka).
6. Run Spark quality checks on the ingested datasets.
7. Launch the FastAPI portal (`http://localhost:<API_PORT>/`) and Grafana dashboard.

## Project Structure
```
📁 .
├─ .env                # Environment variables (ports, credentials)
├─ docker-compose.yml
├─ airflow/
├─ api/
├─ dbt/
├─ quality/
├─ grafana/
├─ n8n/
├─ spark/
├─ scripts/
│   ├─ maintenance/    # start_and_test.bat & related maintenance scripts
│   ├─ verify/         # verify_pipeline.bat
│   ├─ tests/          # test_ingest.py
│   └─ export/         # (empty – for future export scripts)
└─ docs/
    ├─ folder_structure.md
    └─ architecture.md
```

## Development
- Use **Airflow** to schedule DAGs for continual ingestion.
- Use **dbt** to transform raw tables into analytics marts.
- Use **Great Expectations** (under `quality/`) for data‑quality validation.
- Extend the FastAPI service in `api/app/` to add new endpoints.

## License
MIT © 2024‑2026 fframe11
