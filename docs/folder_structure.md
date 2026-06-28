# Project Folder Structure

This document outlines the standard monorepo folder structure for the Adaptive Data Reliability & Lineage Platform (ADRLP).

---

## 1. Directory Tree

```
/ (Project Root)
├── .gitignore
├── README.md
├── docker-compose.yml              # Orchestrates Postgres, Airflow, API, Grafana
├── Makefile                        # Shortcuts for starting, stopping, and migrating
├── database/                       # Database initialization and DDL migrations
│   ├── init.sql                    # Initial schema and tables creation
│   └── migrations/                 # DDL migration scripts
│       └── 001_create_metadata.sql
├── airflow/                        # Airflow configuration and DAGs
│   ├── Dockerfile
│   ├── requirements.txt            # Airflow custom dependencies (e.g. Great Expectations, dbt-postgres)
│   ├── airflow.cfg
│   ├── dags/
│   │   ├── common/                 # Shared python modules for helper functions
│   │   │   ├── lineage.py
│   │   │   └── data_quality.py
│   │   ├── ingest_sources_dag.py   # Ingestion DAG
│   │   └── run_etl_dag.py          # Transformation DAG
│   └── plugins/
├── dbt/                            # dbt transformation project
│   ├── dbt_project.yml
│   ├── profiles.yml                # Connection profile pointing to local Postgres
│   ├── models/
│   │   ├── staging/                # Staging layer: cast types and clean headers
│   │   └── marts/                  # Marts layer: business-ready tables
│   ├── tests/                      # dbt custom schema/data tests
│   └── seeds/                      # Static reference files
├── api/                            # Serving Layer (FastAPI)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                     # App entrypoint
│   └── app/
│       ├── core/                   # Config, DB connections, utilities
│       ├── api/                    # API routers (v1/data, v1/lineage, v1/quality)
│       ├── models/                 # SQLAlchemy database models
│       ├── schemas/                # Pydantic validation schemas
│       └── services/               # Lineage extraction logic & business logic
├── quality/                        # Great Expectations Configuration
│   ├── great_expectations.yml      # GE global configuration
│   ├── expectations/               # Expectation Suites (JSON)
│   │   ├── raw_source_expectations.json
│   │   └── transformed_expectations.json
│   └── checkpoints/                # GE Checkpoint definitions
│       └── default_checkpoint.yml
└── grafana/                        # Observability provisioning
    ├── provisioning/
    │   ├── datasources/
    │   │   └── postgres.yaml       # Autoprovisioned database connection
    │   └── dashboards/
    │       ├── dashboards.yaml     # Autoprovisioned dashboard settings
    │       └── adrlp_observability.json # Dashboard layout
```

---

## 2. Directory Descriptions

- **`/database`**: Contains raw SQL DDL and DML statements to initialize schema scopes (`raw`, `analytics`, `metadata`, `quarantine`) inside PostgreSQL.
- **`/airflow`**: Runs isolated in Docker, mounting `./dags` to keep local sync. Contains the DAGs responsible for reading raw sources (APIs, files, external DBs) and loading them.
- **`/dbt`**: House all dbt models. Triggered from Airflow using `DbtRunOperator` / BashCommands, writing clean transformed datasets into PostgreSQL `analytics` schema.
- **`/quality`**: Configuration and JSON assets for Great Expectations. Enables assertion checks on `raw` and `transformed` tables.
- **`/api`**: A FastAPI application. Serves cleaned data and operational metadata to downstream consumers.
- **`/grafana`**: Automates visualization configuration to run out-of-the-box upon `docker compose up`.
