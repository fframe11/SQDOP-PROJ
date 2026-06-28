# Service Map & Boundaries

This document defines the containerized service boundaries, network communication, and interaction protocols for the platform.

---

## 1. Container Boundaries

The entire platform is defined and deployed locally via a single `docker-compose.yml` file, containing the following service definitions:

```
                  +-----------------------------------+
                  |           Docker Bridge           |
                  +-----------------------------------+
                    /        |             |        \
                   /         |             |         \
                  v          v             v          v
            +-----------+ +-----------+ +-----------+ +-----------+
            |  Database | |  Airflow  | | Serving   | | Observ-   |
            | (Postgres)| | (Orchestr)| |   (API)   | |  ability  |
            +-----------+ +-----------+ +-----------+ +-----------+
```

### 1.1 database (PostgreSQL)
- **Image**: `postgres:15-alpine`
- **Port**: `5432` (Internal and mapped to host)
- **Volumes**: `postgres_data` (for persistence)
- **Role**: Houses all source-replicated tables (`raw`), clean business marts (`analytics`), operational logs (`metadata`), and failed rows (`quarantine`).

### 1.2 airflow (Apache Airflow)
- **Image**: Custom image inheriting from `apache/airflow:2.7.2-python3.10`
- **Dependencies**: Python packages (`dbt-postgres`, `great-expectations`, `psycopg2-binary`) installed during build.
- **Volumes**:
  - `./airflow/dags` mounted to `/opt/airflow/dags`
  - `./dbt` mounted to `/opt/dbt`
  - `./quality` mounted to `/opt/quality`
- **Role**: Orchestrates pipeline flow. Runs dbt transformations and Great Expectations validation checkpoints as local CLI processes.

### 1.3 api (FastAPI)
- **Image**: Custom image built from `./api/Dockerfile`
- **Port**: `8000:8000` (Mapped to host)
- **Role**: Serves operational metadata (quality scores, lineage graph JSON, run history) and processed data to downstream clients.

### 1.4 grafana (Grafana)
- **Image**: `grafana/grafana-oss:latest`
- **Port**: `3000:3000` (Mapped to host)
- **Volumes**: `./grafana/provisioning` mounted to `/etc/grafana/provisioning`
- **Role**: Query-only visualization engine for metadata metrics.

---

## 2. Interaction Protocols & Flow Control

| Initiator | Receiver | Protocol | Purpose |
| :--- | :--- | :--- | :--- |
| **Airflow** | **Postgres** | TCP/SQL (psycopg2) | Read source configurations, write raw records, record run metadata. |
| **Airflow** | **dbt** | CLI Invocation | Run `dbt run` and `dbt test`. dbt in turn connects to Postgres. |
| **Airflow** | **Great Expectations** | CLI/Python API | Run validations, log metrics directly into Postgres `metadata` schema. |
| **FastAPI** | **Postgres** | TCP/SQL (SQLAlchemy) | Fetch metrics and analytics data. |
| **Grafana** | **Postgres** | TCP/SQL (PostgreSQL Driver) | Retrieve dashboard telemetry (Quality Scores, Run Times, Failures). |

---

## 3. Storage Sharing & Configuration

- **Shared Volume**: Since this platform runs locally on Docker Compose, configuration mappings for Great Expectations suites (`/quality`) and dbt project configs (`/dbt`) are mounted as read-only or read-write volumes into the Airflow container to avoid cross-container volume-sharing overhead.
- **Environment Configuration**: A root `.env` file contains credentials (`POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, etc.) shared across all containers.
