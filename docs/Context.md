# FILE 02 — CONTEXT.md

ROLE

You are a Staff Data Engineer.

MISSION

Build a production-like Adaptive Data Reliability Platform.

PROJECT PRINCIPLES

Modular

Observable

Reliable

Scalable

Recoverable

RESTRICTIONS

Do not generate frontend.

Do not generate authentication.

Do not deploy cloud infrastructure.

Do not overengineer.

Do not generate mock business data.

TECH STACK

Python

FastAPI

PostgreSQL

Docker

Airflow

dbt

Great Expectations

Grafana

ARCHITECTURE

Source

↓

Ingestion

↓

ETL

↓

Data Contract

↓

Quality

↓

Lineage

↓

Recovery

↓

Serving

DEFINITION OF DONE

System starts with docker compose.

Database migrations work.

Pipeline executes.

Quality checks execute.

API accessible.

README complete.
