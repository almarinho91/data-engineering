# Event Analytics Pipeline

Small data engineering project built to work with event-level data (application logs).

The pipeline generates synthetic events to simulate real-world scenarios such as
duplicates, late-arriving events, and incomplete records. Data is ingested into
DuckDB using a raw → staging → mart structure.

## Features
- Event-level ingestion (JSONL)
- Idempotent loads using business keys
- Canonical staging model
- Daily analytics metrics
- User sessionization
- Basic data quality checks
- Export to CSV and Parquet

## How to run

```bash
python -m ingestion.generate_events
python -m ingestion.init_db
python -m ingestion.ingest_events
python -m ingestion.run_sql
python -m ingestion.check_data
python -m ingestion.export_mart