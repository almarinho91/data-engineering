# Hamburg Weather Analytics

This project implements a **simple end-to-end data engineering pipeline** that ingests hourly weather data from the German Weather Service (DWD), processes it into analytical layers, and produces daily weather metrics for Hamburg.

---

## Architecture Overview

The pipeline follows a classic **raw → staging → mart** architecture using **DuckDB** as the analytical warehouse.

```
DWD Open Data (ZIP files)
        ↓
Python ingestion
        ↓
raw.dwd_hourly_temperature
        ↓
stg.weather_hourly (deduplicated)
        ↓
mart.weather_daily (daily metrics)
```

---

## Data Ingestion

- Weather data is sourced from the **DWD Open Data platform**
- The pipeline automatically:
  - loads official station metadata
  - selects the **nearest available weather station to Hamburg**
  - validates data availability
  - downloads the corresponding hourly temperature ZIP file

---

## Data Layers

### Raw (`raw`)
- Stores hourly observations as received from the source
- May contain duplicates or re-published records
- Includes ingestion metadata (`source_file`, `ingested_at`)

### Staging (`stg`)
- Defines the **canonical dataset**
- Ensures **one row per station per hour**
- Deduplicates records using a window function (`ROW_NUMBER()`)

### Mart (`mart`)
- Aggregates hourly data into **daily metrics**
- Provides analytics-ready tables:
  - minimum temperature
  - maximum temperature
  - average temperature
  - average humidity
  - number of observed hours per day

---

## Tech Stack

- Python 3
- DuckDB
- Pandas
- SQL
- Requests
- dotenv

---

## How to Run

### 1. Setup environment
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment variables
Create a `.env` file based on `.env.example`:

```env
CITY_LAT=53.5511
CITY_LON=9.9937
```

### 3. Run the pipeline
```bash
python -m ingestion.init_db
python -m ingestion.ingest_hamburg
python -m ingestion.run_sql
python -m ingestion.query_mart
python -m ingestion.check_data
python -m ingestion.export_mart
```

---

## Output

The final table `mart.weather_daily` contains one row per day with aggregated weather metrics for Hamburg, ready for analysis or visualization.

---
