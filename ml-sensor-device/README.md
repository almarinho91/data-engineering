# ML Sensor Service (NASA C-MAPSS)

End-to-end project that combines data engineering + machine learning + a small API.

The pipeline ingests NASA C-MAPSS turbofan engine degradation data (FD001), builds time-series features, trains a baseline RUL (Remaining Useful Life) model, and serves predictions via FastAPI.

## Pipeline
1. Ingest raw data into DuckDB (`raw.cmapss_cycles`)
2. Create canonical staging table (`stg.cmapss_cycles`)
3. Build ML features + label (`features.engine_features`)
4. Train baseline model and save artifact (`models/rul_model.joblib`)
5. Serve predictions through an API (`POST /predict`)

## How to run

### 1) Setup
```bash
pip install -r requirements.txt
python -m ingestion.init_db
```

### 2) Download dataset

Place these files in:

```bash
data/cmapss/
train_FD001.txt
test_FD001.txt
RUL_FD001.txt
```

### 3) Ingest + transform
```bash
python -m ingestion.ingest_cmapss_fd001
python -m ingestion.run_sql
python -m ingestion.check_data
```

### 4) Train model
```bash
python -m model.train
```
### 5) Run API
```bash
uvicorn api.main:app --reload
```

Open:
```bash
http://127.0.0.1:8000/docs

GET /health

POST /predict
```

### Notes
- DuckDB is used as a lightweight local warehouse.
- The model is a baseline RandomForestRegressor trained on engineered features (rolling means + deltas).