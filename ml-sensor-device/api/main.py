from pathlib import Path

import joblib
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel

MODEL_PATH = Path("models/rul_model.joblib")

app = FastAPI(title="ML Sensor Service")

model = joblib.load(MODEL_PATH)


FEATURE_COLS = [
    "sensor_2", "sensor_3", "sensor_4", "sensor_7",
    "sensor_11", "sensor_12", "sensor_15", "sensor_20", "sensor_21",
    "sensor_2_delta", "sensor_3_delta", "sensor_4_delta", "sensor_7_delta",
    "sensor_11_delta", "sensor_12_delta", "sensor_15_delta",
    "sensor_20_delta", "sensor_21_delta",
    "sensor_2_rollmean_5", "sensor_3_rollmean_5", "sensor_4_rollmean_5",
    "sensor_7_rollmean_5", "sensor_11_rollmean_5", "sensor_12_rollmean_5",
    "sensor_15_rollmean_5", "sensor_20_rollmean_5", "sensor_21_rollmean_5",
]


class SensorPayload(BaseModel):
    sensor_2: float
    sensor_3: float
    sensor_4: float
    sensor_7: float
    sensor_11: float
    sensor_12: float
    sensor_15: float
    sensor_20: float
    sensor_21: float

    sensor_2_delta: float
    sensor_3_delta: float
    sensor_4_delta: float
    sensor_7_delta: float
    sensor_11_delta: float
    sensor_12_delta: float
    sensor_15_delta: float
    sensor_20_delta: float
    sensor_21_delta: float

    sensor_2_rollmean_5: float
    sensor_3_rollmean_5: float
    sensor_4_rollmean_5: float
    sensor_7_rollmean_5: float
    sensor_11_rollmean_5: float
    sensor_12_rollmean_5: float
    sensor_15_rollmean_5: float
    sensor_20_rollmean_5: float
    sensor_21_rollmean_5: float


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/predict")
def predict(payload: SensorPayload):
    row = pd.DataFrame([payload.dict()])[FEATURE_COLS]
    rul_pred = model.predict(row)[0]

    return {
        "predicted_rul": float(rul_pred)
    }
