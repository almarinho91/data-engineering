from pathlib import Path

import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, root_mean_squared_error

from ingestion.db import get_connection


MODEL_DIR = Path("models")
MODEL_DIR.mkdir(exist_ok=True)

MODEL_PATH = MODEL_DIR / "rul_model.joblib"


def main():
    con = get_connection()

    df = con.execute("""
        SELECT *
        FROM features.engine_features
    """).fetchdf()

    con.close()

    # Drop rows with null deltas (first cycle of each unit)
    df = df.dropna().reset_index(drop=True)

    target = "rul"

    feature_cols = [c for c in df.columns if c not in ("unit", "cycle", "rul")]

    # IMPORTANT: split by unit to avoid leakage
    units = df["unit"].unique()
    train_units = set(units[:80])
    test_units = set(units[80:])

    train_df = df[df["unit"].isin(train_units)]
    test_df = df[df["unit"].isin(test_units)]

    X_train = train_df[feature_cols]
    y_train = train_df[target]

    X_test = test_df[feature_cols]
    y_test = test_df[target]

    print(f"Train rows: {len(train_df)}")
    print(f"Test rows: {len(test_df)}")

    model = RandomForestRegressor(
        n_estimators=200,
        max_depth=10,
        random_state=42,
        n_jobs=-1,
    )

    model.fit(X_train, y_train)

    preds = model.predict(X_test)

    mae = mean_absolute_error(y_test, preds)
    rmse = root_mean_squared_error(y_test, preds)

    print(f"MAE: {mae:.2f}")
    print(f"RMSE: {rmse:.2f}")

    joblib.dump(model, MODEL_PATH)

    print(f"Model saved to {MODEL_PATH}")


if __name__ == "__main__":
    main()
