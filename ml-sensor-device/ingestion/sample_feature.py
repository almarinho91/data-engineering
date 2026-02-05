from ingestion.db import get_connection

FEATURE_COLS = [
    "unit","cycle","rul",
    "sensor_2","sensor_3","sensor_4","sensor_7","sensor_11","sensor_12","sensor_15","sensor_20","sensor_21",
    "sensor_2_delta","sensor_3_delta","sensor_4_delta","sensor_7_delta","sensor_11_delta","sensor_12_delta",
    "sensor_15_delta","sensor_20_delta","sensor_21_delta",
    "sensor_2_rollmean_5","sensor_3_rollmean_5","sensor_4_rollmean_5","sensor_7_rollmean_5","sensor_11_rollmean_5",
    "sensor_12_rollmean_5","sensor_15_rollmean_5","sensor_20_rollmean_5","sensor_21_rollmean_5",
]

def main():
    con = get_connection()
    try:
        df = con.execute(f"""
            SELECT {", ".join(FEATURE_COLS)}
            FROM features.engine_features
            WHERE cycle >= 2
              AND sensor_2_delta IS NOT NULL
            LIMIT 1
        """).fetchdf()
    finally:
        con.close()

    row = df.iloc[0]
    print(f"unit={row['unit']} cycle={row['cycle']} true_rul={row['rul']}")
    print(row.drop(labels=["unit","cycle","rul"]).to_json())

if __name__ == "__main__":
    main()