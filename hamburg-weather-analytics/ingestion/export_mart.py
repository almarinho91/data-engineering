# ingestion/export_mart.py
from __future__ import annotations

from pathlib import Path

from ingestion.db import get_connection


def main():
    con = get_connection()
    try:
        exports_dir = Path("exports")
        exports_dir.mkdir(exist_ok=True)

        parquet_path = exports_dir / "weather_daily.parquet"
        csv_path = exports_dir / "weather_daily.csv"

        # Parquet (best for analytics)
        con.execute(f"""
            COPY (
              SELECT *
              FROM mart.weather_daily
              ORDER BY date_utc
            )
            TO '{parquet_path.as_posix()}'
            (FORMAT 'parquet');
        """)

        # CSV (human-friendly)
        con.execute(f"""
            COPY (
              SELECT *
              FROM mart.weather_daily
              ORDER BY date_utc
            )
            TO '{csv_path.as_posix()}'
            (HEADER, DELIMITER ',');
        """)

        print(f"Exported Parquet: {parquet_path}")
        print(f"Exported CSV:    {csv_path}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
