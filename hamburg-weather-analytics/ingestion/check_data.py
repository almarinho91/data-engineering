# ingestion/check_data.py
from __future__ import annotations

from ingestion.db import get_connection


def _print_df(df, title: str):
    print(f"\n--- {title} ---")
    if df.empty:
        print("OK (no rows)")
    else:
        print(df.to_string(index=False))


def main():
    con = get_connection()
    try:
        # 1) Staging must be unique by (station_id, datetime_utc)
        dup_keys = con.execute("""
            SELECT station_id, datetime_utc, COUNT(*) AS cnt
            FROM stg.weather_hourly
            GROUP BY station_id, datetime_utc
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC
            LIMIT 20
        """).fetchdf()

        _print_df(dup_keys, "DQ1 - Duplicate keys in stg.weather_hourly (should be empty)")

        # 2) Days completeness (hours per day). Not always 24 (DST), so we flag < 20 as suspicious.
        hours_per_day = con.execute("""
            SELECT
              station_id,
              CAST(datetime_utc AS DATE) AS date_utc,
              COUNT(*) AS hours_count
            FROM stg.weather_hourly
            GROUP BY station_id, date_utc
            HAVING COUNT(*) < 20
            ORDER BY date_utc DESC
            LIMIT 30
        """).fetchdf()

        _print_df(hours_per_day, "DQ2 - Suspicious incomplete days (<20 hours)")

        # 3) Value ranges (very conservative sanity checks)
        bad_temps = con.execute("""
            SELECT station_id, datetime_utc, temperature_c
            FROM stg.weather_hourly
            WHERE temperature_c IS NOT NULL
              AND (temperature_c < -50 OR temperature_c > 60)
            ORDER BY datetime_utc DESC
            LIMIT 20
        """).fetchdf()

        _print_df(bad_temps, "DQ3 - Temperature out of plausible range [-50, 60]")

        bad_humidity = con.execute("""
            SELECT station_id, datetime_utc, humidity_pct
            FROM stg.weather_hourly
            WHERE humidity_pct IS NOT NULL
              AND (humidity_pct < 0 OR humidity_pct > 100)
            ORDER BY datetime_utc DESC
            LIMIT 20
        """).fetchdf()

        _print_df(bad_humidity, "DQ4 - Humidity out of range [0, 100]")

        critical_failures = 0
        if not dup_keys.empty:
            critical_failures += 1
        if not bad_temps.empty:
            critical_failures += 1
        if not bad_humidity.empty:
            critical_failures += 1

        if critical_failures:
            raise SystemExit(f"\nData quality checks failed ({critical_failures} critical issue(s)).")
        else:
            print("\nData quality checks passed.")

    finally:
        con.close()


if __name__ == "__main__":
    main()
