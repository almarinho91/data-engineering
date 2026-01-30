from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ingestion.db import get_connection


DATA_DIR = Path("data")


def read_jsonl(path: Path) -> pd.DataFrame:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return pd.DataFrame(rows)


def load_into_duckdb(con, df: pd.DataFrame, source_file: str) -> None:
    """
    Idempotent load by business key (event_id):
    - stage df into temp table
    - delete existing event_id keys
    - insert new rows with lineage columns
    """
    con.execute("CREATE TEMP TABLE tmp_events AS SELECT * FROM df")

    # lineage columns
    con.execute("ALTER TABLE tmp_events ADD COLUMN source_file STRING")
    con.execute("UPDATE tmp_events SET source_file = ?", [source_file])

    # loaded_at = processing time (warehouse time)
    con.execute("ALTER TABLE tmp_events ADD COLUMN loaded_at TIMESTAMP")
    con.execute("UPDATE tmp_events SET loaded_at = current_timestamp")

    # delete matching keys
    con.execute("""
        DELETE FROM raw.events t
        USING tmp_events s
        WHERE t.event_id = s.event_id
    """)

    # insert new rows
    con.execute("""
        INSERT INTO raw.events (
          event_id, event_time_utc, ingested_at_utc, user_id, event_type,
          page, referrer, device, country, error_code,
          source_file, loaded_at
        )
        SELECT
          event_id,
          CAST(event_time_utc AS TIMESTAMP),
          CAST(ingested_at_utc AS TIMESTAMP),
          user_id,
          event_type,
          page,
          referrer,
          device,
          country,
          error_code,
          source_file,
          loaded_at
        FROM tmp_events
    """)

    con.execute("DROP TABLE tmp_events")


def main():
    files = sorted(DATA_DIR.glob("events_*.jsonl"))
    if not files:
        raise SystemExit("No files found in data/. Run: python -m ingestion.generate_events")

    con = get_connection()
    try:
        for path in files:
            df = read_jsonl(path)

            # minimal sanity: keep only rows with event_id
            df = df[df["event_id"].notna()].copy()

            load_into_duckdb(con, df, source_file=path.name)
            print(f"Loaded {len(df)} rows from {path.name}")
    finally:
        con.close()

    print("Done. Data is in raw.events")


if __name__ == "__main__":
    main()
    con = get_connection()
    print(con.execute("SELECT COUNT(*) FROM raw.events").fetchall())
    con.close()