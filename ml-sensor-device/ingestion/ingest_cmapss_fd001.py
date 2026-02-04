from __future__ import annotations

from pathlib import Path

import pandas as pd

from ingestion.db import get_connection

DATA_FILE = Path("data/cmapss/train_FD001.txt")

# Columns: unit, cycle, 3 operational settings, 21 sensors = 26 cols
COLS = (
    ["unit", "cycle"]
    + [f"op_setting_{i}" for i in range(1, 4)]
    + [f"sensor_{i}" for i in range(1, 22)]
)


def read_cmapss_txt(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing file: {path}\n"
            "Place train_FD001.txt in data/cmapss/ (see Day 2 instructions)."
        )

    # whitespace separated, no header
    df = pd.read_csv(path, sep=r"\s+", header=None, names=COLS, engine="python")

    # Basic types
    df["unit"] = df["unit"].astype(int)
    df["cycle"] = df["cycle"].astype(int)

    return df


def load_into_duckdb(con, df: pd.DataFrame, source_file: str) -> None:
    """
    Idempotent load pattern:
    - stage into temp table
    - delete matching keys for the same source_file
    - insert fresh rows
    Business key here is (unit, cycle, source_file).
    """
    con.execute("CREATE TEMP TABLE tmp_cycles AS SELECT * FROM df")

    con.execute("ALTER TABLE tmp_cycles ADD COLUMN source_file STRING")
    con.execute("UPDATE tmp_cycles SET source_file = ?", [source_file])

    con.execute("ALTER TABLE tmp_cycles ADD COLUMN loaded_at TIMESTAMP")
    con.execute("UPDATE tmp_cycles SET loaded_at = current_timestamp")

    con.execute("""
        DELETE FROM raw.cmapss_cycles t
        USING tmp_cycles s
        WHERE t.unit = s.unit
          AND t.cycle = s.cycle
          AND t.source_file = s.source_file
    """)

    con.execute("""
        INSERT INTO raw.cmapss_cycles
        SELECT * FROM tmp_cycles
    """)

    con.execute("DROP TABLE tmp_cycles")


def main():
    df = read_cmapss_txt(DATA_FILE)

    print(df.head(5).to_string(index=False))
    print(f"Rows: {len(df):,}")
    print(f"Units: {df['unit'].nunique()}")

    con = get_connection()
    try:
        load_into_duckdb(con, df, source_file=DATA_FILE.name)
    finally:
        con.close()

    print("Loaded into DuckDB: raw.cmapss_cycles")


if __name__ == "__main__":
    main()
