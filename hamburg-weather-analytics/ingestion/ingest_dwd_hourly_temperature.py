import io
import zipfile
from datetime import datetime, timezone

import pandas as pd
import requests


def download_zip(url: str) -> bytes:
    r = requests.get(url, timeout=90)
    r.raise_for_status()
    return r.content


def extract_data_file(zip_bytes: bytes) -> tuple[str, bytes]:
    """
    Prefer the main data file (often contains 'produkt' in its name).
    Fall back to the first file in the ZIP.
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        names = z.namelist()
        if not names:
            raise ValueError("ZIP is empty")

        candidates = [n for n in names if "produkt" in n.lower()]
        chosen = sorted(candidates)[0] if candidates else sorted(names)[0]
        return chosen, z.read(chosen)


def parse_dwd_table(raw_bytes: bytes) -> pd.DataFrame:
    """
    Parse a typical DWD CDC semicolon-separated file.
    Handles missing value marker -999.
    """
    text = raw_bytes.decode("latin-1", errors="replace")
    df = pd.read_csv(io.StringIO(text), sep=";", dtype=str)
    df.columns = [c.strip() for c in df.columns]

    if "STATIONS_ID" not in df.columns or "MESS_DATUM" not in df.columns:
        raise ValueError(f"Unexpected columns. Found: {df.columns.tolist()[:25]}")

    def to_dt_utc(x: str) -> datetime:
        x = str(x).strip()
        return datetime.strptime(x, "%Y%m%d%H").replace(tzinfo=timezone.utc)

    def to_float_or_none(x: str):
        x = str(x).strip()
        if x in ("-999", "-999.0", "", "nan", "NaN", "None"):
            return None
        try:
            return float(x)
        except ValueError:
            return None

    out = pd.DataFrame()
    out["station_id"] = df["STATIONS_ID"].str.strip()
    out["datetime_utc"] = df["MESS_DATUM"].apply(to_dt_utc)

    # Common column names for temperature/humidity in TU product
    out["temperature_c"] = df["TT_TU"].apply(to_float_or_none) if "TT_TU" in df.columns else None
    out["humidity_pct"] = df["RF_TU"].apply(to_float_or_none) if "RF_TU" in df.columns else None

    return out


def deduplicate_latest(df: pd.DataFrame) -> pd.DataFrame:
    # Keep last record per (station_id, datetime_utc) if duplicates appear
    return (
        df.sort_values(["station_id", "datetime_utc"])
          .drop_duplicates(subset=["station_id", "datetime_utc"], keep="last")
    )


def load_into_duckdb(con, df: pd.DataFrame, source_file: str):
    """
    DuckDB upsert pattern:
    - create a temp table
    - delete matching keys from target
    - insert new rows
    """
    con.execute("CREATE TEMP TABLE tmp_weather AS SELECT * FROM df")

    # Add metadata columns
    con.execute("ALTER TABLE tmp_weather ADD COLUMN source_file STRING")
    con.execute("UPDATE tmp_weather SET source_file = ?", [source_file])

    con.execute("ALTER TABLE tmp_weather ADD COLUMN ingested_at TIMESTAMP")
    con.execute("UPDATE tmp_weather SET ingested_at = current_timestamp")

    # Delete existing keys
    con.execute("""
        DELETE FROM raw.dwd_hourly_temperature t
        USING tmp_weather s
        WHERE t.station_id = s.station_id
          AND t.datetime_utc = s.datetime_utc
    """)

    # Insert new rows
    con.execute("""
        INSERT INTO raw.dwd_hourly_temperature
        SELECT station_id, datetime_utc, temperature_c, humidity_pct, source_file, ingested_at
        FROM tmp_weather
    """)

    con.execute("DROP TABLE tmp_weather")