from __future__ import annotations

import os
from dotenv import load_dotenv

from ingestion.db import get_connection
from ingestion.stations import find_nearest_station, build_recent_zip_url

from ingestion.ingest_dwd_hourly_temperature import (
    download_zip,
    extract_data_file,
    parse_dwd_table,
    deduplicate_latest,
    load_into_duckdb,
)

load_dotenv()

# Hamburg city center (default)
DEFAULT_LAT = 53.5511
DEFAULT_LON = 9.9937


def main():
    lat = float(os.getenv("CITY_LAT", DEFAULT_LAT))
    lon = float(os.getenv("CITY_LON", DEFAULT_LON))

    station = find_nearest_station(lat, lon)
    url = build_recent_zip_url(station.station_id)

    print(f"Target point: ({lat}, {lon})")
    print(
        f"Nearest station: {station.station_id:05d} - {station.name} ({station.state}) "
        f"[{station.lat}, {station.lon}]"
    )
    print(f"ZIP URL: {url}")

    zip_bytes = download_zip(url)
    filename, file_bytes = extract_data_file(zip_bytes)

    df = parse_dwd_table(file_bytes)
    df = deduplicate_latest(df)

    print(f"Parsed {len(df)} rows from: {filename}")
    print(df.head(3).to_string(index=False))

    con = get_connection()
    load_into_duckdb(con, df, source_file=filename)
    con.close()

    print("Loaded into DuckDB: raw.dwd_hourly_temperature")


if __name__ == "__main__":
    main()
