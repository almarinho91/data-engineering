import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    warehouse = os.getenv("WAREHOUSE", "duckdb").lower()
    if warehouse != "duckdb":
        raise ValueError(f"Unsupported warehouse: {warehouse}")

    # Project root = parent of this file's folder (ingestion/..)
    project_root = Path(__file__).resolve().parent.parent
    db_name = os.getenv("DUCKDB_PATH", "weather.duckdb")
    db_path = project_root / db_name

    return duckdb.connect(str(db_path))