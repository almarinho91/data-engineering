from pathlib import Path
from ingestion.db import get_connection

SQL_DIR = Path("sql")

def main():
    con = get_connection()
    try:
        for path in sorted(SQL_DIR.glob("*.sql")):
            print(f"Running {path.name}")
            con.execute(path.read_text(encoding="utf-8"))
    finally:
        con.close()

    print("SQL transformations completed.")

if __name__ == "__main__":
    main()
