from pathlib import Path
from ingestion.db import get_connection


def main():
    con = get_connection()
    try:
        export_dir = Path("exports")
        export_dir.mkdir(exist_ok=True)

        # Daily metrics
        con.execute("""
            COPY mart.daily_metrics
            TO 'exports/daily_metrics.parquet'
            (FORMAT 'parquet');
        """)

        con.execute("""
            COPY mart.daily_metrics
            TO 'exports/daily_metrics.csv'
            (HEADER, DELIMITER ',');
        """)

        # Sessions
        con.execute("""
            COPY mart.sessions
            TO 'exports/sessions.parquet'
            (FORMAT 'parquet');
        """)

        con.execute("""
            COPY mart.sessions
            TO 'exports/sessions.csv'
            (HEADER, DELIMITER ',');
        """)

        print("Exports created in /exports")

    finally:
        con.close()


if __name__ == "__main__":
    main()
