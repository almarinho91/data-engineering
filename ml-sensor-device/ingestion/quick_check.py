from ingestion.db import get_connection

def main():
    con = get_connection()
    try:
        print(con.execute("""
            SELECT COUNT(*) AS rows, COUNT(DISTINCT unit) AS units
            FROM raw.cmapss_cycles
        """).fetchdf())

        print(con.execute("""
            SELECT unit, MAX(cycle) AS max_cycle
            FROM raw.cmapss_cycles
            GROUP BY unit
            ORDER BY max_cycle DESC
            LIMIT 10
        """).fetchdf())

        print(con.execute("SELECT COUNT(*) AS raw_rows FROM raw.cmapss_cycles").fetchdf())
        print(con.execute("SELECT COUNT(*) AS stg_rows FROM stg.cmapss_cycles").fetchdf())

        print(con.execute("SELECT COUNT(*) AS feature_rows FROM features.engine_features").fetchdf())

        print(con.execute("""
            SELECT *
            FROM features.engine_features
            ORDER BY rul ASC
            LIMIT 5
        """).fetchdf())
        
    finally:
        con.close()

if __name__ == "__main__":
    main()
