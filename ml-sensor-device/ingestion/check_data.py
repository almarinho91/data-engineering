from ingestion.db import get_connection

def main():
    con = get_connection()
    try:
        # 1) No duplicates in staging by grain
        dup = con.execute("""
            SELECT unit, cycle, source_file, COUNT(*) AS cnt
            FROM stg.cmapss_cycles
            GROUP BY unit, cycle, source_file
            HAVING COUNT(*) > 1
            LIMIT 10
        """).fetchdf()

        # 2) Required fields not null
        missing = con.execute("""
            SELECT COUNT(*) AS missing_rows
            FROM stg.cmapss_cycles
            WHERE unit IS NULL OR cycle IS NULL
        """).fetchdf()

        # 3) Cycle should start at 1 for each unit (common assumption)
        bad_start = con.execute("""
            SELECT unit, MIN(cycle) AS min_cycle
            FROM stg.cmapss_cycles
            GROUP BY unit
            HAVING MIN(cycle) <> 1
            ORDER BY min_cycle
            LIMIT 20
        """).fetchdf()

        print("\n--- DQ1: duplicates (should be empty) ---")
        print("OK" if dup.empty else dup.to_string(index=False))

        print("\n--- DQ2: missing required fields ---")
        print(missing.to_string(index=False))

        print("\n--- DQ3: units with min(cycle) != 1 (should be empty) ---")
        print("OK" if bad_start.empty else bad_start.to_string(index=False))

        if not dup.empty:
            raise SystemExit("DQ failed: duplicates found in staging.")

        print("\nData quality checks passed.")
    finally:
        con.close()

if __name__ == "__main__":
    main()
