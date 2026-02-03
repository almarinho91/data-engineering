from ingestion.db import get_connection


def print_check(title: str, df):
    print(f"\n--- {title} ---")
    if df.empty:
        print("OK")
    else:
        print(df.to_string(index=False))


def main():
    con = get_connection()
    try:
        # 1) No duplicate events in staging
        dup_events = con.execute("""
            SELECT event_id, COUNT(*) AS cnt
            FROM stg.events
            GROUP BY event_id
            HAVING COUNT(*) > 1
        """).fetchdf()

        print_check("DQ1 - Duplicate event_id in stg.events", dup_events)

        # 2) Required fields not null
        missing_fields = con.execute("""
            SELECT COUNT(*) AS missing_rows
            FROM stg.events
            WHERE event_id IS NULL
               OR user_id IS NULL
               OR event_time_utc IS NULL
        """).fetchdf()

        print_check("DQ2 - Missing required fields", missing_fields)

        # 3) Session sanity
        bad_sessions = con.execute("""
            SELECT *
            FROM mart.sessions
            WHERE session_duration_seconds < 0
        """).fetchdf()

        print_check("DQ3 - Negative session duration", bad_sessions)

        if not dup_events.empty or bad_sessions.shape[0] > 0:
            raise SystemExit("Data quality checks failed.")

        print("\nData quality checks passed.")

    finally:
        con.close()


if __name__ == "__main__":
    main()
