from ingestion.db import get_connection

def main():
    con = get_connection()

    print("Latest 10 daily rows:")
    print(con.execute("""
        SELECT *
        FROM mart.weather_daily
        ORDER BY date_utc DESC
        LIMIT 10
    """).fetchdf())

    print("\nTop 10 hottest days:")
    print(con.execute("""
        SELECT date_utc, temp_max_c
        FROM mart.weather_daily
        ORDER BY temp_max_c DESC
        LIMIT 10
    """).fetchdf())

    print("\nTop 10 coldest days:")
    print(con.execute("""
        SELECT date_utc, temp_min_c
        FROM mart.weather_daily
        ORDER BY temp_min_c ASC
        LIMIT 10
    """).fetchdf())

    con.close()

if __name__ == "__main__":
    main()
