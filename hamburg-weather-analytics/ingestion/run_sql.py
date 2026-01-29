from ingestion.db import get_connection

def run_file(con, path: str):
    sql = open(path, "r", encoding="utf-8").read()
    con.execute(sql)
    print(f"Ran {path}")

def main():
    con = get_connection()
    run_file(con, "sql/010_stg_weather_hourly.sql")
    run_file(con, "sql/020_mart_weather_daily.sql")
    con.close()

if __name__ == "__main__":
    main()
