import duckdb
from pathlib import Path
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime


# ============================================================
# FUNCTION: get_latest_hive_parquet_path
# ============================================================
def get_latest_hive_parquet_path(base_folder: str) -> tuple[str, int, int, int]:
    """
    Automatically detect the latest Hive-style partition folder
    (year=YYYY/month=MM/day=DD) and return its parquet path + year/month/day.
    """
    base_path = Path(host_parquet_path(base_folder))
    partitions = list(base_path.glob("year=*/month=*/day=*"))

    if not partitions:
        raise FileNotFoundError(f"No partition folders found under {base_path}")

    latest_partition = max(
        partitions,
        key=lambda p: (
            int(p.parts[-3].split("=")[1]),  # year
            int(p.parts[-2].split("=")[1]),  # month
            int(p.parts[-1].split("=")[1])   # day
        )
    )

    year = int(latest_partition.parts[-3].split("=")[1])
    month = int(latest_partition.parts[-2].split("=")[1])
    day = int(latest_partition.parts[-1].split("=")[1])
    parquet_path = str(latest_partition / "*.parquet")

    print(f"✅ Latest partition detected: {latest_partition}")
    return parquet_path, year, month, day


# ============================================================
# MAIN PROGRAM
# ============================================================
def main():
    # Connect to DuckDB
    con = duckdb.connect()

    # Step 1 - Get latest parquet path + partition info
    latest_path, year, month, day = get_latest_hive_parquet_path("CIS_CUST_DAILY")

    # Step 2 - Load and filter CUST File
    con.execute(f"""
        CREATE OR REPLACE TABLE CIS AS
        SELECT DISTINCT ON (CUSTNO)
            ALIASKEY,
            ALIAS,   
            CUSTNAME,
            CUSTNO,  
            LPAD(CAST(CAST(CUSTBRCH AS INTEGER) AS VARCHAR), 3, '0') AS CUSTBRCH
        FROM read_parquet('{latest_path}')
        WHERE CUSTNAME <> ''
          AND ALIASKEY = 'IC'
          AND INDORG = 'I'
          AND CITIZENSHIP = 'MY'
          AND RACE = 'O'
        ORDER BY CUSTNO
    """)

    # Step 3 - Prepare output
    out1 = f"""
        SELECT *,
               {year} AS year,
               {month} AS month,
               {day} AS day
        FROM CIS
    """

    queries = {"CIS_RACE": out1}

    # Step 4 - Export to Parquet + CSV
    for name, query in queries.items():
        parquet_path = parquet_output_path(name)
        csv_path = csv_output_path(name)

        con.execute(f"""
        COPY ({query})
        TO '{parquet_path}'
        (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
        """)

        con.execute(f"""
        COPY ({query})
        TO '{csv_path}'
        (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);
        """)

    print("✅ All exports completed successfully.")


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    main()
