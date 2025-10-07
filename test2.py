import duckdb
from CIS_PY_READER_JKH import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet
import datetime

# ============================================================
# BATCH DATE SETUP (use yesterday’s date)
# ============================================================
batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ============================================================
# CONNECT TO DUCKDB
# ============================================================
con = duckdb.connect()

# ============================================================
# LOAD HIVE PARQUET (returns path to latest Hive parquet)
# ============================================================
CIS_path, _, _, _ = get_hive_parquet('AMLHRC_EXTRACT_MASSCLS')

# ============================================================
# STEP 1 - LOAD CUST FILE INTO DUCKDB
# ============================================================
con.execute(f"""
    CREATE OR REPLACE TABLE CIS AS
    SELECT *, 
           {year}  AS year,
           {month} AS month,
           {day}   AS day
    FROM read_parquet('{CIS_path}')
""")

# ============================================================
# STEP 2 - QUERY DEFINITIONS
# ============================================================
out1 = """
    SELECT *
    FROM CIS
"""

queries = {
    "test_jkh": out1
}

# ============================================================
# STEP 3 - EXPORT TO PARQUET + CSV
# ============================================================
for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    # ---- Export Parquet ----
    con.execute(f"""
        COPY ({query})
        TO '{parquet_path}'
        (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
    """)

    # ---- Export CSV ----
    con.execute(f"""
        COPY ({query})
        TO '{csv_path}'
        (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);
    """)

# ============================================================
# STEP 4 - CHECK SAMPLE
# ============================================================
print("✅ Export complete.")
print(f"Batch Date: {year}-{month:02d}-{day:02d}")
print(f"Parquet Output: {parquet_output_path('test_jkh')}")
print(f"CSV Output: {csv_output_path('test_jkh')}")
