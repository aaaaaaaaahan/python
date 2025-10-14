import duckdb
from CIS_PY_READER_Hive_copy import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet, get_hive_parquet_dp, get_hive_parquet_loan
import datetime

# ============================================================
# BATCH DATE SETUP (use yesterday’s date)
# ============================================================
batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year1, month1, day1 = batch_date.year, batch_date.month, batch_date.day

# ============================================================
# CONNECT TO DUCKDB
# ============================================================
con = duckdb.connect()

# ============================================================
# LOAD HIVE PARQUET (returns path to latest Hive parquet)
# ============================================================
CIS, year, month, day = get_hive_parquet('AMLHRC_EXTRACT_MASSCLS')
dp, year, month, day = get_hive_parquet_dp('CICMDHLD_RBP2.B033.DP.HOLD.EXT.FILE')
loan, year, month, day = get_hive_parquet_loan('ACCTFILE')

# ============================================================
# STEP 1 - LOAD CUST FILE INTO DUCKDB
# ============================================================
con.execute(f"""
    CREATE OR REPLACE TABLE CIS AS
    SELECT * EXCLUDE (year, month, day),
         {year}  AS year,
         {month1} AS month,
         {day1}   AS day
    FROM read_parquet('{CIS[0]}')
""")

con.execute(f"""
    CREATE OR REPLACE TABLE dp AS
    SELECT *,
         {year}  AS year,
         {month1} AS month,
         {day1}   AS day
    FROM read_parquet('{dp[0]}')
""")

con.execute(f"""
    CREATE OR REPLACE TABLE loan AS
    SELECT *,
         {year}  AS year,
         {month1} AS month,
         {day1}   AS day
    FROM read_parquet('{loan[0]}')
""")

# ============================================================
# STEP 2 - QUERY DEFINITIONS
# ============================================================
out1 = """
    SELECT * 
    FROM CIS
"""

out2 = """
    SELECT * 
    FROM dp
"""

out3 = """
    SELECT * 
    FROM loan
"""

queries = {
    "test_cis" : out1,
    "test_dp"  : out2,
    "test_loan": out3
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
