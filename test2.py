import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
import datetime
from pathlib import Path

# ============================================================
#  CONFIGURATION
# ============================================================
# Define input/output paths (adjust as needed)
host_parquet_path = Path("input_parquet")   # folder containing all input parquet files
parquet_output_path = Path("output_parquet")  # folder to store output parquet
csv_output_path = Path("output_csv")          # folder to store output csv

parquet_output_path.mkdir(exist_ok=True)
csv_output_path.mkdir(exist_ok=True)

# File mapping
RMKFILE = host_parquet_path / "CCRIS_CISRMRK_EMAIL_FIRST.parquet"
CUSTFILE = host_parquet_path / "CIS_CUST_DAILY.parquet"
CIEMLDBT = host_parquet_path / "CIEMLDBT_FB.parquet"

# ============================================================
#  DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()

# ============================================================
#  LOAD INPUT TABLES
# ============================================================
con.execute(f"""
    CREATE OR REPLACE TABLE TBL_EMAIL AS
    SELECT CUSTNO
    FROM read_parquet('{CIEMLDBT}')
""")

con.execute(f"""
    CREATE OR REPLACE TABLE RMK AS
    SELECT 
        SUBSTR(CUSTNO, 1, 20) AS CUSTNO,
        SUBSTR(REMARKS, 1, 60) AS REMARKS
    FROM read_parquet('{RMKFILE}')
""")

con.execute(f"""
    CREATE OR REPLACE TABLE CUS AS
    SELECT 
        CUSTNO, ALIAS, ALIASKEY, INDORG, CUSTNAME, ACCTCODE
    FROM read_parquet('{CUSTFILE}')
    WHERE INDORG = 'I'
      AND CUSTNAME <> ''
      AND ALIAS <> ''
      AND ACCTCODE <> ''
""")

# Remove duplicate CUSTNO
con.execute("CREATE OR REPLACE TABLE CUS AS SELECT DISTINCT ON (CUSTNO) * FROM CUS")

# ============================================================
#  STEP 1 - IDENTIFY CUSTOMER WITH/WITHOUT EMAIL
# ============================================================
# INSERT1: Customer exists in CUS but not in RMK
# DELETE1: Customer exists in both
con.execute("""
    CREATE OR REPLACE TABLE INSERT1 AS
    SELECT B.*
    FROM CUS B
    LEFT JOIN RMK A USING (CUSTNO)
    WHERE A.CUSTNO IS NULL
""")

con.execute("""
    CREATE OR REPLACE TABLE DELETE1 AS
    SELECT B.*
    FROM CUS B
    INNER JOIN RMK A USING (CUSTNO)
""")

# ============================================================
#  STEP 2 - COMPARE AGAINST TABLE CIEMLDBT (TBL_EMAIL)
# ============================================================
# INSERT2: in INSERT1 but not in TBL_EMAIL
# DELETE2: in DELETE1 and in TBL_EMAIL
con.execute("""
    CREATE OR REPLACE TABLE INSERT2 AS
    SELECT C.*
    FROM INSERT1 C
    LEFT JOIN TBL_EMAIL D USING (CUSTNO)
    WHERE D.CUSTNO IS NULL
""")

con.execute("""
    CREATE OR REPLACE TABLE DELETE2 AS
    SELECT E.*
    FROM DELETE1 E
    INNER JOIN TBL_EMAIL F USING (CUSTNO)
""")

# ============================================================
#  STEP 3 - ADD OUTPUT COLUMNS & EXPORT
# ============================================================
prompt_date = datetime.date(2001, 1, 1).strftime("%Y-%m-%d")

con.execute(f"""
    CREATE OR REPLACE TABLE OUT1 AS
    SELECT 
        CUSTNO,
        ALIAS,
        ALIASKEY,
        '{prompt_date}' AS PROMPT_DATE,
        'INIT' AS TELLER_ID,
        'CIEMLFIL' AS REASON
    FROM INSERT2
""")

con.execute(f"""
    CREATE OR REPLACE TABLE OUT2 AS
    SELECT 
        CUSTNO,
        ALIAS,
        ALIASKEY,
        '{prompt_date}' AS PROMPT_DATE,
        COALESCE(TELLER_ID, ' ') AS TELLER_ID,
        COALESCE(REASON, ' ') AS REASON
    FROM DELETE2
""")

# ============================================================
#  EXPORT RESULTS TO PARQUET & CSV
# ============================================================
tables_to_export = ["INSERT2", "DELETE2", "OUT1", "OUT2"]

for t in tables_to_export:
    parquet_file = parquet_output_path / f"{t}.parquet"
    csv_file = csv_output_path / f"{t}.csv"

    con.execute(f"COPY {t} TO '{parquet_file}' (FORMAT 'parquet')")
    con.execute(f"COPY {t} TO '{csv_file}' (HEADER, DELIMITER ',')")

print("✅ Job completed successfully!")
print("Output written to:")
print(f"  - {parquet_output_path}")
print(f"  - {csv_output_path}")
