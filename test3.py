import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ============================================================
# DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()

# ============================================================
# INPUT PARQUET
# ============================================================
# Read RMKFILE
con.execute(f"""
    CREATE OR REPLACE TABLE RMKFILE AS
    SELECT 
        LPAD(CAST(CAST(BANK_NO AS INTEGER) AS VARCHAR),3,'0') AS BANK_NO,
        APPL_CODE,
        APPL_NO,
        CAST(EFF_DATE AS BIGINT) AS EFF_DATE,
        RMK_KEYWORD,
        RMK_LINE_1,
        RMK_LINE_2,
        RMK_LINE_3,
        RMK_LINE_4,
        RMK_LINE_5
    FROM '{host_parquet_path("CIRMRKS_FB.parquet")}'
""")

# ============================================================
# STEP 1: FILTER WHERE APPL_CODE = 'CUST '
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE OKAY AS
    SELECT *
    FROM RMKFILE
    WHERE APPL_CODE = 'CUST'
""")

# ============================================================
# STEP 2: REMOVE DUPLICATES (KEEP LATEST BY APPL_NO, EFF_DATE)
# SAS PROC SORT NODUPKEY DUPOUT=DUPNI
# ============================================================
# Create DUPNI = duplicate records (removed ones)
con.execute("""
    CREATE OR REPLACE TABLE DUPNI AS
    SELECT *
    FROM OKAY
    WHERE (APPL_NO, EFF_DATE) IN (
        SELECT APPL_NO, EFF_DATE
        FROM OKAY
        GROUP BY APPL_NO, EFF_DATE
        HAVING COUNT(*) > 1
    )
""")

# ============================================================
# STEP 3: ADD GROUP_ID & EFF_DATE_ADD
# Equivalent to SAS BY-group increment
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE LATEST AS
    SELECT 
        *,
        DENSE_RANK() OVER (ORDER BY APPL_NO, EFF_DATE) AS GROUP_ID,
        ROW_NUMBER() OVER (PARTITION BY APPL_NO ORDER BY EFF_DATE) AS EFF_DATE_ADD
    FROM DUPNI
""")

# ============================================================
# STEP 4: EXPORT OUTPUT
# ============================================================
# Select and write output with correct field order
result = """
    SELECT
        BANK_NO,
        APPL_CODE,
        APPL_NO,
        EFF_DATE,
        RMK_KEYWORD,
        RMK_LINE_1,
        RMK_LINE_2,
        RMK_LINE_3,
        RMK_LINE_4,
        RMK_LINE_5,
        EFF_DATE_ADD
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM LATEST
""".format(year=year,month=month,day=day)

queries = {
    "CIRMKEFF_UPDATE"                 : result,
}

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
