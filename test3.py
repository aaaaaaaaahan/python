error:
duckdb.duckdb.BinderException: Binder Error: Ambiguous reference to column name "CUSTNO" (use: "A.CUSTNO" or "B.CUSTNO")

program:
import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path, get_hive_parquet
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ============================================================
# DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()
cis = get_hive_parquet('CIS_CUST_DAILY')

# ============================================================
# STEP 1: FILTER CUSTOMER DATA (CUS)
# ============================================================
con.execute(f"""
    CREATE OR REPLACE TABLE CUS AS
    SELECT DISTINCT ON (CUSTNO)
        CUSTNO, 
        ACCTNOC, 
        CUSTNAME, 
        ACCTCODE, 
        DOBDOR
    FROM read_parquet('{cis[0]}')
    WHERE CUSTNAME IS NOT NULL AND CUSTNAME <> ''
""")

# ============================================================
# STEP 2: READ ALIAS DATA (ALIAS)
# ============================================================
# Assuming INPFILE already contains the extracted fields from the fixed-format data
con.execute(f"""
    CREATE OR REPLACE TABLE ALIAS AS
    SELECT *
    FROM '{host_parquet_path("ALLALIAS_FB.parquet")}'
    WHERE KEY_FIELD_1 = 'PP'
    ORDER BY CUSTNO, LAST_CHANGE DESC, PROCESS_TIME DESC
""")

# ============================================================
# STEP 3: MERGE ALIAS AND CUS (MATCH)
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE MATCH AS
    SELECT A.*, 
           B.ACCTNOC, 
           B.ACCTCODE, 
           B.CUSTNAME, 
           B.DOBDOR
    FROM ALIAS A
    INNER JOIN CUS B
    ON A.CUSTNO = B.CUSTNO
    ORDER BY CUSTNO
""")

# ============================================================
# STEP 4: OUTPUT (Equivalent to DATA OUT)
# Keep first record per CUSTNO
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE first AS
    SELECT 
        BANK_NO,
        CUSTNO,
        NAME_LINE,
        DOBDOR
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) AS rn
        FROM MATCH
    )
    WHERE rn = 1
""")

# ============================================================
# WRITE OUTPUT TO PARQUET AND CSV
# ============================================================
print(f"✅ Output generated")
