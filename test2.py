import duckdb
import datetime
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import os

# ============================================================
# CONFIGURATION
# ============================================================
input_path = "/host/cis/parquet/input"
output_path = "/host/cis/output"

ctrl_file = f"{input_path}/CTRLDATE.parquet"
desc_file = f"{input_path}/CIRHODCT.parquet"
ctrl_tbl_file = f"{input_path}/CIRHOBCT.parquet"
dtl_tbl_file = f"{input_path}/CIRHOLDT.parquet"

out_parquet = f"{output_path}/RHOLD_LIST_TOPURGE.parquet"
out_csv = f"{output_path}/RHOLD_LIST_TOPURGE.csv"

# ============================================================
# CONNECT DUCKDB
# ============================================================
con = duckdb.connect(database=':memory:')

# ============================================================
# STEP 1: LOAD DATES
# ============================================================
today = datetime.date.today()
db2date = today.strftime("%Y-%m-%d")
todaysas = today.strftime("%Y%m%d")

print(f"DB2DATE={db2date}")

# ============================================================
# STEP 2: LOAD DESCRIPTION DATA
# ============================================================
con.execute(f"""
CREATE OR REPLACE TABLE CIRHODCT AS 
SELECT * FROM read_parquet('{desc_file}')
""")

# Split into CLASS, DEPT, NATURE
con.execute("""
CREATE OR REPLACE TABLE CLASS AS
SELECT KEY_CODE AS CLASS_CODE, KEY_DESCRIBE AS CLASS_DESC
FROM CIRHODCT WHERE KEY_ID = 'CLASS '
""")

con.execute("""
CREATE OR REPLACE TABLE NATURE AS
SELECT KEY_CODE AS NATURE_CODE, KEY_DESCRIBE AS NATURE_DESC
FROM CIRHODCT WHERE KEY_ID = 'NATURE'
""")

con.execute("""
CREATE OR REPLACE TABLE DEPT AS
SELECT KEY_CODE AS DEPT_CODE, KEY_DESCRIBE AS DEPT_DESC
FROM CIRHODCT WHERE KEY_ID = 'DEPT  '
""")

# ============================================================
# STEP 3: CONTROL LIST
# ============================================================
con.execute(f"""
CREATE OR REPLACE TABLE CIRHOBCT AS 
SELECT * FROM read_parquet('{ctrl_tbl_file}')
""")

# Apply SAS filter logic
con.execute("""
CREATE OR REPLACE TABLE CONTROL AS
SELECT * FROM CIRHOBCT
WHERE (
 (CLASS_CODE = 'CLS0000003' 
  AND NATURE_CODE IN (
    'NAT0000011','NAT0000012','NAT0000013','NAT0000014','NAT0000015','NAT0000016','NAT0000017',
    'NAT0000018','NAT0000019','NAT0000020','NAT0000021','NAT0000022','NAT0000025','NAT0000026',
    'NAT0000046','NAT0000047','NAT0000048','NAT0000049','NAT0000050','NAT0000051','NAT0000052',
    'NAT0000053','NAT0000054','NAT0000055','NAT0000056','NAT0000057','NAT0000058','NAT0000059',
    'NAT0000060','NAT0000061','NAT0000062')
  AND DEPT_CODE IN ('BRD','PBCSS')
 )
 OR
 (CLASS_CODE = 'CLS0000004'
  AND NATURE_CODE IN ('NAT0000027','NAT0000028')
  AND DEPT_CODE = 'AMLCFT')
)
""")

# ============================================================
# STEP 4: DETAIL LIST
# ============================================================
con.execute(f"""
CREATE OR REPLACE TABLE CIRHOLDT AS 
SELECT * FROM read_parquet('{dtl_tbl_file}')
""")

con.execute(f"""
CREATE OR REPLACE TABLE DETAIL AS
SELECT *,
       '{db2date}' AS PURGEDATE,
       CAST(DTL_LASTMNT_DATE AS DATE) AS LAST_DATE
FROM CIRHOLDT
""")

# Purge logic (simulate SAS one-year / two-year)
con.execute(f"""
CREATE OR REPLACE TABLE DETAIL_FILTERED AS
SELECT *
FROM DETAIL
WHERE (
  (CLASS_ID = '0000000075' AND DATE_ADD(LAST_DATE, INTERVAL 365 DAY) < '{db2date}')
  OR (DATE_ADD(LAST_DATE, INTERVAL 732 DAY) < '{db2date}')
)
""")

# ============================================================
# STEP 5: MERGE DETAIL + CONTROL
# ============================================================
con.execute("""
CREATE OR REPLACE TABLE FIRST AS
SELECT d.*, c.GUIDE_CODE, c.CLASS_CODE, c.NATURE_CODE, c.DEPT_CODE
FROM DETAIL_FILTERED d
JOIN CONTROL c USING (CLASS_ID)
""")

# ============================================================
# STEP 6: JOIN WITH DESCRIPTION TABLES
# ============================================================
con.execute("""
CREATE OR REPLACE TABLE FULL_DESC AS
SELECT f.*,
       cls.CLASS_DESC,
       nat.NATURE_DESC,
       dep.DEPT_DESC
FROM FIRST f
LEFT JOIN CLASS cls USING (CLASS_CODE)
LEFT JOIN NATURE nat USING (NATURE_CODE)
LEFT JOIN DEPT dep USING (DEPT_CODE)
""")

# ============================================================
# STEP 7: OUTPUT
# ============================================================
# Export to Parquet and CSV
con.execute(f"COPY FULL_DESC TO '{out_parquet}' (FORMAT PARQUET)")
con.execute(f"COPY FULL_DESC TO '{out_csv}' (HEADER, DELIMITER ',')")

print(f"✅ Process completed successfully.")
print(f"Output Parquet: {out_parquet}")
print(f"Output CSV: {out_csv}")
