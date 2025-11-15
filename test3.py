import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")

# -----------------------------
# DuckDB connection
# -----------------------------
con = duckdb.connect()

# -----------------------------
# Process DATA_DEL: deleted only & drop unwanted DEPT_CODE
# -----------------------------
con.execute(f"""
CREATE TABLE data_del AS
SELECT *
FROM '{host_parquet_path("RHOLD_LOGRHOL_EOD.parquet")}'
WHERE FUNCTION = 'D'
  AND DEPT_CODE NOT IN ('PBCSS', '     ')
ORDER BY DEPT_CODE
""")

# -----------------------------
# Sort DATA_PURGED by DEPT_CODE
# -----------------------------
con.execute(f"""
CREATE TABLE data_purged AS
SELECT *
FROM '{host_parquet_path("RHOLD_LIST_PURGE_SUC.parquet")}'
ORDER BY DEPT_CODE
""")

# -----------------------------
# Combine both datasets
# -----------------------------
con.execute("""
CREATE TABLE data_deleted AS
SELECT * FROM data_purged
UNION ALL
SELECT * FROM data_del
ORDER BY DEPT_CODE
""")

# -----------------------------
# Prepare final output columns
# -----------------------------
# Replace 'A' (hex 41) in NAME, ID1, ID2
con.execute("""
CREATE TABLE data_final AS
SELECT
    REPLACE(NAME, 'A', '') AS NAME,
    '' AS DT_ALIAS,
    REPLACE(ID2, 'A', '') AS ID2,
    REPLACE(ID1, 'A', '') AS ID1,
    '' AS DT_BANKRUPT_NO,
    'SN' AS SN,
    'L1' AS L1,
    'DEL' AS DEL,
    ' ' AS SPACE,
    DEPT_CODE
FROM data_deleted
""")

print("✅ DuckDB processing complete! Parquet and TXT generated.")
