import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# -----------------------------
# File paths (update as needed)
# -----------------------------
data_del_parquet = "RHOLD_LOG_RHOL_EOD.parquet"
data_purge_parquet = "RHOLD_LIST_PURGE_SUC.parquet"
output_parquet = "RHOLD_PBCS_DAILY_DEL.parquet"
output_txt = "RHOLD_PBCS_DAILY_DEL.txt"

# -----------------------------
# DuckDB connection
# -----------------------------
con = duckdb.connect(database=':memory:')

# -----------------------------
# Load parquet files into DuckDB tables
# -----------------------------
con.execute(f"CREATE TABLE data_del AS SELECT * FROM parquet_scan('{data_del_parquet}')")
con.execute(f"CREATE TABLE data_purged AS SELECT * FROM parquet_scan('{data_purge_parquet}')")

# -----------------------------
# Process DATA_DEL: deleted only & drop unwanted DEPT_CODE
# -----------------------------
con.execute("""
CREATE TABLE data_del_filtered AS
SELECT *
FROM data_del
WHERE FUNCTION = 'D'
  AND DEPT_CODE NOT IN ('PBCSS', '     ')
ORDER BY DEPT_CODE
""")

# -----------------------------
# Sort DATA_PURGED by DEPT_CODE
# -----------------------------
con.execute("""
CREATE TABLE data_purged_sorted AS
SELECT *
FROM data_purged
ORDER BY DEPT_CODE
""")

# -----------------------------
# Combine both datasets
# -----------------------------
con.execute("""
CREATE TABLE data_deleted AS
SELECT * FROM data_purged_sorted
UNION ALL
SELECT * FROM data_del_filtered
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

# -----------------------------
# Export to Parquet
# -----------------------------
con.execute(f"COPY data_final TO '{output_parquet}' (FORMAT PARQUET)")

# -----------------------------
# Export to fixed-width TXT
# -----------------------------
# Create the formatted fixed-width string directly in DuckDB
con.execute(f"""
COPY (
    SELECT
        LPAD(NAME,50,' ') ||
        LPAD(DT_ALIAS,30,' ') ||
        LPAD(ID2,12,' ') ||
        LPAD(ID1,12,' ') ||
        LPAD(DT_BANKRUPT_NO,18,' ') ||
        SN ||
        L1 ||
        DEL ||
        SPACE ||
        LPAD(DEPT_CODE,8,' ') AS line
    FROM data_final
) TO '{output_txt}' (FORMAT CSV, DELIMITER '', HEADER FALSE, QUOTE '')
""")

print("✅ DuckDB processing complete! Parquet and TXT generated.")
