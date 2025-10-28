import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pc
import datetime
import os

# ============================================================
# CONFIGURATION
# ============================================================
batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

host_parquet_path = '/host/cis/parquet/sas_parquet'
parquet_output_path = '/host/cis/parquet/python_output'
csv_output_path = '/host/cis/output'

input_file = f"{host_parquet_path}/unload_cirmrks_fb.parquet"
output_file_name = f"CIRMKDU1_DELETE_{year}{month:02d}{day:02d}"

# ============================================================
# DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()

# ============================================================
# LOAD INPUT
# ============================================================
con.execute(f"""
    CREATE OR REPLACE TABLE RMKFILE AS
    SELECT 
        CAST(BANK_NO AS INTEGER) AS BANK_NO,
        TRIM(APPL_CODE) AS APPL_CODE,
        TRIM(APPL_NO) AS APPL_NO,
        CAST(EFF_DATE AS BIGINT) AS EFF_DATE,
        TRIM(RMK_KEYWORD) AS RMK_KEYWORD,
        TRIM(RMK_LINE_1) AS RMK_LINE_1,
        TRIM(RMK_LINE_2) AS RMK_LINE_2,
        TRIM(RMK_LINE_3) AS RMK_LINE_3,
        TRIM(RMK_LINE_4) AS RMK_LINE_4,
        TRIM(RMK_LINE_5) AS RMK_LINE_5
    FROM read_parquet('{input_file}')
""")

# ============================================================
# FUNCTION TO EXTRACT REMARK TYPE
# ============================================================
def extract_remark(keyword):
    con.execute(f"""
        CREATE OR REPLACE TABLE {keyword} AS
        SELECT * 
        FROM RMKFILE
        WHERE APPL_CODE = 'CUST'
        AND RMK_KEYWORD = '{keyword}'
    """)

    # Sort & remove duplicates by BANK_NO, APPL_CODE, APPL_NO (keep latest EFF_DATE)
    con.execute(f"""
        CREATE OR REPLACE TABLE LAST_{keyword} AS
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY BANK_NO, APPL_CODE, APPL_NO
                ORDER BY EFF_DATE DESC
            ) AS RN
            FROM {keyword}
        ) WHERE RN = 1
    """)

    # Extract duplicates (records to delete)
    con.execute(f"""
        CREATE OR REPLACE TABLE DEL_{keyword} AS
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY BANK_NO, APPL_CODE, APPL_NO
                ORDER BY EFF_DATE DESC
            ) AS RN
            FROM {keyword}
        ) WHERE RN > 1
    """)

# ============================================================
# PROCESS EACH REMARK TYPE
# ============================================================
for kw in ['PASSPORT', 'VALID', 'MMTOH', 'PVIP']:
    extract_remark(kw)

# ============================================================
# UNION ALL DELETES
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE OUT_DELETE AS
    SELECT * FROM DEL_PASSPORT
    UNION ALL
    SELECT * FROM DEL_VALID
    UNION ALL
    SELECT * FROM DEL_MMTOH
    UNION ALL
    SELECT * FROM DEL_PVIP
""")

# ============================================================
# EXPORT OUTPUT
# ============================================================
parquet_out = f"{parquet_output_path}/{output_file_name}.parquet"
csv_out = f"{csv_output_path}/{output_file_name}.csv"

con.execute(f"COPY OUT_DELETE TO '{parquet_out}' (FORMAT PARQUET)")
con.execute(f"COPY OUT_DELETE TO '{csv_out}' (HEADER TRUE, DELIMITER ',')")

print(f"✅ Output generated:")
print(f"   • Parquet: {parquet_out}")
print(f"   • CSV: {csv_out}")
