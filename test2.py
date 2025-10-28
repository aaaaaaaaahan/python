import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

# ============================================================
# DATE SETUP
# ============================================================
batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ============================================================
# DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()

# ============================================================
# INPUT PARQUET
# ============================================================
rmk_path = f"{host_parquet_path}/UNLOAD_CIRMRKS_FB.parquet"

# Read RMKFILE
con.execute(f"""
    CREATE OR REPLACE TABLE RMKFILE AS
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
        RMK_LINE_5
    FROM '{rmk_path}'
""")

# ============================================================
# STEP 1: FILTER WHERE APPL_CODE = 'CUST '
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE OKAY AS
    SELECT *
    FROM RMKFILE
    WHERE APPL_CODE = 'CUST '
""")

# ============================================================
# STEP 2: REMOVE DUPLICATES (KEEP LATEST BY APPL_NO, EFF_DATE)
# SAS PROC SORT NODUPKEY DUPOUT=DUPNI
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE DUPNI AS
    SELECT *
    FROM OKAY
    QUALIFY ROW_NUMBER() OVER (PARTITION BY APPL_NO, EFF_DATE ORDER BY EFF_DATE DESC) = 1
""")

# ============================================================
# STEP 3: ADD GROUP_ID & EFF_DATE_ADD
# Equivalent to SAS BY-group increment
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE LATEST AS
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY APPL_NO ORDER BY EFF_DATE) AS EFF_DATE_ADD
    FROM DUPNI
""")

# ============================================================
# STEP 4: EXPORT OUTPUT
# ============================================================
out_parquet = f"{parquet_output_path}/CIRMKEFF_UPDATE_{year}{month:02d}{day:02d}.parquet"
out_csv = f"{csv_output_path}/CIRMKEFF_UPDATE_{year}{month:02d}{day:02d}.csv"

# Select and write output with correct field order
result = con.execute("""
    SELECT
        CAST(BANK_NO AS INTEGER) AS BANK_NO,
        APPL_CODE,
        APPL_NO,
        CAST(EFF_DATE AS BIGINT) AS EFF_DATE,
        RMK_KEYWORD,
        RMK_LINE_1,
        RMK_LINE_2,
        RMK_LINE_3,
        RMK_LINE_4,
        RMK_LINE_5,
        CAST(EFF_DATE_ADD AS INTEGER) AS EFF_DATE_ADD
    FROM LATEST
""").arrow()

# Write to Parquet and CSV
pq.write_table(result, out_parquet)
result.to_pandas().to_csv(out_csv, index=False)

print("✅ CIRMKEF1 conversion completed successfully.")
print(f"Parquet saved to: {out_parquet}")
print(f"CSV saved to: {out_csv}")
