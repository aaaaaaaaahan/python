import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
import datetime
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

# ============================================================
# DATE SETUP (Equivalent to SAS SYMPUT for &CURRDT)
# ============================================================
today = datetime.date.today()

# ============================================================
# DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()

# ============================================================
# READ INPUT PARQUET
# ============================================================
input_parquet = f"{host_parquet_path}/UNLOAD_CIRMRKS_FB.parquet"

# Create a DuckDB view for the Parquet
con.execute(f"""
    CREATE OR REPLACE VIEW remarks AS 
    SELECT 
        BANK_NO,
        APPL_CODE,
        APPL_NO,
        RMK_KEYWORD,
        RMK_LINE_1,
        RMK_LINE_2,
        RMK_LINE_3,
        RMK_LINE_4,
        RMK_LINE_5,
        EXPIRE_DATE
    FROM read_parquet('{input_parquet}')
""")

# ============================================================
# PROCESSING: Extract YYYY, MM, DD from EXPIRE_DATE and build REPDT
# ============================================================
# Assume EXPIRE_DATE format = YYYY-MM-DD or YYYYMMDD or something like that.
# Adjust substring indices based on actual data format.

query = f"""
    WITH tmp AS (
        SELECT
            BANK_NO,
            APPL_CODE,
            APPL_NO,
            RMK_KEYWORD,
            RMK_LINE_1,
            RMK_LINE_2,
            RMK_LINE_3,
            RMK_LINE_4,
            RMK_LINE_5,
            EXPIRE_DATE,
            TRY_CAST(SUBSTR(EXPIRE_DATE, 1, 4) AS INTEGER) AS YYYY,
            TRY_CAST(SUBSTR(EXPIRE_DATE, 5, 2) AS INTEGER) AS MM,
            TRY_CAST(SUBSTR(EXPIRE_DATE, 7, 2) AS INTEGER) AS DD
        FROM remarks
    )
    SELECT 
        *,
        MAKE_DATE(YYYY, MM, DD) AS REPDT
    FROM tmp
    WHERE RMK_KEYWORD IN ('VALID', 'PASSPORT', 'MMTOH')
      AND MAKE_DATE(YYYY, MM, DD) >= '{today}'
"""

remarks_df = con.execute(query).df()

# ============================================================
# OUTPUT TO PARQUET & CSV
# ============================================================
parquet_file = f"{parquet_output_path}/REMARKS_VALID_EXPIRE.parquet"
csv_file = f"{csv_output_path}/REMARKS_VALID_EXPIRE.csv"

# Convert to Arrow Table
arrow_table = pa.Table.from_pandas(remarks_df)
pq.write_table(arrow_table, parquet_file)
remarks_df.to_csv(csv_file, index=False)

# ============================================================
# PRINT FIRST 25 ROWS (Like PROC PRINT OBS=25)
# ============================================================
print("==== SAMPLE OUTPUT (FIRST 25 ROWS) ====")
print(remarks_df.head(25))

print("\n✅ Output generated:")
print(f"Parquet: {parquet_file}")
print(f"CSV: {csv_file}")
