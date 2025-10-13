# ==================================================================== #
#  Program: CMCBMPUF  (Converted from SAS609 to Python)                #
#  Purpose: Purge records more than 1 year old from CBM text dataset   #
# ==================================================================== #

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
from CIS_PY_READER import host_parquet_path, parquet_output_path

# -------------------------------------------------------------------- #
#  1. Setup batch & purge date
# -------------------------------------------------------------------- #
today = datetime.date.today()
purge_date = today - datetime.timedelta(days=365)
purge_date_str = purge_date.strftime("%Y%m%d")

print("PURGE DATE:", purge_date_str)

# -------------------------------------------------------------------- #
#  2. Initialize DuckDB connection
# -------------------------------------------------------------------- #
con = duckdb.connect()

# -------------------------------------------------------------------- #
#  3. Load input data from parquet (converted version of UNLOAD.CMCBMTXT.FB)
# -------------------------------------------------------------------- #
cbmtxt_path = f"{host_parquet_path}/UNLOAD_CMCBMTXT_FB.parquet"

cbmtxt_df = con.execute(f"""
    SELECT 
        CBM_LOAD_DATE,
        CBM_RUN_NO,
        REG_IDNO,
        LAST_UPDATE,
        SUBSTR(LAST_UPDATE, 1, 2)  AS LAST_UPDATE_DD,
        SUBSTR(LAST_UPDATE, 3, 2)  AS LAST_UPDATE_MM,
        SUBSTR(LAST_UPDATE, 5, 4)  AS LAST_UPDATE_YY,
        REG_NEW_IDNO
    FROM read_parquet('{cbmtxt_path}')
""").df()

# -------------------------------------------------------------------- #
#  4. Data Cleaning
# -------------------------------------------------------------------- #
# Remove invalid LAST_UPDATE
cbmtxt_df = cbmtxt_df[cbmtxt_df['LAST_UPDATE'].str.strip() != "-"]

# Create LASTDATE (concatenate year + month + day)
cbmtxt_df['LASTDATE'] = cbmtxt_df['LAST_UPDATE_YY'] + cbmtxt_df['LAST_UPDATE_MM'] + cbmtxt_df['LAST_UPDATE_DD']

# -------------------------------------------------------------------- #
#  5. Sort by CBM_LOAD_DATE
# -------------------------------------------------------------------- #
cbmtxt_df = cbmtxt_df.sort_values(by=['CBM_LOAD_DATE'])

# -------------------------------------------------------------------- #
#  6. Filter (Purge > 1 year old)
# -------------------------------------------------------------------- #
topurge_df = cbmtxt_df[cbmtxt_df['LASTDATE'] < purge_date_str]

print("Preview of Purge Data (first 10 rows):")
print(topurge_df.head(10))

# -------------------------------------------------------------------- #
#  7. Prepare final output (columns sequence follow SAS PUT layout)
# -------------------------------------------------------------------- #
out_df = topurge_df[[
    'CBM_LOAD_DATE',
    'CBM_RUN_NO',
    'REG_IDNO',
    'REG_NEW_IDNO',
    'LAST_UPDATE'
]]

# -------------------------------------------------------------------- #
#  8. Save to Parquet using PyArrow
# -------------------------------------------------------------------- #
table = pa.Table.from_pandas(out_df)
output_path = f"{parquet_output_path}/CBM_PURGE_MORE1Y.parquet"
pq.write_table(table, output_path)

print(f"Output successfully written to: {output_path}")
