import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year1, month1, day1 = batch_date.year, batch_date.month, batch_date.day

# -------------------------------------------------------------------- #
#  1. Setup batch & purge date
# -------------------------------------------------------------------- #
today = datetime.date.today()
purge_date = today - datetime.timedelta(days=365)
purge_date_str = purge_date.strftime("%Y%m%d")

# -------------------------------------------------------------------- #
#  2. Initialize DuckDB connection
# -------------------------------------------------------------------- #
con = duckdb.connect()

# -------------------------------------------------------------------- #
#  3. Load input data from parquet
# -------------------------------------------------------------------- #
cbmtxt_df = con.execute(f"""
    SELECT 
        CBM_LOAD_DATE,
        CBM_RUN_NO,
        REG_IDNO,
        LAST_UPDATE,
        SUBSTR(LAST_UPDATE, 1, 2)  AS LAST_UPDATE_DD,
        SUBSTR(LAST_UPDATE, 3, 2)  AS LAST_UPDATE_MM,
        SUBSTR(LAST_UPDATE, 5, 4)  AS LAST_UPDATE_YY,
        SUBSTR(BUSS_DESC9, 37,20) AS REG_NEW_IDNO
    FROM '{host_parquet_path("UNLOAD_CMCBMTXT_FB.parquet")}'
""").df()

# -------------------------------------------------------------------- #
#  4. Data Cleaning
# -------------------------------------------------------------------- #
cbmtxt_df['LAST_UPDATE'] = cbmtxt_df['LAST_UPDATE'].astype(str)
cbmtxt_df = cbmtxt_df[cbmtxt_df['LAST_UPDATE'].str.strip() != "-"]

cbmtxt_df['LASTDATE'] = (
    cbmtxt_df['LAST_UPDATE_YY'].str.zfill(4) +
    cbmtxt_df['LAST_UPDATE_MM'].str.zfill(2) +
    cbmtxt_df['LAST_UPDATE_DD'].str.zfill(2)
)

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
#  7. Prepare final output
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
pq.write_table(table, f"{parquet_output_path('CBM_PURGE_OUTPUT.parquet')}")

print("✅ Output successfully written to parquet file.")
