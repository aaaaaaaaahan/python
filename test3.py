import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path, get_hive_parquet
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
#  3. Load input data from parquet (converted version of UNLOAD.CMCBMTXT.FB)
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
""")

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
print(f"Output successfully written")
