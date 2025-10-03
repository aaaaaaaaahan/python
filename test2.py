import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import datetime
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

# ========================================
# STEP 1: Setup Purge Date (Today - 365 days)
# ========================================
purge_date = (datetime.date.today() - datetime.timedelta(days=365))
purge_date_str = purge_date.strftime("%Y%m%d")  # SAS used YYMMDDN8.

# ========================================
# STEP 2: Connect to DuckDB
# ========================================
con = duckdb.connect()

# ========================================
# STEP 3: Read Input Parquet (CBMFILE)
# ========================================
input_path = f"{host_parquet_path}/UNLOAD_CMCBMTXT_FB.parquet"

con.execute(f"""
    CREATE OR REPLACE TABLE CBMTXT AS
    SELECT 
        CBM_LOAD_DATE,
        CBM_RUN_NO,
        REG_IDNO,
        LAST_UPDATE,
        LAST_UPDATE_DD,
        LAST_UPDATE_MM,
        LAST_UPDATE_YY,
        REG_NEW_IDNO
    FROM read_parquet('{input_path}')
""")

# ========================================
# STEP 4: Clean Data (Remove blank LAST_UPDATE, Build LASTDATE)
# ========================================
con.execute("""
    CREATE OR REPLACE TABLE CBMTXT_CLEAN AS
    SELECT *,
           CASE WHEN LAST_UPDATE = '-         ' THEN NULL ELSE LAST_UPDATE END AS CLEAN_LAST_UPDATE,
           (LAST_UPDATE_YY || LAST_UPDATE_MM || LAST_UPDATE_DD) AS LASTDATE
    FROM CBMTXT
    WHERE LAST_UPDATE IS NOT NULL AND LAST_UPDATE <> '-         '
""")

# ========================================
# STEP 5: Sort by CBM_LOAD_DATE
# ========================================
con.execute("""
    CREATE OR REPLACE TABLE CBMTXT_SORTED AS
    SELECT *
    FROM CBMTXT_CLEAN
    ORDER BY CBM_LOAD_DATE
""")

# ========================================
# STEP 6: Filter for purge condition (LASTDATE < purge_date)
# ========================================
con.execute(f"""
    CREATE OR REPLACE TABLE TOPURGE AS
    SELECT *
    FROM CBMTXT_SORTED
    WHERE LASTDATE < '{purge_date_str}'
""")

# ========================================
# STEP 7: Final Output (same layout as SAS PUT)
# ========================================
con.execute("""
    CREATE OR REPLACE TABLE OUT AS
    SELECT 
        CBM_LOAD_DATE,
        CBM_RUN_NO,
        REG_IDNO,
        REG_NEW_IDNO,
        CLEAN_LAST_UPDATE AS LAST_UPDATE
    FROM TOPURGE
""")

# Fetch as Arrow Table
out_arrow = con.execute("SELECT * FROM OUT").fetch_arrow_table()

# ========================================
# STEP 8: Write to Parquet and CSV
# ========================================
pq_output = f"{parquet_output_path}/CBM_PURGE_MORE1Y.parquet"
csv_output = f"{csv_output_path}/CBM_PURGE_MORE1Y.csv"

pq.write_table(out_arrow, pq_output)
csv.write_csv(out_arrow, csv_output)

print("✅ Job completed. Output saved:")
print(f"   Parquet: {pq_output}")
print(f"   CSV    : {csv_output}")
