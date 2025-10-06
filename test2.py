import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv
from pyarrow import compute as pc
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

# ====================================================
# CONNECT TO DUCKDB
# ====================================================
con = duckdb.connect()

# ====================================================
# STEP 1 - Load RHOLD FULL LIST Parquet file
# ====================================================
# Assumed parquet file already exists in host_parquet_path
rhold_path = f"{host_parquet_path}/RHOLD_FULL_LIST.parquet"

con.execute(f"""
    CREATE OR REPLACE TABLE RHOLDFULL AS 
    SELECT 
        CLASS_CODE,
        CLASS_DESC,
        NATURE_CODE,
        NATURE_DESC,
        DEPT_CODE,
        DEPT_DESC,
        GUIDE_CODE,
        CLASS_ID,
        INDORG,
        NAME,
        ID1,
        ID2,
        DTL_REMARK1,
        DTL_REMARK2,
        DTL_REMARK3,
        DTL_REMARK4,
        DTL_REMARK5,
        DTL_CRT_DATE,
        DTL_CRT_TIME,
        DTL_LASTOPERATOR,
        DTL_LASTMNT_DATE,
        DTL_LASTMNT_TIME,
        CONTACT1,
        CONTACT2,
        CONTACT3
    FROM read_parquet('{rhold_path}')
    WHERE CLASS_CODE = 'CLS0000004'
      AND NATURE_CODE = 'NAT0000045'
""")

# ====================================================
# STEP 2 - Add computed columns (LEN1, NAME_FIRST_CHAR)
# ====================================================
con.execute("""
    CREATE OR REPLACE TABLE RHOLDFULL AS 
    SELECT *,
           LENGTH(NAME) AS LEN1,
           SUBSTR(NAME, 1, 1) AS NAME_FIRST_CHAR
    FROM RHOLDFULL
""")

# ====================================================
# STEP 3 - Sort by CLASS_ID
# ====================================================
con.execute("""
    CREATE OR REPLACE TABLE RHOLDFULL_SORTED AS
    SELECT * FROM RHOLDFULL
    ORDER BY CLASS_ID
""")

# ====================================================
# STEP 4 - Prepare output structure (same as SAS OUTPUT)
# ====================================================
con.execute("""
    CREATE OR REPLACE TABLE OUTPUT AS
    SELECT 
        CLASS_ID,
        INDORG,
        NAME,
        ID1,
        ID2,
        CLASS_CODE,
        CLASS_DESC,
        NATURE_CODE,
        NATURE_DESC,
        DEPT_CODE,
        DEPT_DESC,
        GUIDE_CODE,
        DTL_REMARK1,
        DTL_REMARK2,
        DTL_REMARK3,
        DTL_REMARK4,
        DTL_REMARK5,
        DTL_CRT_DATE,
        DTL_CRT_TIME,
        DTL_LASTOPERATOR,
        DTL_LASTMNT_DATE,
        DTL_LASTMNT_TIME,
        CONTACT1,
        CONTACT2,
        CONTACT3,
        NAME_FIRST_CHAR,
        LPAD(CAST(LEN1 AS VARCHAR), 3, '0') AS LEN1_FORMATTED
    FROM RHOLDFULL_SORTED
""")

# ====================================================
# STEP 5 - Fetch sample output preview
# ====================================================
print("=== RHOLDFULL (First 10 Rows) ===")
print(con.execute("SELECT * FROM RHOLDFULL_SORTED LIMIT 10").fetchdf())

print("=== OUTPUT (First 5 Rows) ===")
print(con.execute("SELECT * FROM OUTPUT LIMIT 5").fetchdf())

# ====================================================
# STEP 6 - Write output to Parquet and CSV via PyArrow
# ====================================================
output_table = con.execute("SELECT * FROM OUTPUT").arrow()

parquet_outfile = f"{parquet_output_path}/SNGLVIEW_RHOLD_EXTRACT.parquet"
csv_outfile = f"{csv_output_path}/SNGLVIEW_RHOLD_EXTRACT.csv"

# Save to Parquet
pq.write_table(output_table, parquet_outfile)

# Save to CSV
csv.write_csv(output_table, csv_outfile)

print(f"\nOutput files generated:\n- {parquet_outfile}\n- {csv_outfile}")
