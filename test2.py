import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

# ============================================================
# DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()

# ============================================================
# LOAD INPUT PARQUET FILES
# ============================================================
custfile_path = f"{host_parquet_path}/CIS_CUST_DAILY.parquet"
inpfile_path = f"{host_parquet_path}/UNLOAD_ALLALIAS_FB.parquet"

# Register tables in DuckDB
con.execute(f"CREATE OR REPLACE VIEW CUSTFILE AS SELECT * FROM read_parquet('{custfile_path}')")
con.execute(f"CREATE OR REPLACE VIEW INPFILE AS SELECT * FROM read_parquet('{inpfile_path}')")

# ============================================================
# STEP 1: FILTER CUSTOMER DATA (CUS)
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE CUS AS
    SELECT CUSTNO, ACCTNOC, CUSTNAME, ACCTCODE, DOBDOR
    FROM CUSTFILE
    WHERE CUSTNAME IS NOT NULL AND CUSTNAME <> ''
""")

# Remove duplicates by CUSTNO (equivalent to PROC SORT NODUPKEY)
con.execute("""
    CREATE OR REPLACE TABLE CUS AS
    SELECT DISTINCT ON (CUSTNO) *
    FROM CUS
""")

# ============================================================
# STEP 2: READ ALIAS DATA (ALIAS)
# ============================================================
# Assuming INPFILE already contains the extracted fields from the fixed-format data
con.execute("""
    CREATE OR REPLACE TABLE ALIAS AS
    SELECT *
    FROM INPFILE
    WHERE KEY_FIELD_1 = 'PP'
""")

# Sort by CUSTNO, LAST_CHANGE DESC, PROCESS_TIME DESC
con.execute("""
    CREATE OR REPLACE TABLE ALIAS AS
    SELECT *
    FROM ALIAS
    ORDER BY CUSTNO, LAST_CHANGE DESC, PROCESS_TIME DESC
""")

# ============================================================
# STEP 3: MERGE ALIAS AND CUS (MATCH)
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE MATCH AS
    SELECT A.*, 
           B.ACCTNOC, 
           B.ACCTCODE, 
           B.CUSTNAME, 
           B.DOBDOR
    FROM ALIAS A
    INNER JOIN CUS B
    ON A.CUSTNO = B.CUSTNO
""")

# Sort by CUSTNO
con.execute("""
    CREATE OR REPLACE TABLE MATCH AS
    SELECT * FROM MATCH ORDER BY CUSTNO
""")

# ============================================================
# STEP 4: OUTPUT (Equivalent to DATA OUT)
# Keep first record per CUSTNO
# ============================================================
out_df = con.execute("""
    SELECT 
        BANK_NO,
        CUSTNO,
        NAME_LINE,
        DOBDOR
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) AS rn
        FROM MATCH
    )
    WHERE rn = 1
""").fetchdf()

# ============================================================
# WRITE OUTPUT TO PARQUET AND CSV
# ============================================================
parquet_out_path = f"{parquet_output_path}/CIS_EBANKING_ALIAS.parquet"
csv_out_path = f"{csv_output_path}/CIS_EBANKING_ALIAS.csv"

# Save as Parquet
table = pa.Table.from_pandas(out_df)
pq.write_table(table, parquet_out_path)

# Save as CSV
out_df.to_csv(csv_out_path, index=False)

print(f"✅ Output generated:\n  - Parquet: {parquet_out_path}\n  - CSV: {csv_out_path}")
