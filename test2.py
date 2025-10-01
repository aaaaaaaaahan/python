import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pyarrow import Table

# ============================================================
# CONNECT TO DUCKDB
# ============================================================
con = duckdb.connect()

# ============================================================
# STEP 1: READ INPUT PARQUET (IDFILE -> UNLOAD.ALLALIAS.OUT)
# ============================================================
# Assuming the parquet file already exists
input_file = "UNLOAD.ALLALIAS.OUT.parquet"  

con.execute(f"""
    CREATE OR REPLACE TABLE TAXID AS
    SELECT
        SUBSTRING(CAST(CUSTNO AS VARCHAR), 1, 11) AS CUSTNO,
        EFFDATE,
        EFFTIME,
        ALIASKEY,
        ALIAS,
        MNTDATE,
        SUBSTRING(ALIAS, 1, 10) AS ALIAS10
    FROM parquet_scan('{input_file}')
""")

# ============================================================
# STEP 2: REMOVE DUPLICATES BY CUSTNO (PROC SORT NODUPKEY)
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE TAXID AS
    SELECT * 
    FROM TAXID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) = 1
""")

# ============================================================
# STEP 3: PREVIEW FIRST 10 RECORDS (PROC PRINT OBS=10)
# ============================================================
print("=== Preview First 10 Records (TAXID) ===")
print(con.execute("SELECT * FROM TAXID LIMIT 10").fetchdf())

# ============================================================
# STEP 4: GENERATE CCRIS OUTPUT (like SAS PUT statement)
# ============================================================
result = con.execute("""
    SELECT
        CUSTNO,
        '1' AS DUMMYFIELD,
        ALIASKEY,
        ALIAS,
        ALIASKEY AS ALIASKEY_PAD,   -- SAS expands to length 15
        ALIAS10
    FROM TAXID
""").fetch_arrow_table()

# ============================================================
# STEP 5: WRITE OUTPUT TO PARQUET (instead of FB flat file)
# ============================================================
output_file = "CCRIS.ALIAS.GDG.parquet"
pq.write_table(result, output_file, compression="snappy")

print(f"\n✅ Output written to: {output_file}")
