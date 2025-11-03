import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
from pathlib import Path

# ---------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------
liab_parquet = "LIABFILE.parquet"                 # Input: LIABFILE
cis_parquet = "CIS_CUST_DAILY.parquet"            # Input: CIS.CUST.DAILY
output_parquet = "LIABFILE_GUARANTR_CIS.parquet"  # Output Parquet
output_csv = "LIABFILE_GUARANTR_CIS.csv"          # Output CSV

# ---------------------------------------------------------------------
# CREATE CONNECTION
# ---------------------------------------------------------------------
con = duckdb.connect()

# ---------------------------------------------------------------------
# STEP 1: READ INPUT FILES
# ---------------------------------------------------------------------
con.execute(f"""
    CREATE TABLE LIAB AS
    SELECT
        CAST(HIRERACCTNO AS BIGINT) AS HIRERACCTNO,
        CAST(HIRERNOTE AS INTEGER) AS HIRERNOTE,
        CAST(GTORACCTNO AS BIGINT) AS GTORACCTNO
    FROM read_parquet('{liab_parquet}')
""")

con.execute(f"""
    CREATE TABLE CIS AS
    SELECT
        CUSTNO,
        CUSTNAME,
        ALIASKEY,
        ALIAS,
        PRISEC,
        ACCTNO AS GTORACCTNO,
        RLENCODE,
        INDORG,
        ACCTCODE
    FROM read_parquet('{cis_parquet}')
    WHERE ACCTCODE = 'LN'
""")

# ---------------------------------------------------------------------
# STEP 2: SORT BOTH TABLES BY GTORACCTNO (SAS PROC SORT)
# ---------------------------------------------------------------------
con.execute("""
    CREATE TABLE LIAB_SORTED AS
    SELECT * FROM LIAB ORDER BY GTORACCTNO
""")
con.execute("""
    CREATE TABLE CIS_SORTED AS
    SELECT * FROM CIS ORDER BY GTORACCTNO
""")

# ---------------------------------------------------------------------
# STEP 3: MERGE LIKE SAS (BY GTORACCTNO, IF P)
# ---------------------------------------------------------------------
con.execute("""
    CREATE TABLE GTOR AS
    SELECT 
        L.HIRERACCTNO,
        L.HIRERNOTE,
        L.GTORACCTNO,
        C.CUSTNO,
        C.ALIASKEY,
        C.ALIAS
    FROM LIAB_SORTED L
    LEFT JOIN CIS_SORTED C
    ON L.GTORACCTNO = C.GTORACCTNO
""")

# ---------------------------------------------------------------------
# STEP 4: WRITE OUTPUT FILES (LIKE "FILE OUTFILE" IN SAS)
# ---------------------------------------------------------------------
# Fetch data as Arrow table
out_table = con.execute("SELECT * FROM GTOR").arrow()

# Save as Parquet
pq.write_table(out_table, output_parquet)

# Save as CSV
con.execute(f"""
    COPY GTOR TO '{output_csv}' (HEADER, DELIMITER ',')
""")

# ---------------------------------------------------------------------
# STEP 5: OPTIONAL — DISPLAY SAMPLE OUTPUT (LIKE PROC PRINT OBS=50)
# ---------------------------------------------------------------------
print("=== LIAB SAMPLE (50 rows) ===")
print(con.execute("SELECT * FROM LIAB LIMIT 50").fetchdf())

print("\n=== CIS SAMPLE (50 rows) ===")
print(con.execute("SELECT * FROM CIS LIMIT 50").fetchdf())

print("\n=== FINAL MERGED OUTPUT (50 rows) ===")
print(con.execute("SELECT * FROM GTOR LIMIT 50").fetchdf())

# ---------------------------------------------------------------------
# CLEANUP
# ---------------------------------------------------------------------
con.close()
