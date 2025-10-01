import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# =============================================================
# 1. Setup paths (assume you already have Parquet input)
# =============================================================
custfile_parquet = "CUSTFILE_CUSTDLY.parquet"
outfile_primary   = "LOANS_CUST_PRIMARY.parquet"
outfile_secondary = "LOANS_CUST_SCNDARY.parquet"

# =============================================================
# 2. Connect DuckDB and Load
# =============================================================
con = duckdb.connect()

# Load the input file into DuckDB
con.execute(f"""
    CREATE OR REPLACE TABLE custdly AS 
    SELECT *
    FROM parquet_scan('{custfile_parquet}')
""")

# =============================================================
# 3. Process logic (equivalent to SAS DATA steps)
# =============================================================

# Remove rows where CUSTNAME is blank
# Map PRISEC 901 -> 'P', 902 -> 'S'
# Split into Primary and Secondary based on ACCTCODE='LN' and PRIMSEC
con.execute("""
    CREATE OR REPLACE TABLE cus1 AS
    SELECT 
        CUSTNO,
        ACCTNOC,
        CUSTNAME,
        ACCTCODE,
        DOBDOR,
        LONGNAME,
        INDORG,
        CASE WHEN PRISEC = 901 THEN 'P'
             WHEN PRISEC = 902 THEN 'S'
             ELSE NULL END AS PRIMSEC
    FROM custdly
    WHERE CUSTNAME <> ''
      AND ACCTCODE = 'LN'
      AND PRISEC = 901
""")

con.execute("""
    CREATE OR REPLACE TABLE cus2 AS
    SELECT 
        CUSTNO,
        ACCTNOC,
        CUSTNAME,
        ACCTCODE,
        DOBDOR,
        LONGNAME,
        INDORG,
        CASE WHEN PRISEC = 901 THEN 'P'
             WHEN PRISEC = 902 THEN 'S'
             ELSE NULL END AS PRIMSEC
    FROM custdly
    WHERE CUSTNAME <> ''
      AND ACCTCODE = 'LN'
      AND PRISEC = 902
""")

# =============================================================
# 4. Sort by CUSTNO (PROC SORT in SAS)
# =============================================================
cus1_arrow = con.execute("SELECT * FROM cus1 ORDER BY CUSTNO").arrow()
cus2_arrow = con.execute("SELECT * FROM cus2 ORDER BY CUSTNO").arrow()

# =============================================================
# 5. Write to Parquet using PyArrow
# =============================================================
pq.write_table(cus1_arrow, outfile_primary)
pq.write_table(cus2_arrow, outfile_secondary)

# =============================================================
# 6. Optional: Print first 10 records like PROC PRINT
# =============================================================
print("LOANS CUST PRIMARY (first 10):")
print(con.execute("SELECT * FROM cus1 ORDER BY CUSTNO LIMIT 10").df())

print("LOANS CUST SECONDARY (first 10):")
print(con.execute("SELECT * FROM cus2 ORDER BY CUSTNO LIMIT 10").df())
