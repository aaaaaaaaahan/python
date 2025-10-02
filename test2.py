import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import pyarrow.compute as pc
from pathlib import Path

# ==============================
# PATH SETTINGS
# ==============================
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

unqacct_parquet = Path(host_parquet_path) / "ACTIVE.ACCOUNT.UNIQUE.parquet"
cisfile_parquet = Path(host_parquet_path) / "CIS.CUST.DAILY.parquet"
addrfile_parquet = Path(host_parquet_path) / "CCRIS.CISADDR.GDG.parquet"
outfile_parquet = Path(parquet_output_path) / "ECCRIS.BLANK.ADDR.POSTCODE.parquet"
outfile_txt = Path(csv_output_path) / "ECCRIS.BLANK.ADDR.POSTCODE.txt"

# ==============================
# CONNECT TO DUCKDB
# ==============================
con = duckdb.connect()

# ==============================
# STEP 1 - Load Unique Account File
# ==============================
con.execute(f"""
    CREATE OR REPLACE TABLE ACCT AS
    SELECT 
        CUSTNO,
        ACCTCODE,
        ACCTNOC,
        NOTENO,
        ALIASKEY,
        ALIAS,
        CUSTNAME,
        BRANCH,
        OPENDATE,
        OPENIND
    FROM read_parquet('{unqacct_parquet}')
    ORDER BY CUSTNO, ACCTNOC
""")

print("Sample ACCT data:")
print(con.execute("SELECT * FROM ACCT LIMIT 5").fetchdf())

# ==============================
# STEP 2 - Load CIS (Customer Daily)
# ==============================
con.execute(f"""
    CREATE OR REPLACE TABLE CIS AS
    SELECT *
    FROM read_parquet('{cisfile_parquet}')
    WHERE ACCTCODE <> '' 
      AND CUSTNAME <> '' 
      AND ACCTCODE IN ('DP','LN')
""")

print("Sample CIS data:")
print(con.execute("SELECT * FROM CIS LIMIT 5").fetchdf())

# Deduplicate by ADDREF
con.execute("CREATE OR REPLACE TABLE CIS AS SELECT DISTINCT ON (ADDREF) * FROM CIS")

# ==============================
# STEP 3 - Load Address File
# ==============================
con.execute(f"""
    CREATE OR REPLACE TABLE ADDR AS
    SELECT 
        ADDREF,
        LINE1ADR,
        LINE2ADR,
        LINE3ADR,
        LINE4ADR,
        LINE5ADR,
        ADDR_WEF,
        ZIPCODE,
        STATE,
        COUNTRY,
        CASE WHEN ZIPCODE = '' THEN '100' ELSE NULL END AS ERRORCODE,
        CASE WHEN ZIPCODE = '' THEN 'ZIPCODE' ELSE NULL END AS FIELDTYPE,
        CASE WHEN ZIPCODE = '' THEN 'INVALID POSTCODE' ELSE NULL END AS FIELDVALUE,
        CASE WHEN ZIPCODE = '' THEN 'PLS CHECK POSTCODE' ELSE NULL END AS REMARKS
    FROM read_parquet('{addrfile_parquet}')
""")

# Deduplicate by ADDREF
con.execute("CREATE OR REPLACE TABLE ADDR AS SELECT DISTINCT ON (ADDREF) * FROM ADDR")

print("Sample ADDR data:")
print(con.execute("SELECT * FROM ADDR LIMIT 5").fetchdf())

# ==============================
# STEP 4 - Merge OUT1 (bad postcode) with CIS
# ==============================
con.execute("""
    CREATE OR REPLACE TABLE MERGE1 AS
    SELECT A.*, B.*
    FROM ADDR A
    JOIN CIS B ON A.ADDREF = B.ADDREF
    WHERE A.ERRORCODE IS NOT NULL
""")

# ==============================
# STEP 5 - Merge with ACCT
# ==============================
con.execute("""
    CREATE OR REPLACE TABLE MERGE2 AS
    SELECT A.*, B.*
    FROM ACCT A
    JOIN MERGE1 B ON A.CUSTNO = B.CUSTNO AND A.ACCTNOC = B.ACCTNOC
""")

# ==============================
# STEP 6 - Final Transformation
# ==============================
con.execute("""
    CREATE OR REPLACE TABLE OUT AS
    SELECT 
        BRANCH,
        ACCTCODE,
        ACCTNOC,
        CASE 
            WHEN PRISEC = 901 THEN 'P'
            WHEN PRISEC = 902 THEN 'S'
            ELSE NULL
        END AS PRIMSEC,
        CUSTNO,
        ERRORCODE,
        FIELDTYPE,
        FIELDVALUE,
        REMARKS
    FROM MERGE2
""")

# ==============================
# SAVE TO PARQUET + TXT
# ==============================
out_table = con.execute("SELECT * FROM OUT").fetch_arrow_table()

# Save parquet
pq.write_table(out_table, outfile_parquet)

# Save as fixed-width TXT (simulate SAS PUT formatting)
with open(outfile_txt, "w") as f:
    for row in out_table.to_pylist():
        line = (
            f"{row['BRANCH']:<5}"
            f"{row['ACCTCODE']:<5}"
            f"{row['ACCTNOC']:<20}"
            f"{row['PRIMSEC'] or ' ':<1}"
            f"{row['CUSTNO']:<11}"
            f"{row['ERRORCODE'] or '':<3}"
            f"{row['FIELDTYPE'] or '':<20}"
            f"{row['FIELDVALUE'] or '':<30}"
            f"{row['REMARKS'] or '':<40}"
        )
        f.write(line + "\n")

print(f"✅ Output written to {outfile_parquet} and {outfile_txt}")
