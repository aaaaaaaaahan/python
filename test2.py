import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv

from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

# ==================================
# CONNECT TO DUCKDB
# ==================================
con = duckdb.connect()

# ====================================
# STEP 1 - Load UNQACCT (Active Account Unique)
# ====================================
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
    FROM read_parquet('{host_parquet_path}/ACTIVE.ACCOUNT.UNIQUE.parquet')
    ORDER BY CUSTNO, ACCTNOC
""")

print("Preview ACCT:")
print(con.execute("SELECT * FROM ACCT LIMIT 5").fetchdf())


# ====================================
# STEP 2 - Load CIS Customer Daily
# ====================================
con.execute(f"""
    CREATE OR REPLACE TABLE CIS AS
    SELECT * 
    FROM read_parquet('{host_parquet_path}/CIS.CUST.DAILY.parquet')
    WHERE CUSTNAME <> '' AND ACCTCODE IN ('DP','LN')
""")

print("Preview CIS:")
print(con.execute("SELECT * FROM CIS LIMIT 5").fetchdf())


# ====================================
# STEP 3 - Apply ERROR RULES
# ====================================

# Error 001 - Citizenship missing/invalid
con.execute("""
    CREATE OR REPLACE TABLE OUT1 AS
    SELECT *,
           'CITIZENSHIP' AS FIELDTYPE,
           CASE 
             WHEN CITIZENSHIP = ''  THEN 'BLANK CITIZENSHIP'
             WHEN CITIZENSHIP = 'OT' THEN 'OTHER CITIZENSHIP'
             WHEN CITIZENSHIP = '99' THEN 'UNKNOWN CITIZENSHIP'
             ELSE CITIZENSHIP
           END AS FIELDVALUE,
           'PLS MAINTAIN CITIZENSHIP' AS REMARKS,
           '001' AS ERRORCODE
    FROM CIS
    WHERE CITIZENSHIP IN ('OT','', '99')
""")

# Error 002 - Missing Individual ID
con.execute("""
    CREATE OR REPLACE TABLE OUT2 AS
    SELECT *,
           'ALIAS TYPE' AS FIELDTYPE,
           CASE 
             WHEN ALIASKEY = '' THEN 'BLANK ID (INDV CUST)'
             ELSE ALIASKEY || ALIAS
           END AS FIELDVALUE,
           'PLS MAINTAIN INDIVIDUAL ID' AS REMARKS,
           '002' AS ERRORCODE
    FROM CIS
    WHERE INDORG = 'I' AND ALIASKEY NOT IN ('IC','PP','ML','PL','BC')
""")

# Error 003 - Missing Organisation ID
con.execute("""
    CREATE OR REPLACE TABLE OUT3 AS
    SELECT *,
           'ALIAS TYPE' AS FIELDTYPE,
           CASE 
             WHEN ALIASKEY = '' THEN 'BLANK ID (ORG CUST)'
             ELSE ALIASKEY || ALIAS
           END AS FIELDVALUE,
           'PLS MAINTAIN ORGANISATION ID' AS REMARKS,
           '003' AS ERRORCODE
    FROM CIS
    WHERE INDORG = 'O' AND ALIASKEY NOT IN ('BR','CI','PC','SA')
""")

# Error 004 - Missing DOB for Individual
con.execute("""
    CREATE OR REPLACE TABLE OUT4 AS
    SELECT *,
           'DATE OF BIRTH' AS FIELDTYPE,
           'BLANK DOB' AS FIELDVALUE,
           'PLS MAINTAIN DATE OF BIRTH' AS REMARKS,
           '004' AS ERRORCODE
    FROM CIS
    WHERE DOBCC = '' AND INDORG = 'I'
""")

# Error 005 - Missing Date of Register for Organisation
con.execute("""
    CREATE OR REPLACE TABLE OUT5 AS
    SELECT *,
           'BUS. SINCE' AS FIELDTYPE,
           'BLANK DOR' AS FIELDVALUE,
           'PLS MAINTAIN BUS. SINCE DATE' AS REMARKS,
           '005' AS ERRORCODE
    FROM CIS
    WHERE DOBCC = '' AND INDORG = 'O'
""")

# ====================================
# STEP 4 - Consolidate Errors
# ====================================
con.execute("""
    CREATE OR REPLACE TABLE ERROREC AS
    SELECT * FROM OUT1
    UNION ALL SELECT * FROM OUT2
    UNION ALL SELECT * FROM OUT3
    UNION ALL SELECT * FROM OUT4
    UNION ALL SELECT * FROM OUT5
""")

# ====================================
# STEP 5 - Merge with ACCT
# ====================================
con.execute("""
    CREATE OR REPLACE TABLE MERGE AS
    SELECT 
        A.CUSTNO, A.ACCTNOC, 
        B.BRANCH, B.ACCTCODE,
        CASE WHEN A.PRISEC = 901 THEN 'P'
             WHEN A.PRISEC = 902 THEN 'S'
             ELSE '' END AS PRIMSEC,
        A.ERRORCODE, A.FIELDTYPE, A.FIELDVALUE, A.REMARKS
    FROM ERROREC A
    JOIN ACCT B
      ON A.CUSTNO = B.CUSTNO AND A.ACCTNOC = B.ACCTNOC
""")

# ====================================
# STEP 6 - Export to ERRFILE (fixed-width style in SAS → CSV here)
# ====================================
err_tbl = con.execute("SELECT * FROM MERGE").arrow()
csv.write_csv(err_tbl, f"{csv_output_path}/CIS.CCRIS.ERROR.txt")

print("✅ ERRFILE generated at:", f"{csv_output_path}/CIS.CCRIS.ERROR.txt")
