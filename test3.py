import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")
DATE3 = batch_date.strftime("%Y%m%d")     # format YYYYMMDD

# =============================================================================
# DuckDB connection
# =============================================================================
con = duckdb.connect()
cis = get_hive_parquet('CIS_CUST_DAILY')

# =============================================================================
# LOAD CIS (CISFILE.CUSTDLY)
# =============================================================================
con.execute(f"""
    CREATE TABLE CIS AS
    SELECT *
    EXCLUDE (ALIAS,ALIASKEY)
    FROM read_parquet('{cis[0]}')
    WHERE (
         ACCTNO BETWEEN 1000000000 AND 1999999999 OR
         ACCTNO BETWEEN 3000000000 AND 3999999999 OR
         ACCTNO BETWEEN 4000000000 AND 4999999999 OR
         ACCTNO BETWEEN 5000000000 AND 5999999999 OR
         ACCTNO BETWEEN 6000000000 AND 6999999999 OR
         ACCTNO BETWEEN 7000000000 AND 7999999999
    )
    ORDER BY CUSTNO
""")


# =============================================================================
# LOAD HR FILE (WITH VALIDATION)
# =============================================================================
con.execute(f"""
    CREATE TABLE HR_RAW AS
    SELECT *
    FROM '{host_parquet_path("HCMS_STAFF_RESIGN.parquet")}'
""")

# Validate HEADER date (DATAINDC=0, HEADERDATE must match DATE3)
hdr = con.execute("""
    SELECT HEADERDATE 
    FROM HR_RAW 
    WHERE DATAINDC = '0'
""").fetchone()

if hdr and hdr[0] != DATE3:
    raise Exception(f"ABORT 77: HEADERDATE {hdr[0]} != REPORT DATE {DATE3}")

# Extract HR + OLD_IC
con.execute("""
    CREATE TABLE HR AS
    SELECT *
    FROM HR_RAW
    WHERE DATAINDC = '1' AND REGEXP_MATCH(ALIAS, '^[0-9]{12}$')
""")

con.execute(f"""
    CREATE TABLE OLD_IC AS
    SELECT *, '003 IC NOT 12 DIGIT      ' AS remarks
    FROM HR_RAW
    WHERE DATAINDC = '1' AND NOT REGEXP_MATCH(ALIAS, '^[0-9]{12}$')
""")

# Validate TRAILER (DATAINDC=9)
trailer = con.execute("""
    SELECT total_rec
    FROM HR_RAW WHERE DATAINDC='9'
""").fetchone()

count_hr = con.execute("SELECT COUNT(*) FROM HR").fetchone()[0]

if trailer and int(trailer[0]) != count_hr:
    raise Exception(f"ABORT 88: trailer count {trailer[0]} != HR count {count_hr}")


# =============================================================================
# LOAD ALS FILE
# =============================================================================
con.execute(f"""
    CREATE TABLE ALS AS
    SELECT *
    FROM '{host_parquet_path("ALLALIAS_FIX.parquet")}'
    WHERE ALIASKEY = 'IC'
""")


# =============================================================================
# MATCH 1: HR + ALS → RESULT1, NO_IC
# =============================================================================
con.execute("""
    CREATE TABLE RESULT1 AS
    SELECT hr.*, als.CUSTNO AS CUSTNO
    FROM HR hr
    JOIN ALS als USING (ALIAS)
""")

con.execute("""
    CREATE TABLE NO_IC AS
    SELECT hr.*, '001 STAFF IC NOT FOUND   ' AS REMARKS
    FROM HR hr
    LEFT JOIN ALS als USING (ALIAS)
    WHERE als.ALIAS IS NULL
""")


# =============================================================================
# MATCH 2: RESULT1 + CIS → MATCH2, NO_ACCT
# =============================================================================
con.execute("""
    CREATE TABLE MATCH2 AS
    SELECT r.*, c.CUSTNAME, c.ACCTCODE, c.ACCTNOC, c.PRISEC
    FROM RESULT1 r
    JOIN CIS c USING (CUSTNO)
""")

con.execute("""
    CREATE TABLE NO_ACCT AS
    SELECT r.*, '002 CIS WITH NO ACCOUNT  ' AS REMARKS
    FROM RESULT1 r
    LEFT JOIN CIS c USING (CUSTNO)
    WHERE c.CUSTNO IS NULL
""")
