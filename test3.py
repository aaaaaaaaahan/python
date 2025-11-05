import duckdb
from CIS_PY_READER import host_parquet_path, get_hive_parquet, parquet_output_path, csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ---------------------------------------------------------------------
# Job: CIHRCOT1  |  Converted from SAS to Python + DuckDB
# Purpose: Process high-risk customer OT account declarations
# Split by first character of COSTCTR into:
#   PBB  (Conventional) = COSTCTR[1] <> '3'
#   PIBB (Islamic)       = COSTCTR[1] = '3'
# ---------------------------------------------------------------------

# ---------------------------
# Connect to DuckDB
# ---------------------------
con = duckdb.connect()
hrcstot = get_hive_parquet('CIS_HRCCUST_OTACCTS')

# ---------------------------------------------------------------------
# Step 1: Load Input Tables
# ---------------------------------------------------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE CISOT AS
    SELECT 
        BANKNUM,
        CUSTBRCH,
        CUSTNO,
        CUSTNAME,
        RACE,
        CITIZENSHIP,
        INDORG,
        PRIMSEC,
        CUSTLASTDATECC,
        CUSTLASTDATEYY,
        CUSTLASTDATEMM,
        CUSTLASTDATEDD,
        ALIASKEY,
        ALIAS,
        HRCCODES,
        ACCTCODE,
        ACCTNO
    FROM read_parquet('{hrcstot[0]}')
""")

con.execute(f"""
    CREATE OR REPLACE TABLE OTDATA AS
    SELECT DISTINCT
        C_CIS_APPL_CODE AS ACCTCODE,
        U_CIS_APPL_NO AS ACCTNO,
        C_CIS_STATUS AS ACCSTAT,
        LPAD(CAST(CAST(U_CIS_BRANCH AS BIGINT) AS VARCHAR), 7, '0') AS BRANCH,
        LPAD(CAST(CAST(U_CIS_COST_CNTR AS BIGINT) AS VARCHAR), 4, '0') AS COSTCTR,
        REPLACE(D_CIS_ACCT_OPEN, '-', '') AS OPDATE,
        REPLACE(D_CIS_ACCT_CLOSED, '-', '') AS CLDATE
    FROM '{host_parquet_path("UNLOAD_CIACCTT_FB.parquet")}'
""")

# ---------------------------------------------------------------------
# Step 2: Merge & Classify GOOD / BAD OT Accounts
# ---------------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE MERGED AS
    SELECT 
        A.*,
        B.BANKNUM, B.CUSTBRCH, B.CUSTNO, B.CUSTNAME,
        B.RACE, B.CITIZENSHIP, B.INDORG, B.PRIMSEC,
        B.CUSTLASTDATECC, B.CUSTLASTDATEYY, B.CUSTLASTDATEMM, B.CUSTLASTDATEDD,
        B.ALIASKEY, B.ALIAS, B.HRCCODES
    FROM OTDATA A
    JOIN CISOT B USING (ACCTNO)
""")

# GOOD: ACCSTAT not in ('C','B','P','Z') and ACCTNO not blank
con.execute("""
    CREATE OR REPLACE TABLE GOODOT AS
    SELECT * FROM MERGED
    WHERE ACCSTAT NOT IN ('C','B','P','Z') AND ACCTNO <> ''
""")

# BAD: Remaining or closed
con.execute("""
    CREATE OR REPLACE TABLE BADOT AS
    SELECT * FROM MERGED
    WHERE ACCSTAT IN ('C','B','P','Z') OR ACCTNO = ''
""")

# ---------------------------------------------------------------------
# Step 4: Split GOOD accounts into PBB and PIBB using first char of COSTCTR
# COSTCTR[1] = '3' → PIBB (Islamic)
# COSTCTR[1] <> '3' → PBB (Conventional)
# ---------------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE OTCONV AS
    SELECT * FROM GOODOT WHERE SUBSTR(COSTCTR, 1, 1) <> '3'
""")

con.execute("""
    CREATE OR REPLACE TABLE OTPIBB AS
    SELECT * FROM GOODOT WHERE SUBSTR(COSTCTR, 1, 1) = '3'
""")

# ---------------------------------------------------------------------
# Output as Parquet and CSV
# ---------------------------------------------------------------------
out1 = """
    SELECT
        *
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM GOODOT
""".format(year=year,month=month,day=day)

out2 = """
    SELECT
        *
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM BADOT
""".format(year=year,month=month,day=day)

out3 = """
    SELECT
        *
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM OTCONV
""".format(year=year,month=month,day=day)

out4 = """
    SELECT
        *
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM OTPIBB
""".format(year=year,month=month,day=day)

queries = {
    "CIS_HRCCUST_OTACCTS_GOOD"                      : out1,
    "CIS_HRCCUST_OTACCTS_CLOSED"                    : out2,
    "CIS_HRCCUST_OTACCTS_GOOD_PBB"                  : out3,
    "CIS_HRCCUST_OTACCTS_GOOD_PIBB"                 : out4
}

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    con.execute(f"""
    COPY ({query})
    TO '{parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);  
     """)
    
    con.execute(f"""
    COPY ({query})
    TO '{csv_path}'
    (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);  
     """)
