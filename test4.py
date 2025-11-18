import duckdb
from CIS_PY_READER import host_parquet_path, get_hive_parquet, parquet_output_path, csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ==========================================
# 1) CONFIG
# ==========================================
con = duckdb.connect()
hrcstdp = get_hive_parquet('CIS_HRCCUST_DPACCTS')

# ========================================================
# 2) CISDP — SAS fixed-col input rewritten using DuckDB
# ========================================================
# Assumption: parquet already contains correct columns
# Otherwise, use substr() to slice fields
con.execute(f"""
    CREATE TABLE CISDP AS 
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
    FROM read_parquet('{hrcstdp[0]}')
    ORDER BY ACCTNO
""")

# ========================================================
# 3) DPDATA — SAS INPUT with conditional fields
# ========================================================
con.execute(f"""
    CREATE TABLE DPDATA AS
    SELECT
        CAST(BANKNO AS INTEGER) AS BANKNO,
        CAST(REPTNO AS INTEGER) AS REPTNO,
        CAST(FMTCODE AS INTEGER) AS FMTCODE,
        LPAD(CAST(CAST(BRANCH AS INT) AS VARCHAR),3,'0') AS BRANCH,
        LPAD(CAST(CAST(ACCTNO AS BIGINT) AS VARCHAR),11,'0') AS ACCTNO,
        CLOSEDT  AS CLSDATE,
        REOPENDT AS OPENDATE,
        LEDGBAL  AS LEDBAL,
        OPENIND  AS ACCSTAT,
        COSTCTR,
        -- SAS: TMPACCT = PUT(ACCTNO,Z10.)
        LPAD(ACCTNO::VARCHAR, 10, '0') AS TMPACCT
    FROM '{host_parquet_path("DPTRBLGS_CIS.parquet")}'
    WHERE REPTNO = 1001
      AND FMTCODE IN (1,5,10,11,19,20,21,22)
      AND BRANCH <> 0
      AND OPENDATE <> 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY ACCTNO) = 1
    ORDER BY ACCTNO
""")

# ========================================================
# 4) MERGE → GOODDP / BADDP  (SAS MERGE logic)
# ========================================================
con.execute("""
    CREATE TABLE MERGED AS
    SELECT
        A.*,
        B.BANKNUM, B.CUSTBRCH, B.CUSTNO, B.CUSTNAME, B.RACE,
        B.CITIZENSHIP, B.INDORG, B.PRIMSEC,
        B.CUSTLASTDATECC, B.CUSTLASTDATEYY, B.CUSTLASTDATEMM, B.CUSTLASTDATEDD,
        B.ALIASKEY, B.ALIAS, B.HRCCODES, B.ACCTCODE
    FROM DPDATA A
    JOIN CISDP B USING (ACCTNO);
""")

# GOOD / BAD based on SAS conditions
con.execute("""
    CREATE TABLE GOODDP AS
    SELECT *
    FROM MERGED
    WHERE
        (
            SUBSTR(TMPACCT, 1, 1) IN ('1', '3')
            AND ACCSTAT NOT IN ('C', 'B', 'P', 'Z')
        )
        OR
        (
            SUBSTR(TMPACCT, 1, 1) NOT IN ('1','3')
            AND (ACCSTAT NOT IN ('C','B','P','Z') OR LEDBAL <> 0)
        )
    ORDER BY CUSTNO, ACCTNO;
""")

con.execute("""
    CREATE TABLE BADDP AS
    SELECT *
    FROM MERGED
    EXCEPT
    SELECT * FROM GOODDP;
""")

# ========================================================
# 5) PBB / PIBB SPLIT (SAS SORT OUTFIL)
#    OUTFIL:
#    - IF FIELD 210 != '3' → PBB (conventional)
#    - IF FIELD 210 == '3' → PIBB (Islamic)
# ========================================================
con.execute("""
    CREATE TABLE GOOD_PBB AS
    SELECT * FROM GOODDP
    WHERE COSTCTR <> 3;     -- matches SAS INCLUDE=(210,1,CH,NE,'3')
""")

con.execute("""
    CREATE TABLE GOOD_PIBB AS
    SELECT * FROM GOODDP
    WHERE COSTCTR = 3;      -- matches SAS INCLUDE=(210,1,CH,EQ,'3')
""")
