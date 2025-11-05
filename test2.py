import duckdb
import datetime
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

# -------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------
input_path = Path("/host/cis/input")
output_path = Path("/host/cis/output")

hrc_unld = input_path / "UNLOAD_CIHRCAPT.parquet"
hst_unld = input_path / "UNLOAD_CIHRCHST.parquet"

parquet_out = output_path / "CISHRC_STATUS_DAILY.parquet"
csv_out = output_path / "CISHRC_STATUS_DAILY.csv"

# -------------------------------------------------------------------
# GET TODAY'S DATE
# -------------------------------------------------------------------
today = datetime.date.today().strftime("%Y-%m-%d")

# -------------------------------------------------------------------
# CONNECT TO DUCKDB
# -------------------------------------------------------------------
con = duckdb.connect()

# -------------------------------------------------------------------
# LOAD HRC RECORDS
# -------------------------------------------------------------------
con.execute(f"""
CREATE OR REPLACE TABLE HRCRECS AS
SELECT
    ALIAS,
    BRCHCODE,
    ACCTTYPE,
    APPROVALSTATUS,
    ACCTNO,
    CISNO,
    CREATIONDATE,
    PRIMARYJOINT,
    CISJOINTID1,
    CISJOINTID2,
    CISJOINTID3,
    CISJOINTID4,
    CISJOINTID5,
    CUSTTYPE,
    CUSTNAME,
    CUSTGENDER,
    CUSTDOBDOR,
    CUSTEMPLOYER,
    CUSTADDR1,
    CUSTADDR2,
    CUSTADDR3,
    CUSTADDR4,
    CUSTADDR5,
    CUSTPHONE,
    CUSTPEP,
    DTCORGUNIT,
    DTCINDUSTRY,
    DTCNATION,
    DTCOCCUP,
    DTCACCTTYPE,
    DTCCOMPFORM,
    DTCWEIGHTAGE,
    DTCTOTAL,
    DTCSCORE1,
    DTCSCORE2,
    DTCSCORE3,
    DTCSCORE4,
    DTCSCORE5,
    DTCSCORE6,
    ACCTPURPOSE,
    ACCTREMARKS,
    SOURCEFUND,
    SOURCEDETAILS,
    PEPINFO,
    PEPWEALTH,
    PEPFUNDS,
    BRCHRECOMDETAILS,
    BRCHEDITOPER,
    BRCHAPPROVEOPER,
    BRCHCOMMENTS,
    BRCHREWORK,
    HOVERIFYOPER,
    HOVERIFYDATE,
    HOVERIFYCOMMENTS,
    HOVERIFYREMARKS,
    HOVERIFYREWORK,
    HOAPPROVEOPER,
    HOAPPROVEDATE,
    HOAPPROVEREMARKS,
    HOCOMPLYREWORK,
    UPDATEDATE,
    UPDATETIME
FROM read_parquet('{hrc_unld}')
WHERE UPDATEDATE = '{today}'
""")

# -------------------------------------------------------------------
# LOAD HISTORY RECORDS
# -------------------------------------------------------------------
con.execute(f"""
CREATE OR REPLACE TABLE HISRECS AS
SELECT
    ALIAS,
    BRCHCODE,
    PRIMARYJOINT,
    CISJOINTID1,
    CISJOINTID2,
    CISJOINTID3,
    CISJOINTID4,
    CISJOINTID5,
    UPDATEDATE,
    UPDATETIME,
    SUBTYPE,
    ACCTTYPE,
    OPERATOR,
    REMARKS
FROM read_parquet('{hst_unld}')
WHERE SUBTYPE IN ('DELAPP','HOEDEL','DELHOE')
  AND UPDATEDATE = '{today}'
""")

# -------------------------------------------------------------------
# MERGE HRC + HISTORY
# -------------------------------------------------------------------
con.execute("""
CREATE OR REPLACE TABLE MRGHRC AS
SELECT
    A.ALIAS,
    A.BRCHCODE,
    A.ACCTTYPE,
    A.APPROVALSTATUS,
    A.ACCTNO,
    A.CISNO,
    A.CREATIONDATE,
    A.PRIMARYJOINT,
    A.CISJOINTID1,
    A.CISJOINTID2,
    A.CISJOINTID3,
    A.CISJOINTID4,
    A.CISJOINTID5,
    A.CUSTTYPE,
    A.CUSTNAME,
    A.CUSTGENDER,
    A.CUSTDOBDOR,
    A.CUSTEMPLOYER,
    A.CUSTADDR1,
    A.CUSTADDR2,
    A.CUSTADDR3,
    A.CUSTADDR4,
    A.CUSTADDR5,
    A.CUSTPHONE,
    A.CUSTPEP,
    A.DTCORGUNIT,
    A.DTCINDUSTRY,
    A.DTCNATION,
    A.DTCOCCUP,
    A.DTCACCTTYPE,
    A.DTCCOMPFORM,
    A.DTCWEIGHTAGE,
    A.DTCTOTAL,
    A.DTCSCORE1,
    A.DTCSCORE2,
    A.DTCSCORE3,
    A.DTCSCORE4,
    A.DTCSCORE5,
    A.DTCSCORE6,
    A.ACCTPURPOSE,
    A.ACCTREMARKS,
    A.SOURCEFUND,
    A.SOURCEDETAILS,
    A.PEPINFO,
    A.PEPWEALTH,
    A.PEPFUNDS,
    A.BRCHRECOMDETAILS,
    A.BRCHEDITOPER,
    A.BRCHAPPROVEOPER,
    A.BRCHCOMMENTS,
    A.BRCHREWORK,
    A.HOVERIFYOPER,
    A.HOVERIFYDATE,
    A.HOVERIFYCOMMENTS,
    A.HOVERIFYREMARKS,
    A.HOVERIFYREWORK,
    A.HOAPPROVEOPER,
    A.HOAPPROVEDATE,
    A.HOAPPROVEREMARKS,
    A.HOCOMPLYREWORK,
    A.UPDATEDATE,
    A.UPDATETIME,
    B.SUBTYPE,
    B.OPERATOR,
    B.REMARKS AS HIS_REMARKS,
    0 AS HOEREJ,
    0 AS HOEDEL,
    0 AS HOEPDREV,
    0 AS HOEPDAPPR,
    0 AS HOEAPPR,
    0 AS HOEPDNOTE,
    0 AS HOENOTED,
    0 AS HOEACCT,
    0 AS HOEXACCT,
    0 AS BRHREJ,
    0 AS BRHDEL,
    0 AS BRHCOM,
    0 AS BRHEDD,
    0 AS BRHAPPR,
    0 AS BRHACCT,
    0 AS BRHXACCT,
    0 AS DELHOE,
    0 AS DELAP1,
    0 AS HOENOTED1,
    0 AS HOEPDNOTE1
FROM HRCRECS A
LEFT JOIN HISRECS B
ON A.ALIAS = B.ALIAS
AND A.BRCHCODE = B.BRCHCODE
AND A.ACCTTYPE = B.ACCTTYPE
AND A.PRIMARYJOINT = B.PRIMARYJOINT
AND A.CISJOINTID1 = B.CISJOINTID1
AND A.CISJOINTID2 = B.CISJOINTID2
AND A.CISJOINTID3 = B.CISJOINTID3
AND A.CISJOINTID4 = B.CISJOINTID4
AND A.CISJOINTID5 = B.CISJOINTID5
""")

# -------------------------------------------------------------------
# APPLY STATUS LOGIC
# -------------------------------------------------------------------
con.execute("""
UPDATE MRGHRC
SET HOEREJ = CASE WHEN APPROVALSTATUS = '01' THEN 1 ELSE 0 END,
    HOEAPPR = CASE WHEN APPROVALSTATUS = '02' THEN 1 ELSE 0 END,
    HOEPDAPPR = CASE WHEN APPROVALSTATUS = '03' THEN 1 ELSE 0 END,
    HOEPDREV = CASE WHEN APPROVALSTATUS = '04' THEN 1 ELSE 0 END,
    BRHCOM = CASE WHEN APPROVALSTATUS = '05' THEN 1 ELSE 0 END,
    BRHEDD = CASE WHEN APPROVALSTATUS = '06' THEN 1 ELSE 0 END,
    BRHDEL = CASE WHEN APPROVALSTATUS = '07' AND (SUBTYPE <> 'HOEDEL' OR SUBTYPE IS NULL) THEN 1 ELSE 0 END,
    HOEDEL = CASE WHEN APPROVALSTATUS = '07' AND SUBTYPE = 'HOEDEL' THEN 1 ELSE 0 END,
    BRHAPPR = CASE WHEN APPROVALSTATUS = '08' THEN 1 ELSE 0 END,
    BRHREJ = CASE WHEN APPROVALSTATUS = '09' THEN 1 ELSE 0 END,
    DELAP1 = CASE WHEN APPROVALSTATUS = '10' THEN 1 ELSE 0 END,
    DELHOE = CASE WHEN APPROVALSTATUS = '11' THEN 1 ELSE 0 END
""")

con.execute("""
UPDATE MRGHRC
SET
    HOEACCT = CASE WHEN APPROVALSTATUS = '02' AND ACCTNO <> '' THEN 1 ELSE 0 END,
    HOEXACCT = CASE WHEN APPROVALSTATUS = '02' AND ACCTNO = '' THEN 1 ELSE 0 END,
    BRHACCT = CASE WHEN APPROVALSTATUS = '08' AND ACCTNO <> '' THEN 1 ELSE 0 END,
    BRHXACCT = CASE WHEN APPROVALSTATUS = '08' AND ACCTNO = '' THEN 1 ELSE 0 END,
    HOEPDNOTE = CASE WHEN APPROVALSTATUS = '08' AND ACCTNO <> '' AND INSTR(HOVERIFYREMARKS, 'Noted by') <= 0 THEN 1 ELSE 0 END,
    HOENOTED = CASE WHEN APPROVALSTATUS = '08' AND ACCTNO <> '' AND INSTR(HOVERIFYREMARKS, 'Noted by') > 0 THEN 1 ELSE 0 END,
    HOEPDNOTE1 = CASE WHEN APPROVALSTATUS = '08' AND ACCTNO <> '' 
                          AND ACCTTYPE IN ('CA','FD','FC','SDB') 
                          AND INSTR(HOVERIFYREMARKS, 'Noted by') <= 0 THEN 1 ELSE 0 END,
    HOENOTED1 = CASE WHEN APPROVALSTATUS = '08' AND ACCTNO <> '' 
                          AND ACCTTYPE IN ('CA','FD','FC','SDB') 
                          AND INSTR(HOVERIFYREMARKS, 'Noted by') > 0 THEN 1 ELSE 0 END
""")

# -------------------------------------------------------------------
# BRANCH SUMMARY
# -------------------------------------------------------------------
summary_arrow = con.execute("""
SELECT
    BRCHCODE,
    SUM(HOEREJ) AS HOEREJ,
    SUM(HOEDEL) AS HOEDEL,
    SUM(HOEPDREV) AS HOEPDREV,
    SUM(HOEPDAPPR) AS HOEPDAPPR,
    SUM(HOEAPPR) AS HOEAPPR,
    SUM(HOEPDNOTE) AS HOEPDNOTE,
    SUM(HOEACCT) AS HOEACCT,
    SUM(HOEXACCT) AS HOEXACCT,
    SUM(BRHREJ) AS BRHREJ,
    SUM(BRHDEL) AS BRHDEL,
    SUM(BRHCOM) AS BRHCOM,
    SUM(BRHEDD) AS BRHEDD,
    SUM(BRHAPPR) AS BRHAPPR,
    SUM(BRHACCT) AS BRHACCT,
    SUM(BRHXACCT) AS BRHXACCT,
    SUM(DELAP1) AS DELAP1,
    SUM(DELHOE) AS DELHOE,
    SUM(HOENOTED) AS HOENOTED,
    SUM(HOENOTED1) AS HOENOTED1,
    SUM(HOEPDNOTE1) AS HOEPDNOTE1,
    COUNT(*) AS TOTAL
FROM MRGHRC
GROUP BY BRCHCODE
ORDER BY BRCHCODE
""").arrow()

# -------------------------------------------------------------------
# WRITE OUTPUT PARQUET & CSV
# -------------------------------------------------------------------
pq.write_table(summary_arrow, parquet_out)
con.execute(f"COPY (SELECT * FROM read_parquet('{parquet_out}')) TO '{csv_out}' (HEADER, DELIMITER ',');")

print(f"✅ CIHRCDR1 Report generated for {today}")
print(f"   Parquet: {parquet_out}")
print(f"   CSV: {csv_out}")
