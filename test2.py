import duckdb
import datetime
from CIS_PY_READER import host_parquet_path, csv_output_path

# --------------------------
# Batch / report date
# --------------------------
batch_date = datetime.date.today() - datetime.timedelta(days=1)
report_date = batch_date.strftime("%d-%m-%Y")
yyyymm = batch_date.strftime("%Y%m")  # SAS YYYYMM equivalent

# --------------------------
# Connect to DuckDB
# --------------------------
con = duckdb.connect()

# --------------------------
# Load HRC raw for current YYYYMM
# --------------------------
con.execute(f"""
CREATE OR REPLACE TABLE HRCRECS_RAW AS
SELECT *
FROM '{host_parquet_path("UNLOAD_CIHRCAPT_FB.parquet")}'
WHERE SUBSTR(UPDATEDATE,1,7) = '{yyyymm}'
""")

# --------------------------
# Apply SAS deletion rule: APPROVALSTATUS='02' AND ACCTTYPE NOT IN (...) --> DELETE
# --------------------------
con.execute("""
CREATE OR REPLACE TABLE HRCRECS AS
SELECT *
FROM HRCRECS_RAW
WHERE NOT (
    APPROVALSTATUS = '02'
    AND ACCTTYPE NOT IN ('CA','SA','SDB','FD','FC','FCI','O','FDF')
)
""")

# --------------------------
# Load HISRECS raw filtered by SUBTYPE and YYYYMM
# --------------------------
con.execute(f"""
CREATE OR REPLACE TABLE HISRECS_RAW AS
SELECT *
FROM '{host_parquet_path("UNLOAD_CIHRCHST_FB.parquet")}'
WHERE SUBTYPE IN ('DELAPP','HOEDEL','DELHOE')
  AND SUBSTR(UPDATEDATE,1,7) = '{yyyymm}'
""")

# --------------------------
# Deduplicate HISRECS: keep latest UPDATETIME per key + SUBTYPE
# --------------------------
con.execute("""
CREATE OR REPLACE TABLE HISRECS AS
SELECT
    ALIAS, BRCHCODE, PRIMARYJOINT, CISJOINTID1, CISJOINTID2,
    CISJOINTID3, CISJOINTID4, CISJOINTID5, UPDATEDATE, UPDATETIME,
    SUBTYPE, ACCTTYPE, OPERATOR, REMARKS
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY ALIAS, BRCHCODE, ACCTTYPE,
                          PRIMARYJOINT, CISJOINTID1, CISJOINTID2,
                          CISJOINTID3, CISJOINTID4, CISJOINTID5, SUBTYPE
             ORDER BY UPDATETIME DESC
           ) AS rn
    FROM HISRECS_RAW
) t
WHERE rn = 1;
""")

# --------------------------
# Merge HRC + HIS (left join) and initialize flags
# --------------------------
con.execute("""
CREATE OR REPLACE TABLE MRGHRC AS
SELECT
    A.*,
    B.SUBTYPE,
    B.OPERATOR,
    B.REMARKS AS HIS_REMARKS,
    0 AS HOEREJ, 0 AS HOEDEL, 0 AS HOEPDREV, 0 AS HOEPDAPPR, 0 AS HOEAPPR,
    0 AS HOEPDNOTE, 0 AS HOENOTED, 0 AS HOEACCT, 0 AS HOEXACCT,
    0 AS BRHREJ, 0 AS BRHDEL, 0 AS BRHCOM, 0 AS BRHEDD, 0 AS BRHAPPR,
    0 AS BRHACCT, 0 AS BRHXACCT, 0 AS DELHOE, 0 AS DELAP1,
    0 AS HOENOTED1, 0 AS HOEPDNOTE1
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

# --------------------------
# Apply status mapping logic (SAS SELECT WHEN blocks)
# --------------------------
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

# --------------------------
# ACCTNO and HOVERIFYREMARKS checks
# --------------------------
con.execute("""
UPDATE MRGHRC
SET
    HOEACCT = CASE WHEN APPROVALSTATUS = '02' AND TRIM(COALESCE(ACCTNO, '')) <> '' THEN 1 ELSE 0 END,
    HOEXACCT = CASE WHEN APPROVALSTATUS = '02' AND TRIM(COALESCE(ACCTNO, '')) = '' THEN 1 ELSE 0 END,
    BRHACCT = CASE WHEN APPROVALSTATUS = '08' AND TRIM(COALESCE(ACCTNO, '')) <> '' THEN 1 ELSE 0 END,
    BRHXACCT = CASE WHEN APPROVALSTATUS = '08' AND TRIM(COALESCE(ACCTNO, '')) = '' THEN 1 ELSE 0 END,
    HOEPDNOTE = CASE WHEN APPROVALSTATUS = '08' AND TRIM(COALESCE(ACCTNO, '')) <> '' AND INSTR(COALESCE(HOVERIFYREMARKS, ''), 'Noted by') <= 0 THEN 1 ELSE 0 END,
    HOENOTED = CASE WHEN APPROVALSTATUS = '08' AND TRIM(COALESCE(ACCTNO, '')) <> '' AND INSTR(COALESCE(HOVERIFYREMARKS, ''), 'Noted by') > 0 THEN 1 ELSE 0 END,
    HOEPDNOTE1 = CASE WHEN APPROVALSTATUS = '08' AND TRIM(COALESCE(ACCTNO, '')) <> '' 
                          AND ACCTTYPE IN ('CA','FD','FC','SDB') 
                          AND INSTR(COALESCE(HOVERIFYREMARKS, ''), 'Noted by') <= 0 THEN 1 ELSE 0 END,
    HOENOTED1 = CASE WHEN APPROVALSTATUS = '08' AND TRIM(COALESCE(ACCTNO, '')) <> '' 
                          AND ACCTTYPE IN ('CA','FD','FC','SDB') 
                          AND INSTR(COALESCE(HOVERIFYREMARKS, ''), 'Noted by') > 0 THEN 1 ELSE 0 END
""")

# --------------------------
# Branch summary (PROC SUMMARY)
# --------------------------
con.execute("""
CREATE OR REPLACE TABLE SUMMARY AS
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
    SUM(HOEPDNOTE1) AS HOEPDNOTE1,
    SUM(HOENOTED1) AS HOENOTED1,
    COUNT(*) AS TOTAL
FROM MRGHRC
GROUP BY BRCHCODE
ORDER BY BRCHCODE
""")

# --------------------------
# TXT Output (match SAS PUT layout)
# --------------------------
txt_path = csv_output_path(f"CISHRC_STATUS_DAILY_{report_date}").replace('.csv','.txt')

rows = con.execute("""
    SELECT
        BRCHCODE,
        HOEREJ, HOEDEL, HOEPDREV, HOEPDAPPR, HOEAPPR,
        HOEPDNOTE, HOEACCT, HOEXACCT, BRHREJ, DELAP1,
        BRHCOM, BRHEDD, BRHAPPR, BRHACCT, BRHXACCT,
        HOENOTED, HOEPDNOTE1, HOENOTED1, TOTAL
    FROM SUMMARY
    ORDER BY BRCHCODE
""").fetchall()

header = (
    f"{'BRANCH':<7}" +
    f"{'':1}" +  # spacer
    "HOE REJECT, HOE DELETE, PEND REVIEW, PEND APPROVAL, HOE APPROVED, HOE PEND NOTE, " +
    "HOE APPR ACCT OPEN, HOE APPR NO ACCT, BRANCH REJECT, BRANCH DELETE, BRANCH RECOM, " +
    "BRANCH EDD, BRANCH APPROVED, BRANCH APPR ACCT, BRANCH APPR NO ACCT, HOE NOTED, " +
    "PENDING NOTING (HOE), NOTED (HOE), TOTAL"
)

def z8(val):
    try:
        return f"{int(val):0>8d}"
    except Exception:
        return "00000000"

with open(txt_path, 'w', encoding='utf-8') as f:
    f.write(header + "\n")
    for r in rows:
        brchcode = (r[0] or "").ljust(7)[:7]
        parts = [
            brchcode, ", ",
            z8(r[1]), ", ",
            z8(r[2]), ", ",
            z8(r[3]), ", ",
            z8(r[4]), ", ",
            z8(r[5]), ", ",
            z8(r[6]), ", ",
            z8(r[7]), ", ",
            z8(r[8]), ", ",
            z8(r[9]), ", ",
            z8(r[10]), ", ",
            z8(r[11]), ", ",
            z8(r[12]), ", ",
            z8(r[13]), ", ",
            z8(r[14]), ", ",
            z8(r[15]), ", ",
            z8(r[16]), ", ",
            z8(r[17]), ", ",
            z8(r[18]), ", ",
            z8(r[19])
        ]
        f.write("".join(parts) + "\n")
