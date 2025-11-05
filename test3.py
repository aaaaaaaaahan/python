import duckdb
import datetime
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

# ---------------------------
# Config / date handling
# ---------------------------
batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day
today = batch_date.strftime("%Y-%m-%d")   # string used for filtering

# output "name" used by helper functions
OUT_NAME = "CISHRC_STATUS_DAILY"

# ---------------------------
# Connect to DuckDB
# ---------------------------
con = duckdb.connect()

# ---------------------------
# Load HRC raw for today's date
# ---------------------------
con.execute(f"""
CREATE OR REPLACE TABLE HRCRECS_RAW AS
SELECT *
FROM '{host_parquet_path("UNLOAD_CIHRCAPT_FB.parquet")}'
WHERE UPDATEDATE = '{today}'
""")

# ---------------------------
# Apply SAS deletion rule:
# If APPROVALSTATUS = '02' and ACCTTYPE NOT IN (...) --> DELETE (exclude)
# (SAS: WHEN('02') ... IF ACCTTYPE NOT IN(...) THEN DELETE;)
# ---------------------------
con.execute("""
CREATE OR REPLACE TABLE HRCRECS AS
SELECT *
FROM HRCRECS_RAW
WHERE NOT (
    APPROVALSTATUS = '02'
    AND ACCTTYPE NOT IN ('CA','SA','SDB','FD','FC','FCI','O','FDF')
)
""")

# ---------------------------
# Load HISRECS raw for today's date and required SUBTYPEs
# ---------------------------
con.execute(f"""
CREATE OR REPLACE TABLE HISRECS_RAW AS
SELECT *
FROM '{host_parquet_path("UNLOAD_CIHRCHST_FB.parquet")}'
WHERE SUBTYPE IN ('DELAPP','HOEDEL','DELHOE')
  AND UPDATEDATE = '{today}'
""")

# ---------------------------
# Deduplicate HISRECS:
# keep latest UPDATETIME per key + SUBTYPE (SAS: PROC SORT ... NODUPKEY BY ... SUBTYPE)
# ---------------------------
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

# ---------------------------
# Merge HRC + HIS (left join) and initialize flags (same fields as SAS MRGHRC)
# ---------------------------
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

# ---------------------------
# Apply status mapping logic (SAS SELECT WHEN blocks)
# Use TRIM(COALESCE(...,'')) for ACCTNO checks and COALESCE for remarks
# ---------------------------
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

# second update for ACCTNO-related flags and 'Noted by' checks
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

# ---------------------------
# Branch summary: aggregate sums by BRCHCODE (matching PROC SUMMARY)
# ---------------------------
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
    SUM(HOENOTED1) AS HOENOTED1,
    SUM(HOEPDNOTE1) AS HOEPDNOTE1,
    COUNT(*) AS TOTAL
FROM MRGHRC
GROUP BY BRCHCODE
ORDER BY BRCHCODE
""")

# ---------------------------
# Write OUTPUT PARQUET & CSV (optional, keep parity with your existing pipeline)
# ---------------------------
out1 = f"""
    SELECT
        BRCHCODE,
        HOEREJ, HOEDEL, HOEPDREV, HOEPDAPPR, HOEAPPR,
        HOEPDNOTE, HOEACCT, HOEXACCT, BRHREJ, DELAP1,
        BRHCOM, BRHEDD, BRHAPPR, BRHACCT, BRHXACCT,
        HOENOTED, HOEPDNOTE1, HOENOTED1, TOTAL,
        {year} AS year, {month} AS month, {day} AS day
    FROM SUMMARY
    ORDER BY BRCHCODE
"""

# write parquet (partitioned)
parquet_path = parquet_output_path(OUT_NAME)
con.execute(f"""
COPY ({out1})
TO '{parquet_path}'
(FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
""")

# write a CSV as well (we'll derive TXT from this path)
csv_path = csv_output_path(OUT_NAME)
con.execute(f"""
COPY ({out1})
TO '{csv_path}'
(FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);
""")

# ---------------------------
# Create TXT file in SAS OUTRECS layout (match SAS PUT positions & field order)
# SAS header & PUT order (we mimic it)
# ---------------------------
# Decide txt file path:
txt_path = csv_path
if txt_path.lower().endswith('.csv'):
    txt_path = txt_path[:-4] + '.txt'
else:
    txt_path = txt_path + '.txt'

# Fetch rows from SUMMARY (we used the same SELECT as out1 but without year/month/day)
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

# Build header (match SAS header labels and approximate positions)
header = (
    f"{'BRANCH':<7}" +
    f"{'':1}" +  # spacer to approximately match positions
    "HOE REJECT, HOE DELETE, PEND REVIEW, PEND APPROVAL, HOE APPROVED, HOE PEND NOTE, " +
    "HOE APPR ACCT OPEN, HOE APPR NO ACCT, BRANCH REJECT, BRANCH DELETE, BRANCH RECOM, " +
    "BRANCH EDD, BRANCH APPROVED, BRANCH APPR ACCT, BRANCH APPR NO ACCT, HOE NOTED, " +
    "PENDING NOTING (HOE), NOTED (HOE), TOTAL"
)

# Now write to txt_path with formatting similar to SAS PUT:
# SAS used Z8. (zero-padded width 8) for numeric fields and $7. for branch.
def z8(val):
    try:
        ival = int(val)
    except Exception:
        ival = 0
    return f"{ival:0>8d}"  # zero-padded to width 8

with open(txt_path, 'w', encoding='utf-8') as f:
    # write header line (SAS prints header only once at _N_=1)
    f.write(header + "\n")
    for r in rows:
        # r tuple as (BRCHCODE, HOEREJ, HOEDEL, HOEPDREV, HOEPDAPPR, HOEAPPR,
        #   HOEPDNOTE, HOEACCT, HOEXACCT, BRHREJ, DELAP1, BRHCOM, BRHEDD,
        #   BRHAPPR, BRHACCT, BRHXACCT, HOENOTED, HOEPDNOTE1, HOENOTED1, TOTAL)
        brchcode = (r[0] or "").ljust(7)[:7]
        # Compose the line following the SAS numeric ordering and comma separators
        parts = [
            brchcode,
            ", ",
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
        line = "".join(parts)
        f.write(line + "\n")

print("TXT output written to:", txt_path)
print("CSV output written to:", csv_path)
print("Parquet output written to:", parquet_path)
