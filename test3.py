import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")

# ============================================================
# 2) INPUT PARQUET FILES (assumed already converted)
# ============================================================
HRCUNLD_PARQUET = "HRCUNLD.parquet"
DPTRBALS_PARQUET = "DPTRBALS.parquet"
OUTPUT_TXT = "HRCDAILY_ACCTLIST.txt"
OUTPUT_PARQUET = "HRCDAILY_ACCTLIST.parquet"

con = duckdb.connect()

# ============================================================
# 3) Load HRCUNLD (fixed-width schema reproduced exactly)
# ============================================================
con.execute(f"""
    CREATE TABLE HRCRECS AS
    SELECT
        ALIAS,
        BRCHCODE,
        ACCTTYPE,
        APPROVALSTATUS,
        ACCTNOC,
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
    FROM '{host_parquet_path("UNLOAD_CIHRCAPT_FB.parquet")}'
    WHERE ACCTNOC IS NOT NULL AND ACCTNOC <> ''
    ORDER BY ACCTNOC, BRCHCODE
""")

print("Loaded HRCUNLD ✓")

# ============================================================
# 4) Load DPTRBALS with SAS logic
# ============================================================
con.execute(f"""
    CREATE TABLE DEPOSIT AS
    WITH RAW AS (
        SELECT *,
            CAST(REPTNO AS INTEGER) AS REPTNO, 
            CAST(FMTCODE AS INTEGER) AS FMTCODE,
            CAST(BANKNO AS INTEGER) AS BANKNO,
            PAD(CAST(CAST(BRANCH AS INT) AS VARCHAR),3,'0') AS BRANCH,
            LPAD(CAST(CAST(ACCTNO AS BIGINT) AS VARCHAR),11,'0') AS ACCTNO,
            CAST(REOPENDT AS BIGINT) AS OPENDATE
        FROM '{host_parquet_path("DPTRBLGS_CIS.parquet")}'
    )
    SELECT
        LPAD(CAST(ACCTNO AS VARCHAR), 10, '0') AS ACCTNOC,
        OPENDATE,
        '' AS NOTENOC
    FROM RAW
    WHERE REPTNO = 1001
      AND FMTCODE IN (1,10,22)
      AND OPENDATE = '{batch_date}'
""")

print("Loaded DPTRBALS ✓")

# ============================================================
# 5) Merge (same as SAS MERGE BY ACCTNOC)
# ============================================================
con.execute("""
    CREATE TABLE MRGHRC AS
    SELECT *
    FROM HRCRECS H
    JOIN DEPOSIT D USING (ACCTNOC)
    ORDER BY BRCHCODE, ACCTNOC
""")

print("Merged HRC + DEPOSIT ✓")

# ============================================================
# 6) Output TXT (same header as SAS PUT statement)
# ============================================================
header = (
    "BRANCH,ID NUMBER,NAME,APPLICATION DATE,TYPE OF ACCOUNT,"
    "APPLICATION STATUS,ACCOUNT NO,ACCOUNT OPEN DATE,CIS NO"
)

out = con.execute("""
    SELECT
        BRCHCODE,
        ALIAS,
        CUSTNAME,
        CREATIONDATE,
        ACCTTYPE,
        APPROVALSTATUS,
        ACCTNOC,
        OPENDATE,
        CISNO
    FROM MRGHRC
""").fetchall()

# ============================================================
# 7) Output Parquet
# ============================================================
print("DONE ✓")
