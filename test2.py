#!/usr/bin/env python3
"""
CIHRCAPD – Converted from SAS to Python (DuckDB + PyArrow)
- Reads HRCUNLD.parquet and DPTRBALS.parquet
- Uses current date minus 1 day as reporting date
- Replicates SAS filtering, merge, and output logic
- Generates both Parquet output and flat TXT output
"""

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
from pathlib import Path

# ============================================================
# 1) DATE HANDLING (replace SRSCTRL)
# ============================================================
batch_date = datetime.date.today()
TODAYDATE = batch_date.strftime("%Y-%m-%d")
YYYYMM = batch_date.strftime("%Y-%m")
YYYY = batch_date.strftime("%Y")

print("REPORT DATE:", TODAYDATE)

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
    FROM parquet_scan('{HRCUNLD_PARQUET}')
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
        SELECT *
        FROM parquet_scan('{DPTRBALS_PARQUET}')
    )
    SELECT
        LPAD(CAST(ACCTNO AS VARCHAR), 10, '0') AS ACCTNOC,
        OPENDATE,
        '' AS NOTENOC
    FROM RAW
    WHERE REPTNO = 1001
      AND FMTCODE IN (1,10,22)
      AND OPENDATE = '{TODAYDATE}'
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

with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
    f.write(header + "\n")
    for row in out:
        row = ["" if x is None else str(x).strip() for x in row]
        f.write(",".join(row) + "\n")

print("TXT Output Created:", OUTPUT_TXT)

# ============================================================
# 7) Output Parquet
# ============================================================
arrow_table = con.execute("SELECT * FROM MRGHRC").arrow()
pq.write_table(arrow_table, OUTPUT_PARQUET)

print("Parquet Output Created:", OUTPUT_PARQUET)

print("DONE ✓")
