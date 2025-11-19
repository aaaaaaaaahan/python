#!/usr/bin/env python3
"""
Converted from SAS to Python using DuckDB + PyArrow.

- Input: All parquet files already available.
- Processing: DuckDB SQL.
- Output: 2 fixed-width TXT files (NOTFOUND, OUTFILE).
"""

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
from pathlib import Path

# =============================================================================
# 1) CONFIGURATION
# =============================================================================
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

batch_date = datetime.date.today() - datetime.timedelta(days=1)
DATE3 = batch_date.strftime("%Y%m%d")     # format YYYYMMDD

# input parquet files
ALSFILE = host_parquet_path("UNLOAD.ALLALIAS.FIX.parquet")
HRFILE  = host_parquet_path("HCMS.STAFF.RESIGN.parquet")
CISFILE = host_parquet_path("CIS.CUST.DAILY.parquet")

# output fixed-width files
OUT_NOTFOUND = csv_output_path("CIS.EMPLOYEE.RESIGN.NOTFOUND.txt")
OUT_FILE     = csv_output_path("CIS.EMPLOYEE.RESIGN.txt")

con = duckdb.connect()

# =============================================================================
# 2) LOAD CIS (CISFILE.CUSTDLY)
# =============================================================================
con.execute(f"""
    CREATE TABLE CIS AS
    SELECT *
    FROM read_parquet('{CISFILE}')
    WHERE (
         acctno BETWEEN 1000000000 AND 1999999999 OR
         acctno BETWEEN 3000000000 AND 3999999999 OR
         acctno BETWEEN 4000000000 AND 4999999999 OR
         acctno BETWEEN 5000000000 AND 5999999999 OR
         acctno BETWEEN 6000000000 AND 6999999999 OR
         acctno BETWEEN 7000000000 AND 7999999999
    )
""")

con.execute("CREATE INDEX idx_cis_custno ON CIS(custno)")


# =============================================================================
# 3) LOAD HR FILE (WITH VALIDATION)
# =============================================================================
con.execute(f"""
    CREATE TABLE HR_RAW AS
    SELECT *
    FROM read_parquet('{HRFILE}')
""")

# Validate HEADER date (DATAINDC=0, HEADERDATE must match DATE3)
hdr = con.execute("""
    SELECT headerdate 
    FROM HR_RAW 
    WHERE dataindc = '0'
""").fetchone()

if hdr and hdr[0] != DATE3:
    raise Exception(f"ABORT 77: HEADERDATE {hdr[0]} != REPORT DATE {DATE3}")

# Extract HR + OLD_IC
con.execute("""
    CREATE TABLE HR AS
    SELECT *
    FROM HR_RAW
    WHERE dataindc = '1' AND REGEXP_MATCH(alias, '^[0-9]{12}$')
""")

con.execute(f"""
    CREATE TABLE OLD_IC AS
    SELECT *, '003 IC NOT 12 DIGIT      ' AS remarks
    FROM HR_RAW
    WHERE dataindc = '1' AND NOT REGEXP_MATCH(alias, '^[0-9]{12}$')
""")

# Validate TRAILER (DATAINDC=9)
trailer = con.execute("""
    SELECT total_rec
    FROM HR_RAW WHERE dataindc='9'
""").fetchone()

count_hr = con.execute("SELECT COUNT(*) FROM HR").fetchone()[0]

if trailer and int(trailer[0]) != count_hr:
    raise Exception(f"ABORT 88: trailer count {trailer[0]} != HR count {count_hr}")


con.execute("CREATE INDEX idx_hr_alias ON HR(alias)")


# =============================================================================
# 4) LOAD ALS FILE
# =============================================================================
con.execute(f"""
    CREATE TABLE ALS AS
    SELECT *
    FROM read_parquet('{ALSFILE}')
    WHERE aliaskey = 'IC'
""")

con.execute("CREATE INDEX idx_als_alias ON ALS(alias)")


# =============================================================================
# 5) MATCH 1: HR + ALS → RESULT1, NO_IC
# =============================================================================
con.execute("""
    CREATE TABLE RESULT1 AS
    SELECT hr.*, als.custno AS custno
    FROM HR hr
    JOIN ALS als USING (alias)
""")

con.execute("""
    CREATE TABLE NO_IC AS
    SELECT hr.*, '001 STAFF IC NOT FOUND   ' AS remarks
    FROM HR hr
    LEFT JOIN ALS als USING (alias)
    WHERE als.alias IS NULL
""")


# =============================================================================
# 6) MATCH 2: RESULT1 + CIS → MATCH2, NO_ACCT
# =============================================================================
con.execute("""
    CREATE TABLE MATCH2 AS
    SELECT r.*, c.custname, c.acctcode, c.acctnoc, c.prisec
    FROM RESULT1 r
    JOIN CIS c USING (custno)
""")

con.execute("""
    CREATE TABLE NO_ACCT AS
    SELECT r.*, '002 CIS WITH NO ACCOUNT  ' AS remarks
    FROM RESULT1 r
    LEFT JOIN CIS c USING (custno)
    WHERE c.custno IS NULL
""")


# =============================================================================
# 7) OUTPUT NOTFOUND FILE
# =============================================================================
notfound_df = con.execute("""
    SELECT remarks, orgid, staffid, alias, hrname, custno
    FROM NO_IC
    UNION ALL
    SELECT remarks, orgid, staffid, alias, hrname, custno
    FROM NO_ACCT
    UNION ALL
    SELECT remarks, orgid, staffid, alias, hrname, custno
    FROM OLD_IC
    ORDER BY remarks, staffid
""").fetchdf()

with open(OUT_NOTFOUND, "w") as f:
    for row in notfound_df.itertuples():
        f.write(
            f"{row.remarks:<25}"
            f"{row.orgid:<13}"
            f"{row.staffid:<9}"
            f"{row.alias:<15}"
            f"{row.hrname:<40}"
            f"{row.custno:<11}"
            "\n"
        )


# =============================================================================
# 8) OUTPUT OUTFILE
# =============================================================================
match2_df = con.execute("""
    SELECT staffid, custno, hrname, custname, aliaskey, alias,
           CASE WHEN prisec=901 THEN 'P'
                WHEN prisec=902 THEN 'S'
                ELSE '' END AS primsec,
           acctcode, acctnoc
    FROM MATCH2
    ORDER BY staffid
""").fetchdf()

with open(OUT_FILE, "w") as f:
    for row in match2_df.itertuples():
        f.write(
            f"{row.staffid:<10}"
            f"{row.custno:<11}"
            f"{row.hrname:<40}"
            f"{row.custname:<40}"
            f"{row.aliaskey:<3}"
            f"{row.alias:<15}"
            f"{row.primsec:<1}"
            f"{row.acctcode:<5}"
            f"{row.acctnoc:<20}"
            "\n"
        )

print("✓ Completed: NOTFOUND + OUTFILE generated.")
