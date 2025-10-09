import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pc
import datetime
from pathlib import Path

# ============================================================
# SETUP SECTION
# ============================================================

batch_date = datetime.date.today() - datetime.timedelta(days=1)
year1, month1, day1 = batch_date.year, batch_date.month, batch_date.day

# Define file paths
input_file = "/host/cis/parquet/UNLOAD_CIREPTTT_FB.parquet"
output_folder = Path("/host/cis/output/cisumrep")
output_folder.mkdir(parents=True, exist_ok=True)

output_parquet = output_folder / f"cisumrep_{year1}{month1:02}{day1:02}.parquet"
output_csv = output_folder / f"cisumrep_{year1}{month1:02}{day1:02}.csv"

# ============================================================
# DUCKDB PROCESSING
# ============================================================

con = duckdb.connect()

# Load Parquet file into DuckDB
con.execute(f"""
    CREATE TABLE reptfile AS
    SELECT * FROM read_parquet('{input_file}');
""")

# ============================================================
# FILTERING & SPLITTING INTO HRCDATA AND XHRCDATA
# ============================================================
# Delete certain deposit records and compute CNTVIEW
con.execute("""
    CREATE TABLE base AS
    SELECT *,
        CASE WHEN (REVIEWED = 'Y' OR VIEWED = 'Y') THEN 1 ELSE 0 END AS CNTVIEW
    FROM reptfile
    WHERE NOT (
        RECTYPE = 'DPST' AND APPLCODE = 'DP' AND REMARK3 IN (
            '126','127','128','129','140','141','142','143','144','145','146','147',
            '148','149','171','172','173'
        )
    );
""")

# Split HRC and Non-HRC datasets
con.execute("""
    CREATE TABLE HRCDATA AS
    SELECT * FROM base
    WHERE RECTYPE = 'DPST' AND REMARK1 <> '' AND REMARK2 <> '';
""")

con.execute("""
    CREATE TABLE XHRCDATA AS
    SELECT * FROM base
    WHERE NOT (RECTYPE = 'DPST' AND REMARK1 <> '' AND REMARK2 <> '');
""")

# ============================================================
# HRC SUMMARY
# ============================================================
con.execute("""
    CREATE TABLE TEMP AS
    SELECT BANKNO, RECTYPE, REPORTDATE, REPORTNO, BRANCHNO,
           COUNT(*) AS TOTAL,
           SUM(CNTVIEW) AS CNTVIEW
    FROM HRCDATA
    GROUP BY BANKNO, RECTYPE, REPORTDATE, REPORTNO, BRANCHNO;
""")

# ============================================================
# NON-HRC SUMMARY
# ============================================================
con.execute("""
    CREATE TABLE TEMP1 AS
    SELECT BANKNO, RECTYPE, REPORTDATE, REPORTNO, BRANCHNO,
           COUNT(*) AS TOTAL,
           SUM(CNTVIEW) AS CNTVIEW
    FROM XHRCDATA
    GROUP BY BANKNO, RECTYPE, REPORTDATE, REPORTNO, BRANCHNO;
""")

# ============================================================
# HRC RECORDS (PTAGE < 100)
# ============================================================
con.execute("""
    CREATE TABLE HRCRECS AS
    SELECT *, 'Y' AS ISHRC, (CNTVIEW * 100.0 / TOTAL) AS PTAGE
    FROM TEMP
    WHERE (CNTVIEW * 100.0 / TOTAL) < 100;
""")

# ============================================================
# NON-HRC RECORDS (PTAGE < 10)
# ============================================================
con.execute("""
    CREATE TABLE NONHRCRECS AS
    SELECT *, 'N' AS ISHRC, (CNTVIEW * 100.0 / TOTAL) AS PTAGE
    FROM TEMP1
    WHERE (CNTVIEW * 100.0 / TOTAL) < 10;
""")

# ============================================================
# MERGED RECORDS
# ============================================================
con.execute("""
    CREATE TABLE MRGRECORDS AS
    SELECT *,
           substr(REPORTDATE, 7, 4) AS YYYY,
           substr(REPORTDATE, 4, 2) AS MM,
           substr(REPORTDATE, 1, 2) AS DD
    FROM (
        SELECT * FROM HRCRECS
        UNION ALL
        SELECT * FROM NONHRCRECS
    );
""")

# ============================================================
# FINAL OUTPUT
# ============================================================
table = con.execute("""
    SELECT 
        BRANCHNO,
        REPORTDATE,
        BANKNO,
        REPORTNO,
        RECTYPE,
        ISHRC,
        TOTAL,
        CNTVIEW,
        ROUND(PTAGE, 2) AS PTAGE
    FROM MRGRECORDS
    ORDER BY BRANCHNO, YYYY, MM, DD, BANKNO, REPORTNO, RECTYPE;
""").arrow()

# ============================================================
# SAVE TO PARQUET & CSV (using PyArrow)
# ============================================================
pq.write_table(table, output_parquet)
pc.write_csv(table, output_csv)

print("✅ CISUMREP process completed successfully!")
print(f"Output Parquet: {output_parquet}")
print(f"Output CSV: {output_csv}")
