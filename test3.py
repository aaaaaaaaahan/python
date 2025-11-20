import duckdb
from CIS_PY_READER import host_parquet_path, csv_output_path, get_hive_parquet
import datetime

today = datetime.date.today()
batch_date = today - datetime.timedelta(days=1)
report_date_str = batch_date.strftime("%d-%m-%Y")

# -----------------------------
# CONNECT DUCKDB
# -----------------------------
con = duckdb.connect()
infile1 = get_hive_parquet('CIS_EMPLOYEE_RESIGN_NOTFOUND')
infile2 = get_hive_parquet('CIS_EMPLOYEE_RESIGN')

# -----------------------------
# CREATE TEMP TABLES
# -----------------------------
con.execute(f"""
CREATE TABLE TEMP1 AS
SELECT
    REMARKS,
    ORGID,
    STAFFID,
    ALIAS,
    HRNAME,
    CUSTNO,

    -- FIXED: pad missing fields to match TEMP2
    NULL AS CUSTNAME,
    NULL AS ALIASKEY,
    NULL AS PRIMSEC,
    NULL AS ACCTCODE,
    NULL AS ACCTNOC
FROM read_parquet('{infile1[0]}')
""")

con.execute(f"""
CREATE TABLE TEMP2 AS
SELECT
    CASE WHEN HRNAME <> CUSTNAME THEN '004 NAME DISCREPANCY     ' ELSE '' END AS REMARKS,
    NULL AS ORGID,
    STAFFID,
    ALIAS,
    HRNAME,
    CUSTNO,
    CUSTNAME,
    ALIASKEY,
    PRIMSEC,
    ACCTCODE,
    ACCTNOC
FROM read_parquet('{infile2[0]}')
WHERE HRNAME <> CUSTNAME
""")

con.execute(f"""
CREATE TABLE TEMP3 AS
SELECT
    '005 FAILED TO REMOVE TAG ' AS REMARKS,
    NULL AS ORGID,
    NULL AS STAFFID,
    NULL AS ALIAS,
    NULL AS HRNAME,
    CUSTNO,
    NULL AS CUSTNAME,
    NULL AS ALIASKEY,
    NULL AS PRIMSEC,
    NULL AS ACCTCODE,
    NULL AS ACCTNOC
FROM read_parquet('{host_parquet_path("CUSTCODE_EMPL_ERR.parquet")}')
""")

# -----------------------------
# COMBINE ALL RECORDS
# -----------------------------
con.execute("""
CREATE TABLE ALLREC AS
SELECT * FROM TEMP1
UNION ALL
SELECT * FROM TEMP2
UNION ALL
SELECT * FROM TEMP3
""")

# REMOVE DUPLICATES
con.execute("""
CREATE TABLE ALLREC_NODUP AS
SELECT DISTINCT *
FROM ALLREC
""")

# -----------------------------
# FETCH ALL RECORDS
# -----------------------------
records = con.execute("SELECT * FROM ALLREC_NODUP").fetchall()

# -----------------------------
# OUTPUT DETAILED TXT REPORT
# -----------------------------
report_path = csv_output_path(f"CIS_EMPLOYEE_REPORT_{report_date_str}").replace(".csv", ".txt")

with open(report_path, "w") as rpt:
    pagecnt = 0
    linecnt = 0
    grcust = 0
    current_remarks = None

    def new_page_header():
        global pagecnt, linecnt
        pagecnt += 1
        linecnt = 9
        rpt.write(f"REPORT ID   : HRD RESIGN{'':25}PUBLIC BANK BERHAD{'':5}PAGE : {pagecnt}\n")
        rpt.write(f"PROGRAM ID  : CIRESIRP{'':65}REPORT DATE : {report_date_str}\n")
        rpt.write(f"BRANCH      : 0000000{'':25}EXCEPTION REPORT FOR RESIGNED STAFF\n")
        rpt.write(" "*46 + "===================================\n")
        rpt.write(f"{'STAFFID':<10}{'ALIAS':<16}{'HR NAME / CIS NAME':<41}"
                  f"{'REMARKS':<26}{'CUSTNO':<12}{'ACCTCODE':<9}{'ACCTNOC':<20}\n")
        rpt.write(f"{'='*9:<10}{'='*15:<16}{'='*40:<41}{'='*25:<26}"
                  f"{'='*11:<12}{'='*8:<9}{'='*20:<20}\n")

    new_page_header()

    for rec in records:
        linecnt += 1
        grcust += 1

        # same your variable mapping
        remarks = rec[0]
        staffid = rec[2]
        alias = rec[3]
        hrname = rec[4]
        custno = rec[5]
        custname = rec[6]
        acctcode = rec[9]
        acctnoc = rec[10]

        rpt.write(f"{(staffid or ''):<10}{(alias or ''):<16}{(hrname or ''):<41}{(remarks or ''):<26}"
                  f"{(custno or ''):<12}{(acctcode or ''):<9}{(acctnoc or ''):<20}\n")

        # NAME DISCREPANCY
        if remarks.strip() == '004 NAME DISCREPANCY':
            rpt.write(f"{'':27}{(custname or ''):<40}\n")
            linecnt += 1

        if linecnt >= 40:
            new_page_header()

    rpt.write(f"\nTOTAL RECORDS = {grcust}\n")
