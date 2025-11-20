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
    CUSTNO
FROM read_parquet('{infile1[0]}')
""")

con.execute(f"""
CREATE TABLE TEMP2 AS
SELECT
    CASE WHEN HRNAME <> CUSTNAME THEN '004 NAME DISCREPANCY     ' ELSE '' END AS REMARKS,
    STAFFID,
    CUSTNO,
    HRNAME,
    CUSTNAME,
    ALIASKEY,
    ALIAS,
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
    CUSTNO
FROM '{host_parquet_path("CUSTCODE_EMPL_ERR.parquet")}'
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
        rpt.write(f"BRANCH      : 0000000{'':15}EXCEPTION REPORT FOR RESIGNED STAFF\n")
        rpt.write(" "*46 + "===================================\n")
        rpt.write(f"{'STAFFID':<10}{'ALIAS':<16}{'HR NAME / CIS NAME':<40}"
                  f"{'REMARKS':<25}{'CUSTNO':<11}{'ACCTCODE':<6}{'ACCTNOC':<20}\n")
        rpt.write(f"{'='*9:<10}{'='*15:<16}{'='*40:<40}{'='*25:<25}"
                  f"{'='*11:<11}{'='*6:<6}{'='*20:<20}\n")

    new_page_header()

    for rec in records:
        # Adjust columns based on record length
        linecnt += 1
        grcust += 1

        staffid = rec[2] if len(rec) > 2 else ''
        alias = rec[3] if len(rec) > 3 else ''
        hrname = rec[4] if len(rec) > 4 else ''
        remarks = rec[0] if len(rec) > 0 else ''
        custno = rec[5] if len(rec) > 5 else ''
        acctcode = rec[8] if len(rec) > 8 else ''
        acctnoc = rec[9] if len(rec) > 9 else ''
        custname = rec[4] if len(rec) > 4 else ''

        rpt.write(f"{staffid:<10}{alias:<16}{hrname:<40}{remarks:<25}"
                  f"{custno:<11}{acctcode:<6}{acctnoc:<20}\n")

        # print CUSTNAME for NAME DISCREPANCY
        if remarks.strip() == '004 NAME DISCREPANCY':
            rpt.write(f"{'':27}{custname:<40}\n")
            linecnt += 1

        if linecnt >= 40:
            new_page_header()

    rpt.write(f"\nTOTAL RECORDS = {grcust}\n")

print("Detailed employee report TXT generated.")
