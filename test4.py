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
# COMBINE + REMOVE DUP + SORT (MATCH SAS PROC SORT)
# -----------------------------
con.execute("""
CREATE TABLE ALLREC_NODUP AS
SELECT DISTINCT *
FROM (
    SELECT * FROM TEMP1
    UNION ALL
    SELECT * FROM TEMP2
    UNION ALL
    SELECT * FROM TEMP3
)
ORDER BY REMARKS, STAFFID, ALIAS, ACCTNOC
""")

# FETCH
records = con.execute("SELECT * FROM ALLREC_NODUP").fetchall()

# -----------------------------
# OUTPUT
# -----------------------------
report_path = csv_output_path(f"CIS_EMPLOYEE_REPORT_{report_date_str}").replace(".csv", ".txt")

with open(report_path, "w") as rpt:

    # IF NO RECORDS → print “NO RECORDS TODAY”
    if len(records) == 0:
        rpt.write(" " * 44 + "**********************************\n")
        rpt.write(" " * 44 + "*                                *\n")
        rpt.write(" " * 44 + "*       NO RECORDS TODAY         *\n")
        rpt.write(" " * 44 + "*                                *\n")
        rpt.write(" " * 44 + "**********************************\n")
        exit()

    pagecnt = 0
    linecnt = 0

    def new_page_header():
        nonlocal pagecnt, linecnt
        pagecnt += 1
        linecnt = 9

        rpt.write(f"REPORT ID   : HRD RESIGN{'':25}PUBLIC BANK BERHAD{'':5}PAGE        : {pagecnt:4}\n")
        rpt.write(f"PROGRAM ID  : CIRESIRP{'':65}REPORT DATE : {report_date_str}\n")
        rpt.write(f"BRANCH      : 0000000{'':25}EXCEPTION REPORT FOR RESIGNED STAFF\n")
        rpt.write(" " * 46 + "===================================\n")
        rpt.write(
            f"{'STAFFID':<9} {'ALIAS':<12} {'HR NAME / CIS NAME':<40}"
            f"{'REMARKS':<25}{'CUSTNO':<11}{'ACCTCODE':<5}{'ACCTNOC':<20}\n"
        )
        rpt.write(
            f"{'='*9:<9} {'='*15:<15} {'='*40:<40}"
            f"{'='*25:<25}{'='*11:<11}{'='*5:<5}{'='*20:<20}\n"
        )

    # Start first page
    new_page_header()

    current_remarks = None
    grcust = 0

    # -----------------------------
    # MAIN LOOP
    # -----------------------------
    for rec in records:

        remarks = rec[0]
        staffid = rec[2] or ""
        alias = rec[3] or ""
        hrname = rec[4] or ""
        custno = rec[5] or ""
        custname = rec[6] or ""
        acctcode = rec[9] or ""
        acctnoc = rec[10] or ""

        # If new remarks group → print group total & new page
        if current_remarks is not None and remarks != current_remarks:
            rpt.write(f"{'TOTAL RECORDS = ':<23}{grcust:7}\n\n")
            grcust = 0
            new_page_header()

        current_remarks = remarks
        grcust += 1
        linecnt += 1

        # FIXED WIDTH MATCHING SAS
        rpt.write(
            f"{staffid:<9} {alias:<12} {hrname:<40}"
            f"{remarks:<25}{custno:<11}{acctcode:<5}{acctnoc:<20}\n"
        )

        # NAME DISCREPANCY extra line
        if remarks.strip() == "004 NAME DISCREPANCY":
            rpt.write(f"{'':26}{custname:<40}\n")
            linecnt += 1

        if linecnt >= 40:
            new_page_header()

    # FINAL GROUP TOTAL
    rpt.write(f"{'TOTAL RECORDS = ':<23}{grcust:7}\n")
