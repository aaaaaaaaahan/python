import duckdb
from CIS_PY_READER import csv_output_path, get_hive_parquet
import datetime

# -----------------------------
# SET DATES
# -----------------------------
batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")

# -----------------------------
# CONNECT TO DUCKDB
# -----------------------------
con = duckdb.connect()

# -----------------------------
# GET INPUT FILES
# -----------------------------
eccris = get_hive_parquet('ECCRIS_BLANK_ADDR_POSTCODE')
ccris = get_hive_parquet('CIS_CCRIS_ERROR')

# -----------------------------
# PART 1: CONCAT 2 INPUT FILES
# -----------------------------
con.execute(f"""
CREATE TABLE ERRFILE AS
SELECT * FROM read_parquet('{eccris[0]}')
UNION ALL
SELECT * FROM read_parquet('{ccris[0]}')
""")

con.execute("""
CREATE TABLE ERRFILE_SORTED AS
SELECT *
FROM ERRFILE
ORDER BY BRANCH, CUSTNO, ACCTNOC;
""")

# -----------------------------
# PART 1: GENERATE DETAILED REPORT TXT
# -----------------------------
hkctrl_path = csv_output_path(f"CIS_HSEKEEP_CENTRAL_{report_date}").replace(".csv", ".txt")

error_codes = ['001','002','003','004','005','100']
error_desc_map = {
    '001': 'TOTAL UNKNOWN CITIZENSHIP',
    '002': 'TOTAL BLANK ID (INDV)',
    '003': 'TOTAL BLANK ID (ORG)',
    '004': 'TOTAL BLANK DATE OF BIRTH',
    '005': 'TOTAL BLANK DATE OF REG',
    '100': 'TOTAL INVALID POSTCODE'
}

# Fetch all records (tuple list)
records = con.execute("SELECT * FROM ERRFILE_SORTED").fetchall()

# Column mapping
col_index = {
    'BRANCH': 0,
    'ACCTCODE': 1,
    'ACCTNOC': 2,
    'PRIMSEC': 3,
    'CUSTNO': 4,
    'ERRORCODE': 5,
    'FIELDTYPE': 6,
    'FIELDVALUE': 7,
    'REMARKS': 8
}

with open(hkctrl_path, "w") as rpt_file:
    rpt_file.write(f"REPORT ID   : CIS HSEKEEP RPT{'':45}PUBLIC BANK BERHAD\n")
    rpt_file.write(f"PROGRAM ID  : CIHKCTRL{'':55}REPORT DATE : {day}/{month}/{year}\n")
    rpt_file.write("BRANCH      : ALL\n")
    rpt_file.write("MISSING FIELDS DETECTED IN CIS SYSTEM FOR DATA SCRUBBING\n")
    rpt_file.write("="*56 + "\n")
    rpt_file.write(f"{'ACCOUNT':<23}{'CUSTNO':<12}{'FIELD TYPE':<20}{'FIELD VALUE':<30}{'REMARKS':<40}\n")
    rpt_file.write(f"{'='*7:<7}{'='*6:<6}{'='*10:<10}{'='*11:<11}{'='*6:<6}\n")

    branch = None
    brcust = 0
    grcust = 0
    err_counts = {code:0 for code in error_codes}

    for rec in records:
        rec_branch = rec[col_index['BRANCH']]
        if branch != rec_branch:
            branch = rec_branch
            brcust = 0

        rpt_file.write(
            f"{rec[col_index['ACCTCODE']]:<5}"
            f"{rec[col_index['ACCTNOC']]:<20}"
            f"{rec[col_index['CUSTNO']]:<11}"
            f"{rec[col_index['FIELDTYPE']]:<20}"
            f"{rec[col_index['FIELDVALUE']]:<30}"
            f"{rec[col_index['REMARKS']]:<40}\n"
        )

        code = rec[col_index['ERRORCODE']]
        if code in err_counts:
            err_counts[code] += 1
        brcust += 1
        grcust += 1

    for code, count in err_counts.items():
        if count != 0:
            rpt_file.write(f"{error_desc_map[code]:<45}{count:>9}\n")
    rpt_file.write(f"{'TOTAL ERRORS':<45}{brcust:>9}\n")
    rpt_file.write(f"GRAND TOTAL OF ALL BRANCHES = {grcust}\n")

# -----------------------------
# PART 2: GENERATE SUMMARY (TOTLIST & SUMLIST)
# -----------------------------
con.execute("""
CREATE TABLE SUM1 AS
SELECT *,
CASE 
    WHEN ERRORCODE='001' THEN 'EMPTY CITIZENSHIP'
    WHEN ERRORCODE='002' THEN 'EMPTY INDIVIDUAL ID'
    WHEN ERRORCODE='003' THEN 'EMPTY ORGANISATION ID'
    WHEN ERRORCODE='004' THEN 'EMPTY DATE OF BIRTH'
    WHEN ERRORCODE='005' THEN 'EMPTY DATE OF REGISTRATION'
    WHEN ERRORCODE='100' THEN 'EMPTY POSTCODE'
END AS ERRORDESC
FROM ERRFILE_SORTED;
""")

# TOTLIST: summary per error code
totlist = con.execute("""
SELECT ERRORCODE, ERRORDESC, COUNT(*) AS ERRORTOTAL
FROM SUM1
GROUP BY ERRORCODE, ERRORDESC
ORDER BY ERRORCODE;
""").fetchall()

# SUMLIST: summary per branch
sumlist = con.execute("""
SELECT BRANCH, COUNT(*) AS TOTAL_ERRORS
FROM SUM1
GROUP BY BRANCH
ORDER BY BRANCH;
""").fetchall()

# -----------------------------
# PART 3: CONCAT SUMMARY & EXPORT TXT
# -----------------------------
sum_path_txt = csv_output_path(f"CIS_HSEKEEP_CENTRAL_SUM_{report_date}").replace(".csv", ".txt")

with open(sum_path_txt, "w") as f:
    f.write(f"REPORT ID   : CIS HSEKEEP SUM{'':40}PUBLIC BANK BERHAD\n")
    f.write(f"PROGRAM ID  : CIHKCTRL{'':55}REPORT DATE : {day}/{month}/{year}\n")
    f.write("DATA SCRUBBING SUMMARY REPORT\n")
    f.write("=============================\n")
    f.write(f"{'ERROR DESCRIPTION/BRANCH':<47}{'TOTAL RECORDS':>12}\n")
    f.write("="*60 + "\n")

    # TOTLIST
    for rec in totlist:
        # rec = (ERRORCODE, ERRORDESC, ERRORTOTAL)
        f.write(f"{rec[1]:<47}{rec[2]:>12}\n")

    # SUMLIST
    for rec in sumlist:
        # rec = (BRANCH, TOTAL_ERRORS)
        f.write(f"{rec[0]:<47}{rec[1]:>12}\n")

print("TXT reports generated successfully using fetchall() (safe for 10M+ records).")
