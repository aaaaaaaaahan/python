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
# CONCAT INPUT FILES
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
# COLUMN INDEX
# -----------------------------
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

error_codes = ['001','002','003','004','005','100']
error_desc_map = {
    '001': 'TOTAL UNKNOWN CITIZENSHIP',
    '002': 'TOTAL BLANK ID (INDV)',
    '003': 'TOTAL BLANK ID (ORG)',
    '004': 'TOTAL BLANK DATE OF BIRTH',
    '005': 'TOTAL BLANK DATE OF REG',
    '100': 'TOTAL INVALID POSTCODE'
}

# -----------------------------
# FETCH ALL RECORDS
# -----------------------------
records = con.execute("SELECT * FROM ERRFILE_SORTED").fetchall()

# -----------------------------
# PART 1: DETAILED CENTRAL REPORT
# -----------------------------
detailed_path = csv_output_path(f"CIS_HSEKEEP_CENTRAL_{report_date}").replace(".csv", ".txt")

with open(detailed_path, "w") as rpt_file:

    branch = None
    brcust = 0
    grcust = 0
    err_counts = {code:0 for code in error_codes}
    linecnt = 0
    pagecnt = 0

    def print_page_header(branch_val):
        nonlocal pagecnt, linecnt
        pagecnt += 1
        linecnt = 9  # header lines
        rpt_file.write(f"REPORT ID   : CIS HSEKEEP RPT{'':23}PUBLIC BANK BERHAD\n")
        rpt_file.write(f"PROGRAM ID  : CIHKCTRL{'':55}REPORT DATE : {day}/{month}/{year}\n")
        rpt_file.write(f"BRANCH      : {branch_val}\n")
        rpt_file.write("MISSING FIELDS DETECTED IN CIS SYSTEM FOR DATA SCRUBBING\n")
        rpt_file.write("="*56 + "\n")
        rpt_file.write(f"{'ACCOUNT':<23}{'CUSTNO':<12}{'FIELD TYPE':<20}{'FIELD VALUE':<30}{'REMARKS':<40}\n")
        rpt_file.write(f"{'='*7:<7}{'='*6:<6}{'='*10:<10}{'='*11:<11}{'='*6:<6}\n")

    for rec in records:
        rec_branch = rec[col_index['BRANCH']]
        if branch != rec_branch:
            if branch is not None and brcust > 0:
                for code, count in err_counts.items():
                    if count != 0:
                        rpt_file.write(f"{error_desc_map[code]:<45}{count:>9}\n")
                rpt_file.write(f"{'TOTAL ERRORS':<45}{brcust:>9}\n\n")
                err_counts = {code:0 for code in error_codes}
                brcust = 0
            branch = rec_branch
            print_page_header(branch)

        rpt_file.write(
            f"{rec[col_index['ACCTCODE']]:<23}"
            f"{rec[col_index['CUSTNO']]:<12}"
            f"{rec[col_index['FIELDTYPE']]:<20}"
            f"{rec[col_index['FIELDVALUE']]:<30}"
            f"{rec[col_index['REMARKS']]:<40}\n"
        )

        code = rec[col_index['ERRORCODE']]
        if code in err_counts:
            err_counts[code] += 1
        brcust += 1
        grcust += 1
        linecnt += 1

        if linecnt >= 52:
            print_page_header(branch)

    if brcust > 0:
        for code, count in err_counts.items():
            if count != 0:
                rpt_file.write(f"{error_desc_map[code]:<45}{count:>9}\n")
        rpt_file.write(f"{'TOTAL ERRORS':<45}{brcust:>9}\n\n")

    rpt_file.write(f"GRAND TOTAL OF ALL BRANCHES = {grcust}\n")

print("Detailed central report TXT generated.")

# -----------------------------
# PART 2: CENTRAL SUMMARY REPORT
# -----------------------------
summary_path = csv_output_path(f"CIS_HSEKEEP_CENTRAL_SUM_{report_date}").replace(".csv", ".txt")

# Prepare overview and branch summaries
overview = con.execute(f"""
SELECT ERRORCODE, 
       CASE 
           WHEN ERRORCODE='001' THEN 'EMPTY CITIZENSHIP'
           WHEN ERRORCODE='002' THEN 'EMPTY INDIVIDUAL ID'
           WHEN ERRORCODE='003' THEN 'EMPTY ORGANISATION ID'
           WHEN ERRORCODE='004' THEN 'EMPTY DATE OF BIRTH'
           WHEN ERRORCODE='005' THEN 'EMPTY DATE OF REGISTRATION'
           WHEN ERRORCODE='100' THEN 'EMPTY POSTCODE'
       END AS ERRORDESC,
       COUNT(*) AS ERRORTOTAL
FROM ERRFILE_SORTED
GROUP BY ERRORCODE
ORDER BY ERRORCODE
""").fetchall()

branch_summary = con.execute(f"""
SELECT BRANCH, ERRORCODE,
       CASE 
           WHEN ERRORCODE='001' THEN 'TOTAL UNKNOWN CITIZENSHIP'
           WHEN ERRORCODE='002' THEN 'TOTAL BLANK ID (INDV)'
           WHEN ERRORCODE='003' THEN 'TOTAL BLANK ID (ORG)'
           WHEN ERRORCODE='004' THEN 'TOTAL BLANK DATE OF BIRTH'
           WHEN ERRORCODE='005' THEN 'TOTAL BLANK DATE OF REG'
           WHEN ERRORCODE='100' THEN 'TOTAL INVALID POSTCODE'
       END AS ERRORDESC,
       COUNT(*) AS ERRORTOTAL
FROM ERRFILE_SORTED
GROUP BY BRANCH, ERRORCODE
ORDER BY BRANCH, ERRORCODE
""").fetchall()

# Aggregate branch totals
from collections import defaultdict

branch_totals = defaultdict(int)
for rec in branch_summary:
    branch_totals[rec[0]] += rec[3]

grand_total = sum(rec[2] for rec in overview)

# Write summary TXT
with open(summary_path, "w") as f:
    f.write(f"REPORT ID   : CIS HSEKEEP SUM{'':23}PUBLIC BANK BERHAD                         PAGE        :    1\n")
    f.write(f"PROGRAM ID  : CIHKCTRL{'':65}REPORT DATE : {day}/{month}/{year}\n")
    f.write(f"BRANCH      : 0000000                         DATA SCRUBBING SUMMARY REPORT    \n")
    f.write("                                              =============================\n\n")
    f.write("******    OVERVIEW    ******\n\n")
    f.write(f"{'ERROR DESCRIPTION':<40}{'TOTAL RECORDS':>20}\n")
    f.write(f"{'='*17:<40}{'='*12:>20}\n")
    for rec in overview:
        f.write(f"{rec[1]:<40}{rec[2]:>20}\n")
    f.write(f"{'GRAND TOTAL  =':<40}{grand_total:>20}\n\n")

    f.write("******    SUMMARY BY BRANCH AND ERROR TYPE      ******\n\n")
    f.write(f"{'BRANCH':<8}{'ERROR DESCRIPTION':<45}{'TOTAL RECORD':>15}\n")
    f.write(f"{'------':<8}{'-----------------':<45}{'------------':>15}\n")

    current_branch = None
    for rec in branch_summary:
        br, code, desc, count = rec
        if current_branch != br:
            if current_branch is not None:
                f.write(f"{current_branch:<8}{'TOTAL ERRORS':<45}{branch_totals[current_branch]:>15} **\n")
                f.write("-"*70 + "\n")
            current_branch = br
        f.write(f"{br:<8}{desc:<45}{count:>15}\n")
    if current_branch is not None:
        f.write(f"{current_branch:<8}{'TOTAL ERRORS':<45}{branch_totals[current_branch]:>15} **\n")

print("Central summary TXT generated successfully.")
