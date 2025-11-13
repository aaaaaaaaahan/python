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
# DETAILED REPORT TXT
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

records = con.execute("SELECT * FROM ERRFILE_SORTED").fetchall()

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

    branch = None
    brcust = 0
    grcust = 0
    err_counts = {code:0 for code in error_codes}
    linecnt = 0
    pagecnt = 0

    def print_page_header(branch_val):
        nonlocal pagecnt, linecnt
        pagecnt += 1
        linecnt = 9  # header uses 9 lines
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
            # If not first branch, print previous branch subtotals
            if branch is not None and brcust > 0:
                for code, count in err_counts.items():
                    if count != 0:
                        rpt_file.write(f"{error_desc_map[code]:<30} = {count:>5}\n")
                rpt_file.write(f"{'TOTAL ERRORS':<30} = {brcust:>5}\n\n")
                # reset branch counts
                err_counts = {code:0 for code in error_codes}
                brcust = 0
            branch = rec_branch
            print_page_header(branch)

        # print record
        rpt_file.write(
            f"{rec[col_index['ACCTCODE']]:<23}"
            f"{rec[col_index['CUSTNO']]:<12}"
            f"{rec[col_index['FIELDTYPE']]:<20}"
            f"{rec[col_index['FIELDVALUE']]:<30}"
            f"{rec[col_index['REMARKS']]:<40}\n"
        )

        # update counters
        code = rec[col_index['ERRORCODE']]
        if code in err_counts:
            err_counts[code] += 1
        brcust += 1
        grcust += 1
        linecnt += 1

        # simulate new page if more than 52 lines
        if linecnt >= 52:
            print_page_header(branch)

    # print last branch subtotal
    if brcust > 0:
        for code, count in err_counts.items():
            if count != 0:
                rpt_file.write(f"{error_desc_map[code]:<30} = {count:>5}\n")
        rpt_file.write(f"{'TOTAL ERRORS':<30} = {brcust:>5}\n\n")

    # grand total
    rpt_file.write(f"GRAND TOTAL OF ALL BRANCHES = {grcust}\n")

print("Detailed TXT report generated successfully!")
