import duckdb
import datetime
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")
DAY = day
MONTH = month
YEAR = year

# -----------------------------
# CONNECT DUCKDB
# -----------------------------
con = duckdb.connect()

# -----------------------------
# LOAD DATA
# -----------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE brfile AS
    SELECT
        BANKNBR,
        CAST(BRNBR AS INTEGER) AS BRNBR,
        BRABBRV,
        BRNAME,
        BRADDRL1,
        BRADDRL2,
        BRADDRL3,
        BRPHONE,
        CAST(BRSTCODE AS INTEGER) AS BRSTCODE,
        BRRPS
    FROM '{host_parquet_path("EBANK_BRANCH_OFFICER_COMBINE.parquet")}'
""")

branches = con.execute("SELECT * FROM brfile ORDER BY BRNBR").fetchall()

# -----------------------------
# REPORT PARAMETERS
# -----------------------------
lines_per_page = 52
linecnt = 0
pagecnt = 0
brcnt = 0
report_lines = []

def print_page_header(banknbr, pagecnt):
    if banknbr == 'B':
        bankname = 'PUBLIC BANK BERHAD'
    elif banknbr == 'F':
        bankname = 'PUBLIC FINANCE BERHAD'

    header = [
        f"REPORT ID   : BNKCTL/BR/FILE/RPTS{'':55}{bankname:<20}PAGE        : {pagecnt:4}",
        f"PROGRAM ID  : CIBRRPTB{'':70}REPORT DATE : {DAY}/{MONTH}/{YEAR}",
        f"{'':52}BRANCH GENERAL INFO REPORT",
        f"{'':52}==========================",
        "",
        f"  BR NBR  ABBRV  NAME                 ADDRESS                            PHONE       STATE CODE",
        f"  ------  -----  ----                 -------                            -----       ----------"
    ]
    return header

# -----------------------------
# GENERATE REPORT
# -----------------------------
for row in branches:
    banknbr, brnbr, brabbrv, brname, addr1, addr2, addr3, phone, brstcode, brrps = row

    # Check page break
    if linecnt == 0 or linecnt >= lines_per_page:
        pagecnt += 1
        report_lines.extend(print_page_header(banknbr, pagecnt))
        linecnt = 8  # header lines

    # Branch info (exact SAS formatting)
    report_lines.append(f"{brnbr:>7}  {brabbrv:<3}  {brname:<20}  {addr1:<35}  {phone:<11}  {brstcode:>3}")
    report_lines.append(f"{'':45}{addr2:<35}")
    report_lines.append(f"{'':45}{addr3:<35}")

    linecnt += 6  # SAS adds 6 per branch (3 lines plus spacing)
    brcnt += 1

# Add total at end
report_lines.append(f"\nTOTAL NUMBER OF BRANCH = {brcnt:4d}")
