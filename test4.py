import duckdb
import datetime
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet

# -----------------------------
# DATE / REPORT PARAMETERS
# -----------------------------
batch_date = datetime.date.today() - datetime.timedelta(days=1)
DAY, MONTH, YEAR = batch_date.day, batch_date.month, batch_date.year
report_date_str = batch_date.strftime("%d-%m-%Y")
lines_per_page = 52

# -----------------------------
# CONNECT DUCKDB AND LOAD DATA
# -----------------------------
con = duckdb.connect()
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
# REPORT OUTPUT
# -----------------------------
report_path = csv_output_path(f"BRANCH_GENERAL_INFO_{report_date_str}").replace(".csv", ".txt")
linecnt = 0
pagecnt = 0
brcnt = 0
report_lines = []

# -----------------------------
# PAGE HEADER FUNCTION
# -----------------------------
def new_page_header(banknbr):
    global pagecnt, linecnt
    pagecnt += 1
    linecnt = 8  # header lines count

    bankname = 'PUBLIC BANK BERHAD' if banknbr == 'B' else 'PUBLIC FINANCE BERHAD'

    header = [
        f"REPORT ID   : BNKCTL/BR/FILE/RPTS{'':55}{bankname:<20}PAGE        : {pagecnt:4}",
        f"PROGRAM ID  : CIBRRPTB{'':70}REPORT DATE : {DAY}/{MONTH}/{YEAR}",
        f"{'':52}BRANCH GENERAL INFO REPORT",
        f"{'':52}==========================",
        "",
        f"  BR NBR  ABBRV  NAME                 ADDRESS                            PHONE       STATE CODE",
        f"  ------  -----  ----                 -------                            -----       ----------"
    ]
    report_lines.extend(header)

# -----------------------------
# GENERATE REPORT
# -----------------------------
if len(branches) == 0:
    new_page_header('B')
    report_lines.append("\n" + " " * 20 + "******** NO RECORDS TODAY ********\n")
else:
    for row in branches:
        banknbr, brnbr, brabbrv, brname, addr1, addr2, addr3, phone, brstcode, brrps = row

        # PAGE BREAK
        if linecnt == 0 or linecnt >= lines_per_page:
            new_page_header(banknbr)

        # BRANCH DATA (SAS formatting: 3 lines per branch)
        report_lines.append(f"{brnbr:>7}  {brabbrv:<3}  {brname:<20}  {addr1:<35}  {phone:<11}  {brstcode:>3}")
        report_lines.append(f"{'':45}{addr2:<35}")
        report_lines.append(f"{'':45}{addr3:<35}")

        linecnt += 6  # 3 lines + 3 spacing (like SAS)
        brcnt += 1

    # FINAL TOTAL
    report_lines.append(f"\nTOTAL NUMBER OF BRANCH = {brcnt:4d}")

# -----------------------------
# WRITE TO TXT
# -----------------------------
with open(report_path, "w") as rpt:
    for line in report_lines:
        rpt.write(line + "\n")
