import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import date

# -----------------------------
# CONFIGURATION
# -----------------------------
insfile_path = 'EBANK.BRANCH.OFFICER.COMBINE.parquet'  # Input Parquet
txt_output_path = 'BRANCH_GENERAL_INFO_REPORT.txt'     # Output text file
parquet_output_path = 'BRANCH_GENERAL_INFO_REPORT.parquet'  # Optional Parquet output

# -----------------------------
# DATE VARIABLES
# -----------------------------
today = date.today()
DAY = f"{today.day:02d}"
MONTH = f"{today.month:02d}"
YEAR = str(today.year)[2:]  # last 2 digits

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
    FROM read_parquet('{insfile_path}')
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
    bankname = 'UNKNOWN BANK'
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

# -----------------------------
# WRITE TXT REPORT
# -----------------------------
with open(txt_output_path, 'w') as f:
    for line in report_lines:
        f.write(line + '\n')

# -----------------------------
# WRITE PARQUET (OPTIONAL)
# -----------------------------
table = pa.Table.from_pandas(con.execute("SELECT * FROM brfile ORDER BY BRNBR").fetch_df())
pq.write_table(table, parquet_output_path)

print(f"Report generated: {txt_output_path}")
