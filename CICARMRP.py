import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import datetime

# ==============================
# Part 1: Setup Dates (CTRLDATE)
# ==============================
ctrl_tbl = duckdb.read_parquet("SRSCTRL1.parquet").to_df()

# Assume columns SRSYY, SRSMM, SRSDD exist in parquet
yy, mm, dd = int(ctrl_tbl.loc[0,"SRSYY"]), int(ctrl_tbl.loc[0,"SRSMM"]), int(ctrl_tbl.loc[0,"SRSDD"])
report_date = datetime.date(yy, mm, dd).strftime("%d-%m-%Y")

# ==============================
# Part 2: Load Input Data
# ==============================
con = duckdb.connect()
con.execute("INSTALL parquet; LOAD parquet;")

con.execute("""
    CREATE OR REPLACE TABLE RDATA AS
    SELECT * FROM read_parquet('UNOFAC_CARD_MTHLY.parquet')
""")

# ==============================
# Part 3: Sorting (same as PROC SORT)
# ==============================
con.execute("""
    CREATE OR REPLACE TABLE RDATA_SORT AS
    SELECT *
    FROM RDATA
    ORDER BY BRANCH, MATCHBY, ACCTNO, APPLCODE
""")

# ==============================
# Part 4: Process Report
# ==============================
df = con.execute("SELECT * FROM RDATA_SORT").df()

lines = []
pagecnt = 0
linecnt = 0

# Grand totals
ind_total = 0
org_total = 0
grand_total = 0

def new_page(branch):
    global pagecnt, linecnt
    pagecnt += 1
    linecnt = 0
    lines.append(f"REPORT NO : UN/OFAC/CARD/ALL/ACCT".ljust(50) +
                 f"P U B L I C  B A N K  B H D".ljust(60) +
                 f"PAGE : {pagecnt}")
    lines.append(f"PROGRAM ID: CICARMRP".ljust(50) +
                 f"MONTHLY UNICARD EXCEPTION REPORT FOR OFAC & UN LIST".ljust(60) +
                 f"REPORT DATE: {report_date}")
    lines.append(f"BRANCH NO : {branch}")
    lines.append("NAME".ljust(45) + "ID NO.".ljust(24) + "DATE OPEN".ljust(14) +
                 "CARD TYPE".ljust(12) + "CARD NO.".ljust(22) + "REMARKS.")
    lines.append("="*120)
    lines.append("UN/OFAC NAME".ljust(45) + "UN/OFAC IC NO".ljust(18) + "UN/OFAC OTHER ID")
    lines.append("-"*120)
    linecnt += 7

current_branch = None
current_matchby = None

for _, row in df.iterrows():
    if linecnt >= 55 or row.BRANCH != current_branch:
        new_page(row.BRANCH)

    if row.MATCHBY != current_matchby:
        if row.MATCHBY.strip() == "ICD":
            lines.append("Customer with NAME AND ID fully matched")
            lines.append("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        elif row.MATCHBY.strip() == "NAME":
            lines.append("Customer NAME matched but ID unmatched/unavailable OR customer ID matched but NAME unmatched")
            lines.append("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        current_matchby = row.MATCHBY
        linecnt += 2

    # Record printing logic
    if not row.ALIAS.strip() or (row.TAXID.strip() and row.OTHERID.strip() == row.TAXID.strip()):
        line = f"{row.PRIMNAME:<40}{row.TAXID:<20}{row.ACTOPND:<12}{row.APPLCODE:<8}{row.ACCTNO:<20}{row.DISPRMK:<8}"
    else:
        line = f"{row.PRIMNAME:<40}{row.ALIAS:<20}{row.ACTOPND:<12}{row.APPLCODE:<8}{row.ACCTNO:<20}{row.DISPRMK:<8}"
    lines.append(line)
    linecnt += 1

    if row.MATCHBY.strip() == "ICD":
        lines.append(f"    {row.NAME:<40}{row.NEWIC:<16}{row.OTHERID:<40}{row.SOURCE:<10}")
        linecnt += 1

    # Totals
    if row.INDORG.strip() == "I":
        ind_total += 1
    elif row.INDORG.strip() == "O":
        org_total += 1
    grand_total += 1

# ==============================
# Part 5: Totals
# ==============================
lines.append("")
lines.append(f"TOTAL NUMBER OF RECORDS (INDIVIDUAL)   : {ind_total}")
lines.append(f"TOTAL NUMBER OF RECORDS (ORGANISATION) : {org_total}")
lines.append(f"GRAND TOTAL OF RECORD TO BE MATCH      : {grand_total}")

# ==============================
# Part 6: Output with PyArrow
# ==============================
report_text = "\n".join(lines)
report_table = pa.table({"REPORT": [report_text]})

pq.write_table(report_table, "UNOFAC_CARD_MTLRPT.parquet")

# If CSV format is also needed:
csv.write_csv(report_table, "UNOFAC_CARD_MTLRPT.csv")
