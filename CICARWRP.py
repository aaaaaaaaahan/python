import duckdb
import pyarrow as pa
import pyarrow.csv as csv
from datetime import date

# ================================================================
# Part 1: Setup (DuckDB connection, date handling)
# ================================================================
con = duckdb.connect()

# --- Control file (equivalent to CTRLDATE dataset) ---
ctrl = con.execute("""
    SELECT
        SRSYY,
        SRSMM,
        SRSDD
    FROM 'SRSCTRL1.parquet'
""").fetchdf()

SRSYY, SRSMM, SRSDD = int(ctrl.iloc[0].SRSYY), int(ctrl.iloc[0].SRSMM), int(ctrl.iloc[0].SRSDD)
rept_date = date(SRSYY, SRSMM, SRSDD).strftime("%d-%m-%Y")

# ================================================================
# Part 2: Read Input parquet (explicit field list)
# ================================================================
rdata = con.execute("""
    SELECT
        BANKNO,
        ACTOPND,
        BRANCH,
        APPLCODE,
        ACCTNO,
        INDORG,
        CUSTNAME,
        ALIASKEY,
        ALIAS,
        TAXID,
        IO,
        NAME,
        NEWIC,
        OTHERID,
        SOURCE,
        LSTMNTDATE,
        REMARK1,
        REMARK2,
        REMARK3,
        REMARK4,
        REMARK5,
        REMARK6,
        REMARK7,
        REMARK8,
        REMARK9,
        REMARK10,
        CTDATE,
        MATCHBY,
        DISPRMK
    FROM 'UNOFAC_CARD_WEEKLY.parquet'
""").arrow()

# ================================================================
# Part 3: Handle Empty Report
# ================================================================
if rdata.num_rows == 0:
    empty_text = [
        "**********************************",
        "*                                *",
        "*         EMPTY REPORT           *",
        "*                                *",
        "**********************************"
    ]
    tbl = pa.table({"REPORT": pa.array(empty_text)})
    with pa.OSFile("UNOFAC_CARD_WKLRPT.csv", "wb") as sink:
        csv.write_csv(tbl, sink)
    exit()

# ================================================================
# Part 4: Sorting (like PROC SORT BY BRANCH MATCHBY ACCTNO APPLCODE)
# ================================================================
rdata = rdata.sort_by([
    ("BRANCH", "ascending"),
    ("MATCHBY", "ascending"),
    ("ACCTNO", "ascending"),
    ("APPLCODE", "ascending")
])

# ================================================================
# Part 5: Generate Report Rows
# ================================================================
rows = []
linecnt, pagecnt = 0, 0
grdtot_ind, grdtot_org, grdtot_all = 0, 0, 0

def newpage(branch):
    """Simulate SAS NEWPAGE section"""
    global pagecnt, linecnt
    pagecnt += 1
    linecnt = 0
    return [
        f"REPORT NO : UN/OFAC/CARD/NEW/ACCT   P U B L I C  B A N K  B H D     PAGE : {pagecnt}",
        f"PROGRAM ID: CICARWRP                WEEKLY UNICARD EXCEPTION REPORT FOR OFAC & UN LIST   REPORT DATE: {rept_date}",
        f"BRANCH NO : {branch}",
        "NAME                                     ID NO.     DATE OPEN  CARD TYPE  CARD NO.            REMARKS.",
        "======================================== ==================== ========== ========== ================ ========",
        "UN/OFAC NAME                            UN/OFAC IC NO         UN/OFAC OTHER ID",
        "------------                            -------------         ----------------"
    ]

branch_prev, matchby_prev, acct_prev = None, None, None

for rec in rdata.to_pylist():
    # New branch or page overflow
    if branch_prev is None or rec["BRANCH"] != branch_prev or linecnt >= 55:
        rows.extend(newpage(rec["BRANCH"]))
        linecnt += 7

    # New MATCHBY section
    if matchby_prev != rec["MATCHBY"]:
        if rec["MATCHBY"] == "ICD ":
            rows.append("Customer with NAME AND ID fully matched")
            rows.append("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        elif rec["MATCHBY"] == "NAME":
            rows.append("Customer NAME matched but ID unmatched/unavailable OR customer ID matched but NAME unmatched")
            rows.append("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        linecnt += 2

    # First record of ACCTNO
    if acct_prev != rec["ACCTNO"]:
        if (rec["ALIAS"].strip() == "") or (rec["TAXID"].strip() and rec["OTHERID"].strip() == rec["TAXID"].strip()):
            rows.append(f"{rec['CUSTNAME']:<40}{rec['TAXID']:<20}{rec['ACTOPND']:<12}{rec['APPLCODE']:<8}{rec['ACCTNO']:<22}{rec['DISPRMK'] or ''}")
        else:
            rows.append(f"{rec['CUSTNAME']:<40}{rec['ALIAS']:<20}{rec['ACTOPND']:<12}{rec['APPLCODE']:<8}{rec['ACCTNO']:<22}{rec['DISPRMK'] or ''}")
        linecnt += 1

        if rec["MATCHBY"] == "ICD ":
            rows.append(f"     {rec['NAME']:<40}{rec['NEWIC']:<20}{rec['OTHERID']:<42}{rec['SOURCE'] or ''}")
            linecnt += 1

        if rec["INDORG"] == "I":
            grdtot_ind += 1
        if rec["INDORG"] == "O":
            grdtot_org += 1
        grdtot_all += 1
    else:
        if rec["MATCHBY"] == "ICD ":
            rows.append(f"     {rec['NAME']:<40}{rec['NEWIC']:<20}{rec['OTHERID']:<42}{rec['SOURCE'] or ''}")
            linecnt += 1

    branch_prev, matchby_prev, acct_prev = rec["BRANCH"], rec["MATCHBY"], rec["ACCTNO"]

# ================================================================
# Part 6: Totals
# ================================================================
rows.append(f"TOTAL NUMBER OF RECORDS (INDIVIDUAL)   : {grdtot_ind}")
rows.append(f"TOTAL NUMBER OF RECORDS (ORGANISATION) : {grdtot_org}")
rows.append(f"GRAND TOTAL OF RECORD TO BE MATCH      : {grdtot_all}")

# ================================================================
# Part 7: Output using PyArrow CSV
# ================================================================
tbl = pa.table({"REPORT": pa.array(rows)})
with pa.OSFile("UNOFAC_CARD_WKLRPT.csv", "wb") as sink:
    csv.write_csv(tbl, sink)

