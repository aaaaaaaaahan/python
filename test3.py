import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path, get_hive_parquet
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year1, month1, day1 = batch_date.year, batch_date.month, batch_date.day

# =========================
#   REPORT DATE
# =========================
report_date = (datetime.date.today()).strftime("%d-%m-%Y")

# =========================
#   CONNECT TO DUCKDB
# =========================
con = duckdb.connect()
sdb_rhl, year, month, day = get_hive_parquet('CIS_SDB_MATCH_RHL')
sdb_dwj, year, month, day = get_hive_parquet('CIS_SDB_MATCH_DWJ')

# =========================
#   LOAD & SORT DATA
# =========================
# Read and combine both parquet files
con.execute(f"""
    CREATE TABLE combined AS
    SELECT * FROM read_parquet('{sdb_rhl[0]}')
    UNION ALL
    SELECT * FROM read_parquet('{sdb_dwj[0]}')
""")

# Deduplicate and sort
con.execute("""
    CREATE TABLE dwj AS
    SELECT DISTINCT BOXNO, SDBNAME, IDNUMBER, BRANCH
    FROM combined
    ORDER BY BRANCH, BOXNO, SDBNAME, IDNUMBER
""")

# Fetch all records
data = con.execute("SELECT * FROM dwj").fetchall()
columns = [desc[0] for desc in con.description]

# ============================================================
# GENERATE REPORT AS TXT FILE (PYARROW)
# ============================================================
output_txt = f"/host/cis/output/CIS_SDB_MATCH_FRPT_{report_date}.txt"

if not data:
    # No matching records
    with open(output_txt, "w") as f:
        f.write("\n" * 2)
        f.write(" " * 15 + "**********************************\n")
        f.write(" " * 15 + "*       NO MATCHING RECORDS      *\n")
        f.write(" " * 15 + "**********************************\n")
else:
    with open(output_txt, "w") as f:
        page_cnt = 1
        line_cnt = 0
        grand_total = 0
        current_branch = None

        state = {'page_cnt': 1, 'line_cnt': 0}
        def print_header(branch):
            state['line_cnt'] = 9
            f.write(f"PAGE : {state['page_cnt']}")
            f.write(f"REPORT ID   : SDB/SCREEN/FULL{' ' * 35}PUBLIC BANK BERHAD{' ' * 15}PAGE : {page_cnt:4d}\n")
            f.write(f"PROGRAM ID  : CISDBFRP{' ' * 55}REPORT DATE : {report_date}\n")
            f.write(f"BRANCH      : {branch or '0000001'}{' ' * 10}SDB FULL DATABASE SCREENING\n")
            f.write(" " * 50 + "===========================\n")
            f.write(f"{'BOX NO':<10}{'NAME (HIRER S NAME)':<40}{'CUSTOMER ID':<20}\n")
            f.write("-" * 120 + "\n")
            line_cnt = 9

        # Start first page
        print_header(None)

        for row in data:
            boxno, sdbname, idnumber, branch = row
            if current_branch != branch:
                page_cnt += 1
                print_header(branch)
                current_branch = branch

            f.write(f"{boxno:<10}{sdbname:<45}{idnumber:<20}\n")
            grand_total += 1
            line_cnt += 1

            # New page if too many lines
            if line_cnt > 55:
                page_cnt += 1
                print_header(branch)

        # Footer
        f.write("\n" * 2)
        f.write(f"GRAND TOTAL OF ALL BRANCHES = {grand_total:>9}\n")
