import duckdb
from CIS_PY_READER import get_hive_parquet
import datetime

# ============================================================
# DATE SETUP
# ============================================================
batch_date = (datetime.date.today() - datetime.timedelta(days=1))
report_date = datetime.date.today().strftime("%d-%m-%Y")

# ============================================================
# CONNECT TO DUCKDB
# ============================================================
con = duckdb.connect()
sdb_rhl, _, _, _ = get_hive_parquet('CIS_SDB_MATCH_RHL')
sdb_dwj, _, _, _ = get_hive_parquet('CIS_SDB_MATCH_DWJ')

# ============================================================
# LOAD & SORT DATA
# ============================================================
con.execute(f"""
    CREATE TABLE combined AS
    SELECT * FROM read_parquet('{sdb_rhl[0]}')
    UNION ALL
    SELECT * FROM read_parquet('{sdb_dwj[0]}')
""")

con.execute("""
    CREATE TABLE dwj AS
    SELECT DISTINCT BOXNO, SDBNAME, IDNUMBER, BRANCH
    FROM combined
    ORDER BY BRANCH, BOXNO, SDBNAME, IDNUMBER
""")

data = con.execute("SELECT * FROM dwj").fetchall()

# ============================================================
# OUTPUT REPORT
# ============================================================
output_txt = f"/host/cis/output/CIS_SDB_MATCH_FRPT_{report_date}.txt"

with open(output_txt, "w") as f:
    if not data:
        # No matching records case
        f.write(" " * 15 + "**********************************\n")
        f.write(" " * 15 + "*                                *\n")
        f.write(" " * 15 + "*       NO MATCHING RECORDS      *\n")
        f.write(" " * 15 + "*                                *\n")
        f.write(" " * 15 + "**********************************\n")
    else:
        # Initialize counters
        page_cnt = 0
        line_cnt = 0
        grand_total = 0
        current_branch = None

        def print_header(f, branch, page_cnt, report_date):
            """Print SAS-style page header"""
            branch_display = branch if branch else "0000001"
            f.write(f"{'REPORT ID   : SDB/SCREEN/FULL':<54}{'PUBLIC BANK BERHAD':<40}{'PAGE        : '}{page_cnt:>4}\n")
            f.write(f"{'PROGRAM ID  : CISDBFRP':<94}{'REPORT DATE : ' + report_date}\n")
            f.write(f"{'BRANCH      : ' + branch_display:<54}{'SDB FULL DATABASE SCREENING'}\n")
            f.write(f"{'':<49}{'==========================='}\n")
            f.write(f"{'BOX NO':<10}{'NAME (HIRER S NAME)':<40}{'CUSTOMER ID':<20}\n")
            f.write(f"{'-'*40}{'-'*40}{'-'*40}\n")

        first_page = True

        for row in data:
            boxno, sdbname, idnumber, branch = row

            # Start new page when branch changes or line limit reached
            if first_page or branch != current_branch or line_cnt >= 52:
                page_cnt += 1
                print_header(f, branch, page_cnt, report_date)
                current_branch = branch
                line_cnt = 9
                first_page = False

            # Print each record
            f.write(f"{' ' * 1}{boxno:<6}{' ' * 5}{sdbname:<40}{' ' * 2}{idnumber:<20}\n")
            line_cnt += 1
            grand_total += 1

        # --- END OF REPORT footer ---
        f.write(" " * 55 + "                      \n")
        f.write(" " * 55 + "****END OF REPORT ****\n")
        f.write(" " * 55 + "                      \n\n")
        f.write(f"{' ' * 3}GRAND TOTAL OF ALL BRANCHES = {grand_total:>9}\n")
