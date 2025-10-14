import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
from datetime import datetime
import os

# ============================================================
# CONFIGURATION
# ============================================================
# Assume parquet files already exist
input_dw_parquet = "/host/cis/parquet/sas_parquet/CIS_SDB_MATCH_DWJ.parquet"
input_rh_parquet = "/host/cis/parquet/sas_parquet/CIS_SDB_MATCH_RHL.parquet"
output_txt = "/host/cis/output/CIS_SDB_MATCH_FRPT.txt"

# Report date (today or any control date)
report_date = datetime.today().strftime("%d/%m/%Y")

# ============================================================
# PROCESS INPUT FILES USING DUCKDB
# ============================================================
con = duckdb.connect(database=':memory:')

# Read and combine both parquet files
con.execute(f"""
    CREATE TABLE combined AS
    SELECT * FROM read_parquet('{input_dw_parquet}')
    UNION ALL
    SELECT * FROM read_parquet('{input_rh_parquet}')
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

        def print_header(branch):
            nonlocal page_cnt, line_cnt
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

print(f"✅ Report generated successfully: {output_txt}")
