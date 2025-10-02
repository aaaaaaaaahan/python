import duckdb
import datetime
import os

# =========================
#   CONFIG
# =========================
input_parquet = "CIS.RACE.parquet"   # assumed CIS.RACE is converted into parquet
output_txt = "ETHNIC_REPORT_MONTHLY.txt"

# =========================
#   REPORT DATE
# =========================
report_date = (datetime.date.today()).strftime("%d-%m-%Y")

# =========================
#   CONNECT TO DUCKDB
# =========================
con = duckdb.connect()

# =========================
#   LOAD & SORT DATA
# =========================
df = con.execute(f"""
    SELECT 
        SUBSTR(aliaskey, 1, 2) AS aliaskey,
        SUBSTR(alias, 1, 12)   AS alias,
        SUBSTR(custname, 1, 40) AS custname,
        SUBSTR(custno, 1, 11)  AS custno,
        SUBSTR(custbrch, 1, 3) AS custbrch
    FROM read_parquet('{input_parquet}')
    ORDER BY custbrch
""").fetchdf()

# =========================
#   WRITE REPORT
# =========================
with open(output_txt, "w") as f:
    if df.empty:
        f.write("**********************************\n")
        f.write("*                                *\n")
        f.write("*         EMPTY REPORT           *\n")
        f.write("*                                *\n")
        f.write("**********************************\n")
    else:
        grand_total = 0
        linecnt = 0
        pagecnt = 0
        current_branch = None
        branch_count = 0

        def print_header(branch, pagecnt):
            f.write(f"REPORT NO : ETHNIC/OTHERS".ljust(54))
            f.write(f"PUBLIC BANK BERHAD".ljust(40))
            f.write(f"PAGE : {pagecnt:4d}\n")
            f.write(f"PROGRAM ID  : CISECOMT".ljust(94))
            f.write(f"REPORT DATE : {report_date}\n")
            f.write(f"BRANCH      : 00{branch}\n")
            f.write("LIST OF MALAYSIAN WITH ETHNIC CODE OTHERS\n")
            f.write("=========================================\n")
            f.write("CIS NUMBER   MYKAD NUMBER       NAME                                BRANCH\n")
            f.write("===========  ================== ==================================== ========\n")

        for _, row in df.iterrows():
            # new branch or first record
            if current_branch != row["custbrch"]:
                # print branch total if previous branch exists
                if current_branch is not None:
                    f.write(f"{'':25}TOTAL = {branch_count:9d}\n\n")
                    branch_count = 0

                # new page header
                pagecnt += 1
                print_header(row["custbrch"], pagecnt)
                linecnt = 7
                current_branch = row["custbrch"]

            # write record
            f.write(f"{row['custno']:<11}  {row['alias']:<20} {row['custname']:<40} {row['custbrch']:<8}\n")
            branch_count += 1
            grand_total += 1
            linecnt += 1

        # final branch total
        f.write(f"{'':25}TOTAL = {branch_count:9d}\n\n")

        # grand total
        f.write(f"   GRAND TOTAL OF ALL BRANCHES = {grand_total:9d}\n")

print(f"Report generated: {os.path.abspath(output_txt)}")
