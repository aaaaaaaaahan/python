import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path, get_hive_parquet
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year1, month1, day1 = batch_date.year, batch_date.month, batch_date.day

#---------------------------------------------------------------------#
# Original Program: CISECOMT                                          #
#---------------------------------------------------------------------#
# ESMR 2015-707 MONTHLY REPORT EVERY 15TH                             #
# INITIALIZE DATASETS                                                 #
#---------------------------------------------------------------------#

# =========================
#   REPORT DATE
# =========================
report_date = (datetime.date.today()).strftime("%d-%m-%Y")

# =========================
#   CONNECT TO DUCKDB
# =========================
con = duckdb.connect()
cis_race, year, month, day = get_hive_parquet('CIS_RACE')

# =========================
#   LOAD & SORT DATA
# =========================
df = con.execute(f"""
    SELECT 
        ALIASKEY,
        ALIAS,   
        CUSTNAME,
        CUSTNO,  
        CUSTBRCH
    FROM read_parquet('{cis_race[0]}')
    ORDER BY CUSTBRCH
""").fetchdf()

# =========================
#   WRITE REPORT
# =========================
output_txt = f"/host/cis/output/ETHNIC_REPORT_MONTHLY_{report_date}.txt"

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
            if current_branch != row["CUSTBRCH"]:
                # print branch total if previous branch exists
                if current_branch is not None:
                    f.write(f"{'':25}TOTAL = {branch_count:9d}\n\n")
                    branch_count = 0

                # new page header
                pagecnt += 1
                print_header(row["CUSTBRCH"], pagecnt)
                linecnt = 7
                current_branch = row["CUSTBRCH"]

            # write record
            f.write(f"{row['CUSTNO']:<11}  {row['ALIAS']:<20} {row['CUSTNAME']:<40} {row['CUSTBRCH']:<8}\n")
            branch_count += 1
            grand_total += 1
            linecnt += 1

        # final branch total
        f.write(f"{'':25}TOTAL = {branch_count:9d}\n\n")

        # grand total
        f.write(f"   GRAND TOTAL OF ALL BRANCHES = {grand_total:9d}\n")
