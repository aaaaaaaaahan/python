import duckdb
from datetime import datetime
from textwrap import dedent

# ================================================================
# Input Parquet datasets (already converted)
# ================================================================
ctrl_parquet = "SRSCTRL1.parquet"        # control date
error_parquet = "BNMCTR_ERROR.parquet"

# Output text reports
detail_txt = "BNMCTR_ERROR_RPT.txt"
summary_txt = "BNMCTR_ERROR_SUM.txt"

# ================================================================
# Step 1: Get Reporting Date (from CTRLDATE parquet)
# ================================================================
con = duckdb.connect()

ctrl_df = con.execute(f"""
    SELECT CAST(substr(column0,1,4) AS INT) AS SRSYY,
           CAST(substr(column0,5,2) AS INT) AS SRSMM,
           CAST(substr(column0,7,2) AS INT) AS SRSDD
    FROM read_parquet('{ctrl_parquet}')
""").df()

if not ctrl_df.empty:
    year, month, day = ctrl_df.iloc[0]
    report_date = datetime(year, month, day).strftime("%d-%m-%Y")
else:
    report_date = datetime.today().strftime("%d-%m-%Y")

# ================================================================
# Step 2: Parse Input Error File (BNMCTR_ERROR parquet)
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE error_raw AS
    SELECT
        substr(column0, 1, 3)  AS BANKNO,
        substr(column0, 4, 7)  AS BRANCH,
        substr(column0,11,20)  AS ACCTNO,
        substr(column0,31,11)  AS CUSTNO,
        substr(column0,42,20)  AS FIELDTYPE,
        substr(column0,62,30)  AS FIELDVALUE,
        substr(column0,92,40)  AS REMARKS
    FROM read_parquet('{error_parquet}')
""")

# Deduplicate like PROC SORT NODUPKEY
con.execute("""
    CREATE OR REPLACE TABLE error_clean AS
    SELECT DISTINCT BRANCH, ACCTNO, CUSTNO, FIELDTYPE, FIELDVALUE, REMARKS
    FROM error_raw
    WHERE TRIM(ACCTNO) <> ''
""")

# ================================================================
# Step 3: Fetch Data
# ================================================================
detail_df = con.execute("""
    SELECT BRANCH, ACCTNO, CUSTNO, FIELDTYPE, FIELDVALUE, REMARKS
    FROM error_clean
    ORDER BY BRANCH, ACCTNO, CUSTNO
""").df()

summary_df = con.execute("""
    SELECT BRANCH, COUNT(*) AS TOTAL_RECORD
    FROM error_clean
    GROUP BY BRANCH
    ORDER BY BRANCH
""").df()

grand_totals = con.execute("""
    SELECT COUNT(*) AS TOTAL_RECORDS,
           COUNT(DISTINCT BRANCH) AS TOTAL_BRANCH
    FROM error_clean
""").df().iloc[0]

# ================================================================
# Step 4: Write Detail Report (like SAS RPTFILE)
# ================================================================
with open(detail_txt, "w") as f:
    if detail_df.empty:
        f.write(dedent(f"""
        **********************************
        *                                *
        *       NO ERROR RECORDS         *
        *                                *
        **********************************
        """))
    else:
        pagecnt = 0
        current_branch = None
        brcust = 0
        grcust = 0

        for idx, row in detail_df.iterrows():
            branch = row["BRANCH"]

            # New page / new branch
            if branch != current_branch:
                pagecnt += 1
                current_branch = branch
                brcust = 0

                # Header
                f.write("\f")  # form feed = new page
                f.write(f"REPORT ID   : CTR ECP PBB{'':20}PUBLIC BANK BERHAD   PAGE: {pagecnt:4d}\n")
                f.write(f"PROGRAM ID  : CIBNCR01{'':20}REPORT DATE : {report_date}\n")
                f.write(f"BRANCH      : {branch:7s}   CASH THRESHOLD REPORTING - DATA AMENDMENT\n")
                f.write("==============================================\n")
                f.write(f"{'ACCT NUMBER':20} {'CUSTOMER NO':11} {'FIELD TYPE':20} {'FIELD VALUE':30} {'REMARKS':40}\n")
                f.write(f"{'='*11:20} {'='*11:11} {'='*10:20} {'='*11:30} {'='*7:40}\n")

            # Detail line
            f.write(f"{row['ACCTNO']:<20} {row['CUSTNO']:<11} {row['FIELDTYPE']:<20} {row['FIELDVALUE']:<30} {row['REMARKS']:<40}\n")
            brcust += 1
            grcust += 1

            # If last record of branch → print total
            if (idx == len(detail_df)-1) or (detail_df.iloc[idx+1]["BRANCH"] != branch):
                f.write(f"TOTAL RECORDS = {brcust:7d}\n")

# ================================================================
# Step 5: Write Summary Report (like SAS SUMFILE)
# ================================================================
with open(summary_txt, "w") as f:
    if summary_df.empty:
        f.write(dedent(f"""
        **********************************
        *                                *
        *       NO ERROR RECORDS         *
        *                                *
        **********************************
        """))
    else:
        pagecnt = 1
        f.write(f"REPORT ID   : CTR ECP SUM{'':20}PUBLIC BANK BERHAD   PAGE: {pagecnt:4d}\n")
        f.write(f"PROGRAM ID  : CIBNCR01{'':20}REPORT DATE : {report_date}\n")
        f.write("CASH THRESHOLD REPORTING - DATA AMENDMENT (SUMMARY)\n")
        f.write("===================================================\n\n")
        f.write(f"{'BRANCH':<10}{'TOTAL RECORD':>20}\n")
        f.write(f"{'======':<10}{'============':>20}\n")

        for _, row in summary_df.iterrows():
            f.write(f"{row['BRANCH']:<10}{row['TOTAL_RECORD']:>20}\n")

        f.write("\n")
        f.write(f"{'TOTAL RECORDS  :':<20}{grand_totals['TOTAL_RECORDS']:>11}\n")
        f.write(f"{'TOTAL BRANCH   :':<20}{grand_totals['TOTAL_BRANCH']:>11}\n")

print("✅ Detail report written to", detail_txt)
print("✅ Summary report written to", summary_txt)
