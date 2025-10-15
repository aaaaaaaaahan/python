import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
import datetime
import os

# ============================================================
# PATH CONFIGURATION
# ============================================================
yesterday_parquet = "/host/cis/parquet/CIS.SDB.MATCH.FULL_yesterday.parquet"
today_parquet = "/host/cis/parquet/CIS.SDB.MATCH.FULL_today.parquet"
output_txt = "/host/cis/output/CIS.SDB.MATCH.NRPT.txt"

# ============================================================
# BATCH DATE (today)
# ============================================================
report_date = datetime.date.today().strftime("%d/%m/%Y")

# ============================================================
# CREATE DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()

# ============================================================
# READ OLD (YESTERDAY) & NEW (TODAY) LISTS
# ============================================================
old_query = f"""
    SELECT 
        BOXNO,
        SDBNAME,
        IDNUMBER,
        BRANCH
    FROM read_parquet('{yesterday_parquet}')
"""
new_query = f"""
    SELECT 
        BOXNO,
        SDBNAME,
        IDNUMBER,
        BRANCH
    FROM read_parquet('{today_parquet}')
"""

old_df = con.execute(old_query).arrow()
new_df = con.execute(new_query).arrow()

# Remove duplicates
old_df = pc.unique(old_df)
new_df = pc.unique(new_df)

# ============================================================
# FIND NEW RECORDS (TODAY BUT NOT IN YESTERDAY)
# ============================================================
con.register("old_tbl", old_df)
con.register("new_tbl", new_df)

comp_arrow = con.execute("""
    SELECT n.*
    FROM new_tbl n
    LEFT JOIN old_tbl o
    ON n.BOXNO = o.BOXNO
       AND n.SDBNAME = o.SDBNAME
       AND n.IDNUMBER = o.IDNUMBER
    WHERE o.BOXNO IS NULL
    ORDER BY n.BRANCH, n.BOXNO, n.SDBNAME, n.IDNUMBER
""").arrow()

SELECT n.BOXNO, n.SDBNAME, n.IDNUMBER, n.BRANCH
FROM new_nodup n
LEFT JOIN old_nodup o
  ON n.BOXNO = o.BOXNO
 AND n.SDBNAME = o.SDBNAME
 AND n.IDNUMBER = o.IDNUMBER
WHERE o.BOXNO IS NULL


# ============================================================
# REPORT GENERATION
# ============================================================
lines = []
page_count = 1
line_count = 0
grand_total = 0

def write_page_header(branch, page):
    return [
        f"REPORT ID   : SDB/SCREEN/NEW{' ' * 35}PUBLIC BANK BERHAD{' ' * 20}PAGE : {page:04}",
        f"PROGRAM ID  : CISDBNRP{' ' * 60}REPORT DATE : {report_date}",
        f"BRANCH      : {branch or '00000'}{' ' * 10}SDB NEW RECORDS SCREENING",
        " " * 50 + "==========================",
        "  BOX NO    NAME (HIRER'S NAME)                     CUSTOMER ID",
        "  ------------------------------------------------------------------------------"
    ]

if comp_arrow.num_rows == 0:
    lines.append("\n" * 3)
    lines += [
        " " * 15 + "**********************************",
        " " * 15 + "*                                *",
        " " * 15 + "*       NO MATCHING RECORDS      *",
        " " * 15 + "*                                *",
        " " * 15 + "**********************************",
    ]
else:
    comp_table = comp_arrow.to_pydict()
    current_branch = None
    branch_total = 0

    for i in range(comp_arrow.num_rows):
        boxno = comp_table["BOXNO"][i]
        sdbname = comp_table["SDBNAME"][i]
        idnum = comp_table["IDNUMBER"][i]
        branch = comp_table["BRANCH"][i]

        # new page or new branch
        if line_count == 0 or branch != current_branch or line_count >= 52:
            if line_count != 0:
                lines.append("\n" * 3)  # spacing between pages
            lines += write_page_header(branch, page_count)
            page_count += 1
            line_count = 9
            current_branch = branch

        # write record
        lines.append(f"  {boxno:<6}   {sdbname:<40} {idnum:<20}")
        line_count += 1
        branch_total += 1
        grand_total += 1

    lines.append("")
    lines.append(f"   GRAND TOTAL OF ALL BRANCHES = {grand_total:9}")

# ============================================================
# OUTPUT TO TEXT FILE
# ============================================================
with open(output_txt, "w", encoding="utf-8") as f:
    for line in lines:
        f.write(line + "\n")

print(f"Report generated successfully: {output_txt}")
