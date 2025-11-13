import duckdb
from CIS_PY_READER import host_parquet_path, csv_output_path, get_hive_parquet
import datetime
import pyarrow as pa
import pyarrow.parquet as pq

# -----------------------------
# SET DATES
# -----------------------------
batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")

# -----------------------------
# CONNECT TO DUCKDB
# -----------------------------
con = duckdb.connect()

# -----------------------------
# GET INPUT FILES
# -----------------------------
eccris = get_hive_parquet('ECCRIS_BLANK_ADDR_POSTCODE')
ccris = get_hive_parquet('CIS_CCRIS_ERROR')

# -----------------------------
# PART 1: CONCAT 2 INPUT FILES
# -----------------------------
con.execute(f"""
CREATE TABLE ERRFILE AS
SELECT * FROM read_parquet('{eccris[0]}')
UNION ALL
SELECT * FROM read_parquet('{ccris[0]}')
""")

con.execute("""
CREATE TABLE ERRFILE_SORTED AS
SELECT *
FROM ERRFILE
ORDER BY BRANCH, CUSTNO, ACCTNOC;
""")

# -----------------------------
# PART 1: GENERATE DETAILED REPORT TXT & PARQUET
# -----------------------------
hkctrl_path = csv_output_path(f"CIS_HSEKEEP_CENTRAL_{report_date}").replace(".csv", ".txt")
hkctrl_parquet = csv_output_path(f"CIS_HSEKEEP_CENTRAL_{report_date}").replace(".csv", ".parquet")

error_codes = ['001','002','003','004','005','100']
error_desc_map = {
    '001': 'TOTAL UNKNOWN CITIZENSHIP',
    '002': 'TOTAL BLANK ID (INDV)',
    '003': 'TOTAL BLANK ID (ORG)',
    '004': 'TOTAL BLANK DATE OF BIRTH',
    '005': 'TOTAL BLANK DATE OF REG',
    '100': 'TOTAL INVALID POSTCODE'
}

records = con.execute("SELECT * FROM ERRFILE_SORTED").fetchall()

with open(hkctrl_path, "w") as rpt_file:
    rpt_file.write(f"REPORT ID   : CIS HSEKEEP RPT{'':45}PUBLIC BANK BERHAD\n")
    rpt_file.write(f"PROGRAM ID  : CIHKCTRL{'':55}REPORT DATE : {day}/{month}/{year}\n")
    rpt_file.write("BRANCH      : ALL\n")
    rpt_file.write("MISSING FIELDS DETECTED IN CIS SYSTEM FOR DATA SCRUBBING\n")
    rpt_file.write("="*56 + "\n")
    rpt_file.write(f"{'ACCOUNT':<23}{'CUSTNO':<12}{'FIELD TYPE':<20}{'FIELD VALUE':<30}{'REMARKS':<40}\n")
    rpt_file.write(f"{'='*7:<7}{'='*6:<6}{'='*10:<10}{'='*11:<11}{'='*6:<6}\n")

    branch = None
    brcust = 0
    grcust = 0
    err_counts = {code:0 for code in error_codes}

    for rec in records:
        rec_branch = rec['BRANCH']
        if branch != rec_branch:
            branch = rec_branch
            brcust = 0

        rpt_file.write(f"{rec['ACCTCODE']:<5}{rec['ACCTNOC']:<20}{rec['CUSTNO']:<11}"
                       f"{rec['FIELDTYPE']:<20}{rec['FIELDVALUE']:<30}{rec['REMARKS']:<40}\n")

        code = rec['ERRORCODE']
        if code in err_counts:
            err_counts[code] += 1
        brcust += 1
        grcust += 1

    # Write totals per branch
    for code, count in err_counts.items():
        if count != 0:
            rpt_file.write(f"{error_desc_map[code]:<45}{count:>9}\n")
    rpt_file.write(f"{'TOTAL ERRORS':<45}{brcust:>9}\n")
    rpt_file.write(f"GRAND TOTAL OF ALL BRANCHES = {grcust}\n")

# Export detailed Parquet
con.execute(f"COPY ERRFILE_SORTED TO '{hkctrl_parquet}' (FORMAT PARQUET);")

# -----------------------------
# PART 2: GENERATE SUMMARY (TOTLIST & SUMLIST)
# -----------------------------
con.execute("""
CREATE TABLE SUM1 AS
SELECT *,
CASE 
    WHEN ERRORCODE='001' THEN 'EMPTY CITIZENSHIP'
    WHEN ERRORCODE='002' THEN 'EMPTY INDIVIDUAL ID'
    WHEN ERRORCODE='003' THEN 'EMPTY ORGANISATION ID'
    WHEN ERRORCODE='004' THEN 'EMPTY DATE OF BIRTH'
    WHEN ERRORCODE='005' THEN 'EMPTY DATE OF REGISTRATION'
    WHEN ERRORCODE='100' THEN 'EMPTY POSTCODE'
END AS ERRORDESC
FROM ERRFILE_SORTED;
""")

# TOTLIST: count per ERRORCODE
totlist_table = con.execute("""
SELECT ERRORCODE, ERRORDESC, COUNT(*) AS ERRORTOTAL
FROM SUM1
GROUP BY ERRORCODE, ERRORDESC
ORDER BY ERRORCODE;
""").arrow()

# SUMLIST: per branch summary (TOTAL ERRORS)
sumlist_table = con.execute("""
SELECT BRANCH, COUNT(*) AS TOTAL_ERRORS
FROM SUM1
GROUP BY BRANCH
ORDER BY BRANCH;
""").arrow()

# -----------------------------
# PART 3: CONCAT SUMMARY & EXPORT TXT & PARQUET
# -----------------------------
# Align columns for concatenation
totlist_table = totlist_table.append_column("BRANCH", pa.array(['']*totlist_table.num_rows))
totlist_table = totlist_table.append_column("TOTAL_ERRORS", pa.array([0]*totlist_table.num_rows))

sumlist_table = sumlist_table.append_column("ERRORCODE", pa.array(['']*sumlist_table.num_rows))
sumlist_table = sumlist_table.append_column("ERRORDESC", pa.array(['']*sumlist_table.num_rows))
sumlist_table = sumlist_table.append_column("ERRORTOTAL", sumlist_table["TOTAL_ERRORS"])

# Concatenate TOTLIST + SUMLIST
final_summary_table = pa.concat_tables([totlist_table, sumlist_table])

# Paths
sum_path_txt = csv_output_path(f"CIS_HSEKEEP_CENTRAL_SUM_{report_date}").replace(".csv", ".txt")
sum_path_parquet = csv_output_path(f"CIS_HSEKEEP_CENTRAL_SUM_{report_date}").replace(".csv", ".parquet")

# Write TXT
with open(sum_path_txt, "w") as f:
    f.write(f"REPORT ID   : CIS HSEKEEP SUM{'':40}PUBLIC BANK BERHAD\n")
    f.write(f"PROGRAM ID  : CIHKCTRL{'':55}REPORT DATE : {day}/{month}/{year}\n")
    f.write("DATA SCRUBBING SUMMARY REPORT\n")
    f.write("=============================\n")
    f.write(f"{'ERROR DESCRIPTION/BRANCH':<47}{'TOTAL RECORDS':>12}\n")
    f.write("="*60 + "\n")
    for i in range(final_summary_table.num_rows):
        row = final_summary_table.slice(i,1)
        desc_or_branch = row.column("ERRORDESC")[0].as_py() or row.column("BRANCH")[0].as_py()
        total = row.column("ERRORTOTAL")[0].as_py()
        f.write(f"{desc_or_branch:<47}{total:>12}\n")

# Write Parquet
pq.write_table(final_summary_table, sum_path_parquet)

print("All parts complete: detailed report + final summary (TXT & Parquet) generated without pandas.")
