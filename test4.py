import duckdb
import pyarrow.parquet as pq
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet
import datetime

batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")

# -----------------------------
# Connect to DuckDB
# -----------------------------
con = duckdb.connect()
cis = get_hive_parquet('CIS_CUST_DAILY')

# -----------------------------
# 1. Build CUST (ACCTCODE='DP' AND PRISEC = 901)
# -----------------------------
con.execute(f"""
    CREATE TABLE cust AS
    SELECT DISTINCT (ON ACCTNOC)
        CUSTNO,
        ACCTNOC,
        ACCTCODE,
        CUSTNAME,
        TAXID,
        ALIASKEY,
        ALIAS,
        JOINTACC,
        LPAD(CAST(CUSTBRCH AS VARCHAR), 7, '0') AS BRANCH
    FROM read_parquet('{cis[0]}')
    WHERE ACCTCODE = 'DP'
      AND PRISEC = 901
    ORDER BY CUSTNO
""")

# -----------------------------
# 2. Sort HRC
# -----------------------------
con.execute(f"""
    CREATE TABLE hrc_sorted AS
    SELECT *
    FROM read_parquet('{host_parquet_path("CUSTCODE.parquet")}')
    ORDER BY CUSTNO
""")

# -----------------------------
# 3. Merge HRC + CUST → CUSTACCT
# -----------------------------
con.execute("""
    CREATE TABLE custacct AS
    SELECT *
    FROM hrc_sorted h
    LEFT JOIN cust c USING (CUSTNO)
    WHERE c.CUSTNO IS NOT NULL
""")

con.execute("CREATE TABLE custacct2 AS SELECT * FROM custacct ORDER BY ACCTNOC")

# -----------------------------
# 4. HRMS (converted CSV → parquet)
# -----------------------------
con.execute(f"""
    CREATE TABLE hrms2 AS
    SELECT * FROM read_parquet('{host_parquet_path("HCMS_STFF_TAG.parquet")}')
    ORDER BY ACCTNOC
""")

# -----------------------------
# 5. Merge HRMS + CUSTACCT
# -----------------------------
con.execute("""
    CREATE TABLE mergefound AS
    SELECT *
    FROM hrms2 h
    INNER JOIN custacct2 c USING(ACCTNOC)
""")

con.execute("""
    CREATE TABLE mergexmtch AS
    SELECT *
    FROM hrms2 h
    LEFT JOIN custacct2 c USING(ACCTNOC)
    WHERE c.CUSTNO IS NULL
""")

# -----------------------------
# 6. NOTFOUND output
# -----------------------------
rows = con.execute("""
    SELECT ORGCODE, STAFFNO, STAFFNAME, ACCTNOC, NEWIC, OLDIC, BRANCHCODE
    FROM mergexmtch
    ORDER BY ORGCODE, STAFFNAME, ACCTNOC
""").fetchall()

with open(NOTFND, "w") as f:
    for r in rows:
        f.write(
            f"{r[0]:<3}{r[1]:<9}{r[2]:<40}{r[3]:<11}"
            f"{r[4]:<12}{r[5]:<10}{r[6]:<3}\n"
        )

# -----------------------------
# 7. DPTEAM output
# -----------------------------
rows = con.execute("""
    SELECT STAFFNO, CUSTNO, ACCTCODE, ACCTNOC, JOINTACC, STAFFNAME, BRANCHCODE
    FROM mergefound
    ORDER BY STAFFNO, CUSTNO, ACCTCODE, ACCTNOC, JOINTACC
""").fetchall()

with open(DPFILE, "w") as f:
    for r in rows:
        f.write(
            f"{r[0]:<9}{r[1]:<20}{r[2]:<5}{r[3]:<11}"
            f"{r[4]:<1}{r[5]:<40}{r[6]:<3}\n"
        )

# -----------------------------
# 8. UPDATE output (loop C01–C20 + forced 002)
# -----------------------------
def write_update_line(f, custno, code, rectype, branch, filecode, staffno, staffname):
    f.write(
        f" {custno:<11}{code:0>3}{rectype:<1}{branch:<7}"
        f"{filecode:<1}{staffno:<9}{staffname:<40}\n"
    )

rows = con.execute("""
    SELECT CUSTNO, RECTYPE, BRANCH, FILECODE, STAFFNO, STAFFNAME,
           C01,C02,C03,C04,C05,C06,C07,C08,C09,C10,
           C11,C12,C13,C14,C15,C16,C17,C18,C19,C20
    FROM mergefound
""").fetchall()

with open(OUTFILE, "w") as f:
    for r in rows:
        custno, rectype, branch, filecode, staffno, staffname = r[:6]
        codes = r[6:]

        # Forced SAS 002
        write_update_line(f, custno, 2, rectype, branch, filecode, staffno, staffname)

        # C01–C20 loop
        for code in codes:
            if code not in (0, None):
                write_update_line(f, custno, code, rectype, branch, filecode, staffno, staffname)

# -----------------------------
# 9. Optional Parquet Output for UPDATE
# -----------------------------
update_table = con.execute("SELECT * FROM mergefound").arrow()
pq.write_table(update_table, OUT_PARQUET)

print("Completed CICUSCD5 conversion successfully.")
