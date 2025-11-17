import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import datetime

# ------------------------------------------------
# Input + Output Paths
# ------------------------------------------------
CUSTFILE = "CUSTFILE.CUSTDLY.parquet"
CUSTCODE = "CUSTCODE.parquet"
HRMSFILE = "HRMSFILE.parquet"

OUTFILE = "CICUSCD5.UPDATE.txt"
NOTFND = "CICUSCD5.NOTFND.txt"
DPFILE = "CICUSCD5.UPDATE.DP.TEMP.txt"

OUT_PARQUET = "CICUSCD5.UPDATE.parquet"   # optional

# ------------------------------------------------
# 1. Connect to DuckDB
# ------------------------------------------------
con = duckdb.connect()

# ------------------------------------------------
# 2. Load Input Tables
# ------------------------------------------------
con.execute(f"CREATE TABLE custfile AS SELECT * FROM parquet_scan('{CUSTFILE}')")
con.execute(f"CREATE TABLE hrc AS SELECT * FROM parquet_scan('{CUSTCODE}')")
con.execute(f"CREATE TABLE hrms AS SELECT * FROM parquet_scan('{HRMSFILE}')")

# ------------------------------------------------
# 3. Build CUST (ACCTCODE='DP' AND PRISEC = 901)
# ------------------------------------------------
con.execute("""
    CREATE TABLE cust AS
    SELECT
        CUSTNO,
        ACCTNOC,
        ACCTCODE,
        CUSTNAME,
        TAXID,
        ALIASKEY,
        ALIAS,
        JOINTACC,
        LPAD(CAST(CUSTBRCH AS VARCHAR), 7, '0') AS BRANCH
    FROM custfile
    WHERE ACCTCODE = 'DP'
      AND PRISEC = 901
""")

# CUST sorted
con.execute("CREATE TABLE cust1 AS SELECT * FROM cust ORDER BY ACCTNOC")
con.execute("CREATE TABLE cust2 AS SELECT * FROM cust1 ORDER BY CUSTNO")

# ------------------------------------------------
# 4. Sort HRC (C01–C20)
# ------------------------------------------------
con.execute("CREATE TABLE hrc_sorted AS SELECT * FROM hrc ORDER BY CUSTNO")

# ------------------------------------------------
# 5. Merge HRC + CUST → CUSTACCT
# ------------------------------------------------
con.execute("""
    CREATE TABLE custacct AS
    SELECT *
    FROM hrc_sorted h
    LEFT JOIN cust2 c USING (CUSTNO)
    WHERE c.CUSTNO IS NOT NULL
""")

con.execute("CREATE TABLE custacct2 AS SELECT * FROM custacct ORDER BY ACCTNOC")

# ------------------------------------------------
# 6. HRMS (converted from CSV earlier)
# ------------------------------------------------
con.execute("CREATE TABLE hrms2 AS SELECT * FROM hrms ORDER BY ACCTNOC")

# ------------------------------------------------
# 7. MERGE HRMS + CUSTACCT
# ------------------------------------------------
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

# ------------------------------------------------
# 8. NOTFOUND output (same fixed width as SAS)
# ------------------------------------------------
rows = con.execute("""
    SELECT 
        ORGCODE,
        STAFFNO,
        STAFFNAME,
        ACCTNOC,
        NEWIC,
        OLDIC,
        BRANCHCODE
    FROM mergexmtch
    ORDER BY ORGCODE, STAFFNAME, ACCTNOC
""").fetchall()

with open(NOTFND, "w") as f:
    for r in rows:
        f.write(
            f"{r[0]:<3}"
            f"{r[1]:<9}"
            f"{r[2]:<40}"
            f"{r[3]:<11}"
            f"{r[4]:<12}"
            f"{r[5]:<10}"
            f"{r[6]:<3}"
            "\n"
        )

# ------------------------------------------------
# 9. DPTEAM output
# ------------------------------------------------
rows = con.execute("""
    SELECT STAFFNO, CUSTNO, ACCTCODE, ACCTNOC, JOINTACC, STAFFNAME, BRANCHCODE
    FROM mergefound
    ORDER BY STAFFNO, CUSTNO, ACCTCODE, ACCTNOC, JOINTACC
""").fetchall()

with open(DPFILE, "w") as f:
    for r in rows:
        f.write(
            f"{r[0]:<9}"
            f"{r[1]:<20}"
            f"{r[2]:<5}"
            f"{r[3]:<11}"
            f"{r[4]:<1}"
            f"{r[5]:<40}"
            f"{r[6]:<3}"
            "\n"
        )

# ------------------------------------------------
# 10. UPDATE output (C01–C20 + forced 002)
# ------------------------------------------------

def write_update_line(f, custno, code, rectype, branch, filecode, staffno, staffname):
    f.write(
        f" {custno:<11}"
        f"{code:0>3}"
        f"{rectype:<1}"
        f"{branch:<7}"
        f"{filecode:<1}"
        f"{staffno:<9}"
        f"{staffname:<40}\n"
    )

rows = con.execute("""
    SELECT
        CUSTNO, RECTYPE, BRANCH, FILECODE, STAFFNO, STAFFNAME,
        C01,C02,C03,C04,C05,C06,C07,C08,C09,C10,
        C11,C12,C13,C14,C15,C16,C17,C18,C19,C20
    FROM mergefound
""").fetchall()

with open(OUTFILE, "w") as f:
    for r in rows:
        custno, rectype, branch, filecode, staffno, staffname = r[:6]
        codes = r[6:]

        # SAS always outputs 002
        write_update_line(f, custno, 2, rectype, branch, filecode, staffno, staffname)

        # C01 – C20 same SAS logic (no loop in SAS)
        for code in codes:
            if code not in (0, None):
                write_update_line(f, custno, code, rectype, branch, filecode, staffno, staffname)

# ------------------------------------------------
# 11. Optional Parquet Output for UPDATE
# ------------------------------------------------
update_table = con.execute("""
    SELECT * FROM mergefound
""").arrow()

pq.write_table(update_table, OUT_PARQUET)

print("Completed CICUSCD5 conversion successfully.")
