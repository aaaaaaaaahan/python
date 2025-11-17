import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet
import datetime

batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")

# ------------------------------------------------
# 1. Connect to DuckDB
# ------------------------------------------------
con = duckdb.connect()
cis = get_hive_parquet('CIS_CUST_DAILY')

# ------------------------------------------------
# 2. Build CUST (ACCTCODE='DP' AND PRISEC = 901)
# ------------------------------------------------
con.execute(f"""
    CREATE TABLE cust AS
    SELECT DISTINCT ON(ACCTNOC)
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

print("Cust Daily:")
print(con.execute("SELECT * FROM cust LIMIT 500").fetchdf())

# ------------------------------------------------
# 3. Sort HRC (C01–C20 from HRCCODES)
# ------------------------------------------------
con.execute(f"""
    CREATE TABLE hrc_sorted AS
    SELECT 
        CUSTNO,
        CUSTTYPE AS RECTYPE,
        BRANCH,
        SUBSTR(HRCCODES,  1, 3)::INTEGER AS C01,
        SUBSTR(HRCCODES,  4, 3)::INTEGER AS C02,
        SUBSTR(HRCCODES,  7, 3)::INTEGER AS C03,
        SUBSTR(HRCCODES, 10, 3)::INTEGER AS C04,
        SUBSTR(HRCCODES, 13, 3)::INTEGER AS C05,
        SUBSTR(HRCCODES, 16, 3)::INTEGER AS C06,
        SUBSTR(HRCCODES, 19, 3)::INTEGER AS C07,
        SUBSTR(HRCCODES, 22, 3)::INTEGER AS C08,
        SUBSTR(HRCCODES, 25, 3)::INTEGER AS C09,
        SUBSTR(HRCCODES, 28, 3)::INTEGER AS C10,
        SUBSTR(HRCCODES, 31, 3)::INTEGER AS C11,
        SUBSTR(HRCCODES, 34, 3)::INTEGER AS C12,
        SUBSTR(HRCCODES, 37, 3)::INTEGER AS C13,
        SUBSTR(HRCCODES, 40, 3)::INTEGER AS C14,
        SUBSTR(HRCCODES, 43, 3)::INTEGER AS C15,
        SUBSTR(HRCCODES, 46, 3)::INTEGER AS C16,
        SUBSTR(HRCCODES, 49, 3)::INTEGER AS C17,
        SUBSTR(HRCCODES, 52, 3)::INTEGER AS C18,
        SUBSTR(HRCCODES, 55, 3)::INTEGER AS C19,
        SUBSTR(HRCCODES, 58, 3)::INTEGER AS C20
    FROM read_parquet('{host_parquet_path("CUSTCODE.parquet")}')
    ORDER BY CUSTNO
""")

print("Custcode:")
print(con.execute("SELECT * FROM hrc_sorted LIMIT 500").fetchdf())

# ------------------------------------------------
# 4. Merge HRC + CUST → CUSTACCT
# ------------------------------------------------
con.execute("""
    CREATE TABLE custacct AS
    SELECT *
    FROM hrc_sorted h
    LEFT JOIN cust c USING (CUSTNO)
    WHERE c.CUSTNO IS NOT NULL
""")
con.execute("CREATE TABLE custacct2 AS SELECT * FROM custacct ORDER BY ACCTNOC")

print("HRC + Cust Daily:")
print(con.execute("SELECT * FROM custacct2 LIMIT 500").fetchdf())

# ------------------------------------------------
# 5. HRMS (converted from CSV earlier)
# ------------------------------------------------
con.execute(f"""CREATE TABLE hrms2 AS 
    SELECT
        *,
        'B' AS FILECODE,
        ACCTNO AS ACCTNOC
    FROM read_parquet('{host_parquet_path("HCMS_STAFF_TAG.parquet")}') 
    ORDER BY ACCTNOC
""")

print("Staff Tag:")
print(con.execute("SELECT * FROM hrms2 LIMIT 500").fetchdf())

# ------------------------------------------------
# 6. MERGE HRMS + CUSTACCT
# ------------------------------------------------
con.execute("""
    CREATE TABLE mergefound AS
    SELECT *
    FROM hrms2 h
    INNER JOIN custacct2 c USING(ACCTNOC)
""")

print("Merge Found:")
print(con.execute("SELECT * FROM mergefound LIMIT 500").fetchdf())

con.execute("""
    CREATE TABLE mergexmtch AS
    SELECT *
    FROM hrms2 h
    LEFT JOIN custacct2 c USING(ACCTNOC)
    WHERE c.CUSTNO IS NULL
""")

print("Merge X Found:")
print(con.execute("SELECT * FROM mergexmtch LIMIT 500").fetchdf())

# ------------------------------------------------
# 7. TXT, CSV & Parquet OUTPUT (3 files)
# ------------------------------------------------
files = {
    "CICUSCD5_NOTFND": """
        SELECT 
            ORGCODE,
            STAFFNO,
            STAFFNAME,
            ACCTNOC,
            NEWIC,
            OLDIC,
            BRANCHCODE, 
            {year} AS year,
            {month} AS month,
            {day} AS day
        FROM mergexmtch
        ORDER BY ORGCODE, STAFFNAME, ACCTNOC
    """.format(year=year,month=month,day=day),
    "CICUSCD5_UPDATE_DP_TEMP": """
        SELECT 
            STAFFNO, CUSTNO, ACCTCODE, ACCTNOC, JOINTACC, STAFFNAME, BRANCHCODE, {year} AS year, {month} AS month, {day} AS day
        FROM mergefound
        ORDER BY STAFFNO, CUSTNO, ACCTCODE, ACCTNOC, JOINTACC
    """.format(year=year,month=month,day=day),
    "CICUSCD5_UPDATE": """
        SELECT
            CUSTNO, RECTYPE, BRANCH, FILECODE, STAFFNO, STAFFNAME,
            C01,C02,C03,C04,C05,C06,C07,C08,C09,C10,
            C11,C12,C13,C14,C15,C16,C17,C18,C19,C20,
            {year} AS year, {month} AS month, {day} AS day
        FROM mergefound
    """.format(year=year,month=month,day=day)
}

for name, query in files.items():
    # ---------------- TXT
    txt_path = csv_output_path(f"{name}_{report_date}").replace(".csv", ".txt")
    df = con.execute(query).fetchdf()
    with open(txt_path, "w", encoding="utf-8") as f:
        if name == "CICUSCD5_UPDATE":
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

            for _, row in df.iterrows():
                custno, rectype, branch, filecode, staffno, staffname = row[:6]
                codes = row[6:]
                write_update_line(f, custno, 2, rectype, branch, filecode, staffno, staffname)
                for code in codes:
                    if code not in (0, None):
                        write_update_line(f, custno, code, rectype, branch, filecode, staffno, staffname)
        else:
            for _, row in df.iterrows():
                if name == "CICUSCD5_NOTFND":
                    line = (
                        f"{str(row['ORGCODE']).ljust(3)}"
                        f"{str(row['STAFFNO']).ljust(9)}"
                        f"{str(row['STAFFNAME']).ljust(40)}"
                        f"{str(row['ACCTNOC']).ljust(11)}"
                        f"{str(row['NEWIC']).ljust(12)}"
                        f"{str(row['OLDIC']).ljust(10)}"
                        f"{str(row['BRANCHCODE']).ljust(3)}"
                    )
                else:  # DPFILE
                    line = (
                        f"{str(row['STAFFNO']).ljust(9)}"
                        f"{str(row['CUSTNO']).ljust(20)}"
                        f"{str(row['ACCTCODE']).ljust(5)}"
                        f"{str(row['ACCTNOC']).ljust(11)}"
                        f"{str(row['JOINTACC']).ljust(1)}"
                        f"{str(row['STAFFNAME']).ljust(40)}"
                        f"{str(row['BRANCHCODE']).ljust(3)}"
                    )
                f.write(line + "\n")

    # ---------------- CSV
    csv_path = csv_output_path(name)
    con.execute(f"""
        COPY ({query})
        TO '{csv_path}'
        (FORMAT CSV, HEADER, DELIMITER ';', OVERWRITE_OR_IGNORE TRUE)
    """)

    # ---------------- Parquet
    parquet_path = parquet_output_path(name)
    con.execute(f"""
        COPY ({query})
        TO '{parquet_path}'
        (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE TRUE)
    """)

print("Completed full processing: TXT, CSV, and Parquet for 3 files (NOTFND, DPFILE, OUTFILE).")
