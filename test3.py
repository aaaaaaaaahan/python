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

# ------------------------------------------------
# 5. HRMS (converted from CSV earlier)
# ------------------------------------------------
con.execute(f"""CREATE TABLE hrms2 AS 
    SELECT
        * ,
        'B' AS FILECODE,        
        LPAD(CAST(ACCTNO AS VARCHAR),11, '0') AS ACCTNOC
    FROM read_parquet('{host_parquet_path("HCMS_STAFF_TAG.parquet")}') 
    ORDER BY ACCTNOC
""")

# ------------------------------------------------
# 6. MERGE HRMS + CUSTACCT
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
# 6a. EXPAND mergefound for TXT/Parquet same rows (C01–C20)
# ------------------------------------------------
con.execute("""
    CREATE TABLE mergefound_expanded AS
    SELECT
        custno, rectype, branch, filecode, staffno, staffname,
        code
    FROM mergefound,
    UNNEST(ARRAY[C01,C02,C03,C04,C05,C06,C07,C08,C09,C10,
                C11,C12,C13,C14,C15,C16,C17,C18,C19,C20]) AS t(code)
    WHERE code IS NOT NULL AND code != 0
""")

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
            custno, LPAD(CAST(code AS VARCHAR),3,'0') AS code,
            rectype, branch, filecode, staffno, staffname,
            {year} AS year, {month} AS month, {day} AS day
        FROM mergefound_expanded
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
                    f" {custno:<19}"
                    f"{code:0>3}"
                    f"{rectype:<1}"
                    f"{branch:<7}"
                    f"{filecode:<1}"
                    f"{staffno:<9}"
                    f"{staffname:<40}\n"
                )

            for _, row in df.iterrows():
                custno, code, rectype, branch, filecode, staffno, staffname = row[:7]
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
                else:  # DP_TEMP
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

# =====================================================
# RE-SORT CUSTCODE AND REFORMAT TO FIT PROGRAM CIUPDCCD
# =====================================================

# ---------------------------
# Step 1: Read Parquet
# ---------------------------
con.execute(f"""
CREATE OR REPLACE TABLE temp1 AS
SELECT 
    *,
    code AS F1,
FROM mergefound_expanded
""")

# ---------------------------
# Step 2: Sort and remove duplicates
# ---------------------------
con.execute("""
CREATE OR REPLACE TABLE temp_sorted AS
SELECT DISTINCT CUSTNO, F1, RECTYPE, BRANCH, FILECODE, STAFFNO, STAFFNAME
FROM temp1
ORDER BY CUSTNO, F1
""")

# ---------------------------
# Step 3: Array-like columns W1-W20
# We assign row numbers per CUSTNO and pivot F1 into W1-W10
# ---------------------------
con.execute("""
CREATE OR REPLACE TABLE temp2 AS
WITH numbered AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY F1) AS rn
    FROM temp_sorted
)
SELECT
    CUSTNO,
    RECTYPE,
    BRANCH,
    FILECODE,
    STAFFNO,
    STAFFNAME,
    COALESCE(MAX(CASE WHEN rn=1 THEN F1 END),0) AS W1,
    COALESCE(MAX(CASE WHEN rn=2 THEN F1 END),0) AS W2,
    COALESCE(MAX(CASE WHEN rn=3 THEN F1 END),0) AS W3,
    COALESCE(MAX(CASE WHEN rn=4 THEN F1 END),0) AS W4,
    COALESCE(MAX(CASE WHEN rn=5 THEN F1 END),0) AS W5,
    COALESCE(MAX(CASE WHEN rn=6 THEN F1 END),0) AS W6,
    COALESCE(MAX(CASE WHEN rn=7 THEN F1 END),0) AS W7,
    COALESCE(MAX(CASE WHEN rn=8 THEN F1 END),0) AS W8,
    COALESCE(MAX(CASE WHEN rn=9 THEN F1 END),0) AS W9,
    COALESCE(MAX(CASE WHEN rn=10 THEN F1 END),0) AS W10,
    0 AS W11,
    0 AS W12,
    0 AS W13,
    0 AS W14,
    0 AS W15,
    0 AS W16,
    0 AS W17,
    0 AS W18,
    0 AS W19,
    0 AS W20
FROM numbered
GROUP BY CUSTNO, RECTYPE, BRANCH, FILECODE, STAFFNO, STAFFNAME
ORDER BY CUSTNO
""")

# ---------------------------
# Step 4: Export Parquet
# ---------------------------
con.execute(f"COPY temp2 TO '{output_parquet}' (FORMAT PARQUET)")

# ---------------------------
# Step 5: Export fixed-width TXT
# ---------------------------
# Use DuckDB string formatting to mimic SAS PUT Z3. and fixed-width columns
query_txt = """
SELECT
    lpad(CUSTNO,11,' ') ||
    lpad(RECTYPE,1,' ') ||
    lpad(BRANCH,7,' ') ||
    lpad(W1,3,'0') || lpad(W2,3,'0') || lpad(W3,3,'0') || lpad(W4,3,'0') || lpad(W5,3,'0') ||
    lpad(W6,3,'0') || lpad(W7,3,'0') || lpad(W8,3,'0') || lpad(W9,3,'0') || lpad(W10,3,'0') ||
    lpad(W11,3,'0') || lpad(W12,3,'0') || lpad(W13,3,'0') || lpad(W14,3,'0') || lpad(W15,3,'0') ||
    lpad(W16,3,'0') || lpad(W17,3,'0') || lpad(W18,3,'0') || lpad(W19,3,'0') || lpad(W20,3,'0') ||
    lpad(FILECODE,1,' ') ||
    lpad(STAFFNO,9,' ') ||
    lpad(STAFFNAME,40,' ')
FROM temp2
"""

# Fetch as Python list and write to file
result = con.execute(query_txt).fetchall()
with open(output_txt, 'w') as f:
    for row in result:
        f.write(row[0] + '\n')
