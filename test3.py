import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")

# =========================================================
# CONFIG
# =========================================================
#INPUT1 = "/host/input/WINDOW.SIGNATOR.CA0801.parquet"        # contains ACCTNO, C1, C2, C3, C4
#SORT_OUT = "/host/output/WINDOW.SIGNATOR.CA0801.SORT.parquet"

#BRANCH_FILE = "/host/input/PBB.BRANCH.parquet"               # fields: BRANCH, BRHABV
#MERGED_TXT = "/host/output/WINDOW.SIGNATOR.MERGED.txt"
#MERGED_PARQUET = "/host/output/WINDOW.SIGNATOR.MERGED.parquet"

con = duckdb.connect()


# =========================================================
# STEP 1 — READ INPUT (HORIZONTAL)
# =========================================================
con.execute(f"""
    CREATE TABLE ACCT AS
    SELECT * FROM '{host_parquet_path("WINDOW_SIGNATOR_CA0801.parquet")}'
""")


# =========================================================
# STEP 2 — TRANSPOSE (HORIZONTAL → VERTICAL)
# =========================================================
con.execute("""
    CREATE TABLE SORTED AS
    SELECT 
        ACCTNO,
        UNNEST([C1, C2, C3, C4]) AS NOMINEE
    FROM ACCT
""")

# =========================================================
# STEP 3 — READ STATUS + BRANCH INFO (FROM CA0801 AGAIN)
# =========================================================
# Your CA0801 input does NOT include STATUS/BRANCH fields.
# You MUST confirm field names. I assume your parquet contains:

# ACCTNO, STATUS, BRANCH

con.execute(f"""
    CREATE TABLE NOMIIN AS
    SELECT 
        ACCTNO,
        STATUS,
        BRANCH
    FROM '{host_parquet_path("WINDOW_SIGNATOR_CA0801.parquet")}'
""")


# =========================================================
# STEP 4 — READ BRANCH FILE
# =========================================================
# Branch parquet contains: BRANCH, BRHABV

con.execute(f"""
    CREATE TABLE BRHTABLE AS
    SELECT 
        BRANCHNO AS BRANCH,
        ACCTBRABBR AS BRHABV
    FROM '{host_parquet_path("PBBBRCH.parquet")}'
""")


# =========================================================
# STEP 5 — MERGE NOMIIN + BRANCH FILE
# =========================================================
con.execute("""
    CREATE TABLE NOM_BRANCH AS
    SELECT A.ACCTNO, A.STATUS, A.BRANCH, B.BRHABV
    FROM NOMIIN A 
    LEFT JOIN BRHTABLE B USING (BRANCH)
""")


# =========================================================
# STEP 6 — READ SORTED (VERTICAL) AND SPLIT OUTPUT
# =========================================================
# OUTPUT contains: "NAME + space + IC"
# Example: "ALI BIN AHMAD 900101015555A"

con.execute("""
    CREATE TABLE NOMIIN2 AS
    SELECT
        ACCTNO,
        LEFT(NOMINEE, 40) AS NAME,
        SUBSTR(NOMINEE, 41, 30) AS IC_NUMBER
    FROM SORTED
    WHERE TRIM(NOMINEE) <> ''
""")

# Remove missing data
con.execute("""
    DELETE FROM NOMIIN2
    WHERE TRIM(NAME)='' OR TRIM(IC_NUMBER)=''
""")


# =========================================================
# STEP 7 — MATCH TO PRODUCE FINAL MERGED TABLE
# =========================================================
con.execute("""
    CREATE TABLE NOM_FOUND AS
    SELECT 
        A.ACCTNO,
        A.IC_NUMBER,
        A.NAME,
        'Y' AS IND,
        B.STATUS,
        B.BRHABV,
        B.BRANCH
    FROM NOMIIN2 A
    LEFT JOIN NOM_BRANCH B USING (ACCTNO)
    WHERE B.ACCTNO IS NOT NULL
    ORDER BY A.ACCTNO
""")

# (Optional) unmatched but not required for output
con.execute("""
    CREATE TABLE NOM_XFOUND AS
    SELECT *
    FROM NOMIIN2 A
    WHERE NOT EXISTS (SELECT 1 FROM NOM_BRANCH B WHERE A.ACCTNO=B.ACCTNO)
""")

# ----------------------------
# OUTPUT TO PARQUET, CSV, TXT
# ----------------------------
sorted = f"""
    SELECT 
        *,  
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM SORTED
""".format(year=year,month=month,day=day)

merged = f"""
    SELECT 
        *,  
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM NOM_FOUND
""".format(year=year,month=month,day=day)

# Dictionary of outputs for parquet & CSV
queries = {
    "WINDOW_SIGNATOR_CA0801_SORT":              sorted,
    "WINDOW_SIGNATOR_CA0801_MERGED":            merged
    }

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    # COPY to Parquet with partitioning
    con.execute(f"""
        COPY ({query})
        TO '{parquet_path}'
        (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE TRUE)
    """)

    # COPY to CSV with header
    con.execute(f"""
        COPY ({query})
        TO '{csv_path}'
        (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE TRUE)
    """)

# Dictionary for fixed-width TXT
txt_queries = {
        "WINDOW_SIGNATOR_CA0801_MERGED":              merged
    }

for txt_name, txt_query in txt_queries.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(row['ACCTNO']).rjust(32)}"
                f"{str(row['IC_NUMBER']).rjust(42)}"
                f"{str(row['NAME']).rjust(42)}"
                f"{str(row['IND']).rjust(1)}"
                f"{str(row['STATUS']).rjust(1)}"
                f"{str(row['BRHABV']).rjust(3)}"
                f"{str(row['BRANCH']).rjust(3)}"
            )
            f.write(line + "\n")
