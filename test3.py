import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ============================================================
# DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()
print("✅ DuckDB connection established")

# ============================================================
# STEP 1: DUPLICATE REMOVAL (simulate ICETOOL SELECT FIRST)
# ============================================================
print("▶ Step 1: Removing duplicates from LIABFILE_BORWGUAR...")
con.execute(f"""
    CREATE OR REPLACE TABLE borwguar_unq AS
    SELECT *
    FROM '{host_parquet_path("LIABFILE_BORWGUAR.parquet")}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO, substr(IND,1,1) ORDER BY ACCTNO) = 1
""")
count1 = con.execute("SELECT COUNT(*) FROM borwguar_unq").fetchone()[0]
print(f"✅ Step 1 completed: {count1:,} unique records in borwguar_unq")

# ============================================================
# STEP 2: READ RLEN FILES & FILTER
# ============================================================
print("▶ Step 2: Reading and filtering RLEN files...")
con.execute(f"""
    CREATE OR REPLACE VIEW rlen_files AS 
    SELECT * FROM '{host_parquet_path("RLENCA_LN02.parquet")}'
    UNION ALL
    SELECT * FROM '{host_parquet_path("RLENCA_LN08.parquet")}'
""")

con.execute(f"""
    CREATE OR REPLACE TABLE rlen AS
    SELECT 
        U_IBS_APPL_NO   AS ACCTNO,
        C_IBS_APPL_CODE AS ACCTCODE,
        U_IBS_R_APPL_NO AS CUSTNO,
        try_cast(C_IBS_E1_TO_E2 AS INTEGER) AS RLENCODE,
        try_cast(C_IBS_E2_TO_E1 AS INTEGER) AS PRISEC
    FROM rlen_files
    WHERE PRISEC = 901
      AND RLENCODE IN (3,11,12,13,14,16,17,18,19,21,22,23,27,28)
""")
count2 = con.execute("SELECT COUNT(*) FROM rlen").fetchone()[0]
print(f"✅ Step 2 completed: {count2:,} filtered records in rlen")

# ============================================================
# STEP 3: READ BORWGUAR.UNQ AS LOAN AND MAP STAT TO CODE
# ============================================================
print("▶ Step 3: Mapping STAT to CODE in loan table...")
con.execute(f"""
    CREATE OR REPLACE TABLE loan AS
    SELECT 
        substr(ACCTNO,1,11) AS ACCTNO,
        substr(IND,1,3) AS STAT,
        CASE 
            WHEN trim(IND) = 'B' THEN 20
            WHEN trim(IND) = 'G' THEN 17
            WHEN trim(IND) = 'B/G' THEN 28
            ELSE NULL
        END AS CODE
    FROM borwguar_unq
    WHERE IND IN ('B','G','B/G')
""")
count3 = con.execute("SELECT COUNT(*) FROM loan").fetchone()[0]
print(f"✅ Step 3 completed: {count3:,} loan records created")

# ============================================================
# STEP 4: MERGE LOGIC
# ============================================================
print("▶ Step 4: Joining and filtering merge1 table...")
con.execute("""
    CREATE OR REPLACE TABLE merge1 AS
    SELECT 
        l.ACCTNO,
        r.ACCTCODE,
        r.CUSTNO,
        r.RLENCODE,
        l.CODE,
        l.STAT
    FROM loan l
    JOIN rlen r ON l.ACCTNO = r.ACCTNO
    WHERE NOT (
        l.CODE = r.RLENCODE
        OR (r.RLENCODE = 21 AND l.CODE = 17)
    )
""")
count4 = con.execute("SELECT COUNT(*) FROM merge1").fetchone()[0]
print(f"✅ Step 4 completed: {count4:,} merged records in merge1")

# ============================================================
# STEP 5: OUTPUT (SHOW BEFORE & AFTER EFFECT)
# ============================================================
outfile_table = """
    SELECT 
        ACCTCODE,
        ACCTNO,
        CUSTNO,
        LPAD(CAST(RLENCODE AS VARCHAR),3,'0') AS RLENCODE,
        LPAD(CAST(CODE AS VARCHAR),3,'0') AS CODE,
        STAT,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM merge1
""".format(year=year, month=month, day=day)

# ============================================================
# STEP 6: OUTPUT FOR UPDATE (CIUPDRLN)
# ============================================================
updf_table = """
    SELECT 
        '033' AS CONST1,
        ACCTCODE,
        ACCTNO,
        'CUST ' AS CONST2,
        CUSTNO,
        '901' AS CONST3,
        LPAD(CAST(CODE AS VARCHAR),3,'0') AS CODE,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM merge1
""".format(year=year, month=month, day=day)

# ============================================================
# COMPLETION LOG
# ============================================================
out = """
    SELECT
        *,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM borwguar_unq
""".format(year=year, month=month, day=day)

queries = {
    "LIABFILE_BORWGUAR_UNQ": out,
    "CIS_UPDRLEN_BORWGTOR": outfile_table,
    "CIS_UPDRLEN_UPDATE": updf_table
}

print("▶ Step 5 & 6: Exporting outputs to Parquet and CSV...")

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    # Export to Parquet
    con.execute(f"""
    COPY ({query})
    TO '{parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
    """)

    # Export to CSV
    con.execute(f"""
    COPY ({query})
    TO '{csv_path}'
    (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);
    """)

    record_count = con.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
    print(f"📦 {name}: {record_count:,} rows exported → Parquet & CSV")

print("✅ CCRSRLEB Python Conversion Completed Successfully!")
