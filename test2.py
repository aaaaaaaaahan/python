import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ============================================================
# DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()

# ============================================================
# STEP 1: DUPLICATE REMOVAL (simulate ICETOOL SELECT FIRST)
# ============================================================
# Keep first occurrence based on key (ACCTNO + IND)
con.execute(f"""
    CREATE OR REPLACE TABLE borwguar_unq AS
    SELECT *
    FROM '{host_parquet_path("LIABFILE_BORWGUAR.parquet")}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO, substr(IND,1,1) ORDER BY ACCTNO) = 1
""")

# ============================================================
# STEP 2: READ RLEN FILES & FILTER
# ============================================================
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

# ============================================================
# STEP 3: READ BORWGUAR.UNQ AS LOAN AND MAP STAT TO CODE
# ============================================================
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

# ============================================================
# STEP 4: MERGE LOGIC
# ============================================================
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
        STAT
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM merge1
""".format(year=year,month=month,day=day)

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
        LPAD(CAST(CODE AS VARCHAR),3,'0') AS CODE
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM merge1
""".format(year=year,month=month,day=day)

# ============================================================
# COMPLETION LOG
# ============================================================
out = """
    SELECT
        *
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM borwguar_unq
""".format(year=year,month=month,day=day)

queries = {
    "LIABFILE_BORWGUAR_UNQ"                 : out,
    "CIS_UPDRLEN_BORWGTOR"             : outfile_table,
    "CIS_UPDRLEN_UPDATE"             : updf_table
}

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    con.execute(f"""
    COPY ({query})
    TO '{parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);  
     """)
    
    con.execute(f"""
    COPY ({query})
    TO '{csv_path}'
    (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);  
     """)
