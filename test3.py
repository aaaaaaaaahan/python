import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ============================================================
# INITIAL SETUP
# ============================================================
con = duckdb.connect()

# ============================================================
# LOOP GET INPUT PARQUET PATHS
# ============================================================
# Retrieve all relevant SIGNATOR files (CA*, FD*, SA*)
signator_files = []
for prefix in ["WINDOW_SIGNATOR_CA05", "WINDOW_SIGNATOR_FD05", "WINDOW_SIGNATOR_SA05"]:
    paths = host_parquet_path(prefix)
    if paths:
        signator_files.extend(paths)

if not signator_files:
    raise FileNotFoundError("No SIGNATOR parquet files found.")

# ============================================================
# LOAD PARQUET INTO DUCKDB TABLES
# ============================================================
signator_union = " UNION ALL ".join([f"SELECT * FROM read_parquet('{f}')" for f in signator_files])
con.execute(f"CREATE OR REPLACE TABLE SIGNATOR AS {signator_union}")

con.execute(f"""CREATE OR REPLACE TABLE PBBBRCH AS SELECT * FROM '{host_parquet_path("PBBBRCH.parquet")}'""")

# ============================================================
# SIGNATORY CLEANSING (SAS DATA STEP LOGIC)
# ============================================================
con.execute(f"""
    CREATE OR REPLACE TABLE signatory_sorted
    SELECT 
        BANKNO,
        ACCTNO,
        SEQNO,
        NAME,
        COALESCE(NULLIF(MANDATEE_INDC, ''), 'N') AS MANDATEE_INDC,
        REGEXP_REPLACE(IDS, '-', '') AS IDS,
        STATUS,
        BRANCHNO,
        INDORG_INDC,
        'Y' AS SIGNATORY_INDC
    FROM SIGNATOR
    WHERE STATUS = 'A'
        AND NAME <> ''
        AND IDS <> ''
    ORDER BY BRANCHNO
""")

# ============================================================
# BRANCH PROCESSING
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE PBBBRCH_SORTED
    SELECT 
        BRANCHNO, 
        ACCTBRABBR 
    FROM PBBBRCH 
    ORDER BY BRANCHNO
""")

# ============================================================
# MERGE SIGNATORY + BRANCH
# ============================================================
merge = """
    SELECT 
        A.BANKNO,
        A.ACCTNO,
        A.SEQNO,
        A.NAME,
        A.IDS,
        COALESCE(A.SIGNATORY_INDC, 'N') AS SIGNATORY_INDC,
        COALESCE(A.MANDATEE_INDC, 'N') AS MANDATEE_INDC,
        NULL AS NOMINEE_INDC,
        A.STATUS,
        A.BRANCHNO,
        B.ACCTBRABBR,
        A.INDORG_INDC
        ,{year} AS year
        ,{month} AS month
        ,{day} AS day
    FROM signatory_sorted A
    JOIN PBBBRCH_SORTED B
    ON A.BRANCHNO = B.BRANCHNO
""".format(year=year,month=month,day=day)

# ============================================================
# OUTPUT FILES (USING HELPER)
# ============================================================
queries = {
    "WINDOW_SIGNATOR_LOAD"            : merge
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
