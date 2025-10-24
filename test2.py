import duckdb
import pyarrow.parquet as pq
import os
from CIS_PY_READER_copy import host_parquet_path, parquet_output_path, csv_output_path

# ============================================================
# INITIAL SETUP
# ============================================================
con = duckdb.connect()

# ============================================================
# GET INPUT PARQUET PATHS USING HELPER
# ============================================================
# Retrieve all relevant SIGNATOR files (CA*, FD*, SA*)
signator_files = []
for prefix in ["WINDOW.SIGNATOR.CA05", "WINDOW.SIGNATOR.FD05", "WINDOW.SIGNATOR.SA05"]:
    paths = host_parquet_path(prefix)
    if paths:
        signator_files.extend(paths)

if not signator_files:
    raise FileNotFoundError("No SIGNATOR parquet files found via helper.")

# Retrieve branch file path
branch_paths = host_parquet_path("PBB.BRANCH")
if not branch_paths:
    raise FileNotFoundError("Branch parquet not found via helper.")
branch_file = branch_paths[0]

# ============================================================
# LOAD PARQUET INTO DUCKDB TABLES
# ============================================================
signator_union = " UNION ALL ".join([f"SELECT * FROM read_parquet('{f}')" for f in signator_files])
con.execute(f"CREATE OR REPLACE TABLE SIGNATOR AS {signator_union}")

con.execute(f"CREATE OR REPLACE TABLE PBBBRCH AS SELECT * FROM read_parquet('{branch_file}')")

# ============================================================
# SIGNATORY CLEANSING (SAS DATA STEP LOGIC)
# ============================================================
signatory_query = """
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
"""

con.execute("CREATE OR REPLACE TABLE SIGNATORY AS " + signatory_query)
con.execute("CREATE OR REPLACE TABLE SIGNATORY_SORTED AS SELECT * FROM SIGNATORY ORDER BY BRANCHNO")

# ============================================================
# BRANCH PROCESSING
# ============================================================
con.execute("""
CREATE OR REPLACE TABLE PBBBRCH_SORTED AS 
SELECT BRANCHNO, ACCTBRABBR FROM PBBBRCH ORDER BY BRANCHNO
""")

# ============================================================
# MERGE SIGNATORY + BRANCH
# ============================================================
merge_query = """
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
FROM SIGNATORY_SORTED A
JOIN PBBBRCH_SORTED B
ON A.BRANCHNO = B.BRANCHNO
"""
con.execute("CREATE OR REPLACE TABLE MERGE1 AS " + merge_query)

# ============================================================
# OUTPUT FILES (USING HELPER)
# ============================================================
parquet_out = parquet_output_path("WINDOW.SIGNATOR.LOAD.parquet")
csv_out = csv_output_path("WINDOW.SIGNATOR.LOAD.csv")

con.execute(f"COPY MERGE1 TO '{parquet_out}' (FORMAT PARQUET)")
con.execute(f"COPY MERGE1 TO '{csv_out}' (HEADER, DELIMITER ',')")

print("✅ SIGNATORY processing completed.")
print(f"Output Parquet: {parquet_out}")
print(f"Output CSV: {csv_out}")

# Optional preview
print(con.execute("SELECT * FROM MERGE1 LIMIT 10").fetchdf())
