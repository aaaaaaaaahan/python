import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# ===================================================================
# Path Setup
# ===================================================================

custfile_parquet = host_parquet_path("CUSTDLY")       # CIS.CUST.DAILY
staffacc_parquet = host_parquet_path("STAFFACC")      # CICUSCD5.UPDATE.DP.TEMP

output_parquet = parquet_output_path("CICUSCD5_UPDATE_DP")
output_txt = csv_output_path("CICUSCD5_UPDATE_DP.txt")

# ===================================================================
# DuckDB Connection
# ===================================================================

con = duckdb.connect()

# ===================================================================
# Step 1 — CUST dataset (SAS: DATA CUST)
# ===================================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE CUST AS
    SELECT 
        CUSTNO,
        CAST(ACCTNOC AS VARCHAR) AS ACCTNOC,
        ACCTCODE,
        JOINTACC
    FROM read_parquet($1)
    WHERE ACCTCODE = 'DP'
    ORDER BY CUSTNO
""", [custfile_parquet])

# ===================================================================
# Step 2 — STAFFACC dataset (SAS INPUT + NODUPKEY)
# ===================================================================

# STAFFACC parquet already contains fields:
# STAFFNO, CUSTNO, ACCTCODE, ACCTNOC, JOINTACC, STAFFNAME, BRANCHCODE

con.execute("""
    CREATE OR REPLACE TEMP TABLE STAFFACC AS
    SELECT *
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) AS rn
        FROM read_parquet($1)
    )
    WHERE rn = 1
    ORDER BY CUSTNO
""", [staffacc_parquet])

# ===================================================================
# Step 3 — SAS MERGE logic
# MERGE CUST(IN=S) STAFFACC(IN=T) BY CUSTNO; IF T;
# Equivalent: Keep ONLY STAFFACC (right side)
# ===================================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE MERGE AS
    SELECT 
        s.STAFFNO,
        s.CUSTNO,
        s.ACCTCODE,
        s.ACCTNOC,
        s.JOINTACC,
        s.STAFFNAME,
        s.BRANCHCODE
    FROM STAFFACC s
    LEFT JOIN CUST c USING (CUSTNO)
    WHERE s.CUSTNO IS NOT NULL
    ORDER BY CUSTNO, ACCTNOC
""")

# ===================================================================
# Step 4 — Output MERGE to Parquet
# ===================================================================

merge_arrow = con.execute("SELECT * FROM MERGE").arrow()
pq.write_table(merge_arrow, output_parquet)

# ===================================================================
# Step 5 — Output fixed-width TXT (SAS PUT @nn)
# ===================================================================

def fw(s, length):
    s = "" if s is None else str(s)
    return s.ljust(length)[:length]

with open(output_txt, "w") as f:
    for row in merge_arrow.to_pylist():
        line = (
            fw(row["staffno"], 9) +
            fw(row["custno"], 20) +
            fw(row["acctcode"], 5) +
            fw(row["acctnoc"], 11) +
            fw(row["jointacc"], 1) +
            fw(row["staffname"], 40) +
            fw(row["branchcode"], 3)
        )
        f.write(line + "\n")

print("DONE — DuckDB processed everything (Parquet + TXT written).")
