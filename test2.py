import duckdb
import pandas as pd

# ---------------------------
# Paths
# ---------------------------
input_parquet = 'RBP2.B033.CICUSCD5.UPDATE.parquet'
output_parquet = 'RBP2.B033.CICUSCD5.UPDATE.SORT.parquet'
output_txt = 'RBP2.B033.CICUSCD5.UPDATE.SORT.txt'

# ---------------------------
# Connect to DuckDB
# ---------------------------
con = duckdb.connect()

# ---------------------------
# Step 1: Read Parquet
# ---------------------------
con.execute(f"""
CREATE OR REPLACE TABLE temp1 AS
SELECT *
FROM read_parquet('{input_parquet}')
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

con.close()
