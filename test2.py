# ---------------------------------------------------------------------
# CIHRCFZP Job Conversion to Python
# DuckDB for processing | PyArrow for I/O
# ---------------------------------------------------------------------

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
import os

# ---------------------------------------------------------------------
# Step 1: Setup file paths
# ---------------------------------------------------------------------
input_path = "CIDOWFZT.parquet"       # assumed converted from FB dataset
output_parquet = "CIHRCFZP_EXCEL.parquet"
output_txt = "CIHRCFZP_EXCEL.txt"

# ---------------------------------------------------------------------
# Step 2: Connect to DuckDB and read input parquet
# ---------------------------------------------------------------------
con = duckdb.connect()

con.execute(f"""
    CREATE TABLE CIDOWFZT AS 
    SELECT * FROM read_parquet('{input_path}')
""")

# ---------------------------------------------------------------------
# Step 3: Apply filters (similar to SAS conditions)
# ---------------------------------------------------------------------
filtered = con.execute("""
    SELECT *
    FROM CIDOWFZT
    WHERE SOURCE = 'ACCTOPEN'
      AND SCREENDATE10 > '2025-01-01'
    ORDER BY BRANCHABBRV, SCREENDATE
""").arrow()

# ---------------------------------------------------------------------
# Step 4: Write to Parquet
# ---------------------------------------------------------------------
pq.write_table(filtered, output_parquet)
print(f"✅ Parquet output created: {output_parquet}")

# ---------------------------------------------------------------------
# Step 5: Write to TXT with title and header
# ---------------------------------------------------------------------
# Define header title
title = "DETAIL LISTING FOR CIDOWFZT"
delimiter = "|"

# Get column names
columns = filtered.column_names

# Prepare rows
rows = []
for i in range(filtered.num_rows):
    record = [str(filtered.column(c)[i].as_py() if filtered.column(c)[i].as_py() is not None else "") for c in range(len(columns))]
    rows.append(delimiter.join(record))

# Write TXT output
with open(output_txt, "w", encoding="utf-8") as f:
    f.write(f"{title}\n")
    f.write(delimiter.join(columns) + "\n")
    for row in rows:
        f.write(row + "\n")

print(f"✅ TXT output created: {output_txt}")

# ---------------------------------------------------------------------
# Step 6: Optional — show sample output
# ---------------------------------------------------------------------
print("\nSample rows:")
print(con.execute("SELECT * FROM CIDOWFZT LIMIT 5").df())
