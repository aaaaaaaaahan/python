import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from datetime import datetime, timedelta
import os

# -------------------------------
# Configuration / Paths
# -------------------------------
input_parquet_path = "CIBMSYST_PENDING.parquet"   # Input file already converted
output_parquet_path = "OUT_PENDING.parquet"
output_txt_path = "OUT_PENDING.txt"

# -------------------------------
# Get today's date in YYMMDD format
# -------------------------------
today_dt = datetime.today().strftime("%Y-%m-%d")  # Equivalent to SAS &TODAYDT

# -------------------------------
# DuckDB connection
# -------------------------------
con = duckdb.connect(database=':memory:')

# -------------------------------
# Read input Parquet
# -------------------------------
con.execute(f"""
    CREATE TABLE pending AS
    SELECT *
    FROM read_parquet('{input_parquet_path}')
""")

# -------------------------------
# Process data: filter and increment
# -------------------------------
con.execute(f"""
    CREATE TABLE processed AS
    SELECT *,
           PERIOD_OVERDUEX + 1 AS PERIOD_OVERDUE
    FROM pending
    WHERE LOAD_DATE <> '{today_dt}'
""")

# -------------------------------
# Output to Parquet
# -------------------------------
con.execute(f"""
    COPY processed TO '{output_parquet_path}' (FORMAT PARQUET)
""")

# -------------------------------
# Output to TXT (fixed width like SAS PUT)
# -------------------------------
df = con.execute("SELECT LOAD_DATE, BRANCHNO, APPL_CODE, ACCTNOC, PERIOD_OVERDUE FROM processed").df()

with open(output_txt_path, 'w') as f:
    for _, row in df.iterrows():
        line = (
            f"{str(row['LOAD_DATE']).ljust(10)}"
            f"{str(row['BRANCHNO']).ljust(3)}"
            f"{str(row['APPL_CODE']).ljust(5)}"
            f"{str(row['ACCTNOC']).ljust(20)}"
            f"{str(row['PERIOD_OVERDUE']).zfill(3)}"
        )
        f.write(line + "\n")

print("Processing complete!")
