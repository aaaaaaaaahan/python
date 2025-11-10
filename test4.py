#detica_cust_acctbrch is generate from CIDETBRL
#excek mark as complete implement but no found in python server

import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

# ---------------------------------------------------------------------
# Step 3: Connect to DuckDB
# ---------------------------------------------------------------------
con = duckdb.connect()

# Register input Parquet as a DuckDB table
con.execute(f"""
    CREATE TABLE all_job AS
    SELECT 
        *
    FROM '{host_parquet_path("all_job.parquet")}'
""")

con.execute(f"""
    CREATE TABLE cis_job AS
    SELECT 
        *
    FROM '{host_parquet_path("cis_job.parquet")}'
""")

# ---------------------------------------------------------------------
# Step 4: Filter and deduplicate (SAS equivalent logic)
# ---------------------------------------------------------------------
query = f"""
    SELECT DISTINCT
        *
    FROM cis_job A
    JOIN all_job B
    WHERE A.JOBNAME = B.JOBNAME
"""
df = con.execute(query).fetchdf()

# ---------------------------------------------------------------------
# Step 6: Write to TXT (format same as SAS PUT)
# Columns layout:
# @001 CUSTNO(11)
# @021 ACCTCODE(5)
# @026 ACCTNOX(20)
# @046 OPENDX(10)
# ---------------------------------------------------------------------
txt_path = csv_output_path(f"SAS_JOB").replace(".csv", ".txt")

res = con.execute(query)
columns = [desc[0] for desc in res.description]
rows = res.fetchall()

with open(txt_path, "w", encoding="utf-8") as f:
    for _, row in df.iterrows():
        line = (
            f"{str(row['JOBNAME']).ljust(11)}"
        )
        f.write(line + "\n")

print("✅ Processing completed successfully!")
