import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
import datetime
import os

# ---------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
report_date = batch_date.strftime("%d-%m-%Y")

# Input and output paths
input_parquet = host_parquet_path("CIS_INNAMEKY_FAIL.parquet")
backup_parquet = parquet_output_path(f"CIS_INNAMEKY_FBKP_{report_date}.parquet")
output_txt = csv_output_path(f"CIINCKEY_FAILEDREC_{report_date}").replace(".csv", ".txt")

# ---------------------------------------------------------------------
# STEP 1: BACKUP FAILED RECORDS
# ---------------------------------------------------------------------
if os.path.exists(input_parquet):
    table = pq.read_table(input_parquet)
    pq.write_table(table, backup_parquet)
    print(f"✅ Backup created: {backup_parquet}")
else:
    raise FileNotFoundError(f"Input parquet not found: {input_parquet}")

# ---------------------------------------------------------------------
# STEP 2: PROCESS EMPLOYER INFORMATION
# ---------------------------------------------------------------------
con = duckdb.connect()

# Read parquet into DuckDB
con.execute(f"""
    CREATE OR REPLACE TABLE CIINCKEY AS 
    SELECT * FROM read_parquet('{input_parquet}')
""")

# Check if the table has records
row_count = con.execute("SELECT COUNT(*) FROM CIINCKEY").fetchone()[0]

if row_count == 0:
    raise SystemExit("❌ No records found in CIINCKEY (ABORT 77)")

# Retrieve first 100 records for inspection
preview_df = con.execute("SELECT * FROM CIINCKEY LIMIT 100").fetch_df()

# Save preview as text (to mimic PROC PRINT)
preview_text = preview_df.to_string(index=False)
with open(output_txt, "w", encoding="utf-8") as f:
    f.write("FAILED REC\n")
    f.write(preview_text)

print(f"✅ First 100 failed records saved to {output_txt}")
print(f"✅ Total records in CIINCKEY: {row_count}")
