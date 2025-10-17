import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import os

# ==========================================================
# CONFIGURATION
# ==========================================================
host_input_path = "/host/cis/parquet"  # input parquet folder
parquet_input = f"{host_input_path}/MYGST_DELTA.parquet"  # assumed converted parquet
csv_output_path = "/host/cis/output"
output_csv = f"{csv_output_path}/MYGST_DELTA_LOAD.csv"

os.makedirs(csv_output_path, exist_ok=True)

# ==========================================================
# DUCKDB PROCESSING
# ==========================================================
con = duckdb.connect()

# Register the Parquet table
con.register("mygst_parquet", parquet_input)

# Assume the raw Parquet has a single column 'RAW_LINE' with pipe-delimited data
# Split it into fields and filter IDENTIFIER='B'
query = """
SELECT 
    SPLIT_PART(RAW_LINE, '|', 1) AS IDENTIFIER,
    SPLIT_PART(RAW_LINE, '|', 2) AS MYGST_ACCTNO,
    SPLIT_PART(RAW_LINE, '|', 3) AS TAXPAYER_ID,
    SPLIT_PART(RAW_LINE, '|', 4) AS TAXPAYER_IDTYPE,
    SPLIT_PART(RAW_LINE, '|', 5) AS TAXPAYER_NAME,
    SPLIT_PART(RAW_LINE, '|', 6) AS REGISTER_DATE
FROM mygst_parquet
WHERE SPLIT_PART(RAW_LINE, '|', 1) = 'B'
"""

# Execute query and get Arrow Table
result_arrow = con.execute(query).arrow()

# ==========================================================
# OPTIONAL: PREVIEW FIRST 15 ROWS
# ==========================================================
print("Preview first 15 rows:")
print(result_arrow.slice(0, 15).to_pandas())

# ==========================================================
# WRITE OUTPUT CSV
# ==========================================================
csv.write_csv(result_arrow, output_csv)

print(f"✅ Output CSV generated: {output_csv}")
