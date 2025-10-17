import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import os
from datetime import datetime

# ==========================================================
# CONFIGURATION
# ==========================================================
host_input_path = "/host/cis/parquet"  # input parquet folder
parquet_input = f"{host_input_path}/MYGST_DELTA.parquet"  # assumed converted parquet
csv_output_path = "/host/cis/output"
output_csv = f"{csv_output_path}/MYGST_DELTA_LOAD.csv"

# Ensure output directory exists
os.makedirs(csv_output_path, exist_ok=True)

# ==========================================================
# DUCKDB PROCESSING
# ==========================================================
con = duckdb.connect()

# Register the parquet file for query
con.register("mygst_parquet", parquet_input)

# Equivalent to SAS IF IDENTIFIER='B';
query = """
SELECT 
    MYGST_ACCTNO,
    TAXPAYER_ID,
    TAXPAYER_IDTYPE,
    TAXPAYER_NAME,
    REGISTER_DATE
FROM mygst_parquet
WHERE IDENTIFIER = 'B'
"""

# Execute the query
result_arrow = con.execute(query).arrow()

# ==========================================================
# OPTIONAL: PREVIEW FIRST 15 ROWS (Equivalent to PROC PRINT)
# ==========================================================
print("Preview first 15 rows:")
print(result_arrow.slice(0, 15).to_pandas())

# ==========================================================
# WRITE OUTPUT USING PYARROW
# ==========================================================
csv.write_csv(
    result_arrow,
    output_csv
)

print(f"✅ Output CSV generated: {output_csv}")
