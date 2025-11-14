import os
import pyarrow.parquet as pq
import pyarrow as pa
import duckdb
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import math

# --------------------------
# Configuration
# --------------------------
folder = "input_parquets"  # folder containing Parquet files
log_file = os.path.join(folder, "cleaning_log.txt")

# Empty markers
markers = ['NULL', 'NIL', 'NaN']  # uppercased for case-insensitive match

# Number of parallel workers
max_workers = 4

# --------------------------
# Setup DuckDB connection
# --------------------------
con = duckdb.connect()

# --------------------------
# Logging function
# --------------------------
def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(message)

# --------------------------
# Helper function to detect empty values
# --------------------------
def is_empty_value(val):
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    if isinstance(val, bytes):
        # Treat SAS-style all-zero bytes as empty
        if val.hex() == '000000':
            return True
        val = val.decode('utf-8', errors='ignore').strip()
    if isinstance(val, str):
        val = val.strip().upper()
        if val == '' or val in markers:
            return True
    return False

# --------------------------
# Check if a file needs cleaning
# --------------------------
def needs_cleaning(file_path):
    table = pq.read_table(file_path)
    for col in table.schema.names:
        if pa.types.is_string(table.schema.field(col).type):
            if any(is_empty_value(x) for x in table[col].to_pylist()):
                return True
    return False

# --------------------------
# Clean a single Parquet file
# --------------------------
def clean_file(file_name):
    file_path = os.path.join(folder, file_name)

    try:
        if not needs_cleaning(file_path):
            log(f"Skipping {file_name}: no empty markers found")
            return

        schema = pq.read_schema(file_path)
        col_exprs = []

        for col in schema.names:
            if pa.types.is_string(schema.field(col).type):
                # DuckDB SQL expression to clean string columns
                expr = f"""
                CASE 
                    WHEN {col} IS NULL
                         OR UPPER({col}) IN {tuple(markers)}
                         OR LENGTH(TRIM(REPLACE({col}, '\\x00', ''))) = 0
                    THEN ''
                    ELSE {col}
                END AS {col}
                """
            else:
                expr = col
            col_exprs.append(expr)

        temp_file = file_path + ".tmp"

        # Export cleaned data directly to Parquet
        sql = f"""
        COPY (
            SELECT {', '.join(col_exprs)}
            FROM read_parquet('{file_path}')
        ) TO '{temp_file}' (FORMAT PARQUET);
        """
        con.execute(sql)

        # Overwrite original file
        os.replace(temp_file, file_path)
        log(f"Cleaned {file_name}")

    except Exception as e:
        log(f"Error processing {file_name}: {e}")

# --------------------------
# Process all Parquet files in parallel
# --------------------------
if __name__ == "__main__":
    files = [f for f in os.listdir(folder) if f.endswith(".parquet")]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(clean_file, files)

    con.close()
    log("All done!")
