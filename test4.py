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
folder = "/host/cis/parquet/sas_parquet_test_jkh"   # Folder containing Parquet files
log_file = "/host/cis/logs/cleaning_log.txt"

# Empty markers
markers = ['NULL', 'NIL', 'NaN']

max_workers = 4

# --------------------------
# Setup DuckDB
# --------------------------
con = duckdb.connect()

# --------------------------
# Logging
# --------------------------
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as f:
        f.write(f"[{ts}] {msg}\n")
    print(msg)

# --------------------------
# Helper: Detect empty / zero-null binary
# --------------------------
def is_empty_value(val):
    if val is None:
        return True

    if isinstance(val, float) and math.isnan(val):
        return True

    # --- NEW: handle binary ----
    if isinstance(val, bytes):
        hexv = val.hex()
        # If hex is all zeros (any length)
        if len(hexv) > 0 and set(hexv) == {"0"}:
            return True

        # Try decoding safely
        sval = val.decode("utf-8", errors="ignore").replace("\x00", "").strip()
        return sval == ""

    # --- STRING handling ----
    if isinstance(val, str):
        cleaned = val.replace("\x00", "").strip()
        if cleaned == "":
            return True
        if cleaned.upper() in markers:
            return True

    return False

# --------------------------
# Pre-check: does file NEED cleaning?
# --------------------------
def needs_cleaning(file_path):
    table = pq.read_table(file_path)
    for col in table.schema.names:
        arr = table[col].to_pylist()
        if any(is_empty_value(v) for v in arr):
            return True
    return False

# --------------------------
# Clean one parquet file
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
            ftype = schema.field(col).type

            if pa.types.is_string(ftype) or pa.types.is_binary(ftype):
                # --- DuckDB cleaning logic ---
                expr = f"""
                CASE
                    WHEN {col} IS NULL
                    OR LENGTH(REPLACE(CAST({col} AS VARCHAR), '\\x00', '')) = 0
                    OR UPPER(REPLACE(CAST({col} AS VARCHAR), '\\x00', '')) IN {tuple(markers)}
                    THEN ''
                    ELSE CAST({col} AS VARCHAR)
                END AS {col}
                """
            else:
                expr = col

            col_exprs.append(expr)

        temp_file = file_path + ".tmp"

        sql = f"""
        COPY (
            SELECT {", ".join(col_exprs)}
            FROM read_parquet('{file_path}')
        ) TO '{temp_file}' (FORMAT PARQUET);
        """

        con.execute(sql)
        os.replace(temp_file, file_path)

        log(f"Cleaned {file_name}")

    except Exception as e:
        log(f"Error processing {file_name}: {e}")

# --------------------------
# Parallel processing
# --------------------------
if __name__ == "__main__":
    files = [f for f in os.listdir(folder) if f.endswith(".parquet")]

    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        exe.map(clean_file, files)

    con.close()
    log("All done!")
