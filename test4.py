import os
import pyarrow.parquet as pq
import pyarrow as pa
import duckdb
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# --------------------------
# Configuration
# --------------------------
folder = "input_parquets"  # folder containing Parquet files
log_file = os.path.join(folder, "cleaning_log.txt")

# Define all markers for empty values (including SAS binary/hex)
markers = ['NULL', 'NIL', '\x00', '\x20']  # add more if needed

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
# Check if a file needs cleaning
# --------------------------
def needs_cleaning(file_path):
    table = pq.read_table(file_path)
    for col in table.schema.names:
        if pa.types.is_string(table.schema.field(col).type):
            if any(x is None or x in markers for x in table[col].to_pylist()):
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

        # Read schema to build cleaning expressions
        schema = pq.read_schema(file_path)
        col_exprs = []
        for col in schema.names:
            if pa.types.is_string(schema.field(col).type):
                # Replace NULL, NIL, SAS hex (\x00, \x20), etc. with empty string
                expr = f"""
                CASE 
                    WHEN {col} IS NULL OR {col} IN {tuple(markers)} THEN '' 
                    ELSE {col} 
                END AS {col}
                """
            else:
                expr = col
            col_exprs.append(expr)

        # Temporary file to write cleaned data
        temp_file = file_path + ".tmp"

        # Directly export SELECT to Parquet without creating a table
        sql = f"""
        COPY (
            SELECT {', '.join(col_exprs)}
            FROM read_parquet('{file_path}')
        ) TO '{temp_file}' (FORMAT PARQUET);
        """
        con.execute(sql)

        # Overwrite original file safely
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
