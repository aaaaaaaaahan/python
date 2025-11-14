import os
import pyarrow.parquet as pq
import pyarrow as pa
import duckdb
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Configuration
folder = "input_parquets"
markers = ['NULL', 'NIL']
log_file = os.path.join(folder, "cleaning_log.txt")

con = duckdb.connect()

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(message)

def needs_cleaning(file_path):
    """Check if the parquet file contains empty markers."""
    table = pq.read_table(file_path)
    for col in table.schema.names:
        if pa.types.is_string(table.schema.field(col).type):
            if any(x is None or x in markers for x in table[col].to_pylist()):
                return True
    return False

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
                expr = f"CASE WHEN {col} IS NULL OR {col} IN {tuple(markers)} THEN '' ELSE {col} END AS {col}"
            else:
                expr = col
            col_exprs.append(expr)

        temp_file = file_path + ".tmp"

        # Directly export from SELECT to Parquet without creating a table
        sql = f"""
        COPY (
            SELECT {', '.join(col_exprs)}
            FROM read_parquet('{file_path}')
        ) TO '{temp_file}' (FORMAT PARQUET);
        """
        con.execute(sql)

        # Replace original file safely
        os.replace(temp_file, file_path)
        log(f"Cleaned {file_name}")

    except Exception as e:
        log(f"Error processing {file_name}: {e}")

# Process all Parquet files in parallel
with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(clean_file, [f for f in os.listdir(folder) if f.endswith(".parquet")])

con.close()
log("All done!")
