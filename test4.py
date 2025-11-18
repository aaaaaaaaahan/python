#!/usr/bin/env python3
"""
Clean Parquet files (string columns) after converter output.
Removes nulls, SUB, control chars, trims spaces, empty -> NULL.
Overwrites the original Parquet files ONLY if cleaning is needed.
Logs all actions.
"""

import re
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

# -------------------------
# CONFIG
# -------------------------
INPUT_PARQUET_FOLDER = Path("/host/cis/parquet/sas_parquet_test_jkh")
LOG_FILE = Path("/host/cis/logs/parquet_clean_log.txt")

# -------------------------
# Cleaning function
# -------------------------
def clean_column_string_after(s):
    if s is None:
        return None
    s_clean = re.sub(r"[\x41\x00\x1A\x01-\x1F\x7F]", "", str(s))
    s_clean = s_clean.strip()
    return s_clean if s_clean else None

def is_column_clean(series):
    for val in series.dropna():
        if re.search(r"[\x00\x1A\x01-\x1F\x7F]", str(val)):
            return False
    return True

# -------------------------
# Process all Parquet files
# -------------------------
parquet_files = list(INPUT_PARQUET_FOLDER.glob("*.parquet"))

if not parquet_files:
    print(f"No Parquet files found in {INPUT_PARQUET_FOLDER}")
else:
    with open(LOG_FILE, "w") as log:
        for input_file in parquet_files:
            print(f"Checking: {input_file.name}")

            table = pq.read_table(str(input_file))
            df = table.to_pandas()

            string_cols = df.select_dtypes(include="object").columns
            cleaning_needed = any(not is_column_clean(df[col]) for col in string_cols)

            if not cleaning_needed:
                log.write(f"SKIPPED (clean): {input_file.name}\n")
                print(f"⏭ Skipped (already clean): {input_file.name}")
                continue  # <-- TRUE SKIP, DO NOTHING

            # If cleaning is needed, process and overwrite
            for col in string_cols:
                df[col] = df[col].apply(clean_column_string_after)

            pq.write_table(pa.Table.from_pandas(df), str(input_file))
            log.write(f"CLEANED: {input_file.name}\n")
            print(f"✅ Cleaned and overwritten: {input_file.name}")

    print("Processing complete. Log saved to:", LOG_FILE)
