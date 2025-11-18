#!/usr/bin/env python3
"""
Clean Parquet files (string columns) after converter output.
Removes nulls, SUB, control chars, trims spaces, empty -> NULL.
Automatically processes all Parquet files in a manually given folder.
Writes cleaned Parquet to a specified folder with the same filename.
Skips cleaning for files already clean, but still copies to output.
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
INPUT_PARQUET_FOLDER = Path("/host/cis/parquet/sas_parquet_test_jkh")        # input folder
OUTPUT_CLEAN_FOLDER = Path("/host/cis/parquet/sas_parquet_test_jkh_clean") # output folder
LOG_FILE = Path("/host/cis/logs/parquet_clean_log.txt")

OUTPUT_CLEAN_FOLDER.mkdir(parents=True, exist_ok=True)

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
    # Returns True if column has no control chars
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
            output_file = OUTPUT_CLEAN_FOLDER / input_file.name
            print(f"Processing: {input_file} -> {output_file}")

            # Load Parquet to pandas
            table = pq.read_table(str(input_file))
            df = table.to_pandas()

            # Check if cleaning is needed
            string_cols = df.select_dtypes(include="object").columns
            cleaning_needed = False
            for col in string_cols:
                if not is_column_clean(df[col]):
                    cleaning_needed = True
                    break

            if cleaning_needed:
                # Clean all string/object columns
                for col in string_cols:
                    df[col] = df[col].apply(clean_column_string_after)
                log.write(f"CLEANED: {input_file.name}\n")
                print(f"✅ Cleaned: {input_file.name}")
            else:
                log.write(f"SKIPPED (already clean): {input_file.name}\n")
                print(f"⏭ Skipped (already clean): {input_file.name}")

            # Save to output folder
            pq.write_table(pa.Table.from_pandas(df), str(output_file))

    print("Processing complete. Log saved to:", LOG_FILE)
