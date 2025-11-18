#!/usr/bin/env python3
"""
Clean Parquet files (string columns) after converter output.
Removes nulls, SUB, control chars, trims spaces, empty -> NULL.
Writes cleaned Parquet to a new folder with the same filename.
"""

import os
import re
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

# -------------------------
# CONFIG
# -------------------------
INPUT_PARQUET_FOLDER = "sas_parquet"        # converter output folder
OUTPUT_CLEAN_FOLDER = "sas_parquet_cleaned" # folder for cleaned Parquet

# Create output folder if not exists
os.makedirs(OUTPUT_CLEAN_FOLDER, exist_ok=True)

# -------------------------
# Cleaning function
# -------------------------
def clean_column_string_after(s):
    if s is None:
        return None
    # remove null bytes, SUB, other control characters
    s_clean = re.sub(r"[\x00\x1A\x01-\x1F\x7F]", "", str(s))
    s_clean = s_clean.strip()
    return s_clean if s_clean else None

# -------------------------
# Process all Parquet files
# -------------------------
parquet_files = [f for f in os.listdir(INPUT_PARQUET_FOLDER)
                 if f.lower().endswith(".parquet")]

for file_name in parquet_files:
    input_file = os.path.join(INPUT_PARQUET_FOLDER, file_name)
    output_file = os.path.join(OUTPUT_CLEAN_FOLDER, file_name)

    print(f"Processing: {input_file} -> {output_file}")

    # Load Parquet to pandas
    table = pq.read_table(input_file)
    df = table.to_pandas()

    # Clean all string/object columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(clean_column_string_after)

    # Convert back to Parquet and write
    pq.write_table(pa.Table.from_pandas(df), output_file)

print("All files cleaned and saved to:", OUTPUT_CLEAN_FOLDER)
