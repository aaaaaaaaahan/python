#!/usr/bin/env python3
"""
Clean Parquet files (string columns) after converter output.
Removes nulls, SUB, control chars, trims spaces, empty -> space.
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
    """
    Clean a string: remove unwanted characters, trim spaces.
    Replace None or pd.NA with a space.
    """
    if s is None or s is pd.NA:
        return " "  # replace missing with space

    # Remove unwanted characters
    s_clean = re.sub(r"[\x41\x00\x1A\x01-\x1F\x7F]", "", str(s))
    s_clean = s_clean.strip()

    # If empty after cleaning, replace with space
    if not s_clean:
        return " "

    return s_clean

def is_column_clean(series):
    """
    Check if a string column is clean (no unwanted characters).
    """
    for val in series.dropna():
        if re.search(r"[\x41\x00\x1A\x01-\x1F\x7F]", str(val)):
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

            # Read Parquet into Pandas
            table = pq.read_table(str(input_file))
            df = table.to_pandas()

            string_cols = df.select_dtypes(include="object").columns
            cleaning_needed = any(not is_column_clean(df[col]) for col in string_cols)

            if not cleaning_needed:
                # Still replace any pd.NA or None with space if present
                for col in string_cols:
                    if df[col].isna().any():
                        df[col] = df[col].fillna(" ")
                        cleaning_needed = True

                if not cleaning_needed:
                    log.write(f"SKIPPED (clean): {input_file.name}\n")
                    print(f"⏭ Skipped (already clean): {input_file.name}")
                    continue

            # Clean string columns
            for col in string_cols:
                df[col] = df[col].apply(clean_column_string_after)
                df[col] = df[col].fillna(" ")  # ensure no <NA> remains

            # Overwrite Parquet
            pq.write_table(pa.Table.from_pandas(df), str(input_file))
            log.write(f"CLEANED: {input_file.name}\n")
            print(f"✅ Cleaned and overwritten: {input_file.name}")

    print("Processing complete. Log saved to:", LOG_FILE)
