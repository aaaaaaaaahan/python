# ---------------------------------------------------------------
# CIHRCFZX Conversion (SAS → Python using DuckDB + PyArrow)
# ---------------------------------------------------------------
# This program reads input Parquet files, filters data into
# multiple output reports (HRC03, HRC04, HRC05, HRC06),
# and writes each to both Parquet and TXT.
# TXT includes SAS-style title + column headers + data.
# ---------------------------------------------------------------

import duckdb
import os
from datetime import datetime
import pandas as pd

# ---------------------------------------------------------------------
# Helper functions for output paths
# ---------------------------------------------------------------------
def parquet_output_path(name):
    return f"output/{name}.parquet"

def txt_output_path(name):
    return f"output/{name}.txt"

# Ensure output folder exists
os.makedirs("output", exist_ok=True)

# ---------------------------------------------------------------------
# Runtime date info
# ---------------------------------------------------------------------
today = datetime.now()
year = today.year
month = today.month
day = today.day

# ---------------------------------------------------------------------
# Connect to DuckDB
# ---------------------------------------------------------------------
con = duckdb.connect()

# ---------------------------------------------------------------------
# Input Parquet file paths
# ---------------------------------------------------------------------
INPUT_PATH = "input/UNLOAD_CIHRCAPT.parquet"
CTRL_PATH  = "input/CTRLDATE.parquet"

con.execute(f"CREATE OR REPLACE TABLE INDATA AS SELECT * FROM read_parquet('{INPUT_PATH}')")
con.execute(f"CREATE OR REPLACE TABLE CTRL AS SELECT * FROM read_parquet('{CTRL_PATH}')")

# ---------------------------------------------------------------------
# Queries (same logic as SAS)
# ---------------------------------------------------------------------
queries = {
    "HRC03": f"""
        SELECT *, {year} AS year, {month} AS month, {day} AS day
        FROM INDATA
        WHERE APPROVALSTATUS = '03'
        ORDER BY BRANCHCODE, ENTRYDATE, HRCALIAS
    """,
    "HRC04": f"""
        SELECT *, {year} AS year, {month} AS month, {day} AS day
        FROM INDATA
        WHERE APPROVALSTATUS = '04'
        ORDER BY BRANCHCODE, ENTRYDATE, HRCALIAS
    """,
    "HRC05": f"""
        SELECT *, {year} AS year, {month} AS month, {day} AS day
        FROM INDATA
        WHERE APPROVALSTATUS = '05'
        ORDER BY BRANCHCODE, ENTRYDATE, HRCALIAS
    """,
    "HRC06": f"""
        SELECT *, {year} AS year, {month} AS month, {day} AS day
        FROM INDATA
        WHERE APPROVALSTATUS = '06'
        ORDER BY BRANCHCODE, ENTRYDATE, HRCALIAS
    """,
}

# ---------------------------------------------------------------------
# SAS-style titles for each report
# ---------------------------------------------------------------------
titles = {
    "HRC03": "HRC LISTING FOR 03 PENDING APPROVAL",
    "HRC04": "HRC LISTING FOR 04 PENDING REVIEW(HO)",
    "HRC05": "HRC LISTING FOR 05 PENDING CANCELLATION",
    "HRC06": "HRC LISTING FOR 06 PENDING CANCELLATION (HO)",
    "HRC_DELETE_MORE60D": "HRC DELETE MORE THAN 60 DAYS REPORT",
}

# ---------------------------------------------------------------------
# Export logic (Parquet + TXT with title and header)
# ---------------------------------------------------------------------
for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    txt_path = txt_output_path(name)

    # 1️⃣ Export Parquet (structured output)
    con.execute(f"""
        COPY ({query})
        TO '{parquet_path}'
        (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
    """)

    # 2️⃣ Export TXT (human-readable report with title)
    df = con.execute(query).fetchdf()

    # Write title + header + data to TXT
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(f"{titles[name]}\n")
        f.write(",".join(df.columns) + "\n")
        df.to_csv(f, index=False, header=False)

    print(f"✅ {name} exported to TXT and Parquet successfully.")

# ---------------------------------------------------------------------
# Combined OUT dataset (like SAS OUT)
# ---------------------------------------------------------------------
out_query = f"""
    SELECT *
          ,{year} AS year
          ,{month} AS month
          ,{day} AS day
    FROM INDATA
"""

parquet_path = parquet_output_path("HRC_DELETE_MORE60D")
txt_path = txt_output_path("HRC_DELETE_MORE60D")

# Export Parquet
con.execute(f"""
    COPY ({out_query})
    TO '{parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
""")

# Export TXT
df_out = con.execute(out_query).fetchdf()
with open(txt_path, "w", encoding="utf-8") as f:
    f.write(f"{titles['HRC_DELETE_MORE60D']}\n")
    f.write(",".join(df_out.columns) + "\n")
    df_out.to_csv(f, index=False, header=False)

print("✅ HRC_DELETE_MORE60D exported successfully.")
print("🎉 All reports generated successfully.")
