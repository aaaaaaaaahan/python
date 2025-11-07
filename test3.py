import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")

# ---------------------------------------------------------------------
# Step 1: Setup file paths
# ---------------------------------------------------------------------
con = duckdb.connect()

# ---------------------------------------------------------------------
# Step 2: Connect to DuckDB and read input parquet
# ---------------------------------------------------------------------
con.execute(f"""
    CREATE TABLE CIDOWFZT AS 
    SELECT * FROM '{host_parquet_path("CIDOWFZT_FB.parquet")}'
""")

# ---------------------------------------------------------------------
# Step 3: Apply filters (similar to SAS conditions)
# ---------------------------------------------------------------------
filtered = """
    SELECT *,
           {year} AS year,
           {month} AS month,
           {day} AS day
    FROM CIDOWFZT
    WHERE SOURCE = 'ACCTOPEN'
      AND SCREENDATE10 > '2025-01-01'
    ORDER BY BRANCHABBRV, SCREENDATE
"""

# ---------------------------------------------------------------------
# Step 4: Write to Parquet
# ---------------------------------------------------------------------
out_parquet_path = parquet_output_path("CIHRCFZP_EXCEL")
con.execute(f"""
    COPY ({filtered})
    TO '{out_parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
""")

# ---------------------------------------------------------------------
# Step 5: Write to TXT with title and header
# ---------------------------------------------------------------------
# Define header title
title = "DETAIL LISTING FOR CIDOWFZT"
delimiter = "|"

# Get column names
columns = filtered.column_names
txt_path = csv_output_path(f"CIHRCFZP_EXCEL_{report_date}").replace(".csv", ".txt")

# Prepare rows
rows = []
for i in range(filtered.num_rows):
    record = [str(filtered.column(c)[i].as_py() if filtered.column(c)[i].as_py() is not None else "") for c in range(len(columns))]
    rows.append(delimiter.join(record))

# Write TXT output
with open(txt_path, "w", encoding="utf-8") as f:
    f.write(f"{title}\n")
    f.write(delimiter.join(columns) + "\n")
    for row in rows:
        f.write(row + "\n")

print(f"✅ TXT output created: {txt_path}")

# ---------------------------------------------------------------------
# Step 6: Optional — show sample output
# ---------------------------------------------------------------------
print("\nSample rows:")
print(con.execute("SELECT * FROM CIDOWFZT LIMIT 5").df())
