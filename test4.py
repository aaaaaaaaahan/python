import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

# ---------------------------------------------------------------------
# Step 1: Setup batch date and output paths
# ---------------------------------------------------------------------
batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")

# ---------------------------------------------------------------------
# Step 2: Connect to DuckDB and read input parquet
# ---------------------------------------------------------------------
con = duckdb.connect()
con.execute(f"""
    CREATE TABLE CIDOWFZT AS 
    SELECT * FROM '{host_parquet_path("CIDOWFZT_FB.parquet")}'
""")

# ---------------------------------------------------------------------
# Step 3: Apply filters (SAS-equivalent logic)
# ---------------------------------------------------------------------
query = f"""
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
# Step 4: Write to Parquet (Hive partition)
# ---------------------------------------------------------------------
out_parquet_path = parquet_output_path("CIHRCFZP_EXCEL")
con.execute(f"""
    COPY ({query})
    TO '{out_parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
""")
print(f"✅ Parquet output written to: {out_parquet_path}")

# ---------------------------------------------------------------------
# Step 5: Write to TXT with title and header
# ---------------------------------------------------------------------
title = "DETAIL LISTING FOR CIDOWFZT"
delimiter = "|"
txt_path = csv_output_path(f"CIHRCFZP_EXCEL_{report_date}").replace(".csv", ".txt")

# Execute query and fetch all rows + columns
res = con.execute(query)
columns = [desc[0] for desc in res.description]
rows = res.fetchall()

with open(txt_path, "w", encoding="utf-8") as f:
    f.write(f"{title}\n")
    f.write(delimiter.join(columns) + "\n")
    for row in rows:
        f.write(delimiter.join(str(x) if x is not None else "" for x in row) + "\n")

print(f"✅ TXT output created: {txt_path}")

# ---------------------------------------------------------------------
# Step 6: Optional — show sample output
# ---------------------------------------------------------------------
print("\nSample rows:")
print(con.execute("SELECT * FROM CIDOWFZT LIMIT 5").df())
