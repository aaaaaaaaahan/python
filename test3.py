import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ---------------------------------------------------------------------
# Connect to DuckDB
# ---------------------------------------------------------------------
con = duckdb.connect()

# ---------------------------------------------------------------------
# Input Parquet file paths
# ---------------------------------------------------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE INDATA AS 
    SELECT * 
    FROM '{host_parquet_path("UNLOAD_CIHRCAPT_FB.parquet")}'
""")

# ---------------------------------------------------------------------
# Queries (same logic as SAS)
# ---------------------------------------------------------------------
queries = {
    "CIHRCFZX_HRC03": f"""
        SELECT *, {year} AS year, {month} AS month, {day} AS day
        FROM INDATA
        WHERE APPROVALSTATUS = '03'
        ORDER BY BRCHCODE, CREATIONDATE, ALIAS
    """,
    "CIHRCFZX_HRC04": f"""
        SELECT *, {year} AS year, {month} AS month, {day} AS day
        FROM INDATA
        WHERE APPROVALSTATUS = '04'
        ORDER BY BRCHCODE, CREATIONDATE, ALIAS
    """,
    "CIHRCFZX_HRC05": f"""
        SELECT *, {year} AS year, {month} AS month, {day} AS day
        FROM INDATA
        WHERE APPROVALSTATUS = '05'
        ORDER BY BRCHCODE, CREATIONDATE, ALIAS
    """,
    "CIHRCFZX_HRC06": f"""
        SELECT *, {year} AS year, {month} AS month, {day} AS day
        FROM INDATA
        WHERE APPROVALSTATUS = '06'
        ORDER BY BRCHCODE, CREATIONDATE, ALIAS
    """,
}

# ---------------------------------------------------------------------
# SAS-style titles for each report
# ---------------------------------------------------------------------
titles = {
    "CIHRCFZX_HRC03": "HRC LISTING FOR 03 PENDING APPROVAL",
    "CIHRCFZX_HRC04": "HRC LISTING FOR 04 PENDING REVIEW(HO)",
    "CIHRCFZX_HRC05": "HRC LISTING FOR 05 PENDING CANCELLATION",
    "CIHRCFZX_HRC06": "HRC LISTING FOR 06 PENDING CANCELLATION (HO)",
    "UNLOAD_CIHRCAPT_DAY": "HRC DELETE MORE THAN 60 DAYS REPORT",
}

# ---------------------------------------------------------------------
# Export logic (Parquet + TXT with title and header)
# ---------------------------------------------------------------------
for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)
    txt_output_path = csv_path
    if txt_output_path.lower().endswith('.csv'):
        txt_path = txt_output_path[:-4] + '.txt'
    else:
        txt_path = txt_output_path + '.txt'

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

parquet_path = parquet_output_path("UNLOAD_CIHRCAPT_DAY")
txt_path = txt_output_path("UNLOAD_CIHRCAPT_DAY")

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
