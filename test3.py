import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
date = batch_date.strftime("%Y-%m")

# ---------------------------------------------------------------------
# Step 2: Connect to DuckDB
# ---------------------------------------------------------------------
con = duckdb.connect()

# ---------------------------------------------------------------------
# Step 3: Input file (converted parquet from FB)
# ---------------------------------------------------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE INDATA AS 
    SELECT * 
    FROM '{host_parquet_path("UNLOAD_CIHRCAPT_FB.parquet")}'
""")

# ---------------------------------------------------------------------
# Step 4: Subset queries by approval status
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
# Step 5: Titles for each report
# ---------------------------------------------------------------------
titles = {
    "CIHRCFZX_HRC03": "HRC LISTING FOR 03 PENDING APPROVAL",
    "CIHRCFZX_HRC04": "HRC LISTING FOR 04 PENDING REVIEW(HO)",
    "CIHRCFZX_HRC05": "HRC LISTING FOR 05 PENDING CANCELLATION",
    "CIHRCFZX_HRC06": "HRC LISTING FOR 06 PENDING CANCELLATION (HO)",
}

# ---------------------------------------------------------------------
# Step 6: Export each ApprovalStatus dataset
# ---------------------------------------------------------------------
for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)
    txt_path = csv_path.replace(".csv", ".txt")

    # Export Parquet (structured output)
    con.execute(f"""
        COPY ({query})
        TO '{parquet_path}'
        (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
    """)

    # Export TXT with title + header + data
    df = con.execute(query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(f"{titles[name]}\n")
        f.write(",".join(df.columns) + "\n")
        df.to_csv(f, index=False, header=False)

# ---------------------------------------------------------------------
# Step 7: OUT Dataset (UNLOAD_CIHRCAPT_DAY)
# ---------------------------------------------------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE INDATA1 AS
    SELECT *
    FROM (
        SELECT *,
               substring(CREATIONDATE, 1, 7) AS TCREATE
        FROM INDATA
    )
    WHERE TCREATE = '{date}'
    ORDER BY BRCHCODE, APPROVALSTATUS, CREATIONDATE
""")

# --- (A) Parquet Export: Full structured data ---
out_query = f"""
    SELECT *
          ,{year} AS year
          ,{month} AS month
          ,{day} AS day
    FROM INDATA1
"""

out_parquet_path = parquet_output_path("UNLOAD_CIHRCAPT_DAY")

con.execute(f"""
    COPY ({out_query})
    TO '{out_parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
""")

# --- (B) TXT Export: SAS-style output (only selected 23 columns) ---
out_txt_query = f"""
    SELECT 
        ALIAS,
        BRCHCODE,
        ACCTTYPE,
        APPROVALSTATUS,
        ACCTNO,
        CISNO,
        CREATIONDATE,
        CUSTNAME,
        CUSTDOBDOR,
        CUSTPEP,
        DTCTOTAL,
        CUST_DWJONES,
        CUST_RHOLD,
        DTCINDUSTRY,
        DTCNATION,
        DTCOCCUP,
        DTCACCTTYPE,
        DTCCOMPFORM,
        FZ_MATCH_SCORE,
        FZ_INDC,
        FZ_CUSTCITZN,
        EMPLOYMENT_TYPE,
        SUB_ACCT_TYPE
    FROM INDATA1
    ORDER BY BRCHCODE, APPROVALSTATUS, CREATIONDATE
"""

df_out = con.execute(out_txt_query).fetchdf()
txt_path = csv_output_path("UNLOAD_CIHRCAPT_DAY").replace(".csv", ".txt")

# Write SAS-style TXT
with open(txt_path, "w", encoding="utf-8") as f:
    f.write("PROGRAM : CIHRCFZX\n")
    f.write("|".join([
        "ALIAS","BRCHCODE","ACCTTYPE","APPROVALSTATUS","ACCTNO","CISNO","CREATIONDATE",
        "CUSTNAME","CUSTDOBDOR","CUSTPEP","DTCTOTAL","CUST_DWJONES","CUST_RHOLD",
        "DTCINDUSTRY","DTCNATION","DTCOCCUP","DTCACCTTYPE","DTCCOMPFORM","FZ_MATCH_SCORE",
        "FZ_INDC","FZ_CUSTCITZN","EMPLOYMENT_TYPE","SUB_ACCT_TYPE"
    ]) + "\n")
    df_out.to_csv(f, index=False, header=False, sep="|")
