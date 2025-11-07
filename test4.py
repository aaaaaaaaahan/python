# ---------------------------------------------------------------------
# Step 7: OUT Dataset (UNLOAD_CIHRCAPT_DAY)
# ---------------------------------------------------------------------
out_query = f"""
    SELECT *,
           substring(CREATIONDATE, 1, 7) AS TCREATE,
           {year} AS year,
           {month} AS month,
           {day} AS day
    FROM INDATA
    WHERE substring(CREATIONDATE, 1, 7) = '{date}'
    ORDER BY BRCHCODE, APPROVALSTATUS, CREATIONDATE
"""

title = "PROGRAM : CIHRCFZX"

# ---------------------------------------------------------------------
# Step 7A: Export Parquet
# ---------------------------------------------------------------------
out_parquet_path = parquet_output_path("UNLOAD_CIHRCAPT_DAY")
con.execute(f"""
    COPY ({out_query})
    TO '{out_parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
""")

# ---------------------------------------------------------------------
# Step 7B: Export TXT (SAS-style, 23 columns only)
# ---------------------------------------------------------------------
df_out = con.execute(out_query).fetchdf()

# Keep only 23 target columns
selected_cols = [
    "ALIAS","BRCHCODE","ACCTTYPE","APPROVALSTATUS","ACCTNO","CISNO","CREATIONDATE",
    "CUSTNAME","CUSTDOBDOR","CUSTPEP","DTCTOTAL","CUST_DWJONES","CUST_RHOLD",
    "DTCINDUSTRY","DTCNATION","DTCOCCUP","DTCACCTTYPE","DTCCOMPFORM","FZ_MATCH_SCORE",
    "FZ_INDC","FZ_CUSTCITZN","EMPLOYMENT_TYPE","SUB_ACCT_TYPE"
]
df_out = df_out[selected_cols]

txt_path = csv_output_path("UNLOAD_CIHRCAPT_DAY").replace(".csv", ".txt")
with open(txt_path, "w", encoding="utf-8") as f:
    f.write(f"{title}\n")
    f.write("|".join(selected_cols) + "\n")
    df_out.to_csv(f, index=False, header=False, sep="|")
