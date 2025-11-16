import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")

# -----------------------------
# Connect to DuckDB
# -----------------------------
con = duckdb.connect()

# -----------------------------
# 1. Load RLEN and filter DP accounts
# -----------------------------
con.execute(f"""
CREATE OR REPLACE TABLE RLEN AS
SELECT *
FROM '{host_parquet_path("RLENCA.parquet")}'
WHERE ACCTCODE = 'DP   '
""")

# -----------------------------
# 2. Load CUST and filter by Citizenship MY and INDORG='I'
# -----------------------------
con.execute(f"""
CREATE OR REPLACE TABLE CUST AS
SELECT *,
       CASE WHEN GENDER='O' THEN 'O' ELSE 'I' END AS INDORG
FROM '{host_parquet_path("ALLCUST_FB.parquet")}'
WHERE CITIZENSHIP='MY'
  AND (CASE WHEN GENDER='O' THEN 'O' ELSE 'I' END)='I'
""")

# -----------------------------
# 3. Load ICID and remove duplicates
# -----------------------------
con.execute(f"""
CREATE OR REPLACE TABLE ICID AS
SELECT DISTINCT ON (CUSTNO) *
FROM '{host_parquet_path("ALLALIAS_FB.parquet")}'
""")

# -----------------------------
# 4. Merge RLEN and ICID for customers with aliases not 'IC'
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE MERGE1 AS
SELECT r.BANKNO, r.ACCTNOC, r.ACCTNO, r.ACCTCODE, r.RLENCODE, r.PRISEC,
       i.CUSTNO, i.ALIASKEY, i.ALIAS
FROM RLEN r
JOIN ICID i
  ON r.CUSTNO = i.CUSTNO
WHERE i.ALIASKEY <> 'IC'
""")

# -----------------------------
# 5. Merge CUST and MERGE1 to get ALLDP
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE ALLDP AS
SELECT c.*, m.BANKNO, m.ACCTNOC, m.ACCTNO, m.ACCTCODE AS ACCTCODE_MERGE,
       m.RLENCODE, m.PRISEC, m.ALIASKEY, m.ALIAS
FROM CUST c
JOIN MERGE1 m
  ON c.CUSTNO = m.CUSTNO
""")

# -----------------------------
# 6. Customers with NO ID
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE NOID AS
SELECT c.*
FROM CUST c
LEFT JOIN ICID i ON c.CUSTNO = i.CUSTNO
WHERE i.CUSTNO IS NULL
""")

# -----------------------------
# 7. NOID with RLEN info
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE NOIDREL AS
SELECT n.*, r.BANKNO, r.ACCTNOC, r.ACCTNO, r.ACCTCODE, r.RLENCODE, r.PRISEC
FROM NOID n
JOIN RLEN r ON n.CUSTNO = r.CUSTNO
""")

# -----------------------------
# 8. Output Queries
# -----------------------------
# Output 1: NOIDREL only
query_noidrel = f"""
SELECT 
    CUSTBRCH,
    CUSTNO,
    ACCTCODE,
    ACCTNO,
    RACE,
    ALIASKEY,
    ALIAS,
    CITIZENSHIP,  
    {year} AS year,
    {month} AS month,
    {day} AS day
FROM NOIDREL
"""

# Output 2: ALLDP + NOIDREL (SAS TEMPOUT)
query_all = f"""
SELECT 
    CUSTBRCH,
    CUSTNO,
    ACCTCODE_MERGE AS ACCTCODE,
    ACCTNO,
    RACE,
    ALIASKEY,
    ALIAS,
    CITIZENSHIP,  
    {year} AS year,
    {month} AS month,
    {day} AS day
FROM ALLDP
UNION ALL
SELECT 
    CUSTBRCH,
    CUSTNO,
    ACCTCODE,
    ACCTNO,
    RACE,
    ALIASKEY,
    ALIAS,
    CITIZENSHIP,  
    {year} AS year,
    {month} AS month,
    {day} AS day
FROM NOIDREL
"""

# -----------------------------
# 9. Write Parquet and CSV
# -----------------------------
outputs = {
    "CIS_RELDP_CUSNOID_WTHACCT": query_noidrel,
    "CIS_RELDP_CUSNOID_ALL": query_all
}

for name, query in outputs.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    con.execute(f"""
        COPY ({query})
        TO '{parquet_path}'
        (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE TRUE)
    """)

    con.execute(f"""
        COPY ({query})
        TO '{csv_path}'
        (FORMAT CSV, HEADER, DELIMITER ';', OVERWRITE_OR_IGNORE TRUE)
    """)

# -----------------------------
# 10. Write Fixed-width TXT (matching SAS positions)
# -----------------------------
txt_outputs = {
    "CIS_RELDP_CUSNOID_WTHACCT": query_noidrel,
    "CIS_RELDP_CUSNOID_ALL": query_all
}

for txt_name, txt_query in txt_outputs.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(row['CUSTBRCH']).zfill(7)}"          # Z7. numeric
                f"{str(row['CUSTNO']).ljust(11)}"
                f"{str(row['ACCTCODE']).ljust(5)}"
                f"{str(row['ACCTNO']).rjust(20)}"          # 20. numeric
                f"{str(row['RACE']).ljust(1)}"
                f"{str(row['ALIASKEY']).ljust(3)}" 
                f"{str(row['ALIAS']).ljust(20)}"
                f"{str(row['CITIZENSHIP']).ljust(2)}"
            )
            f.write(line + "\n")

print("Processing completed. Parquet, CSV, and TXT files are generated.")
