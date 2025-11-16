import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet
import datetime

batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")
DATEYY1 = year
DATEMM1 = month
DATEDD1 = day

# ================================================================
# 2. DUCKDB CONNECTION
# ================================================================
con = duckdb.connect()
cis = get_hive_parquet('CIS_CUST_DAILY')

# ================================================================
# 3. LOAD PARQUET INPUT FILES (already converted)
# ================================================================
CISFILE = "CIS_CUST_DAILY.parquet"            # CIS.CUST.DAILY
CIPHONET = "UNLOAD_CIPHONET_FB.parquet"       # UNLOAD.CIPHONET.FB

# ================================================================
# 4. LOAD CIS (same as DATA CIS; SET CISFILE; IF INDORG='I')
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE CIS AS
    SELECT *
    FROM read_parquet('{cis[0]}')
    WHERE INDORG = 'I'
""")

# Remove duplicates same as PROC SORT NODUPKEY
con.execute("""
    CREATE OR REPLACE TABLE CIS AS
    SELECT *
    FROM CIS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) = 1
""")

# ================================================================
# 5. LOAD PHONE FILE (same as DATA PHONE and INPUT @9 CUSTNO $11.)
# Defaults: PHONE=0, PROMPT=0
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE PHONE AS
    SELECT
        SUBSTR(CUSTNO, 1, 11) AS CUSTNO,
        0 AS PHONE,
        0 AS PROMPT
    FROM '{host_parquet_path("UNLOAD_CIPHONET_FB.parquet")}'
""")

# ================================================================
# 6. MERGE (same as SAS MERGE PHONE(IN=A) CIS(IN=B); IF NOT A THEN OUTPUT)
# Keep CIS rows that do not exist in PHONE
# ================================================================
con.execute("""
    CREATE OR REPLACE TABLE MERGE AS
    SELECT B.*
         , 0 AS PHONE
         , 0 AS PROMPT
    FROM CIS B
    LEFT JOIN PHONE A
           ON A.CUSTNO = B.CUSTNO
    WHERE A.CUSTNO IS NULL
""")

# -----------------------------
# 8. Output Queries
# -----------------------------
# Output 1: NOIDREL only
out = """
SELECT 
    CUSTBRCH,
    CUSTNO,
    ACCTCODE,
    ACCTNO,
    RACE,
    ' ' AS ALIASKEY,
    ' ' AS ALIAS,
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
    "CIPHONET_CUSTNEW": out
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
    "CIPHONET_CUSTNEW": out
}

for txt_name, txt_query in txt_outputs.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(int(row['CUSTBRCH'])).zfill(7)}"          # Z7. numeric
                f"{str(row['CUSTNO']).ljust(11)}"
                f"{str(row['ACCTCODE']).ljust(5)}"
                f"{str(row['ACCTNO']).rjust(20)}"          # 20. numeric
                f"{str(row['RACE']).ljust(1)}"
                f"{str(row['ALIASKEY']).ljust(3)}" 
                f"{str(row['ALIAS']).ljust(20)}"
                f"{str(row['CITIZENSHIP']).ljust(2)}"
            )
            f.write(line + "\n")
