import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path
import datetime

# =========================
#   DATE HANDLING
# =========================
# Equivalent to &DATEMM1, &DATEDD1, &DATEYY1
batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
DATEMM1 = month
DATEDD1 = day
DATEYY1 = year

# =========================
#   CONNECT TO DUCKDB
# =========================
con = duckdb.connect()

# =========================
#   LOAD DATASETS
# =========================
# Assuming already in parquet
#CISFILE = "CIS_CUST_DAILY.parquet"
#CICON1ST = "UNLOAD_CICON1ST_FB.parquet"

# -------------------------
# Step 1: Process CISFILE (CUSTDLY)
# -------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE CIS AS
    SELECT 
        CUSTNO,
        INDORG,
        CUSTCONSENT,
        CAST(CUSTOPENDATE AS VARCHAR) AS CUSTOPENDATEX,
        SUBSTR(CAST(CUSTOPENDATE AS VARCHAR),1,2) AS OPENCUSTMM,
        SUBSTR(CAST(CUSTOPENDATE AS VARCHAR),3,2) AS OPENCUSTDD,
        SUBSTR(CAST(CUSTOPENDATE AS VARCHAR),5,4) AS OPENCUSTYYYY
    FROM read_parquet('/host/cis/parquet/CIS_CUST_DAILY/year=2025/month=9/day=29/data_0.parquet')
""")

# Deduplicate by CUSTNO
con.execute("""
    CREATE OR REPLACE TABLE CIS AS
    SELECT * FROM CIS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) = 1
""")

# -------------------------
# Step 2: Process CONSENT1
# -------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE CONSENT1 AS
    SELECT 
        CI_APPL_NO AS CUSTNO,
        '' AS CONSENT1,
        0 AS PROMPT
    FROM '{host_parquet_path("UNLOAD_CICON1ST_FB.parquet")}'
""")

# Deduplicate by CUSTNO
con.execute("""
    CREATE OR REPLACE TABLE CONSENT1 AS
    SELECT * FROM CONSENT1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) = 1
""")

# -------------------------
# Step 3: Merge
# -------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE MERGE AS
    SELECT 
        B.CUSTNO,
        B.INDORG,
        B.CUSTCONSENT,
        A.CONSENT1,
        A.PROMPT,
        B.OPENCUSTMM,
        B.OPENCUSTDD,
        B.OPENCUSTYYYY
    FROM CONSENT1 A
    FULL OUTER JOIN CIS B
    ON A.CUSTNO = B.CUSTNO
""")

# Apply filtering + update CONSENT1 based on CUSTCONSENT
con.execute(f"""
    CREATE OR REPLACE TABLE MERGE AS
    SELECT
        CUSTNO,
        INDORG,
        CASE 
            WHEN OPENCUSTMM='{DATEMM1}' 
             AND OPENCUSTDD='{DATEDD1}'
             AND OPENCUSTYYYY='{DATEYY1}'
             AND CUSTCONSENT=001 THEN 'Y'
            WHEN OPENCUSTMM='{DATEMM1}' 
             AND OPENCUSTDD='{DATEDD1}'
             AND OPENCUSTYYYY='{DATEYY1}'
             AND CUSTCONSENT=002 THEN 'N'
            ELSE CONSENT1
        END AS CONSENT1,
        PROMPT,
        OPENCUSTMM,
        OPENCUSTDD,
        OPENCUSTYYYY
    FROM MERGE
    WHERE CUSTNO IS NOT NULL
""")

# -------------------------
# Step 4: Build Output (TEMPOUT equivalent)
# -------------------------
result = f"""
    SELECT
        '033' AS CODE1,
        'CUST' AS CODE2,
        CUSTNO,
        CONSENT1,
        INDORG,
        '{DATEYY1}' AS FIRST_DATE_YYYY,
        '-' AS SEP1,
        '{DATEMM1}' AS FIRST_DATE_MM,
        '-' AS SEP2,
        '{DATEDD1}' AS FIRST_DATE_DD,
        PROMPT,
        'INIT' AS PROMPT_SOURCE,
        '{DATEYY1}' AS PROMPT_YYYY,
        '-' AS SEP3,
        '{DATEMM1}' AS PROMPT_MM,
        '-' AS SEP4,
        '{DATEDD1}' AS PROMPT_DD,
        '01.01.01' AS PROMPT_TIME,
        'INIT' AS UPDATE_SOURCE,
        '{DATEYY1}' AS UPDATE_YYYY,
        '-' AS SEP5,
        '{DATEMM1}' AS UPDATE_MM,
        '-' AS SEP6,
        '{DATEDD1}' AS UPDATE_DD,
        '01.01.01' AS UPDATE_TIME,
        'INIT' AS UPDATE_OPERATOR,
        'INIT' AS APP_CODE,
        'INIT' AS APP_NO
        ,{year} AS year
        ,{month} AS month
        ,{day} AS day
    FROM MERGE
""".format(year=year,month=month,day=day)

# =========================
#   WRITE OUTPUT (PyArrow)
# =========================
#pq.write_table(result, "RBP2_B033_CICON1ST_CUSTNEW.parquet")
queries = {
    "CICON1ST_CUSTNEW"            : result
}

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    con.execute(f"""
    COPY ({query})
    TO '{parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);  
     """)
    
    con.execute(f"""
    COPY ({query})
    TO '{csv_path}'
    (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);  
     """)
