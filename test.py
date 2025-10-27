import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# remark: parquet havent generate 24/10/2025

# ============================================================
# DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()

# ============================================================
# DATA TRANSFORMATION
# ============================================================
# UNION ALL ACCTFILE
con.execute(f"""
    CREATE OR REPLACE VIEW acct_raw AS 
    SELECT * FROM '{host_parquet_path("PBCS_ACCT33.parquet")}'
    UNION ALL
    SELECT * FROM '{host_parquet_path("PBCS_ACCT34.parquet")}'
    UNION ALL
    SELECT * FROM '{host_parquet_path("PBCS_ACCT54.parquet")}'
    UNION ALL
    SELECT * FROM '{host_parquet_path("PBCS_ACCT55.parquet")}'
""")

# Simulate SAS data step logic
con.execute("""
CREATE OR REPLACE TABLE acctf1 AS
SELECT
    CAST(OUTSTBAL AS DOUBLE) AS OUTSTBAL,
    OUTSTBALS,
    RIGHT('0000000000000000' || CAST(SUBSTRING(GUARNTOR, 1, 16) AS VARCHAR), 16) AS GUARNTOR,
    CUSTNBR,
    CUSTREF,
    PRIMPROD,
    RIGHT('0000000000000000' || CAST(SUBSTRING(PRIMCARD, 1, 16) AS VARCHAR), 16) AS PRIMCARD,
    CAST(AUTHLIM AS DOUBLE) AS AUTHLIM,
    AUTHLIMS,
    PRODTYPE,

    -- Derived / computed fields (same SAS logic)
    CASE WHEN GUARNTOR IS NOT NULL AND GUARNTOR <> '0000000000000000'
         THEN SUBSTRING(PRIMCARD, 1, 16)
         ELSE NULL
    END AS SUPPCARD,

    CASE 
        WHEN OUTSTBALS = '-' THEN CAST(OUTSTBAL AS DOUBLE) * -1 
        ELSE CAST(OUTSTBAL AS DOUBLE) 
    END +
    CASE 
        WHEN AUTHLIMS = '-' THEN CAST(AUTHLIM AS DOUBLE) * -1 
        ELSE CAST(AUTHLIM AS DOUBLE) 
    END AS TOTALBAL,

    SUBSTRING(PRIMCARD, 1, 14) AS CARDACCT,

    CASE WHEN OUTSTBALS = '-' THEN CAST(OUTSTBAL AS DOUBLE) * -1 ELSE CAST(OUTSTBAL AS DOUBLE) END AS OUTSTBAL_FIX,
    CASE WHEN AUTHLIMS = '-' THEN CAST(AUTHLIM AS DOUBLE) * -1 ELSE CAST(AUTHLIM AS DOUBLE) END AS AUTHLIM_FIX,
            
    CASE 
        WHEN GUARNTOR IS NOT NULL AND GUARNTOR <> '0000000000000000'
        THEN GUARNTOR
        ELSE PRIMCARD
    END AS SORT_KEY

FROM acct_raw
WHERE PRIMCARD <> '0000000000000000'
""")

# ============================================================
# AGGREGATE PRIMARY BALANCE
# ============================================================
con.execute("""
CREATE OR REPLACE TABLE acctf2 AS
SELECT
    CARDACCT,
    SUM(OUTSTBAL_FIX + AUTHLIM_FIX) AS PRIMBAL,
    COUNT(*) AS FREQ
FROM acctf1
GROUP BY CARDACCT
""")

# ============================================================
# MERGE SUMMARY BACK TO DETAIL
# ============================================================
con.execute("""
CREATE OR REPLACE TABLE temp1 AS
SELECT f1.*, f2.CARDACCT, f2.PRIMBAL
FROM acctf1 f1
LEFT JOIN acctf2 f2 USING (CARDACCT)
""")

# ============================================================
# OUTPUT DATASET (TEMPOUT)
# ============================================================
con.execute("""
CREATE OR REPLACE TABLE tempoUT AS
SELECT
    '033' AS RECORD_TYPE,
    PRIMPROD,
    PRIMCARD,
    SUPPCARD,
    OUTSTBAL_FIX AS OUTSTBAL,
    AUTHLIM_FIX AS AUTHLIM,
    OUTSTBAL_FIX + AUTHLIM_FIX AS TOTALBAL,
    CASE 
        WHEN PRODTYPE IN ('C','D') THEN PRIMBAL * -1
        ELSE PRIMBAL
    END AS PRIMBAL,
    PRODTYPE,
    'UNQ' AS INDICATOR,
    CARDACCT
FROM temp1
ORDER BY CARDACCT
""")

# ============================================================
# UNIQUE / DUPLICATE SPLIT (ICETOOL equivalent)
# ============================================================
con.execute("""
CREATE OR REPLACE TABLE acctbal_unq AS
SELECT DISTINCT ON (CARDACCT) 
    RECORD_TYPE, PRIMPROD, PRIMCARD, SUPPCARD,
    OUTSTBAL, AUTHLIM, TOTALBAL, PRIMBAL, PRODTYPE,
    INDICATOR
FROM tempoUT
ORDER BY CARDACCT
""")

con.execute("""
CREATE OR REPLACE TABLE acctbal_dup AS
SELECT t.*
FROM tempoUT t
JOIN (
    SELECT CARDACCT FROM tempoUT GROUP BY CARDACCT HAVING COUNT(*) > 1
) d USING (CARDACCT)
""")

# Update duplicate indicator
con.execute("""
CREATE OR REPLACE TABLE acctbal_dup2 AS
SELECT
    RECORD_TYPE, PRIMPROD, PRIMCARD, SUPPCARD,
    OUTSTBAL, AUTHLIM, TOTALBAL, PRIMBAL, PRODTYPE,
    'DUP' AS INDICATOR
FROM acctbal_dup
ORDER BY PRIMCARD
""")

# ============================================================
# COMBINE UNIQUE + DUPLICATE & OMIT SUPP CARDS
# ============================================================
con.execute("""
CREATE OR REPLACE TABLE acctbal_prim AS
SELECT *
FROM (
    SELECT * FROM acctbal_unq
    UNION ALL
    SELECT * FROM acctbal_dup2
)
WHERE (SUPPCARD IS NULL OR TRIM(SUPPCARD) = '')
ORDER BY PRIMCARD
""")

# ============================================================
# EXPORT OUTPUTS
# ============================================================
out1 = """
    SELECT
        RECORD_TYPE,
        PRIMPROD,
        PRIMCARD,
        SUPPCARD,
        OUTSTBAL,
        AUTHLIM,
        TOTALBAL,
        PRIMBAL,
        PRODTYPE,
        INDICATOR
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM tempoUT
""".format(year=year,month=month,day=day)

out2 = """
    SELECT
        *
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM acctbal_unq
""".format(year=year,month=month,day=day)

out3 = """
    SELECT
        *
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM acctbal_dup2
""".format(year=year,month=month,day=day)

out4 = """
    SELECT
        *
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM acctbal_prim
""".format(year=year,month=month,day=day)

queries = {
    "CIS_CREDITCD_ACCTBAL"                 : out1,
    "CIS_CREDITCD_ACCTBAL_UNQ"             : out2,
    "CIS_CREDITCD_ACCTBAL_DUP"             : out3,
    "CIS_CREDITCD_ACCTBAL_PRIM"            : out4
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
