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
    RIGHT('0000000000000000' || CAST(SUBSTRING(PRIMCARD, 1, 16) AS VARCHAR), 16) AS PRIMCARD,
    RIGHT('0000000000000000' || CAST(SUBSTRING(GUARNTOR, 1, 16) AS VARCHAR), 16) AS GUARNTOR,
    OUTSTBAL,
    CASE WHEN OUTSTBALS = '-' THEN OUTSTBAL * -1 ELSE OUTSTBAL END AS OUTSTBAL_FIX,
    CASE WHEN AUTHLIMS = '-' THEN AUTHLIM * -1 ELSE AUTHLIM END AS AUTHLIM_FIX,
    AUTHLIM,
    AUTHLIMS,
    PRODTYPE,
    SUBSTRING(PRIMCARD, 1, 14) AS CARDACCT,
    CASE 
        WHEN GUARNTOR IS NOT NULL AND GUARNTOR <> '0000000000000000'
        THEN GUARNTOR
        ELSE PRIMCARD
    END AS SORT_KEY,
    OUTSTBAL + AUTHLIM AS TOTALBAL
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
SELECT f1.*, f2.PRIMBAL
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
    'UNQ' AS INDICATOR
FROM temp1
""")

# ============================================================
# UNIQUE / DUPLICATE SPLIT (ICETOOL equivalent)
# ============================================================
con.execute("""
CREATE OR REPLACE TABLE acctbal_unq AS
SELECT DISTINCT ON (CARDACCT) *
FROM tempoUT
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
ORDER BY CARDACCT
""")

# ============================================================
# EXPORT OUTPUTS
# ============================================================
print(f"✅ ETL complete")
