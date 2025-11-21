import duckdb
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path

# =========================
# Paths
# =========================
CTRFILE = "BNMCTR.ACCTDAT1.EDITS.parquet"
ADDRFILE = "CIS.CUST.DAILY.ADDRESS.parquet"

ONELINE_PARQUET = "BNMCTR.ACCTDAT1.ONELINE.parquet"
ONELINE_TXT = "BNMCTR.ACCTDAT1.ONELINE.txt"

OUTFILE2_PARQUET = "BNMCTR.UPDATE.ADR.parquet"
OUTFILE2_TXT = "BNMCTR.UPDATE.ADR.txt"

# =========================
# DuckDB Connection
# =========================
con = duckdb.connect()

# =========================
# 1. FORMAT STEP: Expand to 1-line format
# =========================

con.execute(f"""
CREATE OR REPLACE TABLE CTR AS
SELECT
    CUSTNO1, TOWN1, POSTCODE1, STATE_ID1, ADDREF1,
    CUSTNO2, TOWN2, POSTCODE2, STATE_ID2, ADDREF2,
    CUSTNO3, TOWN3, POSTCODE3, STATE_ID3, ADDREF3,
    CUSTNO4, TOWN4, POSTCODE4, STATE_ID4, ADDREF4,
    CUSTNO5, TOWN5, POSTCODE5, STATE_ID5, ADDREF5
FROM read_parquet('{CTRFILE}');
""")

# Generate ONELINE rows (mimic multiple PUT blocks)
con.execute("""
CREATE OR REPLACE TABLE ONELINE AS

SELECT CUSTNO1 AS ONE_CUSTNO,
       TOWN1   AS ONE_TOWN,
       POSTCODE1 AS ONE_POSTCODE,
       STATE_ID1 AS ONE_STATE_ID,
       ADDREF1 AS ADDREF
FROM CTR WHERE CUSTNO1 <> ''

UNION ALL
SELECT CUSTNO2, TOWN2, POSTCODE2, STATE_ID2, ADDREF2
FROM CTR WHERE CUSTNO2 <> ''

UNION ALL
SELECT CUSTNO3, TOWN3, POSTCODE3, STATE_ID3, ADDREF3
FROM CTR WHERE CUSTNO3 <> ''

UNION ALL
SELECT CUSTNO4, TOWN4, POSTCODE4, STATE_ID4, ADDREF4
FROM CTR WHERE CUSTNO4 <> ''

UNION ALL
SELECT CUSTNO5, TOWN5, POSTCODE5, STATE_ID5, ADDREF5
FROM CTR WHERE CUSTNO5 <> '';
""")

# Save parquet version
con.execute(f"COPY ONELINE TO '{ONELINE_PARQUET}' (FORMAT PARQUET);")

# =========================
# 1b. Write fixed-width ONELINE txt
# =========================

df_oneline = con.execute("SELECT * FROM ONELINE").df()

with open(ONELINE_TXT, "w", encoding="utf-8") as f:
    for _, row in df_oneline.iterrows():
        line = (
            f"{str(row.ONE_CUSTNO):<11}"
            f"{str(row.ONE_TOWN):<30}"
            f"{str(row.ONE_POSTCODE):<5}"
            f"{str(row.ONE_STATE_ID):<2}"
            f"{str(int(row.ADDREF)).zfill(11)}"
        )
        f.write(line + "\n")

print("ONELINE file created.")


# =========================
# 2. ADDRESS FILE PROCESS
# =========================

con.execute(f"""
CREATE OR REPLACE TABLE ADDR AS
SELECT DISTINCT
    ADDREF,
    CITY,
    STATEX,
    STATEID,
    ZIP,
    ZIP2,
    COUNTRY
FROM read_parquet('{ADDRFILE}');
""")

# =========================
# 3. ONE Processing
# =========================

con.execute("""
CREATE OR REPLACE TABLE ONE AS
SELECT
    ONE_CUSTNO,
    ONE_TOWN,
    ONE_POSTCODE,
    ONE_STATE_ID,
    ADDREF,
    'MALAYSIA' AS ONE_COUNTRY,

    CASE ONE_STATE_ID
        WHEN '01' THEN 'JOH'
        WHEN '02' THEN 'KED'
        WHEN '03' THEN 'KEL'
        WHEN '04' THEN 'MEL'
        WHEN '05' THEN 'NEG'
        WHEN '06' THEN 'PAH'
        WHEN '07' THEN 'PUL'
        WHEN '08' THEN 'PRK'
        WHEN '09' THEN 'PER'
        WHEN '10' THEN 'SAB'
        WHEN '11' THEN 'SAR'
        WHEN '12' THEN 'SEL'
        WHEN '13' THEN 'TER'
        WHEN '14' THEN 'KUL'
        WHEN '15' THEN 'LAB'
        WHEN '16' THEN 'PUT'
        ELSE NULL
    END AS ONE_STATE_CODE

FROM ONELINE
WHERE ONE_STATE_ID NOT IN ('17', '  ');
""")


# =========================
# 4. MERGE Step
# =========================

con.execute("""
CREATE OR REPLACE TABLE MERGE AS
SELECT *
FROM ONE O
LEFT JOIN ADDR A
ON O.ADDREF = A.ADDREF
WHERE NOT (
    O.ONE_TOWN       = A.CITY
AND O.ONE_POSTCODE  = A.ZIP
AND O.ONE_STATE_CODE= A.STATEX
AND O.ONE_COUNTRY   = A.COUNTRY
);
""")


# =========================
# 5. Final Output File
# =========================

con.execute(f"""
CREATE OR REPLACE TABLE OUT3 AS
SELECT
    ONE_CUSTNO,
    ADDREF,
    ONE_TOWN,
    ONE_STATE_CODE,
    ONE_POSTCODE,
    ONE_COUNTRY
FROM MERGE
GROUP BY ALL;
""")

# Save parquet
con.execute(f"COPY OUT3 TO '{OUTFILE2_PARQUET}' (FORMAT PARQUET);")

# =========================
# 5b. Write fixed-width OUTFILE txt
# =========================
df_out = con.execute("SELECT * FROM OUT3").df()

with open(OUTFILE2_TXT, "w", encoding="utf-8") as f:
    for _, row in df_out.iterrows():
        line = (
            f"{str(row.ONE_CUSTNO):<11}"
            f"{str(row.ADDREF):<11}"
            f"{str(row.ONE_TOWN):<25}"
            f"{str(row.ONE_STATE_CODE):<3}"
            f"{str(row.ONE_POSTCODE):<5}"
            f"{str(row.ONE_COUNTRY):<10}"
        )
        f.write(line + "\n")

print("UPDATE.ADR output created.")
