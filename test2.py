import duckdb
import pyarrow.parquet as pq
import pandas as pd

# ---------------------------------------------------------------------
# INPUT PARQUET FILES (already prepared)
# ---------------------------------------------------------------------
CISFILE_PATH = "CIS_CUST_DAILY.parquet"
RLEN_PATH = "UNLOAD_RLEN_CA.parquet"
EMAILADD_PATH = "CCRIS_CISRMRK_EMAIL_FIRST.parquet"

# OUTPUT FILES
OUTPUT_PARQUET = "BTRADE_EMAILADD.parquet"
OUTPUT_TXT = "BTRADE_EMAILADD.txt"

# ---------------------------------------------------------------------
# OPEN DUCKDB CONNECTION
# ---------------------------------------------------------------------
con = duckdb.connect()

# Register parquet files as tables
con.execute(f"CREATE VIEW CISFILE AS SELECT * FROM read_parquet('{CISFILE_PATH}')")
con.execute(f"CREATE VIEW RLEN AS SELECT * FROM read_parquet('{RLEN_PATH}')")
con.execute(f"CREATE VIEW EMAIL AS SELECT * FROM read_parquet('{EMAILADD_PATH}')")

# ---------------------------------------------------------------------
# STEP 1: CREATE CIS DATASET
# ---------------------------------------------------------------------
con.execute("""
CREATE OR REPLACE TEMP TABLE CIS AS
SELECT 
    CUSTNO, 
    ALIAS, 
    ALIASKEY
FROM CISFILE
WHERE ACCTCODE = 'LN'
  AND PRISEC = 901
""")

# ---------------------------------------------------------------------
# STEP 2: CREATE RLEN DATASET
# ---------------------------------------------------------------------
# Simulate SAS INPUT positions (we assume columns exist in parquet)
con.execute("""
CREATE OR REPLACE TEMP TABLE RLEN_CLEAN AS
SELECT 
    ACCTNOC,
    ACCTCODE,
    CUSTNO,
    CAST(RLENCODE AS INTEGER) AS RLENCODE,
    CAST(PRISEC AS INTEGER) AS PRISEC
FROM RLEN
WHERE ACCTCODE = 'LN'
  AND PRISEC = 901
""")

# ---------------------------------------------------------------------
# STEP 3: CREATE EMAIL DATASET
# ---------------------------------------------------------------------
con.execute("""
CREATE OR REPLACE TEMP TABLE EMAIL_CLEAN AS
SELECT 
    CUSTNO,
    UPPER(EMAILADD) AS EMAILADD
FROM EMAIL
""")

# ---------------------------------------------------------------------
# STEP 4: MERGE RLEN + EMAIL + CIS
# ---------------------------------------------------------------------
con.execute("""
CREATE OR REPLACE TEMP TABLE BTLIST AS
SELECT 
    R.CUSTNO,
    R.ACCTNOC,
    R.ACCTCODE,
    C.ALIASKEY,
    C.ALIAS,
    E.EMAILADD,
    CASE 
        WHEN substr(R.ACCTNOC, 1, 3) = '025' THEN 'Y'
        WHEN substr(R.ACCTNOC, 1, 4) = '0285' THEN 'Y'
        ELSE NULL
    END AS BTRADE
FROM RLEN_CLEAN R
JOIN EMAIL_CLEAN E ON R.CUSTNO = E.CUSTNO
JOIN CIS C ON R.CUSTNO = C.CUSTNO
WHERE R.ACCTCODE = 'LN'
  AND (substr(R.ACCTNOC, 1, 3) = '025' OR substr(R.ACCTNOC, 1, 4) = '0285')
""")

# Remove duplicates by CUSTNO + ACCTNOC
con.execute("""
CREATE OR REPLACE TEMP TABLE BTLIST_CLEAN AS
SELECT DISTINCT CUSTNO, ACCTNOC, ACCTCODE, BTRADE, EMAILADD, ALIASKEY, ALIAS
FROM BTLIST
""")

# ---------------------------------------------------------------------
# STEP 5: EXPORT TO PARQUET
# ---------------------------------------------------------------------
btlist_df = con.execute("SELECT * FROM BTLIST_CLEAN").fetchdf()
btlist_df.to_parquet(OUTPUT_PARQUET, index=False)
print(f"✅ Parquet output saved to: {OUTPUT_PARQUET}")

# ---------------------------------------------------------------------
# STEP 6: EXPORT TO FIXED-WIDTH TEXT FILE
# ---------------------------------------------------------------------
with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
    for _, row in btlist_df.iterrows():
        # follow SAS PUT formatting positions
        line = (
            f"{row['CUSTNO']:<11}"
            f"{row['ALIASKEY']:<5}"
            f"{row['ALIAS']:<20}"
            f"{row['EMAILADD']:<60}"
            f"{row['ACCTNOC']:<20}"
        )
        f.write(line + "\n")

print(f"✅ Text output saved to: {OUTPUT_TXT}")

# ---------------------------------------------------------------------
# (Optional) Show sample output
# ---------------------------------------------------------------------
print("\nSample output:")
print(btlist_df.head(10))
