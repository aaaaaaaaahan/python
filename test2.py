import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import os

# ============================================================
# PATH SETUP
# ============================================================
input_dir = "/host/cis/parquet/sas_parquet"    # assumed parquet input
output_dir = "/host/cis/output"
os.makedirs(output_dir, exist_ok=True)

# Input parquet files (already converted)
CISFILE = f"{input_dir}/CIS_CUST_DAILY.parquet"
IDFILE  = f"{input_dir}/CIS_CUST_DAILY_IDS.parquet"
CCFILE  = f"{input_dir}/CCRIS_CC_RLNSHIP_SRCH.parquet"

# Output file
OUT_PARQUET = os.path.join(output_dir, "SCEL_LOAN_IDS.parquet")

# ============================================================
# CREATE DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()

# ============================================================
# REGISTER INPUT FILES
# ============================================================
con.register("CISFILE", CISFILE)
con.register("IDFILE", IDFILE)
con.register("CCFILE", CCFILE)

# ============================================================
# 1. CCPARTNER: Extract relationship code '050'
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE CCPARTNER AS
    SELECT 
        CUSTNO,
        'Y' AS PARTNER_INDC,
        CODE,
        DESC
    FROM CCFILE
    WHERE CODE = '050'
""")

# ============================================================
# 2. RLEN017 and RLEN020: Filter by RLENCODE and ACCTCODE
# ============================================================
account_filter = """
    ACCTCODE IN ('LN','DP')
    AND (
        (ACCTNO BETWEEN 1000000000 AND 1999999999) OR
        (ACCTNO BETWEEN 2000000000 AND 2999999999) OR
        (ACCTNO BETWEEN 3000000000 AND 3999999999) OR
        (ACCTNO BETWEEN 4000000000 AND 4999999999) OR
        (ACCTNO BETWEEN 5000000000 AND 5999999999) OR
        (ACCTNO BETWEEN 6000000000 AND 6999999999) OR
        (ACCTNO BETWEEN 7000000000 AND 7999999999) OR
        (ACCTNO BETWEEN 8000000000 AND 8999999999)
    )
"""

con.execute(f"""
    CREATE OR REPLACE TABLE RLEN017 AS
    SELECT 
        ACCTNO, CUSTNO, BASICGRPCODE,
        'Y' AS GTOR_INDC
    FROM CISFILE
    WHERE {account_filter}
      AND RLENCODE = 017
""")

con.execute(f"""
    CREATE OR REPLACE TABLE RLEN020 AS
    SELECT 
        ACCTNO, CUSTNO, BASICGRPCODE,
        'Y' AS BORROWER_INDC
    FROM CISFILE
    WHERE {account_filter}
      AND RLENCODE = 020
""")

# ============================================================
# 3. Merge RLEN017 and RLEN020
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE MERGE_RLEN AS
    SELECT 
        COALESCE(a.ACCTNO, b.ACCTNO) AS ACCTNO,
        COALESCE(a.CUSTNO, b.CUSTNO) AS CUSTNO,
        COALESCE(a.BASICGRPCODE, b.BASICGRPCODE) AS BASICGRPCODE,
        COALESCE(GTOR_INDC, 'N') AS GTOR_INDC,
        COALESCE(BORROWER_INDC, 'N') AS BORROWER_INDC
    FROM RLEN017 a
    FULL OUTER JOIN RLEN020 b
      ON a.ACCTNO = b.ACCTNO AND a.CUSTNO = b.CUSTNO
""")

# ============================================================
# 4. LNCUST: Loan + Deposit customers
# ============================================================
con.execute(f"""
    CREATE OR REPLACE TABLE LNCUST AS
    SELECT 
        CUSTNO, ACCTNO, ACCTNOC, PRISEC, TAXID,
        RLENCODE, RELATIONDESC, ACCTCODE, BASICGRPCODE
    FROM CISFILE
    WHERE {account_filter}
""")

# ============================================================
# 5. Merge LNCUST with MERGE_RLEN
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE MERGE_GTOR AS
    SELECT 
        l.*, 
        r.BORROWER_INDC, 
        r.GTOR_INDC
    FROM LNCUST l
    LEFT JOIN MERGE_RLEN r
      ON l.CUSTNO = r.CUSTNO AND l.ACCTNO = r.ACCTNO
""")

# ============================================================
# 6. Merge with CCPARTNER
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE MERGE_PARTNER AS
    SELECT 
        g.*, 
        c.PARTNER_INDC
    FROM MERGE_GTOR g
    LEFT JOIN CCPARTNER c
      ON g.CUSTNO = c.CUSTNO
""")

# ============================================================
# 7. Customer IDs (Alias)
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE IDS AS
    SELECT 
        CUSTNO, ALIASKEY, ALIAS
    FROM IDFILE
    WHERE ALIASKEY IN ('IC','BC','PP','ML','PL','BR','CI','PC','SA','GB','LP')
""")

# ============================================================
# 8. Combine into LNDETL
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE LNDETL AS
    SELECT 
        m.CUSTNO,
        m.ACCTNOC,
        m.TAXID,
        m.PRISEC,
        COALESCE(m.BORROWER_INDC, 'N') AS BORROWER_INDC,
        COALESCE(m.GTOR_INDC, 'N') AS GTOR_INDC,
        COALESCE(m.PARTNER_INDC, 'N') AS PARTNER_INDC,
        m.BASICGRPCODE,
        i.ALIASKEY,
        i.ALIAS
    FROM MERGE_PARTNER m
    JOIN IDS i
      ON m.CUSTNO = i.CUSTNO
""")

# ============================================================
# 9. NEWIC and OLDIC logic
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE NEWIC AS
    SELECT 
        *,
        (ALIASKEY || ALIAS) AS CUSTID,
        '' AS TAXID
    FROM LNDETL
""")

con.execute("""
    CREATE OR REPLACE TABLE OLDIC AS
    SELECT 
        *,
        ('OC ' || TAXID) AS CUSTID,
        '' AS ALIASKEY,
        '' AS ALIAS
    FROM LNDETL
    WHERE TAXID != '' AND TAXID != '000000000'
""")

# ============================================================
# 10. Combine both sets
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE CUSTIDS AS
    SELECT * FROM NEWIC
    UNION ALL
    SELECT * FROM OLDIC
""")

# ============================================================
# 11. Final output dataset
# ============================================================
final_df = con.execute("""
    SELECT 
        ACCTNOC,
        CUSTNO,
        CUSTID,
        CASE WHEN PRISEC = 901 THEN 'P'
             WHEN PRISEC = 902 THEN 'S'
             ELSE '' END AS PRIMSEC,
        COALESCE(BORROWER_INDC, 'N') AS BORROWER_INDC,
        COALESCE(GTOR_INDC, 'N') AS GTOR_INDC,
        COALESCE(PARTNER_INDC, 'N') AS PARTNER_INDC,
        BASICGRPCODE
    FROM CUSTIDS
""").arrow()

# ============================================================
# 12. SAVE USING PYARROW
# ============================================================
pq.write_table(final_df, OUT_PARQUET, compression='snappy')

print(f"✅ Process completed. Output saved to: {OUT_PARQUET}")
