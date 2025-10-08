duckdb.duckdb.BinderException: Binder Error: Cannot mix values of type VARCHAR and INTEGER_LITERAL in BETWEEN clause - an explicit cast is required

LINE 10:         (ACCTNO BETWEEN 1000000000 AND 1999999999) OR
                         ^

import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path, get_hive_parquet
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year1, month1, day1 = batch_date.year, batch_date.month, batch_date.day

# ============================================================
# CREATE DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()
CISFILE, year, month, day = get_hive_parquet('CIS_CUST_DAILY')
IDFILE, year, month, day = get_hive_parquet('CIS_CUST_DAILY_IDS')
CCFILE, year, month, day = get_hive_parquet('CCRIS_CC_RLNSHIP_SRCH')

# ============================================================
# 1. CCPARTNER: Extract relationship code '050'
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE CCPARTNER AS
    SELECT 
        CUSTNO1 AS CUSTNO,
        INDORG1 AS INDORG,
        CODE1 AS CODE,
        DESC1 AS DESC,
        CASE
            WHEN CODE = '50' THEN 'Y'
            ELSE 'N'
        END AS PARTNER_INDC
    FROM read_parquet(?)
""", [CCFILE])

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
    FROM read_parquet(?)
    WHERE {account_filter}
      AND RLENCODE = 017
""", [CISFILE])

con.execute(f"""
    CREATE OR REPLACE TABLE RLEN020 AS
    SELECT 
        ACCTNO, CUSTNO, BASICGRPCODE,
        'Y' AS BORROWER_INDC
    FROM read_parquet(?)
    WHERE {account_filter}
      AND RLENCODE = 020
""", [CISFILE])

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
    FROM read_parquet(?)
    WHERE {account_filter}
""", [CISFILE])

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
    FROM read_parquet(?)
    WHERE ALIASKEY IN ('IC','BC','PP','ML','PL','BR','CI','PC','SA','GB','LP')
""", [IDFILE])

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
final_df = """
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
        ,{year1} AS year
        ,{month1} AS month
        ,{day1} AS day
    FROM CUSTIDS
""".format(year1=year1,month1=month1,day1=day1)

# ============================================================
# 12. SAVE USING PYARROW
# ============================================================
queries = {
    "SCEL_LOAN_iDS"            : final_df
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
