# ================================================================
# FROZEN / INACTIVE ACCOUNTS FILE
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE FRZ AS
    WITH raw AS (
        SELECT
            CAST(ACCTNO AS VARCHAR) AS ACCTNO,
            CAST(LCUSTDATEM AS INTEGER) AS LCUSTDATEM,
            CAST(LCUSTDATED AS INTEGER) AS LCUSTDATED,
            CAST(LCUSTDATEY AS INTEGER) AS LCUSTDATEY,
            CURRENCY,
            OPENINDC,
            DORM1,
            POST1,
            POSTDATE,
            POSTREASON,
            POSTINSTRUCTION
        FROM '{host_parquet_path("FROZEN_INACTIVE_ACCT.parquet")}'
        WHERE ACCTNO > '01000000000' AND ACCTNO < '01999999999'
    ),
    dated AS (
        SELECT
            ACCTNO,
            CONCAT(LPAD(LCUSTDATED, 2, '0'), 
                   LPAD(LCUSTDATEM, 2, '0'), 
                   2000+LCUSTDATEY) AS DATE1,
            POSTDATE AS DATE2,
            DORM1,
            POST1
        FROM raw
    ),
    status AS (
        SELECT
            ACCTNO,
            DATE1,
            DATE2,
            CASE
                WHEN POST1 IS NOT NULL AND POST1 <> '' THEN 'FROZEN'
                WHEN DORM1 = 'D' THEN 'DORMANT'
                WHEN DORM1 = 'N' THEN 'INACTIVE'
                ELSE 'UNKNOWN'
            END AS ACCTSTATUS,
            CASE
                WHEN POST1 IS NOT NULL AND POST1 <> '' THEN DATE2
                ELSE DATE1
            END AS DATE3
        FROM dated
    )
    SELECT
        ACCTNO,
        ACCTSTATUS,
        DATE1,
        DATE2,
        DATE3,
        DATE3 AS DATECLSE
    FROM status
    ORDER BY ACCTNO
""")

print("FRZ (first 5 rows):")
print(con.execute("SELECT * FROM FRZ LIMIT 5").fetchdf())
