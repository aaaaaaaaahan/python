# ================================================================
# Deposit master (MYR + FOREX)
# ================================================================
con.execute("""
    CREATE OR REPLACE TABLE DEPOSIT AS
    SELECT * FROM DPMYR
    UNION ALL
    SELECT * FROM FOREXMRG
""")

# Attach Frozen/Inactive (FRZ from previous step)
con.execute("""
    CREATE OR REPLACE TABLE DEPOSIT2 AS
    SELECT d.*, f.ACCTSTATUS AS FRZ_STATUS, f.DATECLSE
    FROM DEPOSIT d
    LEFT JOIN FRZ f
    ON d.ACCTNO = f.ACCTNO
""")
