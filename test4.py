# ============================================================
# STEP 1: FILTER APPL_CODE = 'CUST '
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE OKAY AS
    SELECT *
    FROM RMKFILE
    WHERE TRIM(APPL_CODE) = 'CUST'
""")

# ============================================================
# STEP 2: REMOVE DUPLICATES (KEEP FIRST) + OUTPUT DUPES
# PROC SORT NODUPKEY DUPOUT=DUPNI
# ============================================================
# Create DUPNI = duplicate records (removed ones)
con.execute("""
    CREATE OR REPLACE TABLE DUPNI AS
    SELECT *
    FROM OKAY
    WHERE (APPL_NO, EFF_DATE) IN (
        SELECT APPL_NO, EFF_DATE
        FROM OKAY
        GROUP BY APPL_NO, EFF_DATE
        HAVING COUNT(*) > 1
    )
""")

# Keep only one record per APPL_NO + EFF_DATE in OKAY
con.execute("""
    CREATE OR REPLACE TABLE OKAY AS
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY APPL_NO, EFF_DATE ORDER BY EFF_DATE DESC) AS rn
        FROM OKAY
    )
    WHERE rn = 1
""")
