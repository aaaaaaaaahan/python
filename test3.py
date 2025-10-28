# ============================================================
# STEP 3: ADD GROUP_ID & EFF_DATE_ADD (SAS BY-group equivalent)
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE LATEST AS
    SELECT 
        *,
        DENSE_RANK() OVER (ORDER BY APPL_NO, EFF_DATE) AS GROUP_ID,
        ROW_NUMBER() OVER (PARTITION BY APPL_NO ORDER BY EFF_DATE) AS EFF_DATE_ADD
    FROM DUPNI
""")
