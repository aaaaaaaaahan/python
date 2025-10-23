duckdb.duckdb.BinderException: Binder Error: table emplfull2 has 1 columns but 7 values were supplied

con.execute("""
    CREATE OR REPLACE TABLE emplfull2 AS 
    SELECT 
        NULL AS RECORD_TYPE,
        NULL AS NEW_EMPL_CODE,
        NULL AS OLD_EMPL_CODE,
        NULL AS EMPL_NAME,
        NULL AS ACR_RECEIPT_NO,
        NULL AS ACR_AMOUNT,
        NULL AS LAST_INDICATOR
    LIMIT 0
""")

con.execute("""
    CREATE OR REPLACE TABLE emplfull2 (
        RECORD_TYPE VARCHAR,
        NEW_EMPL_CODE VARCHAR,
        OLD_EMPL_CODE VARCHAR,
        EMPL_NAME VARCHAR,
        ACR_RECEIPT_NO VARCHAR,
        ACR_AMOUNT VARCHAR,
        LAST_INDICATOR VARCHAR
    )
""")
