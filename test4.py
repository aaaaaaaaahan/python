# =====================================================
# PART 2 - COMBINE LATEST + 9 PREVIOUS
# =====================================================
if isinstance(emplfile, str):
    emplfile = [emplfile]
if not emplfile:
    print("ERROR: No parquet generations found. ABORT 66")
    sys.exit(66)

con.execute("CREATE OR REPLACE TABLE emplfull2 AS SELECT NULL AS RECORD_TYPE LIMIT 0")

for idx, p in enumerate(emplfile):
    indicator = 'A' if idx == 0 else 'B'
    con.execute(f"""
        CREATE OR REPLACE TABLE _stg_gen_{idx} AS
        SELECT
            RECORD_TYPE,
            NEW_EMPL_CODE,
            OLD_EMPL_CODE,
            substr(EMPL_NAME,1,40) AS EMPL_NAME,
            ACR_RECEIPT_NO,
            ACR_AMOUNT,
            '{indicator}' AS LAST_INDICATOR
        FROM read_parquet('{p}')
    """)
    con.execute(f"""
        INSERT INTO emplfull2
        SELECT RECORD_TYPE, NEW_EMPL_CODE, OLD_EMPL_CODE, EMPL_NAME,
               ACR_RECEIPT_NO, ACR_AMOUNT, LAST_INDICATOR
        FROM _stg_gen_{idx}
        WHERE RECORD_TYPE = 'D'
    """)
