import duckdb
import datetime

# ---------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------
input_parquet_path = "CIDARPGS.parquet"          # Input file (already Parquet)
output_parquet_path = "CIDARPGS_CONSENT.parquet" # Output Parquet
output_txt_path = "CIDARPGS_CONSENT.txt"         # Output text file

# ---------------------------------------------------------------------
# DUCKDB PROCESSING
# ---------------------------------------------------------------------
con = duckdb.connect()

# Load input parquet into DuckDB
con.execute(f"""
    CREATE OR REPLACE TABLE CISEOD AS
    SELECT *
    FROM read_parquet('{input_parquet_path}')
""")

# ---------------------------------------------------------------------
# Transform logic (convert SAS logic into SQL)
# ---------------------------------------------------------------------
# This will handle:
#  - Filtering REPORTNO=8106 and SORTSETNO=1
#  - Deriving CONSENTX from MISC(A–J)
#  - Mapping 001→Y, 002→N
#  - Formatting date from UPDDATE (PD6 numeric to YYYY-MM-DD)
#  - Add fixed constant fields
# ---------------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE CONSENT AS
    WITH BASE AS (
        SELECT 
            BANKNO,
            REPORTNO,
            SORTSETNO,
            UPDATEOPERATOR,
            UPDDATE,
            CUSTNO,
            CASE 
                WHEN MISCA IN ('07A','07C','07D') THEN CONSENTA
                WHEN MISCB IN ('07A','07C','07D') THEN CONSENTB
                WHEN MISCC IN ('07A','07C','07D') THEN CONSENTC
                WHEN MISCD IN ('07A','07C','07D') THEN CONSENTD
                WHEN MISCE IN ('07A','07C','07D') THEN CONSENTE
                WHEN MISCF IN ('07A','07C','07D') THEN CONSENTF
                WHEN MISCG IN ('07A','07C','07D') THEN CONSENTG
                WHEN MISCH IN ('07A','07C','07D') THEN CONSENTH
                WHEN MISCI IN ('07A','07C','07D') THEN CONSENTI
                WHEN MISCJ IN ('07A','07C','07D') THEN CONSENTJ
                ELSE ''
            END AS CONSENTX
        FROM CISEOD
        WHERE REPORTNO = 8106 AND SORTSETNO = 1
    ),
    FILTERED AS (
        SELECT *
        FROM BASE
        WHERE CONSENTX <> ''
    ),
    FINAL AS (
        SELECT DISTINCT
            BANKNO,
            CUSTNO AS CUSTNOX,
            'CUST' AS APPLCODE,
            'BATCH' AS UPDATESOURCE,
            CASE 
                WHEN CONSENTX = '001' THEN 'Y'
                WHEN CONSENTX = '002' THEN 'N'
                ELSE ''
            END AS CONSENT,
            -- Convert numeric date to YYYY-MM-DD
            CASE 
                WHEN length(cast(UPDDATE AS VARCHAR)) = 6 THEN 
                    substr(lpad(cast(UPDDATE AS VARCHAR),8,'0'),5,4) || '-' ||
                    substr(lpad(cast(UPDDATE AS VARCHAR),8,'0'),3,2) || '-' ||
                    substr(lpad(cast(UPDDATE AS VARCHAR),8,'0'),1,2)
                ELSE ''
            END AS UPDATEDATE,
            '00000000' AS UPDATETIME,
            UPDATEOPERATOR
        FROM FILTERED
    )
    SELECT * FROM FINAL
""")

# ---------------------------------------------------------------------
# OUTPUT PARQUET
# ---------------------------------------------------------------------
con.execute(f"""
    COPY (SELECT * FROM CONSENT)
    TO '{output_parquet_path}' (FORMAT PARQUET);
""")
print(f"✅ Parquet file created: {output_parquet_path}")

# ---------------------------------------------------------------------
# OUTPUT FIXED-WIDTH TEXT
# ---------------------------------------------------------------------
# We can use DuckDB's string formatting functions to simulate SAS PUT positions
# Equivalent to the SAS PUT @positions logic
# ---------------------------------------------------------------------
con.execute(f"""
    COPY (
        SELECT
            printf('%03d', BANKNO) ||
            'CUST' ||
            printf('%011d', CUSTNOX) ||
            CONSENT ||
            UPDATEDATE ||
            'X' ||
            rpad(UPDATESOURCE,5,' ') ||
            UPDATEDATE ||
            UPDATETIME ||
            rpad(UPDATESOURCE,5,' ') ||
            UPDATEDATE ||
            UPDATETIME ||
            rpad(UPDATEOPERATOR,8,' ')
        AS LINE
        FROM CONSENT
    )
    TO '{output_txt_path}' (FORMAT CSV, DELIMITER '', HEADER FALSE);
""")
print(f"✅ Text output created: {output_txt_path}")
