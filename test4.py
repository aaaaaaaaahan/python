import duckdb
import datetime
import sys
from CIS_PY_READER_copy import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet, host_latest_prev_parquet

# =====================================================
# INITIAL SETUP
# =====================================================
batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year1, month1, day1 = batch_date.year, batch_date.month, batch_date.day

# =====================================================
# DUCKDB Connection
# =====================================================
con = duckdb.connect()
emplfile = host_latest_prev_parquet('PERKESO_EMPLFILE_FULL', generation=10)

# =====================================================
# PART 1 - VALIDATION ONLY (NO OUTPUT)
# =====================================================
con.execute(f"""
    CREATE OR REPLACE TABLE emplfull1 AS
    SELECT
        RECORD_TYPE,
        NEW_EMPL_CODE,
        TOTAL_REC,
        OLD_EMPL_CODE,
        EMPL_NAME,
        ACR_RECEIPT_NO,
        ACR_AMOUNT
    FROM read_parquet('{emplfile[0]}')
""")

# Check empty file -> ABORT 66
total_rows = con.execute("SELECT COUNT(*) FROM emplfull1").fetchone()[0]
if total_rows == 0:
    print("ERROR: Input file empty. ABORT 66")
    sys.exit(66)

# Remove header records
con.execute("DELETE FROM emplfull1 WHERE RECORD_TYPE = 'H'")

# Count data records
x = con.execute("SELECT COUNT(*) FROM emplfull1 WHERE RECORD_TYPE = 'D'").fetchone()[0]

# Get footer total
footer_row = con.execute("""
    SELECT SUM(CAST(TOTAL_REC AS BIGINT))
    FROM emplfull1 WHERE RECORD_TYPE = 'F'
""").fetchone()[0]

# Missing footer -> ABORT 77
footer_count = con.execute("SELECT COUNT(*) FROM emplfull1 WHERE RECORD_TYPE = 'F'").fetchone()[0]
if footer_count == 0:
    print("ERROR: No footer record found. ABORT 77")
    sys.exit(77)

# Footer total mismatch -> ABORT 88
if footer_row is None:
    print("ERROR: Footer total is NULL. ABORT 77")
    sys.exit(77)
total_rec_num = int(footer_row)
if total_rec_num != x:
    print(f"ERROR: Footer total ({total_rec_num}) does not match data record count ({x}). ABORT 88")
    sys.exit(88)

# Remove invalid record types
con.execute("DELETE FROM emplfull1 WHERE RECORD_TYPE IN (' ', 'F')")

# Check ACR code
bad_acr_count = con.execute("""
    SELECT COUNT(*) FROM emplfull1
    WHERE RECORD_TYPE = 'D' AND substr(ACR_RECEIPT_NO, 1, 3) != 'ACR'
""").fetchone()[0]
if bad_acr_count > 0:
    print(f"ERROR: {bad_acr_count} record(s) with invalid ACR_RECEIPT_NO. ABORT 55")
    sys.exit(55)

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
        FROM '{host_parquet_path(p)}'
    """)
    con.execute(f"""
        INSERT INTO emplfull2
        SELECT RECORD_TYPE, NEW_EMPL_CODE, OLD_EMPL_CODE, EMPL_NAME,
               ACR_RECEIPT_NO, ACR_AMOUNT, LAST_INDICATOR
        FROM _stg_gen_{idx}
        WHERE RECORD_TYPE = 'D'
    """)

con.execute("""
    CREATE OR REPLACE TABLE emplfull2_sorted AS
    SELECT DISTINCT
        NEW_EMPL_CODE,
        ACR_RECEIPT_NO,
        OLD_EMPL_CODE,
        EMPL_NAME,
        ACR_AMOUNT,
        LAST_INDICATOR
    FROM emplfull2
    ORDER BY NEW_EMPL_CODE, ACR_RECEIPT_NO, LAST_INDICATOR
""")

emplfull_output = """
    SELECT *, 
           {year1} AS year, 
           {month1} AS month, 
           {day1} AS day
    FROM emplfull2_sorted
""".format(year1=year1,month1=month1,day1=day1)

# =====================================================
# PART 3 - REMOVE DUPLICATES (KEEP FIRST)
# =====================================================

con.execute("""
    CREATE OR REPLACE TABLE emplfull_unq AS
    SELECT
        NEW_EMPL_CODE,
        ACR_RECEIPT_NO,
        OLD_EMPL_CODE,
        EMPL_NAME,
        ACR_AMOUNT,
        LAST_INDICATOR
    FROM (
        SELECT *,
            row_number() OVER (PARTITION BY NEW_EMPL_CODE, ACR_RECEIPT_NO
                               ORDER BY LAST_INDICATOR) AS rn
        FROM emplfull2_sorted
    )
    WHERE rn = 1
""")

con.execute("""
    CREATE OR REPLACE TABLE emplfull_unq_sorted AS
    SELECT
        NEW_EMPL_CODE,
        ACR_RECEIPT_NO,
        OLD_EMPL_CODE,
        EMPL_NAME,
        ACR_AMOUNT,
        LAST_INDICATOR
    FROM emplfull_unq
    ORDER BY NEW_EMPL_CODE, ACR_RECEIPT_NO
""")

emplfull_unq = """
    SELECT *, 
           {year1} AS year, 
           {month1} AS month, 
           {day1} AS day
    FROM emplfull_unq_sorted
""".format(year1=year1,month1=month1,day1=day1)

queries = {
    "PERKESO_EMPLFILE_FULLLOAD"                : emplfull_output,
    "PERKESO_EMPLFILE_FULLLOAD_UNQ"            : emplfull_unq
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
