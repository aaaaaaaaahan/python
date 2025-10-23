import duckdb
import datetime
import sys
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet

# =====================================================
# INITIAL SETUP
# =====================================================
batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year1, month1, day1 = batch_date.year, batch_date.month, batch_date.day
con = duckdb.connect()

BASE_FILENAME = "PERKESO.EMPLFILE.FULL.parquet"

# =====================================================
# PART 1 - VALIDATION ONLY (NO OUTPUT)
# =====================================================
print("=== PART 1: Validate latest parquet ===")

latest_path = get_hive_parquet(BASE_FILENAME)

con.execute(f"""
    CREATE OR REPLACE TABLE emplfull_part1 AS
    SELECT
        RECORD_TYPE,
        NEW_EMPL_CODE,
        TOTAL_REC,
        OLD_EMPL_CODE,
        EMPL_NAME,
        ACR_RECEIPT_NO,
        ACR_AMOUNT
    FROM '{host_parquet_path(latest_path)}'
""")

# Check empty file -> ABORT 66
total_rows = con.execute("SELECT COUNT(*) FROM emplfull_part1").fetchone()[0]
if total_rows == 0:
    print("ERROR: Input file empty. ABORT 66")
    sys.exit(66)

# Remove header records
con.execute("DELETE FROM emplfull_part1 WHERE RECORD_TYPE = 'H'")

# Count data records
x = con.execute("SELECT COUNT(*) FROM emplfull_part1 WHERE RECORD_TYPE = 'D'").fetchone()[0]

# Get footer total
footer_row = con.execute("""
    SELECT SUM(CAST(TOTAL_REC AS BIGINT))
    FROM emplfull_part1 WHERE RECORD_TYPE = 'F'
""").fetchone()[0]

# Missing footer -> ABORT 77
footer_count = con.execute("SELECT COUNT(*) FROM emplfull_part1 WHERE RECORD_TYPE = 'F'").fetchone()[0]
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
con.execute("DELETE FROM emplfull_part1 WHERE RECORD_TYPE IN (' ', 'F')")

# Check ACR code
bad_acr_count = con.execute("""
    SELECT COUNT(*) FROM emplfull_part1
    WHERE RECORD_TYPE = 'D' AND substr(ACR_RECEIPT_NO, 1, 3) != 'ACR'
""").fetchone()[0]
if bad_acr_count > 0:
    print(f"ERROR: {bad_acr_count} record(s) with invalid ACR_RECEIPT_NO. ABORT 55")
    sys.exit(55)

print(f"PART 1 validation passed: {x} data record(s) verified successfully.")

# =====================================================
# PART 2 - COMBINE LATEST + 9 PREVIOUS
# =====================================================
print("=== PART 2: Combine latest + 9 previous parquet files ===")

paths = get_hive_parquet(BASE_FILENAME, generations=10)
if isinstance(paths, str):
    paths = [paths]
if not paths:
    print("ERROR: No parquet generations found. ABORT 66")
    sys.exit(66)

con.execute("CREATE OR REPLACE TABLE allfiles_part2 AS SELECT NULL AS RECORD_TYPE LIMIT 0")

for idx, p in enumerate(paths):
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
        INSERT INTO allfiles_part2
        SELECT RECORD_TYPE, NEW_EMPL_CODE, OLD_EMPL_CODE, EMPL_NAME,
               ACR_RECEIPT_NO, ACR_AMOUNT, LAST_INDICATOR
        FROM _stg_gen_{idx}
        WHERE RECORD_TYPE = 'D'
    """)

con.execute("""
    CREATE OR REPLACE TABLE allfiles_part2_sorted AS
    SELECT DISTINCT
        NEW_EMPL_CODE,
        ACR_RECEIPT_NO,
        OLD_EMPL_CODE,
        EMPL_NAME,
        ACR_AMOUNT,
        LAST_INDICATOR
    FROM allfiles_part2
    ORDER BY NEW_EMPL_CODE, ACR_RECEIPT_NO, LAST_INDICATOR
""")

final_query_p2 = f"""
    SELECT *, {year1} AS year, {month1} AS month, {day1} AS day
    FROM allfiles_part2_sorted
"""

name_p2 = "PERKESO_EMPLFILE_FULLLOAD_PART2"
parquet_path_p2 = parquet_output_path(name_p2)
csv_path_p2 = csv_output_path(name_p2)

con.execute(f"""
COPY ({final_query_p2})
TO '{parquet_path_p2}'
(FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
""")

con.execute(f"""
COPY ({final_query_p2})
TO '{csv_path_p2}'
(FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);
""")

print(f"PART 2 completed: Combined {len(paths)} parquet generations.")

# =====================================================
# PART 3 - REMOVE DUPLICATES (KEEP FIRST)
# =====================================================
print("=== PART 3: Remove duplicates ===")

con.execute("""
    CREATE OR REPLACE TABLE allfiles_part3_unq AS
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
        FROM allfiles_part2_sorted
    )
    WHERE rn = 1
""")

con.execute("""
    CREATE OR REPLACE TABLE allfiles_part3_sorted AS
    SELECT
        NEW_EMPL_CODE,
        ACR_RECEIPT_NO,
        OLD_EMPL_CODE,
        EMPL_NAME,
        ACR_AMOUNT,
        LAST_INDICATOR
    FROM allfiles_part3_unq
    ORDER BY NEW_EMPL_CODE, ACR_RECEIPT_NO
""")

final_query_p3 = f"""
    SELECT *, {year1} AS year, {month1} AS month, {day1} AS day
    FROM allfiles_part3_sorted
"""

name_p3 = "PERKESO_EMPLFILE_FULLLOAD_UNQ_PART3"
parquet_path_p3 = parquet_output_path(name_p3)
csv_path_p3 = csv_output_path(name_p3)

con.execute(f"""
COPY ({final_query_p3})
TO '{parquet_path_p3}'
(FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
""")

con.execute(f"""
COPY ({final_query_p3})
TO '{csv_path_p3}'
(FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);
""")

print(f"PART 3 completed: Duplicates removed -> {name_p3}")

con.close()
print("=== ALL PARTS COMPLETED SUCCESSFULLY ===")
