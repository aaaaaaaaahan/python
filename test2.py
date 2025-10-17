import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet
import datetime
import sys

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year1, month1, day1 = batch_date.year, batch_date.month, batch_date.day

con = duckdb.connect()

# =====================================================
# 1️⃣ LOAD MULTIPLE GENERATIONS (LATEST + 9 PREVIOUS)
# =====================================================
files = get_hive_parquet("PERKESO_EMPLFILE_FULL", generations=10)

# Assign last indicator: A = latest, B = older
tables = []
for idx, f in enumerate(files):
    indicator = 'A' if idx == 0 else 'B'
    tbl_name = f"empl_{idx}"
    con.execute(f"""
        CREATE TABLE {tbl_name} AS
        SELECT 
            RECORD_TYPE,
            NEW_EMPL_CODE,
            TOTAL_REC,
            OLD_EMPL_CODE,
            EMPL_NAME,
            ACR_RECEIPT_NO,
            ACR_AMOUNT,
            '{indicator}' AS LAST_INDICATOR
        FROM '{f}'
        WHERE RECORD_TYPE = 'D'
    """)
    tables.append(tbl_name)

# Combine all files
con.execute(f"""
    CREATE TABLE empl_all AS
    SELECT * FROM {' UNION ALL '.join(tables)}
""")

# =====================================================
# 2️⃣ VALIDATION (similar to SAS)
# =====================================================
# Check empty file
count_all = con.execute("SELECT COUNT(*) FROM empl_all").fetchone()[0]
if count_all == 0:
    print("ERROR: Empty input file.")
    sys.exit(66)

# Check ACR receipt validity
acr_invalid = con.execute("""
    SELECT COUNT(*) FROM empl_all
    WHERE substr(ACR_RECEIPT_NO, 1, 3) != 'ACR'
""").fetchone()[0]
if acr_invalid > 0:
    print(f"ERROR: Found {acr_invalid} invalid ACR_RECEIPT_NO values.")
    sys.exit(55)

# =====================================================
# 3️⃣ MERGE AND SORT (NEW + OLD)
# =====================================================
con.execute("""
    CREATE TABLE empl_sorted AS
    SELECT DISTINCT
        NEW_EMPL_CODE,
        ACR_RECEIPT_NO,
        OLD_EMPL_CODE,
        EMPL_NAME,
        ACR_AMOUNT,
        LAST_INDICATOR
    FROM empl_all
    ORDER BY NEW_EMPL_CODE, ACR_RECEIPT_NO, LAST_INDICATOR
""")

# =====================================================
# 4️⃣ REMOVE DUPLICATES (KEEP FIRST OCCURRENCE)
# =====================================================
con.execute("""
    CREATE TABLE empl_unique AS
    SELECT * FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY NEW_EMPL_CODE, ACR_RECEIPT_NO 
                   ORDER BY LAST_INDICATOR
               ) AS rn
        FROM empl_sorted
    ) WHERE rn = 1
""")

# =====================================================
# 5️⃣ OUTPUT TO PARQUET + CSV
# =====================================================
final_query = f"""
    SELECT 
        NEW_EMPL_CODE,
        ACR_RECEIPT_NO,
        OLD_EMPL_CODE,
        EMPL_NAME,
        ACR_AMOUNT,
        LAST_INDICATOR,
        {year1} AS year,
        {month1} AS month,
        {day1} AS day
    FROM empl_unique
"""

queries = {
    "PERKESO_EMPLFILE_FULLLOAD_UNQ": final_query
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

print("✅ Processing completed successfully.")
