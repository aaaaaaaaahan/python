import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import os
import datetime

# ============================================================
# CONFIGURATION
# ============================================================
input_parquet = "/host/cis/parquet/PERKESO_FCLBEISC_FULL.parquet"
output_csv = "/host/cis/output/PERKESO_FCLBEISC_FULLLOAD.csv"

# ============================================================
# DUCKDB PROCESSING
# ============================================================
con = duckdb.connect(database=':memory:')

# Read parquet into DuckDB
con.execute(f"""
    CREATE TABLE fclb AS 
    SELECT 
        record_type,
        new_empl_code,
        total_rec,
        empl_name,
        noticeid,
        amount
    FROM read_parquet('{input_parquet}')
""")

# ============================================================
# 1. FILTER HEADERS AND FOOTERS
# ============================================================
# Remove Header
con.execute("DELETE FROM fclb WHERE record_type = 'H'")

# Count Data Records
data_count = con.execute("SELECT COUNT(*) FROM fclb WHERE record_type = 'D'").fetchone()[0]

# ============================================================
# 2. VALIDATE FOOTER TOTAL
# ============================================================
footer_total = con.execute("""
    SELECT CAST(total_rec AS INTEGER) 
    FROM fclb WHERE record_type = 'F'
""").fetchone()

if footer_total:
    footer_total = footer_total[0]
    if footer_total != data_count:
        raise ValueError(f"Record count mismatch! Expected {footer_total}, found {data_count}. (Abort 88)")
else:
    raise ValueError("No footer record found! (Abort 77)")

# ============================================================
# 3. REMOVE FOOTER RECORD
# ============================================================
con.execute("DELETE FROM fclb WHERE record_type = 'F'")

# ============================================================
# 4. INVALID NOTICE ID CHECK
# ============================================================
# Remove records with blank last 3 chars of NOTICEID
con.execute("""
    DELETE FROM fclb 
    WHERE SUBSTR(noticeid, 15, 3) = '   '
""")

# ============================================================
# 5. SORT RECORDS
# ============================================================
sorted_table = con.execute("""
    SELECT DISTINCT new_empl_code, noticeid, empl_name, amount
    FROM fclb
    ORDER BY new_empl_code, noticeid, empl_name, amount
""").arrow()

# ============================================================
# 6. OUTPUT FILE (CSV)
# ============================================================
# Align columns according to SAS PUT statement (fixed order)
# Positions:
# @001  NEW_EMPL_CODE   $12.
# @013  NOTICEID        $17.
# @030  EMPL_NAME       $100.
# @130  AMOUNT          $14.

formatted_rows = []
for row in sorted_table.to_pylist():
    new_empl_code = str(row["new_empl_code"] or "").ljust(12)
    noticeid = str(row["noticeid"] or "").ljust(17)
    empl_name = str(row["empl_name"] or "").ljust(100)
    amount = str(row["amount"] or "").rjust(14)
    formatted_rows.append(new_empl_code + noticeid + empl_name + amount)

# Write to CSV (fixed-width style, single column)
table = pa.table({'record': formatted_rows})
csv.write_csv(table, output_csv)

print(f"✅ Output written to {output_csv}")
print(f"Total valid records: {len(formatted_rows)}")
