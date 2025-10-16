import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import datetime
import sys
import os

# =====================================================
# CONFIGURATION
# =====================================================
input_parquet = "/path/to/PERKESO.FCLBFILE.FULL.parquet"  # Input Parquet file
output_csv = "/path/to/PERKESO.FCLBFILE.FULLLOAD.csv"     # Output CSV file

# =====================================================
# PROCESS FILE USING DUCKDB
# =====================================================
con = duckdb.connect()

# Read parquet into DuckDB
con.execute(f"""
    CREATE TABLE fclbfull AS 
    SELECT * FROM read_parquet('{input_parquet}')
""")

# =====================================================
# FILTERING & VALIDATION (same as SAS logic)
# =====================================================
# Remove header and footer
con.execute("""
    DELETE FROM fclbfull WHERE record_type = 'H'
""")

# Count data records
x = con.execute("""
    SELECT COUNT(*) FROM fclbfull WHERE record_type = 'D'
""").fetchone()[0]

# Get footer total record
footer_total = con.execute("""
    SELECT CAST(total_rec AS INTEGER) 
    FROM fclbfull 
    WHERE record_type = 'F'
""").fetchone()

if footer_total:
    total_rec_num = footer_total[0]
    if total_rec_num != x:
        print(f"ERROR: Footer total ({total_rec_num}) does not match data record count ({x}).")
        sys.exit(88)
else:
    print("ERROR: No footer record found.")
    sys.exit(77)

# Delete footer records
con.execute("""
    DELETE FROM fclbfull WHERE record_type = 'F'
""")

# Remove invalid NOTICEID (missing last 3 chars)
con.execute("""
    DELETE FROM fclbfull 
    WHERE substr(noticeid, 15, 3) = '   '
""")

# =====================================================
# SORTING (two sorts like SAS)
# =====================================================
con.execute("""
    CREATE TABLE fclbfull_sorted AS
    SELECT DISTINCT * FROM fclbfull
    ORDER BY new_empl_code, noticeid, empl_name, amount
""")

# =====================================================
# EXPORT TO CSV USING PYARROW
# =====================================================
# Fetch as Arrow Table
arrow_table = con.execute("""
    SELECT 
        new_empl_code AS NEW_EMPL_CODE,
        noticeid AS NOTICEID,
        empl_name AS EMPL_NAME,
        amount AS AMOUNT
    FROM fclbfull_sorted
""").arrow()

# Write to CSV (fixed columns, no header)
csv.write_csv(
    arrow_table,
    output_csv,
    write_options=csv.WriteOptions(include_header=False, delimiter='|')  # You can change delimiter if needed
)

print(f"✅ Output file created successfully: {output_csv}")
