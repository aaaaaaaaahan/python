import duckdb
import datetime
import os
import sys

# =====================================
# CONFIGURATION
# =====================================
input_parquet = "/host/cis/parquet/KWSP.EMPLOYER.FILE.parquet"

# =====================================
# PROCESS WITH DUCKDB
# =====================================
con = duckdb.connect()

# Read input Parquet file into DuckDB table
con.execute(f"""
    CREATE OR REPLACE TABLE kwsp AS 
    SELECT * FROM read_parquet('{input_parquet}')
""")

# Ensure expected columns exist
required_cols = ["REC_ID", "EMPLYR_NO", "EMPLYR_NAME1"]
for col in required_cols:
    if col not in [c[0] for c in con.execute("DESCRIBE kwsp").fetchall()]:
        sys.exit(f"ABORT 000: Missing required column {col}")

# Clean and normalize columns
con.execute("""
    UPDATE kwsp
    SET 
        REC_ID = TRIM(CAST(REC_ID AS VARCHAR)),
        EMPLYR_NO = COALESCE(CAST(EMPLYR_NO AS INTEGER), 0),
        EMPLYR_NAME1 = TRIM(CAST(COALESCE(EMPLYR_NAME1, '') AS VARCHAR))
""")

# =====================================
# VALIDATION (simulate SAS ABORT)
# =====================================
# Check missing REC_ID
null_rec = con.execute("SELECT COUNT(*) FROM kwsp WHERE REC_ID IS NULL OR REC_ID = ''").fetchone()[0]
if null_rec > 0:
    sys.exit("ABORT 111: REC_ID missing")

# Check missing Employer Info for REC_ID=01
missing_emp = con.execute("""
    SELECT COUNT(*) FROM kwsp
    WHERE REC_ID = '01' AND (EMPLYR_NO = 0 OR EMPLYR_NAME1 = '')
""").fetchone()[0]
if missing_emp > 0:
    sys.exit("ABORT 222: Missing Employer Info for REC_ID=01")

# Check last record REC_ID
last_rec_id = con.execute("SELECT REC_ID FROM kwsp ORDER BY rowid DESC LIMIT 1").fetchone()[0]
if last_rec_id != "02":
    sys.exit("ABORT 333: Last record is not 02")

# Check TOTAL_REC consistency
x_count = con.execute("SELECT COUNT(*) FROM kwsp WHERE REC_ID = '01'").fetchone()[0]
try:
    total_rec = con.execute("""
        SELECT TOTAL_REC FROM kwsp ORDER BY rowid DESC LIMIT 1
    """).fetchone()[0]
except:
    total_rec = x_count

if total_rec != x_count:
    sys.exit("ABORT 444: TOTAL_REC mismatch")

# =====================================
# DERIVED & FIXED FIELDS
# =====================================
# Add or set default columns
con.execute("""
    ALTER TABLE kwsp ADD COLUMN IF NOT EXISTS IND_ORG VARCHAR DEFAULT 'O';
""")
con.execute("""
    ALTER TABLE kwsp ADD COLUMN IF NOT EXISTS ROB_ROC VARCHAR DEFAULT ' ';
""")
con.execute("""
    ALTER TABLE kwsp ADD COLUMN IF NOT EXISTS EMPLYR_NAME2 VARCHAR DEFAULT ' ';
""")

# Clean carriage returns
con.execute("""
    UPDATE kwsp
    SET 
        EMPLYR_NAME1 = REPLACE(EMPLYR_NAME1, '\r', ''),
        EMPLYR_NAME2 = REPLACE(EMPLYR_NAME2, '\r', '')
""")

# =====================================
# FINAL OUTPUT (no COPY)
# =====================================
# Fetch and print the processed output
df_out = con.execute("""
    SELECT 
        REC_ID,
        IND_ORG,
        EMPLYR_NO,
        ROB_ROC,
        EMPLYR_NAME1,
        EMPLYR_NAME2
    FROM kwsp
""").fetchdf()

print("✅ KWSP processing complete. Output preview:")
print(df_out)
