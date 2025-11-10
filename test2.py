import duckdb
import datetime

# -----------------------------
# File paths
# -----------------------------
input_parquet = "UNLOAD_CIHRCRVT.parquet"
init_txt = "CIHRCRVP_INIT.txt"
update_txt = "CIHRCRVP_UPDATE.txt"

# -----------------------------
# Set batch/reporting date
# -----------------------------
batch_date = datetime.date.today()  # replace with control date if needed
date_str = batch_date.strftime("%Y%m%d")  # SAS YYMMDD8 equivalent

# -----------------------------
# Connect DuckDB in memory
# -----------------------------
con = duckdb.connect(database=':memory:')

# -----------------------------
# Load input Parquet file
# -----------------------------
con.execute(f"""
CREATE TABLE allrec AS
SELECT *
FROM read_parquet('{input_parquet}')
""")

# -----------------------------
# Filter records according to SAS logic
# -----------------------------
con.execute(f"""
CREATE TABLE filtered AS
SELECT *
FROM allrec
WHERE HRV_ACCT_OPENDATE = '{date_str}'
  AND HRV_FUZZY_INDC = 'Y'
  AND (HRV_OVERRIDING_INDC IS NULL OR HRV_OVERRIDING_INDC = 'N')
  AND HRV_BRCH_CODE <> '996'
""")

# -----------------------------
# Split high/low score
# -----------------------------
con.execute("""
CREATE TABLE highscore AS
SELECT *
FROM filtered
WHERE HRV_FUZZY_SCORE > 89
ORDER BY HRV_FUZZY_SCORE DESC
""")

# Count total records and highscore
total_all_record = con.execute("SELECT COUNT(*) FROM filtered").fetchone()[0]
total_high = con.execute("SELECT COUNT(*) FROM highscore").fetchone()[0]

# Sampling and %high
total_sampling = round(total_all_record * 0.3)
pct_high = round(total_high / total_all_record * 100) if total_all_record > 0 else 0

# -----------------------------
# Final selection
# -----------------------------
if pct_high >= 30:
    final_records = con.execute("SELECT * FROM highscore").fetchall()
else:
    final_records = con.execute(f"SELECT * FROM filtered LIMIT {total_sampling}").fetchall()

# -----------------------------
# TXT output formatting helper
# -----------------------------
def format_record(row):
    # row = (HRV_MONTH, HRV_BRCH_CODE, HRV_ACCT_TYPE, HRV_ACCT_NO, HRV_CUSTNO, HRV_ACCT_OPENDATE, ...)
    return (
        f"{row[0]:<6}"   # HRV_MONTH 6 char
        f"{row[1]:<7}"   # HRV_BRCH_CODE 7 char
        f"{row[2]:<5}"   # HRV_ACCT_TYPE 5 char
        f"{row[3]:<20}"  # HRV_ACCT_NO 20 char
        f"{row[4]:<20}"  # HRV_CUSTNO 20 char
        f"{row[8]}"      # HRV_ACCT_OPENDATE, original index in SAS
    )

# -----------------------------
# Write INIT TXT (first record only)
# -----------------------------
if final_records:
    with open(init_txt, "w") as f:
        f.write(format_record(final_records[0]) + "\n")

# -----------------------------
# Write UPDATE TXT (all final records)
# -----------------------------
with open(update_txt, "w") as f:
    for rec in final_records:
        f.write(format_record(rec) + "\n")

# -----------------------------
# Summary
# -----------------------------
print("Processing completed.")
print(f"Total records: {total_all_record}")
print(f"Highscore records: {total_high}")
print(f"%High: {pct_high}")
print(f"Sampling count (30%): {total_sampling}")
