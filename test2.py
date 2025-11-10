import duckdb
import datetime

# -----------------------------
# File paths
# -----------------------------
input_parquet = "UNLOAD_CIHRCRVT.parquet"   # Input file already in Parquet
init_txt = "CIHRCRVP_INIT.txt"
update_txt = "CIHRCRVP_UPDATE.txt"
init_parquet = "CIHRCRVP_INIT.parquet"
update_parquet = "CIHRCRVP_UPDATE.parquet"

# -----------------------------
# Set batch/reporting date
# -----------------------------
batch_date = datetime.date.today()  # Replace with control date if needed
date_str = batch_date.strftime("%Y%m%d")  # Equivalent to SAS YYMMDD8.

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
WHERE HRV_ACCT_OPENDATE = '{date_str}'       -- Match reporting date
  AND HRV_FUZZY_INDC = 'Y'                   -- Only fuzzy-indicated accounts
  AND (HRV_OVERRIDING_INDC IS NULL OR HRV_OVERRIDING_INDC = 'N') -- No override
  AND HRV_BRCH_CODE <> '996'                 -- Exclude branch code 996
""")

# -----------------------------
# Split highscore and count
# -----------------------------
con.execute("""
CREATE TABLE highscore AS
SELECT *
FROM filtered
WHERE HRV_FUZZY_SCORE > 89
ORDER BY HRV_FUZZY_SCORE DESC
""")

# Counts for sampling logic
total_all_record = con.execute("SELECT COUNT(*) FROM filtered").fetchone()[0]
total_high = con.execute("SELECT COUNT(*) FROM highscore").fetchone()[0]

# 30% sampling if needed
total_sampling = round(total_all_record * 0.3)
pct_high = round(total_high / total_all_record * 100) if total_all_record > 0 else 0

# -----------------------------
# Determine final records
# -----------------------------
if pct_high >= 30:
    # Use highscore dataset
    con.execute("CREATE TABLE final AS SELECT * FROM highscore")
else:
    # Use first N records from filtered
    con.execute(f"CREATE TABLE final AS SELECT * FROM filtered LIMIT {total_sampling}")

# Fetch final records for TXT output
final_records = con.execute("SELECT * FROM final").fetchall()

# -----------------------------
# Helper function for fixed-width TXT
# -----------------------------
def format_record(row):
    # Adjust indices according to your input Parquet columns
    # Assuming order: HRV_MONTH, HRV_BRCH_CODE, HRV_ACCT_TYPE, HRV_ACCT_NO, HRV_CUSTNO, HRV_ACCT_OPENDATE, ...
    return (
        f"{row[0]:<6}"   # HRV_MONTH 6 char
        f"{row[1]:<7}"   # HRV_BRCH_CODE 7 char
        f"{row[2]:<5}"   # HRV_ACCT_TYPE 5 char
        f"{row[3]:<20}"  # HRV_ACCT_NO 20 char
        f"{row[4]:<20}"  # HRV_CUSTNO 20 char
        f"{row[8]}"      # HRV_ACCT_OPENDATE at index 8, adjust if different
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
# Export Parquet using the same queries
# -----------------------------
# INIT Parquet (first record)
con.execute("CREATE TABLE init_table AS SELECT * FROM final LIMIT 1")
con.execute(f"COPY init_table TO '{init_parquet}' (FORMAT PARQUET)")

# UPDATE Parquet (all final records)
con.execute("CREATE TABLE update_table AS SELECT * FROM final")
con.execute(f"COPY update_table TO '{update_parquet}' (FORMAT PARQUET)")

# -----------------------------
# Summary
# -----------------------------
print("Processing completed.")
print(f"Total records: {total_all_record}")
print(f"Highscore records: {total_high}")
print(f"%High: {pct_high}")
print(f"Sampling count (30%): {total_sampling}")
print(f"TXT and Parquet files are generated for INIT and UPDATE datasets.")
