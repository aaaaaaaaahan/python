import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
import datetime

# -----------------------------
# File paths (adjust as needed)
# -----------------------------
input_parquet = "UNLOAD_CIHRCRVT.parquet"  # Input data
init_output_parquet = "CIHRCRVP_INIT.parquet"
update_output_parquet = "CIHRCRVP_UPDATE.parquet"
init_txt = "CIHRCRVP_INIT.txt"
update_txt = "CIHRCRVP_UPDATE.txt"

# -----------------------------
# Set batch/reporting date
# -----------------------------
batch_date = datetime.date.today()  # Replace with control date if needed
date_str = batch_date.strftime("%Y%m%d")  # Equivalent to SAS YYMMDD8.

# -----------------------------
# Connect to DuckDB in memory
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
# Filter records based on rules
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
# Split records by fuzzy score
# -----------------------------
con.execute("""
CREATE TABLE highscore AS
SELECT *
FROM filtered
WHERE HRV_FUZZY_SCORE > 89
ORDER BY HRV_FUZZY_SCORE DESC
""")

con.execute("""
CREATE TABLE lowscore AS
SELECT *
FROM filtered
WHERE HRV_FUZZY_SCORE <= 89
ORDER BY HRV_FUZZY_SCORE DESC
""")

# -----------------------------
# Count records
# -----------------------------
total_all_record = con.execute("SELECT COUNT(*) FROM filtered").fetchone()[0]
total_high = con.execute("SELECT COUNT(*) FROM highscore").fetchone()[0]

# -----------------------------
# Sampling calculation
# -----------------------------
total_sampling = round(total_all_record * 0.3)  # 30% sampling
pct_high = round(total_high / total_all_record * 100) if total_all_record > 0 else 0

# -----------------------------
# Determine final dataset
# -----------------------------
if pct_high >= 30:
    final_df = con.execute("SELECT * FROM highscore").fetchdf()
else:
    final_df = con.execute(f"SELECT * FROM filtered LIMIT {total_sampling}").fetchdf()

# -----------------------------
# Export INIT file (first record only)
# -----------------------------
init_df = final_df.head(1)
table_init = pa.Table.from_pandas(init_df)
pq.write_table(table_init, init_output_parquet)

with open(init_txt, "w") as f:
    row = init_df.iloc[0]
    f.write(
        f"{row.HRV_MONTH:<6}"
        f"{row.HRV_BRCH_CODE:<7}"
        f"{row.HRV_ACCT_TYPE:<5}"
        f"{row.HRV_ACCT_NO:<20}"
        f"{row.HRV_CUSTNO:<20}"
        f"{row.HRV_ACCT_OPENDATE}\n"
    )

# -----------------------------
# Export UPDATE file (all final records)
# -----------------------------
update_df = final_df
table_update = pa.Table.from_pandas(update_df)
pq.write_table(table_update, update_output_parquet)

with open(update_txt, "w") as f:
    for _, row in update_df.iterrows():
        f.write(
            f"{row.HRV_MONTH:<6}"
            f"{row.HRV_BRCH_CODE:<7}"
            f"{row.HRV_ACCT_TYPE:<5}"
            f"{row.HRV_ACCT_NO:<20}"
            f"{row.HRV_CUSTNO:<20}"
            f"{row.HRV_ACCT_OPENDATE}\n"
        )

# -----------------------------
# Summary output
# -----------------------------
print("Processing completed.")
print(f"Total records: {total_all_record}")
print(f"Highscore records: {total_high}")
print(f"Percentage highscore: {pct_high}%")
print(f"Sampling count (30%): {total_sampling}")
