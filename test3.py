import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")
date_str = batch_date.strftime("%Y%m%d")  # Equivalent to SAS YYMMDD8.
#date_str = '2014-11-04'
# -----------------------------
# Connect DuckDB in memory
# -----------------------------
con = duckdb.connect()

# -----------------------------
# Load input Parquet file
# -----------------------------
con.execute(f"""
    CREATE TABLE allrec AS
    SELECT *
    FROM '{host_parquet_path("UNLOAD_CIHRCRVT_FB.parquet")}'
""")

# -----------------------------
# Filter records according to SAS logic
# -----------------------------
con.execute(f"""
    CREATE TABLE filtered AS
    SELECT *,
    FROM allrec
    WHERE CAST(REPLACE(HRV_ACCT_OPENDATE, '-', '') AS BIGINT) = '{date_str}'       -- Match reporting date
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
init_txt = csv_output_path(f"CIHRCRVP_INIT_{report_date}").replace(".csv", ".txt")

if final_records:
    with open(init_txt, "w") as f:
        f.write(format_record(final_records[0]) + "\n")

# -----------------------------
# Write UPDATE TXT (all final records)
# -----------------------------
update_txt = csv_output_path(f"CIHRCRVP_UPDATE_{report_date}").replace(".csv", ".txt")

with open(update_txt, "w") as f:
    for rec in final_records:
        f.write(format_record(rec) + "\n")

# -----------------------------
# Export Parquet using the same queries
# -----------------------------
# INIT Parquet (first record)
out = """
    SELECT
        *
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM final LIMIT 1
""".format(year=year,month=month,day=day)

# UPDATE Parquet (all final records)
out1 = """
    SELECT
        *
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM final
""".format(year=year,month=month,day=day)

queries = {
    "CIHRCRVP_INIT"                        : out,
    "CIHRCRVP_UPDATE"                      : out1
}

for name, query in queries.items():
    parquet_path = parquet_output_path(name)

    con.execute(f"""
    COPY ({query})
    TO '{parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);  
     """)
