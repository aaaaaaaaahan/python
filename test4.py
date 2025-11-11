import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

# -----------------------------
# Set reporting date (yesterday by default)
# -----------------------------
batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")
date_str = batch_date.strftime("%Y%m%d")  # SAS YYMMDD8 equivalent
# date_str = '20241110'  # <-- Uncomment for manual test

# -----------------------------
# Connect to DuckDB (in-memory)
# -----------------------------
con = duckdb.connect()

# -----------------------------
# Load input Parquet file
# -----------------------------
con.execute(f"""
    CREATE TABLE allrec AS
    SELECT *
    FROM read_parquet('{host_parquet_path("UNLOAD_CIHRCRVT_FB.parquet")}')
""")

# -----------------------------
# Filter records (match SAS logic)
# -----------------------------
con.execute(f"""
    CREATE TABLE filtered AS
    SELECT *
    FROM allrec
    WHERE REPLACE(CAST(HRV_ACCT_OPENDATE AS VARCHAR), '-', '') = '{date_str}'
        AND TRIM(HRV_FUZZY_INDC) = 'Y'
        AND (TRIM(HRV_OVERRIDING_INDC) IN ('', 'N'))
        AND TRIM(HRV_BRCH_CODE) <> '996'
""")

# Debug: check how many records survived the filter
filtered_count = con.execute("SELECT COUNT(*) FROM filtered").fetchone()[0]
print(f"[DEBUG] Filtered records: {filtered_count}")

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

total_all_record = con.execute("SELECT COUNT(*) FROM filtered").fetchone()[0]
total_high = con.execute("SELECT COUNT(*) FROM highscore").fetchone()[0]

print(f"[DEBUG] Total records after filter: {total_all_record}")
print(f"[DEBUG] Total highscore records: {total_high}")

# 30% sampling logic
total_sampling = round(total_all_record * 0.3)
pct_high = round((total_high / total_all_record * 100), 2) if total_all_record > 0 else 0

print(f"[DEBUG] Total sampling target: {total_sampling}")
print(f"[DEBUG] Highscore percentage: {pct_high}%")

# -----------------------------
# Determine final dataset
# -----------------------------
if total_all_record == 0:
    print("[DEBUG] No records found after filtering. Exiting early.")
    con.execute("CREATE TABLE final AS SELECT * FROM filtered WHERE 1=0")  # Empty placeholder
elif pct_high >= 30:
    print("[DEBUG] Using highscore dataset for final output.")
    con.execute("CREATE TABLE final AS SELECT * FROM highscore")
else:
    print("[DEBUG] Using 30% sample from filtered (sorted by fuzzy score).")
    con.execute(f"""
        CREATE TABLE final AS
        SELECT *
        FROM filtered
        ORDER BY HRV_FUZZY_SCORE DESC
        LIMIT {total_sampling}
    """)

final_count = con.execute("SELECT COUNT(*) FROM final").fetchone()[0]
print(f"[DEBUG] Final records selected: {final_count}")

final_records = con.execute("SELECT * FROM final").fetchall()

# -----------------------------
# Helper: fixed-width text output
# -----------------------------
from datetime import datetime as dt

def format_record(row):
    """
    Format record to fixed-width text matching SAS layout:
    1  HRV_MONTH (6)
    7  HRV_BRCH_CODE (7)
    14 HRV_ACCT_TYPE (5)
    19 HRV_ACCT_NO (20)
    39 HRV_CUSTNO (20)
    59 HRV_ACCT_OPENDATE (YYMMDD10)
    """
    try:
        open_date = row[8]
        if isinstance(open_date, str) and '-' in open_date:
            open_date = dt.strptime(open_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        elif isinstance(open_date, datetime.date):
            open_date = open_date.strftime("%Y-%m-%d")
    except Exception:
        open_date = str(row[8])

    return (
        f"{str(row[0])[:6]:<6}"   # HRV_MONTH
        f"{str(row[1])[:7]:<7}"   # HRV_BRCH_CODE
        f"{str(row[2])[:5]:<5}"   # HRV_ACCT_TYPE
        f"{str(row[3])[:20]:<20}" # HRV_ACCT_NO
        f"{str(row[4])[:20]:<20}" # HRV_CUSTNO
        f"{open_date:<10}"        # HRV_ACCT_OPENDATE (formatted)
    )

# -----------------------------
# Write INIT TXT (first record only)
# -----------------------------
init_txt = csv_output_path(f"CIHRCRVP_INIT_{report_date}").replace(".csv", ".txt")

if final_records:
    with open(init_txt, "w") as f:
        f.write(format_record(final_records[0]) + "\n")
    print(f"[DEBUG] INIT file created: {init_txt}")
else:
    print("[DEBUG] No records found — INIT file not created.")

# -----------------------------
# Write UPDATE TXT (all final records)
# -----------------------------
update_txt = csv_output_path(f"CIHRCRVP_UPDATE_{report_date}").replace(".csv", ".txt")

with open(update_txt, "w") as f:
    for rec in final_records:
        f.write(format_record(rec) + "\n")

print(f"[DEBUG] UPDATE file created: {update_txt}")

# -----------------------------
# Export Parquet outputs (INIT + UPDATE)
# -----------------------------
out_init = f"""
    SELECT
        *,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM final
    LIMIT 1
"""

out_update = f"""
    SELECT
        *,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM final
"""

queries = {
    "CIHRCRVP_INIT": out_init,
    "CIHRCRVP_UPDATE": out_update
}

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    con.execute(f"""
        COPY ({query})
        TO '{parquet_path}'
        (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
    """)
    print(f"[DEBUG] Parquet written: {parquet_path}")

print("[DEBUG] Process completed successfully.")
