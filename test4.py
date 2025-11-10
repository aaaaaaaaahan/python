import duckdb
from CIS_PY_READER import host_parquet_path, csv_output_path

con = duckdb.connect()

# Load Parquet files
con.execute(f"CREATE TABLE all_job AS SELECT * FROM read_parquet('{host_parquet_path('all_job.parquet')}')")
con.execute(f"CREATE TABLE cis_job AS SELECT * FROM read_parquet('{host_parquet_path('cis_job.parquet')}')")

# Join and deduplicate JOBNAME
df = con.execute("""
    SELECT DISTINCT A.JOBNAME
    FROM cis_job A
    INNER JOIN all_job B
    ON A.JOBNAME = B.JOBNAME
""").fetchdf()

# Write TXT
txt_path = csv_output_path("SAS_JOB").replace(".csv", ".txt")
with open(txt_path, "w", encoding="utf-8") as f:
    for jobname in df['JOBNAME']:
        f.write(f"{jobname.ljust(11)}\n")

print("✅ Processing completed successfully!")
