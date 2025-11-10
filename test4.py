import duckdb
from CIS_PY_READER import host_parquet_path, csv_output_path

con = duckdb.connect()

# Load Parquet files
con.execute(f"CREATE TABLE sas_job AS SELECT CAST(JOBNAME AS VARCHAR) AS JOBNAME FROM read_parquet('{host_parquet_path('SAS_JOB.parquet')}')")
con.execute(f"CREATE TABLE cis_job AS SELECT * FROM read_parquet('{host_parquet_path('cis_job.parquet')}')")

# Join and deduplicate JOBNAME
query = """
    SELECT DISTINCT A.JOBNAME
    FROM cis_job A
    LEFT JOIN sas_job B
    ON B.JOBNAME != A.JOBNAME
"""

df = con.execute(query).fetchdf()

# Write TXT
txt_path = csv_output_path("UNUSE_SAS_JOB").replace(".csv", ".txt")
with open(txt_path, "w", encoding="utf-8") as f:
    for _, row in df.iterrows():
        line = (
            f"{str(row['JOBNAME']).ljust(8)}"
        )
        f.write(line + "\n")

print("✅ Processing completed successfully!")
