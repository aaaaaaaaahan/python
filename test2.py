import duckdb

# connect to DuckDB (in memory)
con = duckdb.connect()

# list of parquet files to combine
parquet_files = [
    '/host/cis/parquet/sas_parquet/abc_def_20251022.parquet',
    '/host/cis/parquet/sas_parquet/abc_def_20251023.parquet',
    '/host/cis/parquet/sas_parquet/abc_def_20251024.parquet'
]

# build UNION ALL query dynamically
union_query = " UNION ALL ".join([f"SELECT * FROM read_parquet('{f}')" for f in parquet_files])

# create one combined table
con.execute(f"CREATE OR REPLACE TABLE ABC_DEF_COMBINED AS {union_query}")

# check row count (optional)
print(con.execute("SELECT COUNT(*) FROM ABC_DEF_COMBINED").fetchone())
