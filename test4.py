import duckdb
from CIS_PY_READER import get_hive_parquet

# Get previous (-1) and latest (0) parquet paths
old_path, new_path = get_hive_parquet("CIS.SDB.MATCH.FULL", debug=True)

# Create DuckDB tables
con = duckdb.connect()
con.execute(f"CREATE TABLE old AS SELECT * FROM read_parquet('{old_path}')")
con.execute(f"CREATE TABLE new AS SELECT * FROM read_parquet('{new_path}')")
