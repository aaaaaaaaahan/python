import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow.compute as pc

from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

# ==================================
# CONNECT TO DUCKDB
# ==================================
con = duckdb.connect()

# Input parquet file (assumed converted from CIS.CUST.DAILY)
input_path = f"{host_parquet_path}/CIS.CUST.DAILY.parquet"
output_parquet = f"{parquet_output_path}/CIS.RACE.parquet"
output_csv = f"{csv_output_path}/CIS.RACE.csv"

# ==================================
# STEP 1 - LOAD DATA
# ==================================
con.execute(f"""
    CREATE TABLE cis AS 
    SELECT * FROM read_parquet('{input_path}')
""")

# ==================================
# STEP 2 - FILTER DATA
# ==================================
query = """
    SELECT *
    FROM cis
    WHERE CUSTNAME <> ''
      AND ALIASKEY = 'IC'
      AND INDORG = 'I'
      AND CITIZENSHIP = 'MY'
      AND RACE = 'O'
"""

filtered = con.execute(query).arrow()

# ==================================
# STEP 3 - REMOVE DUPLICATES (BY CUSTNO)
# ==================================
filtered = filtered.drop_duplicates(subset=["CUSTNO"])

# ==================================
# STEP 4 - PRINT SAMPLE (OBS=5)
# ==================================
print("Sample records (max 5):")
print(filtered.to_pandas().head(5))

# ==================================
# STEP 5 - FORMAT OUTPUT
# ==================================
# Create formatted string column
def format_row(batch):
    aliaskey = batch["ALIASKEY"].to_pylist()
    alias = batch["ALIAS"].to_pylist()
    custname = batch["CUSTNAME"].to_pylist()
    custno = batch["CUSTNO"].to_pylist()
    custbrch = batch["CUSTBRCH"].to_pylist()

    lines = []
    for i in range(len(aliaskey)):
        line = f"{aliaskey[i]:<3};{alias[i]:<15};{custname[i]:<40};{custno[i]:<11};{custbrch[i]:03};"
        lines.append(line)
    return pa.array(lines)

formatted = pa.Table.from_arrays(
    [format_row(filtered)], names=["OUTPUT_LINE"]
)

# ==================================
# STEP 6 - WRITE OUTPUT
# ==================================
# Write parquet
pq.write_table(filtered, output_parquet)

# Write formatted CSV-like text (like SAS FILE OUTFILE)
csv.write_csv(formatted, output_csv)

print(f"Output written to:\n- {output_parquet}\n- {output_csv}")
