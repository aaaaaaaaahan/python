import duckdb
import datetime
import pyarrow.parquet as pq
from CIS_PY_READER import host_parquet_path, csv_output_path

# ---------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------
batch_date = datetime.date.today() - datetime.timedelta(days=1)
report_date = batch_date.strftime("%Y%m%d")

# Input & Output paths
input_parquet = host_parquet_path("UNLOAD.ALLALIAS.FB")
output_parquet = csv_output_path(f"CIS_MULTIPLE_ALIAS_IC_{report_date}").replace(".csv", ".parquet")
output_txt = csv_output_path(f"CIS_MULTIPLE_ALIAS_IC_{report_date}").replace(".csv", ".txt")

# ---------------------------------------------------------------------
# DUCKDB PROCESSING
# ---------------------------------------------------------------------
con = duckdb.connect()

# Step 1: Read fixed-width Parquet (positions from SAS INPUT)
# Assuming input Parquet has one column per row as 'column1'
con.execute(f"""
    CREATE OR REPLACE TABLE aliasdata AS
    SELECT 
        substr(column1, 5, 11) AS custno,
        substr(column1, 89, 3) AS aliaskey,
        substr(column1, 92, 20) AS alias
    FROM read_parquet('{input_parquet}');
""")

# Step 2: Filter ALIASKEY = 'IC'
con.execute("""
    CREATE OR REPLACE TABLE alias_filtered AS
    SELECT *
    FROM aliasdata
    WHERE aliaskey = 'IC'
""")

# Step 3: Sort by CUSTNO and ALIAS (like PROC SORT)
con.execute("""
    CREATE OR REPLACE TABLE alias_sorted AS
    SELECT *
    FROM alias_filtered
    ORDER BY custno, alias
""")

# Step 4: Find customers with multiple aliases (PROC SQL HAVING COUNT>1)
con.execute("""
    CREATE OR REPLACE TABLE tempals AS
    SELECT custno
    FROM alias_sorted
    GROUP BY custno
    HAVING COUNT(custno) > 1
""")

# Step 5: Output dataset (DATA OUT in SAS)
result_arrow = con.execute("""
    SELECT custno
    FROM tempals
""").fetch_arrow_table()

# ---------------------------------------------------------------------
# WRITE OUTPUT FILES
# ---------------------------------------------------------------------

# 5a: Parquet output
pq.write_table(result_arrow, output_parquet)

# 5b: Fixed-width TXT (like SAS PUT @01 '033' @05 CUSTNO $11.)
with open(output_txt, "w", encoding="utf-8") as f:
    for row in result_arrow.to_pydict()["custno"]:
        line = f"{'033':<3}{row:<11}\n"
        f.write(line)

# ---------------------------------------------------------------------
# LOG & SAMPLE OUTPUT
# ---------------------------------------------------------------------
print("Job: CIMULTIC - COMPLETED")
print(f"Input File  : {input_parquet}")
print(f"Output Parquet: {output_parquet}")
print(f"Output TXT  : {output_txt}")
print("\nSample Output (First 5 Rows):")
print(con.execute("SELECT * FROM tempals LIMIT 5").fetchdf())
