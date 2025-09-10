import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pc
from reader import load_input  # Assuming this still gives Arrow/Polars-like data

#--------------------------------#
# READ PARQUET FILES WITH DUCKDB #
#--------------------------------#
con = duckdb.connect()

# Load Arrow/Parquet data via DuckDB directly
aown_raw   = load_input("ADDRAOWN_FB")   # Expecting Arrow Table or Parquet file path
dpaddr_raw = load_input("DP_CUST_DAILY_ADDRACC")

# Register as DuckDB views
con.register("aown_raw", aown_raw)
con.register("dpaddr_raw", dpaddr_raw)

#--------------------------------#
# Part 1 - PROCESS AOWNFILE      #
#--------------------------------#
con.execute("""
    CREATE OR REPLACE TEMP VIEW aown AS
    SELECT DISTINCT
        CASE 
            WHEN LEFT(ACCTNO,1) = '0' THEN SUBSTRING(ACCTNO,2)
            ELSE ACCTNO
        END AS ACCTNO,
        O_APPL_CODE,
        NA_LINE_TYP1, ADD_NAME_1,
        NA_LINE_TYP2, ADD_NAME_2,
        NA_LINE_TYP3, ADD_NAME_3,
        NA_LINE_TYP4, ADD_NAME_4,
        NA_LINE_TYP5, ADD_NAME_5,
        NA_LINE_TYP6, ADD_NAME_6,
        NA_LINE_TYP7, ADD_NAME_7,
        NA_LINE_TYP8, ADD_NAME_8
    FROM aown_raw
    WHERE O_APPL_CODE = 'DP'
      AND ACCTNO > '10000000000'
""")

print("AOWN")
print(con.execute("SELECT * FROM aown LIMIT 5").fetchdf())

#--------------------------------#
# Part 2 - PROCESS DEPOSIT FILE  #
#--------------------------------#
con.execute("""
    CREATE OR REPLACE TEMP VIEW dpaddr AS
    SELECT DISTINCT
        CAST(ACCTNO AS VARCHAR) AS ACCTNO
    FROM dpaddr_raw
    WHERE ACCTNO > '10000000000'
""")

print("DEPOSIT ADDRESS")
print(con.execute("SELECT * FROM dpaddr LIMIT 5").fetchdf())

#--------------------------------#
# Part 3 - MERGE ON ACCTNO       #
#--------------------------------#
con.execute("""
    CREATE OR REPLACE TEMP VIEW merged AS
    SELECT 
        a.O_APPL_CODE,
        a.ACCTNO,
        a.NA_LINE_TYP1, a.ADD_NAME_1,
        a.NA_LINE_TYP2, a.ADD_NAME_2,
        a.NA_LINE_TYP3, a.ADD_NAME_3,
        a.NA_LINE_TYP4, a.ADD_NAME_4,
        a.NA_LINE_TYP5, a.ADD_NAME_5,
        a.NA_LINE_TYP6, a.ADD_NAME_6,
        a.NA_LINE_TYP7, a.ADD_NAME_7,
        a.NA_LINE_TYP8, a.ADD_NAME_8
    FROM dpaddr d
    INNER JOIN aown a ON d.ACCTNO = a.ACCTNO
    ORDER BY a.ACCTNO
""")

print("MERGED")
print(con.execute("SELECT * FROM merged LIMIT 5").fetchdf())

#--------------------------------#
# Part 4 - CREATE OUTPUT FILE    #
#--------------------------------#
# Fetch merged as Arrow Table
out_arrow: pa.Table = con.execute("SELECT * FROM merged").arrow()

# Write to Parquet
pq.write_table(out_arrow, "output/cis_internal/DAILY_ADDRACC.parquet")

# Write to CSV
with open("output/cis_internal/DAILY_ADDRACC.csv", "wb") as f:
    pc.write_csv(out_arrow, f)

print("OUT FILE SAMPLE")
print(out_arrow.to_pandas().head(5))

