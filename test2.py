# ---------------------------------------------------------------------
# Program: CIDJACCD
# Description: Generate DP Account data from customer account branch info
# Converted from SAS to Python using DuckDB + PyArrow
# ---------------------------------------------------------------------

import duckdb
import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import os

# ---------------------------------------------------------------------
# Step 1: Date setup (replace SAS CTRLDATE logic)
# ---------------------------------------------------------------------
today = datetime.date.today()
six_months_ago = today - datetime.timedelta(days=180)
DATE3 = int(six_months_ago.strftime("%Y%m%d"))  # Used for comparison
YEAR = today.strftime("%Y")
MONTH = today.strftime("%m")
DAY = today.strftime("%d")

# ---------------------------------------------------------------------
# Step 2: File setup (input parquet, output folder)
# ---------------------------------------------------------------------
# Input Parquet path (converted from DETICA_CUST_ACCTBRCH)
input_parquet = "/host/cis/input/DETICA_CUST_ACCTBRCH.parquet"

# Output paths
output_parquet = "/host/cis/output/CIS_DJW_DPACCT.parquet"
output_txt = "/host/cis/output/CIS_DJW_DPACCT.txt"

# Create output directory if not exists
os.makedirs(os.path.dirname(output_parquet), exist_ok=True)

# ---------------------------------------------------------------------
# Step 3: Connect to DuckDB
# ---------------------------------------------------------------------
con = duckdb.connect()

# Register input Parquet as a DuckDB table
con.execute(f"""
    CREATE TABLE ACTBRCH AS
    SELECT 
        CUSTNO,
        PRIMSEC,
        ACCTCODE,
        ACCTNO,
        OPENYY,
        OPENMM,
        OPENDD,
        CONCAT(OPENYY, OPENMM, OPENDD) AS OPENDT,
        CONCAT(OPENYY, '-', OPENMM, '-', OPENDD) AS OPENDX,
        LPAD(CAST(ACCTNO AS VARCHAR), 11, '0') AS ACCTNOX
    FROM read_parquet('{input_parquet}')
""")

# ---------------------------------------------------------------------
# Step 4: Filter and deduplicate (SAS equivalent logic)
# ---------------------------------------------------------------------
query = f"""
    SELECT DISTINCT
        CUSTNO,
        ACCTCODE,
        ACCTNOX,
        OPENDX
    FROM ACTBRCH
    WHERE CAST(REPLACE(OPENDT, '-', '') AS BIGINT) <= {DATE3}
      AND ACCTCODE = 'DP'
    ORDER BY CUSTNO
"""
df = con.execute(query).fetchdf()

# ---------------------------------------------------------------------
# Step 5: Write to Parquet
# ---------------------------------------------------------------------
table = pa.Table.from_pandas(df)
pq.write_table(table, output_parquet)

# ---------------------------------------------------------------------
# Step 6: Write to TXT (format same as SAS PUT)
# Columns layout:
# @001 CUSTNO(11)
# @021 ACCTCODE(5)
# @026 ACCTNOX(20)
# @046 OPENDX(10)
# ---------------------------------------------------------------------
with open(output_txt, "w", encoding="utf-8") as f:
    f.write("Program: CIDJACCD\n")
    f.write(f"DATE GENERATED: {today}\n")
    f.write("CUSTNO       ACCTCODE  ACCTNOX             OPENDX\n")
    f.write("-" * 50 + "\n")
    for _, row in df.iterrows():
        line = (
            f"{str(row['CUSTNO']).ljust(11)}"
            f"{str(row['ACCTCODE']).ljust(5)}"
            f"{str(row['ACCTNOX']).ljust(20)}"
            f"{str(row['OPENDX']).ljust(10)}"
        )
        f.write(line + "\n")

print("✅ Processing completed successfully!")
print(f"Parquet output: {output_parquet}")
print(f"Text output   : {output_txt}")
