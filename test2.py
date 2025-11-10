import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, timedelta

# ---------------------------------------------------------------------
# Program: CIDJACCD
# Purpose: Generate DP Account Data (similar logic to SAS job)
# ---------------------------------------------------------------------

# === Configuration ===
input_ctrl_parquet = "/host/cis/input/SRSCTRL1.parquet"
input_detica_parquet = "/host/cis/input/DETICA_CUST_ACCTBRCH.parquet"

output_txt = "/host/cis/output/CIS_DJW_DPACCT.txt"
output_parquet = "/host/cis/output/CIS_DJW_DPACCT.parquet"

# ---------------------------------------------------------------------
# Step 1: Set Dates (equivalent to SAS date logic)
# ---------------------------------------------------------------------

ctrl_table = duckdb.read_parquet(input_ctrl_parquet).fetchdf()

SRSYY = int(ctrl_table.loc[0, 'SRSYY'])
SRSMM = int(ctrl_table.loc[0, 'SRSMM'])
SRSDD = int(ctrl_table.loc[0, 'SRSDD'])

srs_date = date(SRSYY, SRSMM, SRSDD)
todaysas = srs_date - timedelta(days=180)  # 6 months earlier

DATE3 = todaysas.strftime("%Y%m%d")
YEAR = f"{SRSYY:04d}"
MONTH = f"{SRSMM:02d}"
DAY = f"{SRSDD:02d}"

print(f"Processing date: {srs_date}, Cutoff (DATE3): {DATE3}")

# ---------------------------------------------------------------------
# Step 2: Read ACTBRCH data and apply filters
# ---------------------------------------------------------------------

con = duckdb.connect()

con.execute(f"""
    CREATE OR REPLACE TABLE ACTBRCH AS
    SELECT
        CAST(SUBSTR(CUSTNO, 1, 11) AS VARCHAR) AS CUSTNO,
        CAST(PRIMSEC AS VARCHAR) AS PRIMSEC,
        CAST(ACCTCODE AS VARCHAR) AS ACCTCODE,
        CAST(ACCTNO AS VARCHAR) AS ACCTNO,
        CAST(OPENYY AS VARCHAR) AS OPENYY,
        CAST(OPENMM AS VARCHAR) AS OPENMM,
        CAST(OPENDD AS VARCHAR) AS OPENDD,
        CONCAT(OPENYY, OPENMM, OPENDD) AS OPENDT,
        CONCAT(OPENYY, '-', OPENMM, '-', OPENDD) AS OPENDX
    FROM read_parquet('{input_detica_parquet}')
""")

con.execute(f"""
    DELETE FROM ACTBRCH
    WHERE OPENDT > '{DATE3}' OR ACCTCODE <> 'DP'
""")

con.execute("""
    CREATE OR REPLACE TABLE ACTBRCH AS
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) AS rn
        FROM ACTBRCH
    ) WHERE rn = 1
""")

# ---------------------------------------------------------------------
# Step 3: Generate output dataset (DPACCT)
# ---------------------------------------------------------------------

dpacct = con.execute("""
    SELECT
        CUSTNO,
        ACCTCODE,
        LPAD(ACCTNO, 11, '0') AS ACCTNOX,
        OPENDX
    FROM ACTBRCH
""").df()

# ---------------------------------------------------------------------
# Step 4: Write Output (Parquet + Text)
# ---------------------------------------------------------------------

table = pa.Table.from_pandas(dpacct)
pq.write_table(table, output_parquet)

with open(output_txt, "w") as f:
    f.write("Program: CIDJACCD\n")
    f.write("CUSTNO      ACCTCODE ACCTNOX              OPENDX\n")
    for _, row in dpacct.iterrows():
        line = f"{row.CUSTNO:<11}{row.ACCTCODE:<5}{row.ACCTNOX:<20}{row.OPENDX:<10}\n"
        f.write(line)

print("✅ Processing complete")
print(f"Output written to:\n  TXT: {output_txt}\n  PARQUET: {output_parquet}")
