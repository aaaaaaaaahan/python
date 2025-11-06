import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
import datetime
from pathlib import Path

# ---------------------------------------------------------------------
# Program: CIHRCPUR (Converted from SAS)
# Purpose: Select accounts with approval status in ('05','06')
#          and created more than 60 days ago
# ---------------------------------------------------------------------

# File paths (adjust to your environment)
input_parquet = Path("/data/UNLOAD.CIHRCAPT.parquet")
output_parquet = Path("/output/HRC_DELETE_MORE60D.parquet")
output_csv = Path("/output/HRC_DELETE_MORE60D.csv")

# Connect to DuckDB (in-memory)
con = duckdb.connect()

# ---------------------------------------------------------------------
# Step 1: Define cutoff date (today - 60 days)
# ---------------------------------------------------------------------
start_date = (datetime.date.today() - datetime.timedelta(days=60)).strftime("%Y-%m-%d")

# ---------------------------------------------------------------------
# Step 2: Load input parquet (already converted from FB file)
# ---------------------------------------------------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE HRC AS
    SELECT 
        ALIAS,
        BRCHCODE,
        ACCTTYPE,
        APPROVALSTATUS,
        ACCTNO,
        CISNO,
        CREATIONDATE,
        PRIMARYJOINT,
        CISJOINTID1,
        CISJOINTID2,
        CISJOINTID3,
        CISJOINTID4,
        CISJOINTID5,
        CUSTTYPE,
        CUSTNAME,
        CUSTGENDER,
        CUSTDOBDOR,
        CUSTEMPLOYER,
        CUSTADDR1,
        CUSTADDR2,
        CUSTADDR3,
        CUSTADDR4,
        CUSTADDR5,
        CUSTPHONE,
        CUSTPEP,
        DTCORGUNIT,
        DTCINDUSTRY,
        DTCNATION,
        DTCOCCUP,
        DTCACCTTYPE,
        TEMPCOMPFORM,
        DTCWEIGHTAGE,
        DTCTOTAL,
        DTCSCORE1,
        DTCSCORE2,
        DTCSCORE3,
        DTCSCORE4,
        DTCSCORE5,
        DTCSCORE6,
        ACCTPURPOSE,
        ACCTREMARKS,
        SOURCEFUND,
        SOURCEDETAILS,
        PEPINFO,
        PEPWEALTH,
        PEPFUNDS,
        BRCHRECOMDETAILS,
        BRCHREWORK,
        HOVERIFYDATE,
        HOAPPROVEDATE,
        UPDATEDATE,
        UPDATETIME
    FROM read_parquet('{input_parquet}')
    WHERE APPROVALSTATUS IN ('05','06')
""")

# ---------------------------------------------------------------------
# Step 3: Convert CREATIONDATE to date type and filter older than 60 days
# ---------------------------------------------------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE TODEL AS
    SELECT *
    FROM HRC
    WHERE try_cast(CREATIONDATE AS DATE) < DATE '{start_date}'
""")

# ---------------------------------------------------------------------
# Step 4: Select only required output fields
# ---------------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE OUT AS
    SELECT
        ALIAS,
        BRCHCODE,
        ACCTTYPE,
        APPROVALSTATUS,
        ACCTNO,
        CISNO,
        CREATIONDATE,
        PRIMARYJOINT,
        CISJOINTID1,
        CISJOINTID2,
        CISJOINTID3,
        CISJOINTID4,
        CISJOINTID5
    FROM TODEL
    ORDER BY ALIAS
""")

# ---------------------------------------------------------------------
# Step 5: Export results to Parquet and CSV
# ---------------------------------------------------------------------
con.execute(f"COPY OUT TO '{output_parquet}' (FORMAT PARQUET)")
con.execute(f"COPY OUT TO '{output_csv}' (HEADER, DELIMITER ',')")

# Optional: Preview first few rows
print(con.execute("SELECT * FROM OUT LIMIT 5").df())
print(f"\n✅ Output generated:\n- {output_parquet}\n- {output_csv}")
