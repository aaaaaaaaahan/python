import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path

# ---------------------------------------------------------------------
# Job: CIHRCOT1  |  Converted from SAS to Python + DuckDB
# Purpose: Process high-risk customer OT account declarations
# Split by first character of COSTCTR into:
#   PBB  (Conventional) = COSTCTR[1] <> '3'
#   PIBB (Islamic)       = COSTCTR[1] = '3'
# ---------------------------------------------------------------------

# === CONFIGURATION ===
input_dir = Path("/host/cis/input")
output_dir = Path("/host/cis/output")

# Input Parquet files (already converted)
hrcstot_parquet = input_dir / "HRCSTOT.parquet"    # CIS.HRCCUST.OTACCTS
otfile_parquet = input_dir / "OTFILE.parquet"      # UNLOAD.CIACCTT.FB

# Output Parquet/CSV files
good_path = output_dir / "OTACCTS_GOOD.parquet"
bad_path = output_dir / "OTACCTS_CLOSED.parquet"
good_csv = output_dir / "OTACCTS_GOOD.csv"
bad_csv = output_dir / "OTACCTS_CLOSED.csv"

good_pbb_path = output_dir / "OTACCTS_GOOD_PBB.parquet"
good_pibb_path = output_dir / "OTACCTS_GOOD_PIBB.parquet"

# Create DuckDB connection
con = duckdb.connect()

# ---------------------------------------------------------------------
# Step 1: Load Input Tables
# ---------------------------------------------------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE CISOT AS
    SELECT 
        BANKNUM,
        CUSTBRCH,
        CUSTNO,
        CUSTNAME,
        RACE,
        CITIZENSHIP,
        INDORG,
        PRIMSEC,
        CUSTLASTDATECC,
        CUSTLASTDATEYY,
        CUSTLASTDATEMM,
        CUSTLASTDATEDD,
        ALIASKEY,
        ALIAS,
        HRCCODES,
        ACCTCODE,
        ACCTNO
    FROM read_parquet('{hrcstot_parquet}')
""")

con.execute(f"""
    CREATE OR REPLACE TABLE OTDATA AS
    SELECT DISTINCT
        ACCTCODE,
        ACCTNO,
        ACCSTAT,
        BRANCH,
        COSTCTR,
        REPLACE(OPENDATE, '-', '') AS OPDATE,
        REPLACE(CLSDATE, '-', '') AS CLDATE
    FROM read_parquet('{otfile_parquet}')
""")

# ---------------------------------------------------------------------
# Step 2: Merge & Classify GOOD / BAD OT Accounts
# ---------------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE MERGED AS
    SELECT 
        A.*,
        B.BANKNUM, B.CUSTBRCH, B.CUSTNO, B.CUSTNAME,
        B.RACE, B.CITIZENSHIP, B.INDORG, B.PRIMSEC,
        B.CUSTLASTDATECC, B.CUSTLASTDATEYY, B.CUSTLASTDATEMM, B.CUSTLASTDATEDD,
        B.ALIASKEY, B.ALIAS, B.HRCCODES
    FROM OTDATA A
    JOIN CISOT B USING (ACCTNO)
""")

# GOOD: ACCSTAT not in ('C','B','P','Z') and ACCTNO not blank
con.execute("""
    CREATE OR REPLACE TABLE GOODOT AS
    SELECT * FROM MERGED
    WHERE ACCSTAT NOT IN ('C','B','P','Z') AND ACCTNO <> ''
""")

# BAD: Remaining or closed
con.execute("""
    CREATE OR REPLACE TABLE BADOT AS
    SELECT * FROM MERGED
    WHERE ACCSTAT IN ('C','B','P','Z') OR ACCTNO = ''
""")

# ---------------------------------------------------------------------
# Step 3: Write Outputs (GOOD, BAD)
# ---------------------------------------------------------------------
con.execute(f"COPY GOODOT TO '{good_path}' (FORMAT PARQUET)")
con.execute(f"COPY BADOT TO '{bad_path}' (FORMAT PARQUET)")
con.execute(f"COPY GOODOT TO '{good_csv}' (HEADER, DELIMITER ',')")
con.execute(f"COPY BADOT TO '{bad_csv}' (HEADER, DELIMITER ',')")

# ---------------------------------------------------------------------
# Step 4: Split GOOD accounts into PBB and PIBB using first char of COSTCTR
# COSTCTR[1] = '3' → PIBB (Islamic)
# COSTCTR[1] <> '3' → PBB (Conventional)
# ---------------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE OTCONV AS
    SELECT * FROM GOODOT WHERE SUBSTR(COSTCTR, 1, 1) <> '3'
""")

con.execute("""
    CREATE OR REPLACE TABLE OTPIBB AS
    SELECT * FROM GOODOT WHERE SUBSTR(COSTCTR, 1, 1) = '3'
""")

# Write to Parquet
con.execute(f"COPY OTCONV TO '{good_pbb_path}' (FORMAT PARQUET)")
con.execute(f"COPY OTPIBB TO '{good_pibb_path}' (FORMAT PARQUET)")

print("✅ CIHRCOT1 conversion complete.")
print(f"Outputs saved in: {output_dir}")
