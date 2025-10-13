import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import os

# ------------------------------------------------------------
#  Configuration
# ------------------------------------------------------------
input_path = "/host/cis/parquet/CIS.MYKADBDS.parquet"
output_load_path = "/host/cis/parquet/CIS.MYKADBDS.LOAD.parquet"
output_reject_path = "/host/cis/parquet/CIS.MYKADBDS.REJECT.parquet"

# ------------------------------------------------------------
#  Initialize DuckDB connection (in-memory)
# ------------------------------------------------------------
con = duckdb.connect(database=':memory:')

# ------------------------------------------------------------
#  Step 1: Load Input Data
# ------------------------------------------------------------
con.execute(f"""
    CREATE TABLE MYKAD AS 
    SELECT
        MYKAD_NEW_IC,
        MYKAD_OLD_IC,
        MYKAD_NAME,
        MYKAD_DOB,
        MYKAD_GENDER,
        MYKAD_RELIGION,
        MYKAD_RACE,
        MYKAD_CITIZENSHIP,
        MYKAD_ISSUE_DATE,
        MYKAD_BIRTH_PLACE,
        MYKAD_ADDRESS1,
        MYKAD_ADDRESS2,
        MYKAD_ADDRESS3,
        MYKAD_POSTCODE,
        MYKAD_CITY,
        MYKAD_STATE,
        MYKAD_GMPC_NAME
    FROM read_parquet('{input_path}')
""")

# ------------------------------------------------------------
#  Step 2: Split into LOAD and REJECT datasets
# ------------------------------------------------------------
con.execute("""
    CREATE TABLE MYKAD_LOAD AS 
    SELECT
        MYKAD_NEW_IC,
        MYKAD_OLD_IC,
        MYKAD_NAME,
        MYKAD_DOB,
        MYKAD_GENDER,
        MYKAD_RELIGION,
        MYKAD_RACE,
        MYKAD_CITIZENSHIP,
        MYKAD_ISSUE_DATE,
        MYKAD_BIRTH_PLACE,
        MYKAD_ADDRESS1,
        MYKAD_ADDRESS2,
        MYKAD_ADDRESS3,
        MYKAD_POSTCODE,
        MYKAD_CITY,
        MYKAD_STATE,
        MYKAD_GMPC_NAME
    FROM MYKAD
    WHERE length(MYKAD_NEW_IC) = 12
""")

con.execute("""
    CREATE TABLE MYKAD_REJECT AS 
    SELECT
        MYKAD_NEW_IC,
        MYKAD_OLD_IC,
        MYKAD_NAME,
        MYKAD_DOB,
        MYKAD_GENDER,
        MYKAD_RELIGION,
        MYKAD_RACE,
        MYKAD_CITIZENSHIP,
        MYKAD_ISSUE_DATE,
        MYKAD_BIRTH_PLACE,
        MYKAD_ADDRESS1,
        MYKAD_ADDRESS2,
        MYKAD_ADDRESS3,
        MYKAD_POSTCODE,
        MYKAD_CITY,
        MYKAD_STATE,
        MYKAD_GMPC_NAME
    FROM MYKAD
    WHERE length(MYKAD_NEW_IC) <> 12
""")

# ------------------------------------------------------------
#  Step 3: Sort both datasets by MYKAD_NEW_IC
# ------------------------------------------------------------
con.execute("""
    CREATE TABLE LOADFILE AS 
    SELECT
        MYKAD_NEW_IC,
        MYKAD_OLD_IC,
        MYKAD_NAME,
        MYKAD_DOB,
        MYKAD_GENDER,
        MYKAD_RELIGION,
        MYKAD_RACE,
        MYKAD_CITIZENSHIP,
        MYKAD_ISSUE_DATE,
        MYKAD_BIRTH_PLACE,
        MYKAD_ADDRESS1,
        MYKAD_ADDRESS2,
        MYKAD_ADDRESS3,
        MYKAD_POSTCODE,
        MYKAD_CITY,
        MYKAD_STATE,
        MYKAD_GMPC_NAME
    FROM MYKAD_LOAD
    ORDER BY MYKAD_NEW_IC
""")

con.execute("""
    CREATE TABLE REJECT AS 
    SELECT
        MYKAD_NEW_IC,
        MYKAD_OLD_IC,
        MYKAD_NAME,
        MYKAD_DOB,
        MYKAD_GENDER,
        MYKAD_RELIGION,
        MYKAD_RACE,
        MYKAD_CITIZENSHIP,
        MYKAD_ISSUE_DATE,
        MYKAD_BIRTH_PLACE,
        MYKAD_ADDRESS1,
        MYKAD_ADDRESS2,
        MYKAD_ADDRESS3,
        MYKAD_POSTCODE,
        MYKAD_CITY,
        MYKAD_STATE,
        MYKAD_GMPC_NAME
    FROM MYKAD_REJECT
    ORDER BY MYKAD_NEW_IC
""")

# ------------------------------------------------------------
#  Step 4: Export final results to Parquet
# ------------------------------------------------------------
load_table = con.execute("SELECT * FROM LOADFILE").fetch_arrow_table()
reject_table = con.execute("SELECT * FROM REJECT").fetch_arrow_table()

# Ensure output directories exist
os.makedirs(os.path.dirname(output_load_path), exist_ok=True)
os.makedirs(os.path.dirname(output_reject_path), exist_ok=True)

pq.write_table(load_table, output_load_path)
pq.write_table(reject_table, output_reject_path)

# ------------------------------------------------------------
#  Step 5: Summary Output
# ------------------------------------------------------------
print("✅ MYKAD Data Processing Completed")
print(f"  LOADFILE : {load_table.num_rows} records → {output_load_path}")
print(f"  REJECT   : {reject_table.num_rows} records → {output_reject_path}")
