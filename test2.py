import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# ============================================================
# CONFIGURATION
# ============================================================
# Assume parquet files already exist at these paths
SDBFILE_PATH = "/host/sdb/parquet/BDS_SDB_LIST.parquet"
DOWJONES_PATH = "/host/dwj/parquet/UNLOAD_CIDOWJ1T_FB.parquet"
OUTPUT_PATH = "/host/cis/output/CIS_SDB_MATCH_DWJ.parquet"

# ============================================================
# DUCKDB CONNECTION
# ============================================================
con = duckdb.connect(database=':memory:')

# ============================================================
# LOAD INPUT PARQUETS
# ============================================================
con.execute(f"""
    CREATE TABLE SDBFILE AS SELECT * FROM read_parquet('{SDBFILE_PATH}');
""")

con.execute(f"""
    CREATE TABLE DOWJONES AS SELECT * FROM read_parquet('{DOWJONES_PATH}');
""")

# ============================================================
# STEP 1 - PROCESS DOWJONES (split into DNAME, DID, DNID)
# ============================================================
con.execute("""
    CREATE TABLE DNAME AS
    SELECT CUSTNAME AS NAME, ID
    FROM DOWJONES
    WHERE TRIM(CUSTNAME) <> '';
""")

con.execute("""
    CREATE TABLE DID AS
    SELECT ID, CUSTNAME AS NAME
    FROM DOWJONES
    WHERE TRIM(ID) <> '';
""")

con.execute("""
    CREATE TABLE DNID AS
    SELECT CUSTNAME AS NAME, ID
    FROM DOWJONES
    WHERE TRIM(CUSTNAME) <> '' AND TRIM(ID) <> '';
""")

# Remove duplicates
con.execute("CREATE TABLE DNAME_SORT AS SELECT DISTINCT * FROM DNAME;")
con.execute("CREATE TABLE DID_SORT AS SELECT DISTINCT * FROM DID;")
con.execute("CREATE TABLE DNID_SORT AS SELECT DISTINCT * FROM DNID;")

# ============================================================
# STEP 2 - PROCESS SDBFILE (split into SDBID, SDBNID, SDBNME)
# ============================================================
con.execute("""
    CREATE TABLE SDBFILE_EX AS
    SELECT 
        CAST(BRX AS INTEGER) AS BRX,
        BOXNO,
        IDTYPE,
        SDBNAME AS NAME,
        IDNUMBER AS ID,
        BOXSTATUS
    FROM SDBFILE;
""")

# Split by available fields
con.execute("CREATE TABLE SDBID AS SELECT * FROM SDBFILE_EX WHERE TRIM(ID) <> '';")
con.execute("CREATE TABLE SDBNID AS SELECT * FROM SDBFILE_EX WHERE TRIM(NAME) <> '' AND TRIM(ID) <> '';")
con.execute("CREATE TABLE SDBNME AS SELECT * FROM SDBFILE_EX WHERE TRIM(NAME) <> '';")

# Remove duplicates
con.execute("CREATE TABLE SDBID_SORT AS SELECT DISTINCT * FROM SDBID;")
con.execute("CREATE TABLE SDBNID_SORT AS SELECT DISTINCT * FROM SDBNID;")
con.execute("CREATE TABLE SDBNME_SORT AS SELECT DISTINCT * FROM SDBNME;")

# ============================================================
# STEP 3 - MATCHING (NAME, ID, NAME+ID)
# ============================================================

# NAME MATCH
con.execute("""
    CREATE TABLE MRGNAME AS
    SELECT B.*
    FROM DNAME_SORT A
    JOIN SDBNME_SORT B ON A.NAME = B.NAME;
""")

# ID MATCH
con.execute("""
    CREATE TABLE MRGID AS
    SELECT B.*
    FROM DID_SORT A
    JOIN SDBID_SORT B ON A.ID = B.ID;
""")

# NAME + ID MATCH
con.execute("""
    CREATE TABLE MRGNID AS
    SELECT B.*
    FROM DNID_SORT A
    JOIN SDBNID_SORT B ON A.NAME = B.NAME AND A.ID = B.ID;
""")

# ============================================================
# STEP 4 - COMBINE MATCHES AND DEDUP
# ============================================================
con.execute("""
    CREATE TABLE ALLMATCH AS
    SELECT * FROM (
        SELECT * FROM MRGNAME
        UNION ALL
        SELECT * FROM MRGID
        UNION ALL
        SELECT * FROM MRGNID
    )
    WHERE TRIM(BOXNO) <> '';
""")

con.execute("""
    CREATE TABLE ALLMATCH_SORT AS
    SELECT DISTINCT BRX, BOXNO, NAME, ID, BOXSTATUS
    FROM ALLMATCH
    ORDER BY BRX, BOXNO, NAME, ID;
""")

# ============================================================
# STEP 5 - OUTPUT
# ============================================================
# Convert to Arrow Table and save as Parquet
table = con.execute("SELECT * FROM ALLMATCH_SORT").arrow()
pq.write_table(table, OUTPUT_PATH)

print(f"✅ Output written to {OUTPUT_PATH}")
