import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import os

# ==========================================================
# CONFIGURATION
# ==========================================================
SDBFILE_PATH = "/host/sdb/BDS.SDB.LIST.parquet"
RHOLD_PATH = "/host/rhold/UNLOAD.CIRHOLDT.FB.parquet"
OUTPUT_PATH = "/host/output/CIS.SDB.MATCH.RHL.parquet"

# ==========================================================
# INITIALIZE DUCKDB
# ==========================================================
con = duckdb.connect(database=':memory:')

# ==========================================================
# LOAD INPUT FILES
# ==========================================================
con.execute(f"""
    CREATE VIEW SDBFILE AS SELECT * FROM read_parquet('{SDBFILE_PATH}');
""")

con.execute(f"""
    CREATE VIEW RHOLD AS SELECT * FROM read_parquet('{RHOLD_PATH}');
""")

# ==========================================================
# STEP 1: CREATE SDBID / SDBNID / SDBALL
# ==========================================================
con.execute("""
    CREATE OR REPLACE TEMP VIEW SDBID AS
    SELECT 
        CAST(IDNUMBER AS VARCHAR) AS ID,
        SDBNAME AS NAME,
        RIGHT('00000' || CAST(BRX AS VARCHAR), 5) AS BRANCH,
        BOXNO, IDTYPE, DOBDOR, BOXSTATUS, SDBNAME, IDNUMBER
    FROM SDBFILE
    WHERE TRIM(IDNUMBER) <> '';
""")

con.execute("""
    CREATE OR REPLACE TEMP VIEW SDBNID AS
    SELECT 
        CAST(IDNUMBER AS VARCHAR) AS ID,
        SDBNAME AS NAME,
        RIGHT('00000' || CAST(BRX AS VARCHAR), 5) AS BRANCH,
        BOXNO, IDTYPE, DOBDOR, BOXSTATUS, SDBNAME, IDNUMBER
    FROM SDBFILE
    WHERE TRIM(SDBNAME) <> '' AND TRIM(IDNUMBER) <> '';
""")

con.execute("""
    CREATE OR REPLACE TEMP VIEW SDBALL AS
    SELECT 
        CAST(IDNUMBER AS VARCHAR) AS ID,
        SDBNAME AS NAME,
        RIGHT('00000' || CAST(BRX AS VARCHAR), 5) AS BRANCH,
        BOXNO, IDTYPE, DOBDOR, BOXSTATUS, SDBNAME, IDNUMBER
    FROM SDBFILE
    WHERE TRIM(SDBNAME) <> '';
""")

# ==========================================================
# STEP 2: CREATE RHOLIC / RHOLID / RID / RNAME / RNID
# ==========================================================
con.execute("""
    CREATE OR REPLACE TEMP VIEW RHOLIC AS
    SELECT DISTINCT NAME, ID
    FROM RHOLD
    WHERE TRIM(NAME) <> '' AND TRIM(ID) <> '';
""")

con.execute("""
    CREATE OR REPLACE TEMP VIEW RHOLID AS
    SELECT DISTINCT NAME, ID
    FROM RHOLD
    WHERE TRIM(NAME) <> '' AND TRIM(ID) <> '';
""")

con.execute("""
    CREATE OR REPLACE TEMP VIEW RID AS
    SELECT DISTINCT ID
    FROM (
        SELECT ID FROM RHOLIC
        UNION ALL
        SELECT ID FROM RHOLID
    )
    WHERE TRIM(ID) <> '';
""")

con.execute("""
    CREATE OR REPLACE TEMP VIEW RNID AS
    SELECT DISTINCT NAME, ID
    FROM (
        SELECT NAME, ID FROM RHOLIC
        UNION ALL
        SELECT NAME, ID FROM RHOLID
    )
    WHERE TRIM(NAME) <> '' AND TRIM(ID) <> '';
""")

con.execute("""
    CREATE OR REPLACE TEMP VIEW RNAME AS
    SELECT DISTINCT NAME
    FROM (
        SELECT NAME FROM RHOLIC
        UNION ALL
        SELECT NAME FROM RHOLID
    )
    WHERE TRIM(NAME) <> '';
""")

# ==========================================================
# STEP 3: MERGE MATCHING DATASETS
# ==========================================================
# (1) NAME MATCH
con.execute("""
    CREATE OR REPLACE TEMP VIEW MRGNAME AS
    SELECT DISTINCT b.*
    FROM RNAME a
    JOIN SDBALL b
      ON a.NAME = b.SDBNAME;
""")

# (2) ID MATCH
con.execute("""
    CREATE OR REPLACE TEMP VIEW MRGID AS
    SELECT DISTINCT b.*
    FROM RID a
    JOIN SDBID b
      ON a.ID = b.IDNUMBER;
""")

# (3) NAME + ID MATCH
con.execute("""
    CREATE OR REPLACE TEMP VIEW MRGNID AS
    SELECT DISTINCT b.*
    FROM RNID a
    JOIN SDBNID b
      ON a.NAME = b.SDBNAME AND a.ID = b.IDNUMBER;
""")

# ==========================================================
# STEP 4: COMBINE ALL MATCHES
# ==========================================================
con.execute("""
    CREATE OR REPLACE TEMP VIEW ALLMATCH AS
    SELECT DISTINCT * FROM (
        SELECT * FROM MRGNAME
        UNION
        SELECT * FROM MRGID
        UNION
        SELECT * FROM MRGNID
    );
""")

# ==========================================================
# STEP 5: OUTPUT (Equivalent to FILE OUTPUT)
# ==========================================================
arrow_table = con.execute("""
    SELECT 
        BOXNO AS BOXNO,
        SDBNAME AS SDBNAME,
        IDNUMBER AS IDNUMBER,
        BRANCH AS BRANCH
    FROM ALLMATCH
    ORDER BY BRANCH, BOXNO, SDBNAME, IDNUMBER;
""").arrow()

pq.write_table(arrow_table, OUTPUT_PATH)
print(f"✅ Output written to {OUTPUT_PATH}")

# ==========================================================
# (Optional) Display a sample of first few rows like PROC PRINT
# ==========================================================
print(con.execute("SELECT * FROM ALLMATCH LIMIT 15;").fetchdf())
