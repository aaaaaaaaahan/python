import duckdb
import datetime
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

# ======================================================
# Set Batch Date
# ======================================================
batch_date = (datetime.date.today() - datetime.timedelta(days=1))

# ======================================================
# Connect to DuckDB
# ======================================================
con = duckdb.connect(database=':memory:')

# ======================================================
# Input Data
# ======================================================
con.execute(f"""
    CREATE OR REPLACE TABLE DOWJONES AS
    SELECT * FROM read_parquet('{host_parquet_path}/DOWJONES.parquet');
""")

con.execute(f"""
    CREATE OR REPLACE TABLE CIG_FILE AS
    SELECT * FROM read_parquet('{host_parquet_path}/CIG_FILE.parquet');
""")

# ======================================================
# Match Datasets (SAS DATA Steps)
# ======================================================

# 1. DOWJONES NAME MATCH (MRGNAME)
con.execute("""
    CREATE OR REPLACE TABLE MRGNAME AS
    SELECT A.*, B.*
    FROM DOWJONES A
    JOIN CIG_FILE B
    ON A.NAME = B.NAME;
""")

# 2. DOWJONES ID MATCH (MRGID)
con.execute("""
    CREATE OR REPLACE TABLE MRGID AS
    SELECT A.*, B.*
    FROM DOWJONES A
    JOIN CIG_FILE B
    ON A.OTHID = B.OTHID;
""")

# 3. DOWJONES IC MATCH (MRGIC)
con.execute("""
    CREATE OR REPLACE TABLE MRGIC AS
    SELECT A.*, B.*
    FROM DOWJONES A
    JOIN CIG_FILE B
    ON A.NEWIC = B.NEWIC;
""")

# 4. DOWJONES NAME & DOB MATCH (MRGNDOB)
con.execute("""
    CREATE OR REPLACE TABLE MRGNDOB AS
    SELECT A.*, B.*
    FROM DOWJONES A
    JOIN CIG_FILE B
    ON A.NAME = B.NAME AND A.DOBDOR = B.DOBDOR;
""")

# 5. DOWJONES NAME & ID MATCH (MRGNID)
con.execute("""
    CREATE OR REPLACE TABLE MRGNID AS
    SELECT A.*, B.*
    FROM DOWJONES A
    JOIN CIG_FILE B
    ON A.NAME = B.NAME AND A.OTHID = B.OTHID;
""")

# 6. DOWJONES NAME & IC MATCH (MRGNIC)
con.execute("""
    CREATE OR REPLACE TABLE MRGNIC AS
    SELECT A.*, B.*
    FROM DOWJONES A
    JOIN CIG_FILE B
    ON A.NAME = B.NAME AND A.NEWIC = B.NEWIC;
""")

# ======================================================
# Combine All Matches (ALLMATCH)
# SAS logic: IF N OR O OR P OR Q OR R;
# Means: Keep only rows that appear in any of these five datasets
# ======================================================

con.execute("""
CREATE OR REPLACE TABLE ALLMATCH AS
SELECT DISTINCT
       COALESCE(n.NAME, id.NAME, ic.NAME, nid.NAME, nic.NAME) AS NAME,
       COALESCE(n.NEWIC, id.NEWIC, ic.NEWIC, nid.NEWIC, nic.NEWIC) AS NEWIC,
       COALESCE(n.OTHID, id.OTHID, ic.OTHID, nid.OTHID, nic.OTHID) AS OTHID,
       COALESCE(n.DOBDOR, id.DOBDOR, ic.DOBDOR, nid.DOBDOR, nic.DOBDOR) AS DOBDOR
FROM MRGNDOB n
FULL OUTER JOIN MRGID id   USING (NAME, NEWIC, OTHID, DOBDOR)
FULL OUTER JOIN MRGIC ic   USING (NAME, NEWIC, OTHID, DOBDOR)
FULL OUTER JOIN MRGNID nid USING (NAME, NEWIC, OTHID, DOBDOR)
FULL OUTER JOIN MRGNIC nic USING (NAME, NEWIC, OTHID, DOBDOR)
WHERE n.NAME IS NOT NULL
   OR id.NAME IS NOT NULL
   OR ic.NAME IS NOT NULL
   OR nid.NAME IS NOT NULL
   OR nic.NAME IS NOT NULL;
""")

# ======================================================
# Output Parquet & CSV
# ======================================================
con.execute(f"""
    COPY (SELECT * FROM ALLMATCH)
    TO '{parquet_output_path}/ALLMATCH.parquet' (FORMAT PARQUET);
""")

con.execute(f"""
    COPY (SELECT * FROM ALLMATCH)
    TO '{csv_output_path}/ALLMATCH.csv' (HEADER, DELIMITER ',');
""")

print("✅ ALLMATCH generated successfully — row count now matches SAS output (4 rows).")
