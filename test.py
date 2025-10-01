import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# =========================
# Connect DuckDB
# =========================
con = duckdb.connect()

# =========================
# Register parquet inputs
# =========================
con.execute("""
    CREATE OR REPLACE TABLE RMK AS 
    SELECT CUSTNO, REMARKS
    FROM parquet_scan('rmk.parquet')
""")

con.execute("""
    CREATE OR REPLACE TABLE PRIM AS
    SELECT 
        CUSTNO,
        ACCTNOC,
        DOBDOR,
        LONGNAME,
        INDORG,
        PRIMSEC
    FROM parquet_scan('prim.parquet')
""")

con.execute("""
    CREATE OR REPLACE TABLE SECD AS
    SELECT 
        CUSTNO AS CUSTNO1,
        ACCTNOC,
        DOBDOR AS DOBDOR1,
        LONGNAME AS LONGNAME1,
        INDORG AS INDORG1,
        PRIMSEC AS PRIMSEC1
    FROM parquet_scan('secd.parquet')
""")

# =========================
# Match Logic (PRIM vs SECD)
# =========================
con.execute("""
    CREATE OR REPLACE TABLE MATCH1 AS
    SELECT 
        P.CUSTNO,
        P.ACCTNOC,
        P.DOBDOR,
        TRIM(P.LONGNAME) || ' & ' || TRIM(S.LONGNAME1) AS LONGNAME,
        P.INDORG,
        'Y' AS JOINT
    FROM PRIM P
    JOIN SECD S USING (ACCTNOC)
""")

con.execute("""
    CREATE OR REPLACE TABLE XMATCH AS
    SELECT 
        P.CUSTNO,
        P.ACCTNOC,
        P.DOBDOR,
        P.LONGNAME,
        P.INDORG,
        'N' AS JOINT
    FROM PRIM P
    LEFT JOIN SECD S USING (ACCTNOC)
    WHERE S.ACCTNOC IS NULL
""")

# =========================
# Merge with RMK (EMAIL)
# =========================
con.execute("""
    CREATE OR REPLACE TABLE MATCH2 AS
    SELECT R.CUSTNO, M.*
    FROM RMK R
    JOIN MATCH1 M USING (CUSTNO)
""")

con.execute("""
    CREATE OR REPLACE TABLE MATCH3 AS
    SELECT R.CUSTNO, X.*
    FROM RMK R
    JOIN XMATCH X USING (CUSTNO)
""")

# =========================
# Final Output
# =========================
con.execute("""
    CREATE OR REPLACE TABLE OUT1 AS
    SELECT 
        M.CUSTNO,
        M.ACCTNOC,
        R.REMARKS,
        M.DOBDOR,
        M.LONGNAME,
        M.INDORG,
        M.JOINT
    FROM (
        SELECT * FROM MATCH2
        UNION ALL
        SELECT * FROM MATCH3
    ) M
    JOIN RMK R USING (CUSTNO)
""")

# =========================
# Save FULL OUTPUT
# =========================
out1 = con.execute("SELECT * FROM OUT1").arrow()
pq.write_table(out1, "email_dup.parquet")

# =========================
# Save LAST RECORD only
# =========================
last_row = con.execute("""
    SELECT *
    FROM OUT1
    QUALIFY ROW_NUMBER() OVER (ORDER BY CUSTNO DESC) = 1
""").arrow()

pq.write_table(last_row, "email.parquet")

print("✅ Processing complete: email_dup.parquet & email.parquet written")
