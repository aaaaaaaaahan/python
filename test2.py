import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path, get_hive_parquet
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year1, month1, day1 = batch_date.year, batch_date.month, batch_date.day

#---------------------------------------------------------------------#
# Original Program: CIRMKLNS                                          #
#---------------------------------------------------------------------#
# ESMR 2023-00003043 AUTOMATE THE INTEREST / PROFIT ADVICE TO         #
# CORPORATE BANKING3S CUSTOMERS [PART B]                              #
# ESMR 2024-00003454 AUTOMATE THE INTEREST / PROFIT ADVICE TO         #
# CORPORATE BANKING3S CUSTOMERS - TO FINETUNE THE CUSTOMERS NAME FOR  #
# FOR JOINT BORROWERS                                                 #
#---------------------------------------------------------------------#

# =========================
# Connect DuckDB
# =========================
con = duckdb.connect()
prim, year, month, day = get_hive_parquet('LOANS_CUST_PRIMARY')
secd, year, month, day = get_hive_parquet('LOANS_CUST_SCNDARY')

# =========================
# Register parquet inputs
# =========================
con.execute(f"""
    CREATE OR REPLACE TABLE RMK AS 
    SELECT 
        CUSTNO, 
        RMK_LINE_1 AS REMARKS
    FROM '{host_parquet_path("CCRIS_CISRMRK_EMAIL_FIRST.parquet")}'
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
    FROM read_parquet(?)
""", [prim])

con.execute("""
    CREATE OR REPLACE TABLE SECD AS
    SELECT 
        CUSTNO AS CUSTNO1,
        ACCTNOC,
        DOBDOR AS DOBDOR1,
        LONGNAME AS LONGNAME1,
        INDORG AS INDORG1,
        PRIMSEC AS PRIMSEC1
    FROM read_parquet(?)
""", [secd])

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

final = """
    SELECT 
        *
        ,{year1} AS year
        ,{month1} AS month
        ,{day1} AS day
    FROM OUT1
"""

# =========================
# Save LAST RECORD only
# =========================
last_row = """
    SELECT *
        ,{year1} AS year
        ,{month1} AS month
        ,{day1} AS day
    FROM OUT1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO DESC) = 1
"""

# =========================
# Save FULL OUTPUT
# =========================
queries = {
    "LOANS_CISRMRK_EMAIL_DUP"            : final,
    "LOANS_CISRMRK_EMAIL"                : last_row
}

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    con.execute(f"""
    COPY ({query})
    TO '{parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);  
     """)
    
    con.execute(f"""
    COPY ({query})
    TO '{csv_path}'
    (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);  
     """)
