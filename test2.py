import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
import os

# ==========================================================
# CONFIG
# ==========================================================
custfile_parquet = "RBP2.B033.CIS.CUST.DAILY.parquet"
custcode_parquet = "RBP2.B033.CUSTCODE.C002.parquet"

output_parquet = "RBP2.B033.CUSTCODE.BNKEMP.parquet"
output_txt = "RBP2.B033.CUSTCODE.BNKEMP.txt"

# Delete old output files (same as DISP=MOD,DELETE,DELETE)
for f in [output_parquet, output_txt]:
    if os.path.exists(f):
        os.remove(f)

# ==========================================================
# CONNECT
# ==========================================================
con = duckdb.connect()

# ==========================================================
# 1. LOAD CUST FILE  (like SET CUSTFILE.CUSTDLY)
#    Filter ACCTCODE = 'DP'
#    Keep SAS fields
# ==========================================================
con.execute(f"""
    CREATE OR REPLACE TABLE CUST AS
    SELECT
        CUSTNO,
        CUSTNAME,
        ACCTNOC,
        ACCTCODE,
        JOINTACC
    FROM read_parquet('{custfile_parquet}')
    WHERE ACCTCODE = 'DP'
    ORDER BY CUSTNO
""")

print("CUST sample:")
print(con.execute("SELECT * FROM CUST LIMIT 5").fetchdf())

# ==========================================================
# 2. LOAD BANKEMP (CUSTCODE file)
#    Equivalent to INPUT @1 CUSTNO $20
#    Then dedup by CUSTNO
# ==========================================================
con.execute(f"""
    CREATE OR REPLACE TABLE BANKEMP AS
    SELECT DISTINCT
        CUSTNO
    FROM read_parquet('{custcode_parquet}')
    ORDER BY CUSTNO
""")

print("BANKEMP sample:")
print(con.execute("SELECT * FROM BANKEMP LIMIT 5").fetchdf())

# ==========================================================
# 3. MERGE (LEFT JOIN WHERE BANKEMP EXISTS)
#    SAS: MERGE CUST(IN=S) BANKEMP(IN=T); IF T;
# ==========================================================
con.execute("""
    CREATE OR REPLACE TABLE MERGE AS
    SELECT
        C.CUSTNO,
        C.ACCTCODE,
        C.ACCTNOC,
        C.JOINTACC,
        C.CUSTNAME
    FROM CUST C
    JOIN BANKEMP B
    ON C.CUSTNO = B.CUSTNO
    ORDER BY C.CUSTNO, C.ACCTNOC
""")

print("MERGE sample:")
print(con.execute("SELECT * FROM MERGE LIMIT 5").fetchdf())

# ==========================================================
# 4. REMOVE ACCTNOC = '' (SAS: IF ACCTNOC=' ' THEN DELETE)
# ==========================================================
con.execute("""
    CREATE OR REPLACE TABLE OUT AS
    SELECT *
    FROM MERGE
    WHERE TRIM(ACCTNOC) <> ''
""")

print("OUT sample:")
print(con.execute("SELECT * FROM OUT LIMIT 5").fetchdf())

# ==========================================================
# 5. WRITE PARQUET OUTPUT
# ==========================================================
out_arrow = con.execute("SELECT * FROM OUT").fetch_arrow_table()
pq.write_table(out_arrow, output_parquet)

# ==========================================================
# 6. WRITE FIXED-WIDTH TXT (same as SAS PUT @01 …)
# ==========================================================
with open(output_txt, "w") as f:
    df = con.execute("SELECT * FROM OUT").fetchdf()

    for _, row in df.iterrows():
        line = (
            f"{str(row['CUSTNO']).ljust(20)}"
            f"{str(row['ACCTCODE']).ljust(5)}"
            f"{str(row['ACCTNOC']).ljust(11)}"
            f"{str(row['JOINTACC']).ljust(1)}"
            f"{str(row['CUSTNAME']).ljust(40)}"
        )
        f.write(line + "\n")

print("TXT output written:", output_txt)
print("PARQUET output written:", output_parquet)
