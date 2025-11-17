import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv
import os

# ==========================================
# CONFIG — change these paths for your run
# ==========================================
HRCSTDP_PARQUET = host_parquet_path("HRCSTDP")      # CIS.HRCCUST.DPACCTS
DPFILE_PARQUET  = host_parquet_path("DPTRBLGS")     # DPFILE
OUT_GOOD_PARQ   = parquet_output_path("DP.GOOD.parquet")
OUT_BAD_PARQ    = parquet_output_path("DP.BAD.parquet")
OUT_PBB_PARQ    = parquet_output_path("DP.GOOD.PBB.parquet")
OUT_PIBB_PARQ   = parquet_output_path("DP.GOOD.PIBB.parquet")

OUT_GOOD_TXT    = csv_output_path("DP.GOOD.txt")
OUT_BAD_TXT     = csv_output_path("DP.BAD.txt")
OUT_PBB_TXT     = csv_output_path("DP.GOOD.PBB.txt")
OUT_PIBB_TXT    = csv_output_path("DP.GOOD.PIBB.txt")

con = duckdb.connect()

# ========================================================
# 1) LOAD HRCSTDP & DPFILE (already-parquet assumption)
# ========================================================
con.execute(f"""
    CREATE TABLE HRCSTDP AS 
    SELECT * FROM read_parquet('{HRCSTDP_PARQUET}');
""")

con.execute(f"""
    CREATE TABLE DPFILE AS 
    SELECT * FROM read_parquet('{DPFILE_PARQUET}');
""")

# ========================================================
# 2) CISDP — SAS fixed-col input rewritten using DuckDB
# ========================================================
# Assumption: parquet already contains correct columns
# Otherwise, use substr() to slice fields
con.execute("""
    CREATE TABLE CISDP AS 
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
    FROM HRCSTDP
    ORDER BY ACCTNO;
""")

# ========================================================
# 3) DPDATA — SAS INPUT with conditional fields
# ========================================================
con.execute("""
    CREATE TABLE DPDATA AS
    SELECT
        BANKNO,
        REPTNO,
        FMTCODE,
        BRANCH,
        ACCTNO,
        CLSDATE,
        OPENDATE,
        LEDBAL,
        ACCSTAT,
        COSTCTR,
        -- SAS: TMPACCT = PUT(ACCTNO,Z10.)
        LPAD(ACCTNO::VARCHAR, 10, '0') AS TMPACCT
    FROM DPFILE
    WHERE REPTNO = 1001
      AND FMTCODE IN (1,5,10,11,19,20,21,22)
      AND BRANCH <> 0
      AND OPENDATE <> 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY ACCTNO) = 1
    ORDER BY ACCTNO;
""")

# ========================================================
# 4) MERGE → GOODDP / BADDP  (SAS MERGE logic)
# ========================================================
con.execute("""
    CREATE TABLE MERGED AS
    SELECT
        A.*,
        B.BANKNUM, B.CUSTBRCH, B.CUSTNO, B.CUSTNAME, B.RACE,
        B.CITIZENSHIP, B.INDORG, B.PRIMSEC,
        B.CUSTLASTDATECC, B.CUSTLASTDATEYY, B.CUSTLASTDATEMM, B.CUSTLASTDATEDD,
        B.ALIASKEY, B.ALIAS, B.HRCCODES, B.ACCTCODE
    FROM DPDATA A
    JOIN CISDP B USING (ACCTNO);
""")

# GOOD / BAD based on SAS conditions
con.execute("""
    CREATE TABLE GOODDP AS
    SELECT *
    FROM MERGED
    WHERE
        (
            SUBSTR(TMPACCT, 1, 1) IN ('1', '3')
            AND ACCSTAT NOT IN ('C', 'B', 'P', 'Z')
        )
        OR
        (
            SUBSTR(TMPACCT, 1, 1) NOT IN ('1','3')
            AND (ACCSTAT NOT IN ('C','B','P','Z') OR LEDBAL <> 0)
        )
    ORDER BY CUSTNO, ACCTNO;
""")

con.execute("""
    CREATE TABLE BADDP AS
    SELECT *
    FROM MERGED
    EXCEPT
    SELECT * FROM GOODDP;
""")

# ========================================================
# 5) PBB / PIBB SPLIT (SAS SORT OUTFIL)
#    OUTFIL:
#    - IF FIELD 210 != '3' → PBB (conventional)
#    - IF FIELD 210 == '3' → PIBB (Islamic)
# ========================================================
con.execute("""
    CREATE TABLE GOOD_PBB AS
    SELECT * FROM GOODDP
    WHERE COSTCTR <> 3;     -- matches SAS INCLUDE=(210,1,CH,NE,'3')
""")

con.execute("""
    CREATE TABLE GOOD_PIBB AS
    SELECT * FROM GOODDP
    WHERE COSTCTR = 3;      -- matches SAS INCLUDE=(210,1,CH,EQ,'3')
""")

# ========================================================
# 6) WRITE OUTPUTS (PARQUET & TXT FIXED WIDTH)
# ========================================================
def write_parquet(table_name, out_path):
    tbl = con.execute(f"SELECT * FROM {table_name}").arrow()
    pq.write_table(tbl, out_path)

def write_fixed_width(table_name, out_path):
    df = con.execute(f"SELECT * FROM {table_name}").df()

    def fw(row):
        return (
            f"{row['BANKNUM']:>3}"
            f"{int(row['CUSTBRCH']):05d}"
            f"{row['CUSTNO']:<11}"
            f"{row['CUSTNAME']:<40}"
            f"{row['RACE']}"
            f"{row['CITIZENSHIP']:<2}"
            f"{row['INDORG']}"
            f"{row['PRIMSEC']}"
            f"{row['CUSTLASTDATECC']}"
            f"{row['CUSTLASTDATEYY']}"
            f"{row['CUSTLASTDATEMM']}"
            f"{row['CUSTLASTDATEDD']}"
            f"{row['ALIASKEY']:<3}"
            f"{row['ALIAS']:<20}"
            f"{row['HRCCODES']:<60}"
            f"{int(row['BRANCH']):07d}"
            f"{row['ACCTCODE']:<5}"
            f"{int(row['ACCTNO']):020d}"
            f"{int(row['OPENDATE']):08d}"
            f"{int(row['LEDBAL']):013d}"
            f"{row['ACCSTAT']}"
            f"{int(row['COSTCTR']):04d}"
        )

    with open(out_path, "w") as f:
        for _, r in df.iterrows():
            f.write(fw(r) + "\n")


# PARQUET output
write_parquet("GOODDP", OUT_GOOD_PARQ)
write_parquet("BADDP", OUT_BAD_PARQ)
write_parquet("GOOD_PBB", OUT_PBB_PARQ)
write_parquet("GOOD_PIBB", OUT_PIBB_PARQ)

# TXT output (fixed width matching SAS PUT)
write_fixed_width("GOODDP", OUT_GOOD_TXT)
write_fixed_width("BADDP", OUT_BAD_TXT)
write_fixed_width("GOOD_PBB", OUT_PBB_TXT)
write_fixed_width("GOOD_PIBB", OUT_PIBB_TXT)

print("CIHRCDP1 Python version completed successfully.")
