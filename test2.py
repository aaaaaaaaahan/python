import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

# =====================================================
# FILE PATHS (CHANGE THESE TO YOUR ACTUAL PARQUET FILES)
# =====================================================
CTRLDATE_PARQUET = "SRSCTRL1.parquet"
CIPHONET_PARQUET = "UNLOAD_CIPHONET_FB.parquet"
CISFILE_PARQUET  = "CIS_CUST_DAILY.parquet"
DPTRBALS_PARQUET = "DPTRBLGS.parquet"

OUT_PARQUET = "CIPHONET_ATM_CONTACT.parquet"
OUT_TXT     = "CIPHONET_ATM_CONTACT.txt"


# ========================
# CONNECT TO DUCKDB
# ========================
con = duckdb.connect()


# =====================================================
# STEP 1: CTRLDATE -> SRSDATE
# =====================================================
con.execute(f"""
CREATE OR REPLACE TEMP TABLE SRSDATE AS
SELECT 
    SRSYY,
    SRSMM,
    SRSDD,
    LPAD(CAST(SRSYY AS VARCHAR),4,'0') ||
    LPAD(CAST(SRSMM AS VARCHAR),2,'0') ||
    LPAD(CAST(SRSDD AS VARCHAR),2,'0') AS CURDT
FROM '{CTRLDATE_PARQUET}'
""")

curdt = con.execute("SELECT CURDT FROM SRSDATE LIMIT 1").fetchone()[0]
print(">>> CURDT =", curdt)


# =====================================================
# STEP 2: PHONE DATA
# =====================================================
con.execute(f"""
CREATE OR REPLACE TEMP TABLE PHONE AS
SELECT *,
       LPAD(CAST(UPDYY AS VARCHAR),4,'0') ||
       LPAD(CAST(UPDMM AS VARCHAR),2,'0') ||
       LPAD(CAST(UPDDD AS VARCHAR),2,'0') AS RECDT
FROM '{CIPHONET_PARQUET}'
WHERE UPDSOURCE <> 'INIT'
  AND LPAD(CAST(UPDYY AS VARCHAR),4,'0') ||
      LPAD(CAST(UPDMM AS VARCHAR),2,'0') ||
      LPAD(CAST(UPDDD AS VARCHAR),2,'0') = '{curdt}'
""")

con.execute("""
CREATE OR REPLACE TEMP TABLE PHONE_SORT AS
SELECT * FROM PHONE ORDER BY CUSTNO
""")


# =====================================================
# STEP 3: CIS DATA
# =====================================================
con.execute(f"""
CREATE OR REPLACE TEMP TABLE CIS AS
SELECT DISTINCT
    CUSTNO,
    CUSTNAME,
    SECPHONE,
    ALIASKEY,
    ALIAS
FROM '{CISFILE_PARQUET}'
""")


# =====================================================
# STEP 4: MERGE1 (PHONE + CIS)
# =====================================================
con.execute("""
CREATE OR REPLACE TEMP TABLE MRG1 AS
SELECT 
    P.*,
    C.CUSTNAME,
    C.SECPHONE,
    C.ALIASKEY,
    C.ALIAS
FROM PHONE_SORT P
LEFT JOIN CIS C
ON P.CUSTNO = C.CUSTNO
ORDER BY P.TRXACCTDP
""")


# =====================================================
# STEP 5: DEPOSIT DATA
# =====================================================
con.execute(f"""
CREATE OR REPLACE TEMP TABLE DEPOSIT AS
SELECT
    REPTNO,
    FMTCODE,
    ACCTBRCH,
    TRXACCTDP,
    OPENDATE,
    OPENIND
FROM '{DPTRBALS_PARQUET}'
WHERE REPTNO = 1001
  AND FMTCODE IN (1,10,22)
  AND TRXACCTDP IS NOT NULL
  AND OPENIND = ''
ORDER BY TRXACCTDP
""")


# =====================================================
# STEP 6: MERGE2 (MRG1 + DEPOSIT)
# =====================================================
con.execute("""
CREATE OR REPLACE TEMP TABLE MRG2 AS
SELECT 
    M1.*,
    D.ACCTBRCH,
    D.OPENDATE,
    D.OPENIND
FROM MRG1 M1
LEFT JOIN DEPOSIT D
ON M1.TRXACCTDP = D.TRXACCTDP
ORDER BY M1.CUSTNO
""")


# =====================================================
# STEP 7: FINAL OUTPUT TABLE (TEMPOUT)
# =====================================================
con.execute("""
CREATE OR REPLACE TEMP TABLE TEMPOUT AS
SELECT
    '033' AS HDR,

    TRXAPPL,
    TRXACCTDP,
    PHONENEW,
    PHONEPREV,
    SECPHONE,
    ACCTBRCH,
    CUSTNO,
    CUSTNAME,
    ALIASKEY,
    ALIAS,

    LPAD(CAST(UPDDD AS VARCHAR),2,'0') AS UPDDD,
    LPAD(CAST(UPDMM AS VARCHAR),2,'0') AS UPDMM,
    LPAD(CAST(UPDYY AS VARCHAR),4,'0') AS UPDYY,

    CASE 
        WHEN UPDSOURCE NOT IN ('ATM','EBK') THEN 'OTC'
        ELSE UPDSOURCE
    END AS UPDSOURCE

FROM MRG2
""")


# =====================================================
# STEP 8: SAVE TO PARQUET
# =====================================================
arrow_table = con.execute("SELECT * FROM TEMPOUT").fetch_arrow_table()
pq.write_table(arrow_table, OUT_PARQUET)
print(">>> Parquet written to:", OUT_PARQUET)


# =====================================================
# STEP 9: CREATE FIXED-LENGTH TXT OUTPUT
# =====================================================

def format_line(row):
    return (
        f"{row['HDR']:<3}"                         # 001
        f"{row['TRXAPPL']:<5}"                     # 004
        f"{str(row['TRXACCTDP']).rjust(10)}"       # 009
        f"{str(row['PHONENEW']).zfill(11)}"        # 029
        f"{str(row['PHONEPREV']).zfill(11)}"       # 040
        f"{str(row['SECPHONE']).zfill(11)}"        # 051
        f"{str(row['ACCTBRCH']).zfill(7)}"         # 062
        f"{row['CUSTNO']:<20}"                     # 069
        f"{row['CUSTNAME']:<40}"                   # 089
        f"{row['ALIASKEY']:<3}"                    # 129
        f"{row['ALIAS']:<37}"                      # 132
        f"{row['UPDDD']}/"                         # 169
        f"{row['UPDMM']}/"                         # 172
        f"{row['UPDYY']}"                          # 175
        f"{row['UPDSOURCE']:<5}"                   # 179
    )

df = con.execute("SELECT * FROM TEMPOUT").fetch_df()

with open(OUT_TXT, "w", encoding="utf-8") as fout:
    for _, row in df.iterrows():
        fout.write(format_line(row) + "\n")

print(">>> Fixed-length TXT written to:", OUT_TXT)


# =====================================================
# PROCESS COMPLETE
# =====================================================
print(">>> CIPHB4AF Python conversion completed successfully.")
