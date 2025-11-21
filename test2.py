import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import datetime

# =====================================================
# FILE PATHS (CHANGE THESE TO YOUR ACTUAL PARQUET FILES)
# =====================================================
CIPHONET_PARQUET = "UNLOAD_CIPHONET_FB.parquet"
CISFILE_PARQUET  = "CIS_CUST_DAILY.parquet"
DPTRBALS_PARQUET = "DPTRBLGS.parquet"

OUT_PARQUET = "CIPHONET_ATM_CONTACT.parquet"
OUT_TXT     = "CIPHONET_ATM_CONTACT.txt"


# =====================================================
# DATE HANDLING (REPLACED SRSCTRL1 WITH datetime)
# =====================================================
today = datetime.date.today()
batch_date = today - datetime.timedelta(days=1)   # use yesterday like batch
curdt = batch_date.strftime("%Y%m%d")            # same as SAS CURDT format: YYYYMMDD

print(">>> CURDT from datetime:", curdt)


# ========================
# CONNECT TO DUCKDB
# ========================
con = duckdb.connect()


# =====================================================
# STEP 1: PHONE DATA
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
# STEP 2: CIS DATA
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
# STEP 3: MERGE1 (PHONE + CIS)
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
# STEP 4: DEPOSIT DATA
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
# STEP 5: MERGE2 (MRG1 + DEPOSIT)
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
# STEP 6: FINAL OUTPUT TABLE
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
# STEP 7: SAVE TO PARQUET
# =====================================================
arrow_table = con.execute("SELECT * FROM TEMPOUT").fetch_arrow_table()
pq.write_table(arrow_table, OUT_PARQUET)
print(">>> Parquet written to:", OUT_PARQUET)


# =====================================================
# STEP 8: CREATE FIXED-LENGTH TXT OUTPUT
# =====================================================
def format_line(row):
    return (
        f"{row['HDR']:<3}"
        f"{row['TRXAPPL']:<5}"
        f"{str(row['TRXACCTDP']).rjust(10)}"
        f"{str(row['PHONENEW']).zfill(11)}"
        f"{str(row['PHONEPREV']).zfill(11)}"
        f"{str(row['SECPHONE']).zfill(11)}"
        f"{str(row['ACCTBRCH']).zfill(7)}"
        f"{row['CUSTNO']:<20}"
        f"{row['CUSTNAME']:<40}"
        f"{row['ALIASKEY']:<3}"
        f"{row['ALIAS']:<37}"
        f"{row['UPDDD']}/"
        f"{row['UPDMM']}/"
        f"{row['UPDYY']}"
        f"{row['UPDSOURCE']:<5}"
    )

df = con.execute("SELECT * FROM TEMPOUT").fetch_df()

with open(OUT_TXT, "w", encoding="utf-8") as fout:
    for _, row in df.iterrows():
        fout.write(format_line(row) + "\n")

print(">>> Fixed-length TXT written to:", OUT_TXT)
print(">>> CIPHB4AF Python conversion completed successfully.")
