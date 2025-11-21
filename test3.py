import duckdb
import datetime
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")
curdt = batch_date.strftime("%Y%m%d")

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
cis = get_hive_parquet('CIS_CUST_DAILY')


# =====================================================
# STEP 1: PHONE DATA
# =====================================================
con.execute(f"""
CREATE OR REPLACE TEMP TABLE PHONE AS
SELECT *,
       TRXAPPLNO AS TRXACCTDP,
        MAKE_DATE(CAST(UPDTYY AS INTEGER), CAST(UPDTMM AS INTEGER), CAST(UPDTDD AS INTEGER)) AS RECDT
FROM '{host_parquet_path("UNLOAD_CIPHONET_FB.parquet")}'
WHERE UPDSOURCE <> 'INIT'
  AND RECDT = '{curdt}'
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
FROM read_parquet('{cis[0]}')
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
    CAST(REPTNO AS INTEGER) AS REPTNO,
    CAST(FMTCODE AS INTEGER) AS FMTCODE,
    LPAD(CAST(CAST(BRANCH AS INT) AS VARCHAR),3,'0') AS ACCTBRCH,
    LPAD(CAST(CAST(ACCTNO AS BIGINT) AS VARCHAR),11,'0') AS TRXACCTDP,
    CAST(REOPENDT AS BIGINT) AS OPENDATE,
    OPENIND
FROM '{host_parquet_path("DPTRBLGS_CIS.parquet")}'
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
