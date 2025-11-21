import duckdb
import datetime
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")
#curdt = batch_date.strftime("%Y%m%d")
curdt = 20211101

#---------------------------------------------------------------------#
# Original Program: CIBMSPEN                                          #
#---------------------------------------------------------------------#
# GET BEFORE AND AFTER EFFECT OF PHONE NUMBERS                        #
# IMPL       ESMR      DESC                                           #
# ========== ========= ========================================       #
# 09/02/2010 2009-1889 UPDATE PHONE NUMBER (ATM)                      #
# 19/05/2010 2010-733  PB BANK CARD/DAY2DAY SCREEN : INCL CONTACTNO   #
# 28/07/2010 2010-2215 PROMPT PHONE UPDATE TO ALL ATM TRX TYPES.      #
# 29/07/2010 2010-1013 UPDATE PHONE NUMBER (EBANK) CIPHONET TABLE     #
# 05/10/2010 2010-2314 VALIDATE EBANK/ATM PHONE NUMBER                #
# 26/11/2010 2010-1324 UPDATE PHONE NUMBER (OTC)                      #
# 05/01/2011 2010-4144 VALIDATION LOGIC FOR PB BANK CARD/DAY2DAY      #
# 28/03/2011 2011-0502 BLOCK TETI IS PHONE NOT UPDATED                #
# 21/10/2011 2011-3172 OTC/CASH WITHDRAWAL FD FIXES                   #
#            2011-3700 ADD NEW FIELD - PHONE NEW (CURRENT CHG)        #
# 20/04/2012 A2012-7142 INCREASE REGION SIZE FROM 64M TO 256M         #
#---------------------------------------------------------------------#

# ========================
# CONNECT TO DUCKDB
# ========================
con = duckdb.connect()
cis = get_hive_parquet('CIS_CUST_DAILY')


# =====================================================
# STEP 1: PHONE DATA
# =====================================================
con.execute(f"""
CREATE OR REPLACE TABLE PHONE AS
SELECT *,
       TRXAPPLNO AS TRXACCTDP,
       TRXAPPLCODE AS TRXAPPL,
       LPAD(CAST(CAST(NEWPHONE AS BIGINT) AS VARCHAR),12,'0') AS PHONENEW,
       PHONEPREV,
       STRFTIME(MAKE_DATE(CAST(UPDTYY AS INTEGER), CAST(UPDTMM AS INTEGER), CAST(UPDTDD AS INTEGER)), '%Y%m%d') AS RECDT
FROM '{host_parquet_path("UNLOAD_CIPHONET_FB.parquet")}'
WHERE UPDSOURCE <> 'INIT'
  AND RECDT = {curdt}
""")

con.execute("""
CREATE OR REPLACE TABLE PHONE_SORT AS
SELECT * FROM PHONE ORDER BY CUSTNO
""")


# =====================================================
# STEP 2: CIS DATA
# =====================================================
con.execute(f"""
CREATE OR REPLACE TABLE CIS AS
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
CREATE OR REPLACE TABLE MRG1 AS
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
CREATE OR REPLACE TABLE DEPOSIT AS
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
CREATE OR REPLACE TABLE MRG2 AS
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
CREATE OR REPLACE TABLE TEMPOUT AS
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

    -- Date format: DD/MM/YYYY
    LPAD(CAST(UPDTDD AS VARCHAR),2,'0') || '/' ||
    LPAD(CAST(UPDTMM AS VARCHAR),2,'0') || '/' ||
    LPAD(CAST(UPDTYY AS VARCHAR),4,'0') AS UPDDATE,

    CASE 
        WHEN UPDSOURCE NOT IN ('ATM','EBK') THEN 'OTC'
        ELSE UPDSOURCE
    END AS UPDSOURCE

FROM MRG2
""")

print("CIS (first 5 rows):")
print(con.execute("SELECT * FROM TEMPOUT LIMIT 500").fetchdf())
