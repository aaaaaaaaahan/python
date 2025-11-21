import duckdb
import datetime
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet

# =====================================================
# DATE HANDLING (Replace SRSCTRL1 from SAS)
# =====================================================
# SAS:
# CURDT = YYYYMMDD from CTRLDATE
# Python: use yesterday date like batch job

batch_date = datetime.date.today() - datetime.timedelta(days=1)
curdt = batch_date.strftime("%Y%m%d")   # same as &CURDT in SAS

print(">>> Batch CURDT:", curdt)

# =====================================================
# FILE PATHS (Parquet version of mainframe files)
# =====================================================
CIPHONET_PARQUET = "UNLOAD_CIPHONET_FB.parquet"
CISFILE_PARQUET  = "CIS_CUST_DAILY.parquet"
DPTRBALS_PARQUET = "DPTRBLGS_CIS.parquet"

OUT_PARQUET = "CIPHONET_ATM_CONTACT.parquet"
OUT_TXT     = "CIPHONET_ATM_CONTACT.txt"

# =====================================================
# CONNECT DUCKDB
# =====================================================
con = duckdb.connect()
cis = get_hive_parquet('CIS_CUST_DAILY')

# =====================================================
# STEP 1: PHONE TABLE (Equivalent to DATA PHONE)
# =====================================================
# SAS logic:
# IF UPDSOURCE = 'INIT' THEN DELETE;
# RECDT = YYYYMMDD (from UPDYY,UPDMM,UPDDD)
# IF RECDT = &CURDT THEN OUTPUT;

con.execute(f"""
CREATE OR REPLACE TEMP TABLE PHONE AS
SELECT *,
       -- SAS: RECDT is string YYYYMMDD
       LPAD(CAST(UPDYY AS VARCHAR),4,'0') ||
       LPAD(CAST(UPDMM AS VARCHAR),2,'0') ||
       LPAD(CAST(UPDDD AS VARCHAR),2,'0') AS RECDT

FROM '{host_parquet_path(CIPHONET_PARQUET)}'

WHERE UPDSOURCE <> 'INIT'
  AND (
       LPAD(CAST(UPDYY AS VARCHAR),4,'0') ||
       LPAD(CAST(UPDMM AS VARCHAR),2,'0') ||
       LPAD(CAST(UPDDD AS VARCHAR),2,'0')
      ) = '{curdt}'
""")

# Sort like PROC SORT BY CUSTNO
con.execute("""
CREATE OR REPLACE TEMP TABLE PHONE_SORT AS
SELECT *
FROM PHONE
ORDER BY CUSTNO
""")

# =====================================================
# STEP 2: CIS TABLE (Equivalent to DATA CIS)
# =====================================================
# SAS:
# KEEP CUSTNO CUSTNAME SECPHONE ALIASKEY ALIAS
# PROC SORT NODUPKEY BY CUSTNO

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
# SAS:
# MERGE PHONE(IN=A) CIS(IN=B); BY CUSTNO;
# IF A THEN OUTPUT;

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
# STEP 4: DEPOSIT TABLE (Equivalent to DATA DEPOSIT)
# =====================================================
# SAS:
# IF REPTNO = 1001
# IF FMTCODE IN (1,10,22)
# IF TRXACCTDP NE ''
# IF OPENIND = ' ' (blank)

con.execute(f"""
CREATE OR REPLACE TEMP TABLE DEPOSIT AS
SELECT
    CAST(REPTNO AS INTEGER) AS REPTNO,
    CAST(FMTCODE AS INTEGER) AS FMTCODE,

    -- Format like SAS PD fields
    LPAD(CAST(CAST(BRANCH AS INT) AS VARCHAR),3,'0') AS ACCTBRCH,
    LPAD(CAST(CAST(ACCTNO AS BIGINT) AS VARCHAR),11,'0') AS TRXACCTDP,

    CAST(REOPENDT AS BIGINT) AS OPENDATE,
    OPENIND
FROM '{host_parquet_path(DPTRBALS_PARQUET)}'
WHERE REPTNO = 1001
  AND FMTCODE IN (1,10,22)
  AND TRXACCTDP IS NOT NULL
  AND TRIM(OPENIND) = ''   -- SAS: OPENIND EQ ''
ORDER BY TRXACCTDP
""")

# =====================================================
# STEP 5: MERGE2 (MRG1 + DEPOSIT)
# =====================================================
# SAS:
# MERGE MRG1(IN=A) DEPOSIT(IN=B); BY TRXACCTDP;
# IF A THEN OUTPUT;

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
# STEP 6: FINAL OUTPUT (Equivalent to DATA TEMPOUT)
# =====================================================
# SAS output format:
# DD/MM/YYYY
# UPDSOURCE mapping: not ATM/EBK -> OTC

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

    -- Date format EXACT like SAS: DD/MM/YYYY
    LPAD(CAST(UPDDD AS VARCHAR),2,'0') || '/' ||
    LPAD(CAST(UPDMM AS VARCHAR),2,'0') || '/' ||
    LPAD(CAST(UPDYY AS VARCHAR),4,'0') AS UPDDATE,

    CASE 
        WHEN UPDSOURCE NOT IN ('ATM','EBK') THEN 'OTC'
        ELSE UPDSOURCE
    END AS UPDSOURCE

FROM MRG2
""")

# =====================================================
# OUTPUT TO PARQUET
# =====================================================
con.execute(f"""
COPY TEMPOUT TO '{OUT_PARQUET}' (FORMAT 'parquet');
""")

print("✅ Parquet output done:", OUT_PARQUET)

# =====================================================
# OUTPUT TO FIXED TEXT FILE (Optional)
# =====================================================
# If you want exact 200 LRECL like SAS PUT statement,
# I can build it next with fixed positions.

con.execute(f"""
COPY TEMPOUT TO '{OUT_TXT}'
(HEADER 0, DELIMITER '|');
""")

print("✅ Text output done:", OUT_TXT)
print(">>> Program finished successfully")
