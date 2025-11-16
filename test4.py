import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")
UPDDATX = batch_date.strftime("%d/%m/%Y")

con = duckdb.connect()

# -----------------------------
# 1) Load NEWCHG
# -----------------------------
con.execute(f"""
CREATE OR REPLACE TABLE NEWCHG AS
SELECT
    CUSTNOX          AS CUSTNO,
    ADDREFX          AS ADDREF,
    CUSTNAME,
    PRIPHONEX        AS PRIPHONE,
    SECPHONEX        AS SECPHONE,
    MOBILEPHX        AS MOBILEPH,
    FAXX             AS FAX,
    ALIASKEY,
    ALIAS,
    PROCESSTIME,
    CUSTSTAT,
    TAXCODE,
    TAXID,
    CUSTBRCH,
    COSTCTR,
    CUSTMNTDATE,
    CUSTLASTOPER,
    PRIM_OFF,
    SEC_OFF,
    PRIM_LN_OFF,
    SEC_LN_OFF,
    RACE,
    RESIDENCY,
    CITIZENSHIP,
    OPENDT,
    HRCALL,
    EXPERIENCE,
    HOBBIES,
    RELIGION,
    LANGUAGE,
    INST_SEC,
    CUST_CODE,
    CUSTCONSENT,
    BASICGRPCODE,
    MSICCODE,
    MASCO2008,
    INCOME,
    EDUCATION,
    OCCUP,
    MARITALSTAT,
    OWNRENT,
    EMPNAME,
    DOBDOR,
    SICCODE,
    CORPSTATUS,
    NETWORTH,
    LAST_UPDATE_DATE,
    LAST_UPDATE_TIME,
    LAST_UPDATE_OPER,
    PRCOUNTRY,
    EMPLOYMENT_TYPE,
    EMPLOYMENT_SECTOR,
    EMPLOYMENT_LAST_UPDATE,
    BNMID,
    LONGNAME,
    INDORG,
    RESDESC,
    SALDESC,
    CTZDESC
FROM '{host_parquet_path("CIS_IDIC_DAILY_INEW.parquet")}'
""")
print("NEWCHG sample:")
print(con.query("SELECT * FROM NEWCHG LIMIT 5").df())

# -----------------------------
# 2) Load OLDCHG
# -----------------------------
con.execute(f"""
CREATE OR REPLACE TABLE OLDCHG AS
SELECT
    CUSTNOX,
    ADDREFX,
    CUSTNAME         AS CUSTNAMEX,
    PRIPHONEX,
    SECPHONEX,
    MOBILEPHX,
    FAXX,
    ALIASKEY         AS ALIASKEYX,
    ALIAS            AS ALIASX,
    PROCESSTIME      AS PROCESSTIMEX,
    CUSTSTAT         AS CUSTSTATX,
    TAXCODE          AS TAXCODEX,
    TAXID            AS TAXIDX,
    CUSTBRCH         AS CUSTBRCHX,
    COSTCTR          AS COSTCTRX,
    CUSTMNTDATE      AS CUSTMNTDATEX,
    CUSTLASTOPER     AS CUSTLASTOPERX,
    PRIM_OFF         AS PRIM_OFFX,
    SEC_OFF          AS SEC_OFFX,
    PRIM_LN_OFF      AS PRIM_LN_OFFX,
    SEC_LN_OFF       AS SEC_LN_OFFX,
    RACE             AS RACEX,
    RESIDENCY        AS RESIDENCYX,
    CITIZENSHIP      AS CITIZENSHIPX,
    OPENDT           AS OPENDTX,
    HRCALL           AS HRCALLX,
    EXPERIENCE       AS EXPERIENCEX,
    HOBBIES          AS HOBBIESX,
    RELIGION         AS RELIGIONX,
    LANGUAGE         AS LANGUAGEX,
    INST_SEC         AS INST_SECX,
    CUST_CODE        AS CUST_CODEX,
    CUSTCONSENT      AS CUSTCONSENTX,
    BASICGRPCODE     AS BASICGRPCODEX,
    MSICCODE         AS MSICCODEX,
    MASCO2008        AS MASCO2008X,
    INCOME           AS INCOMEX,
    EDUCATION        AS EDUCATIONX,
    OCCUP            AS OCCUPX,
    MARITALSTAT      AS MARITALSTATX,
    OWNRENT          AS OWNRENTX,
    EMPNAME          AS EMPNAMEX,
    DOBDOR           AS DOBDORX,
    SICCODE          AS SICCODEX,
    CORPSTATUS       AS CORPSTATUSX,
    NETWORTH         AS NETWORTHX,
    LAST_UPDATE_DATE AS LAST_UPDATE_DATEX,
    LAST_UPDATE_TIME AS LAST_UPDATE_TIMEX,
    LAST_UPDATE_OPER AS LAST_UPDATE_OPERX,
    PRCOUNTRY        AS PRCOUNTRYX,
    EMPLOYMENT_TYPE  AS EMPLOYMENT_TYPEX,
    EMPLOYMENT_SECTOR AS EMPLOYMENT_SECTORX,
    EMPLOYMENT_LAST_UPDATE AS EMPLOYMENT_LAST_UPDATEX,
    BNMID            AS BNMIDX,
    LONGNAME         AS LONGNAMEX,
    INDORG           AS INDORGX,
    RESDESC          AS RESDESCX,
    SALDESC          AS SALDESCX,
    CTZDESC          AS CTZDESCX
FROM '{host_parquet_path("CIS_IDIC_DAILY_IOLD.parquet")}'
""")
print("OLDCHG sample:")
print(con.query("SELECT * FROM OLDCHG LIMIT 5").df())

# -----------------------------
# 3) Load ACTIVE
# -----------------------------
con.execute(f"""
CREATE OR REPLACE TABLE ACTIVE AS
SELECT
    CUSTNO,
    ACCTCODE,
    ACCTNOC,
    NOTENOC,
    BANKINDC,
    DATEOPEN,
    DATECLSE,
    ACCTSTATUS
FROM '{host_parquet_path("CIS_CUST_DAILY_ACTVOD.parquet")}'
WHERE ACCTCODE IN ('DP   ', 'LN   ')
""")
print("ACTIVE sample:")
print(con.query("SELECT * FROM ACTIVE LIMIT 5").df())

# -----------------------------
# 4) LISTACT
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE LISTACT AS
SELECT CUSTNO, ACCTCODE, ACCTNOC
FROM (
    SELECT
        CUSTNO,
        ACCTCODE,
        ACCTNOC,
        ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY DATEOPEN DESC) AS rn,
        TRIM(DATECLSE) AS DATECLSE_TRIM
    FROM ACTIVE
) t
WHERE rn = 1
  AND (DATECLSE_TRIM = '' OR DATECLSE_TRIM = '.' OR DATECLSE_TRIM = '00000000')
""")
print("LISTACT sample:")
print(con.query("SELECT * FROM LISTACT LIMIT 5").df())

# -----------------------------
# 5) NEWACT
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE NEWACT AS
SELECT n.*, l.ACCTNOC
FROM NEWCHG n
JOIN LISTACT l
  ON n.CUSTNO = l.CUSTNO
""")
print("NEWACT sample:")
print(con.query("SELECT * FROM NEWACT LIMIT 5").df())

# -----------------------------
# 6) MERGE_A
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE MERGE_A AS
SELECT n.*, o.*
FROM NEWACT n
JOIN OLDCHG o
  ON n.CUSTNO = o.CUSTNOX
""")
print("MERGE_A sample:")
print(con.query("SELECT * FROM MERGE_A LIMIT 5").df())
