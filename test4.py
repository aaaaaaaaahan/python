import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime, timedelta

# -----------------------------
# Paths
# -----------------------------
# Replace with your paths
newchg_file = "/host/cis/parquet/NEWCHG.parquet"
oldchg_file = "/host/cis/parquet/OLDCHG.parquet"
active_file = "/host/cis/parquet/ACTIVE.parquet"
output_file = "/host/cis/output/CIS_IDIC_DAILY_RPT.txt"

# -----------------------------
# Batch date (same as SAS SRSDATE)
# -----------------------------
batch_date = datetime.today() - timedelta(days=1)
DAY = f"{batch_date.day:02}"
MONTH = f"{batch_date.month:02}"
YEAR = f"{batch_date.year}"

# -----------------------------
# DuckDB connection
# -----------------------------
con = duckdb.connect()

# -----------------------------
# Load parquet files
# -----------------------------
con.execute(f"""
CREATE OR REPLACE TABLE NEWCHG AS
SELECT 
    RUNTIMESTAMP,
    CUSTNOX AS CUSTNO,
    ADDREFX AS ADDREF,
    CUSTNAME,
    PRIPHONEX AS PRIPHONE,
    SECPHONEX AS SECPHONE,
    MOBILEPHX AS MOBILEPH,
    FAXX AS FAX,
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
FROM read_parquet('{newchg_file}')
""")

con.execute(f"""
CREATE OR REPLACE TABLE OLDCHG AS
SELECT 
    RUNTIMESTAMP,
    CUSTNOX AS CUSTNO,
    ADDREFX AS ADDREF,
    CUSTNAME,
    PRIPHONEX AS PRIPHONE,
    SECPHONEX AS SECPHONE,
    MOBILEPHX AS MOBILEPH,
    FAXX AS FAX,
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
FROM read_parquet('{oldchg_file}')
""")

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
FROM read_parquet('{active_file}')
WHERE ACCTCODE IN ('DP   ', 'LN   ')
""")

# -----------------------------
# Keep only active accounts (latest, no closing date)
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE LISTACT AS
SELECT DISTINCT ON (CUSTNO)
    CUSTNO, ACCTCODE, ACCTNOC
FROM ACTIVE
WHERE DATECLSE NOT IN ('       .', '        ', '00000000')
ORDER BY CUSTNO, DATEOPEN DESC
""")

# -----------------------------
# Merge NEWCHG and active accounts
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE NEWACT AS
SELECT N.*
FROM NEWCHG N
JOIN LISTACT L
ON N.CUSTNO = L.CUSTNO
""")

# -----------------------------
# Merge NEWACT and OLDCHG
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE MERGE_A AS
SELECT N.*, O.*
FROM NEWACT N
JOIN OLDCHG O
ON N.CUSTNO = O.CUSTNO
""")

# -----------------------------
# Compare fields and output differences
# -----------------------------
# In Python, we can generate all changes similar to SAS C_* tables
fields_to_check = [
    ('CUSTMNTDATE', 'CUSTMNTDATE'),
    ('CUSTLASTOPER', 'CUSTLASTOPER'),
    ('ADDREF', 'ADDREF'),
    ('CUSTNAME', 'CUSTNAME'),
    ('LONGNAME', 'LONGNAME'),
    ('DOBDOR', 'DOBDOR'),
    ('BASICGRPCODE', 'BASICGRPCODE'),
    ('CORPSTATUS', 'CORPSTATUS'),
    ('MSICCODE', 'MSICCODE'),
    ('CUST_CODE', 'CUST_CODE'),
    ('CITIZENSHIP', 'CITIZENSHIP'),
    ('MASCO2008', 'MASCO2008'),
    ('EMPLOYMENT_SECTOR', 'EMPLOYMENT_SECTOR'),
    ('EMPLOYMENT_TYPE', 'EMPLOYMENT_TYPE'),
    ('EMPNAME', 'EMPNAME'),
    ('PRCOUNTRY', 'PRCOUNTRY'),
    ('RESIDENCY', 'RESIDENCY')
]

# For simplicity, we create a long union of differences
diff_queries = []
for field, field_old in fields_to_check:
    diff_queries.append(f"""
    SELECT
        CUSTNO,
        '{field}' AS FIELDS,
        {field_old} AS OLDVALUE,
        {field} AS NEWVALUE,
        CUSTLASTOPER AS UPDOPER,
        CUSTMNTDATE AS UPDDATE,
        ACCTNOC
    FROM MERGE_A
    WHERE {field} IS DISTINCT FROM {field_old}
    """)

con.execute(f"""
CREATE OR REPLACE TABLE TEMPALL AS
{ ' UNION ALL '.join(diff_queries) }
""")

# -----------------------------
# Final merge to set missing UPDDATE/UPDOPER
# -----------------------------
# In SAS they overwrite empty UPDDATE/UPDOPER with CUSTMNTDATE/CUSTLASTOPER
con.execute(f"""
CREATE OR REPLACE TABLE MRGCIS AS
SELECT *,
       COALESCE(UPDDATE, CUSTMNTDATE) AS UPDDATE_FINAL,
       COALESCE(UPDOPER, CUSTLASTOPER) AS UPDOPER_FINAL
FROM TEMPALL
WHERE UPDOPER NOT IN ('ELNBATCH','AMLBATCH','HRCBATCH','CTRBATCH',
                      'CIFLPRCE','CISUPDEC','CIUPDMSX','CIUPDMS9','MAPLOANS','CRIS')
""")

# -----------------------------
# Export to fixed-width text file
# -----------------------------
# Calculate fixed-width fields
con.execute(f"""
COPY (
SELECT
    UPDOPER_FINAL AS UPDOPER,
    CUSTNO,
    ACCTNOC,
    CUSTNAME,
    FIELDS,
    OLDVALUE,
    NEWVALUE,
    '{DAY}/{MONTH}/{YEAR}' AS UPDDATX
FROM MRGCIS
) TO '{output_file}' (FORMAT 'fixedwidth');
""")

print("Processing completed. Output file:", output_file)
