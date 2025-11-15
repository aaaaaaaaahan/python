import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")
UPDDATX = batch_date.strftime("%d/%m/%Y")

# -----------------------
# DuckDB connection
# -----------------------
con = duckdb.connect()
# enable parquet and pyarrow usage by duckdb (duckdb handles read_parquet directly)

# Register parquet files as DuckDB views/tables
con.execute(f"""CREATE VIEW newchg AS 
        SELECT 
            *,
            CUSTNOX AS CUSTNO,
            ADDREFX AS ADDREF
        FROM '{host_parquet_path("CIS_IDIC_DAILY_INEW.parquet")}'
""")

con.execute(f"""CREATE VIEW oldchg AS
        SELECT 
            * ,
            CUSTNOX AS CUSTNO
        FROM '{host_parquet_path("CIS_IDIC_DAILY_IOLD.parquet")}'
""")

con.execute(f"""CREATE VIEW ccrsbank AS 
        SELECT 
            * 
        FROM '{host_parquet_path("CIS_CUST_DAILY_ACTVOD.parquet")}'
""")

# -----------------------
# 1) Build ACTIVE (filter ACCTCODE)
#    SAS:
#      IF ACCTCODE NOT IN ('DP   ','LN   ') THEN DELETE;
# -----------------------
# The SAS ACCTCODE contains padded 5-char strings like 'DP   ' or 'LN   '
# We will use TRIM to be robust.
con.execute("""
CREATE TABLE active AS
SELECT
  CUSTNO,
  ACCTCODE,
  ACCTNOC,
  NOTENOC,
  BANKINDC,
  DATEOPEN,
  DATECLSE,
  ACCTSTATUS
FROM ccrsbank
WHERE trim(ACCTCODE) IN ('DP', 'LN')
""")

# keep latest account per CUSTNO by DATEOPEN descending
# if DATEOPEN is numeric or string, we assume lexicographic ordering works or convert where needed.
con.execute("""
CREATE TABLE listact AS
SELECT CUSTNO, ACCTCODE, ACCTNOC, DATEOPEN, DATECLSE
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY DATEOPEN DESC NULLS LAST) as rn
  FROM active
)
WHERE rn = 1
""")

# Filter out closed accounts (SAS: IF DATECLSE NOT IN ('       .','        ','00000000') THEN DELETE;)
# We'll interpret blank/null or '00000000' as closed -> keep rows where DATECLSE is null/blank or equals special blanks -> SAS kept rows where DATECLSE is NOT in those values -> Actually SAS: IF DATECLSE NOT IN (...) THEN DELETE -> that kept rows with blank/00000000 only. We'll translate to: keep where DATECLSE IS NULL OR trim(DATECLSE) IN ('', '00000000', '.', '.......')
# To be safe: keep where DATECLSE IS NULL OR trim(DATECLSE) IN ('', '00000000', '.', '.......')
con.execute("""
CREATE TABLE listact_clean AS
SELECT CUSTNO, ACCTCODE, ACCTNOC
FROM listact
WHERE DATECLSE IS NULL
   OR trim(DATECLSE) = ''
   OR trim(DATECLSE) = '00000000'
   OR trim(DATECLSE) = '.'
""")

# -----------------------
# 2) NEWACT = MERGE NEWCHG(IN=F) LISTACT(IN=G); BY CUSTNO; IF F AND G;
# -----------------------
con.execute("""
CREATE TABLE newact AS
SELECT n.*, l.ACCTNOC
FROM newchg n
INNER JOIN listact_clean l USING (CUSTNO)
""")

# -----------------------
# 3) MERGE_A = MERGE NEWACT(IN=A) OLDCHG(IN=B); BY CUSTNO; IF A AND B;
# -----------------------
con.execute("""
CREATE TABLE merge_a AS
SELECT a.*, b.* EXCLUDE (CUSTNO)  -- keep oldchg columns with X suffix in parquet if present
FROM newact a
INNER JOIN oldchg b USING (CUSTNO)
""")

# At this point merge_a should have columns from newchg (like CUSTNAME) and oldchg (maybe CUSTNAMEX) depending on parquet schema.
# We'll follow SAS logic and create change tables by comparing pairs like FIELD vs FIELDX.
#
# NOTE: For robust handling: we will attempt to reference both plain column names and X-suffixed names.
# If some columns do not exist in parquet schema, duckdb will error. To avoid runtime error we will build comparisons via a small helper: create temporary views that normalize columns,
# mapping expected names to actual columns if present. For brevity here we will assume the parquet columns follow the SAS variable naming convention:
# New columns: ADDREF, CUSTNAME, LONGNAME, DOBDOR, BASICGRPCODE, CORPSTATUS, MSICCODE, CUST_CODE, CITIZENSHIP, MASCO2008, EMPLOYMENT_SECTOR, EMPLOYMENT_TYPE, EMPNAME,
# LAST_UPDATE_OPER, CUSTMNTDATE, CUSTLASTOPER, etc.
# Old columns (from oldchg) are suffixed with X: ADDREFX, CUSTNAMEX, LONGNAMEX, DOBDORX, BASICGRPCODEX, CORPSTATUSX, MSICCODEX, CUST_CODEX, CITIZENSHIPX, MASCO2008X, ...
#
# If your parquet schema differs, you may need to rename columns or update names below.

# -----------------------
# 4) Build per-change tables (C_DATE, C_OPER, C_ADDREF, C_NAME, C_LONG, C_DOB, C_BGC, C_CORP, C_MSIC, C_CCODE, C_CTZN, C_MASCO, C_EMSEC, C_EMTYP, C_EMNAME, C_PRCTRY, C_RESD)
# We'll create them via SQL UNION ALL of SELECTs where condition true.
# Each SELECT produces: CUSTNO, UPDDATE, UPDOPER, FIELDS, OLDVALUE, NEWVALUE, ACCTNOC, DATEUPD, DATEOPER, CUSTNAME, CUSTLASTOPER, CUSTMNTDATE
# -----------------------

# For readability, create a helper view that exposes both new and old names with consistent aliasing.
# If old columns don't exist (no 'X' suffix), this will set them to NULL.
con.execute("""
CREATE VIEW merged_norm AS
SELECT
  m.*,
  -- try to reference old columns with X-suffix if present; otherwise NULL.
  -- DuckDB will return NULL for columns that don't exist in the SELECT list, so we do safe aliasing by selecting using m."COL" when present.
  -- For clarity here we assume the old columns ARE present with X suffix in the parquet.
  m.ADDREF        AS ADDREF,
  m.ADDREFX       AS ADDREFX,
  m.CUSTNAME      AS CUSTNAME,
  m.CUSTNAMEX     AS CUSTNAMEX,
  m.LONGNAME      AS LONGNAME,
  m.LONGNAMEX     AS LONGNAMEX,
  m.DOBDOR        AS DOBDOR,
  m.DOBDORX       AS DOBDORX,
  m.BASICGRPCODE  AS BASICGRPCODE,
  m.BASICGRPCODEX AS BASICGRPCODEX,
  m.CORPSTATUS    AS CORPSTATUS,
  m.CORPSTATUSX   AS CORPSTATUSX,
  m.MSICCODE      AS MSICCODE,
  m.MSICCODEX     AS MSICCODEX,
  m.CUST_CODE     AS CUST_CODE,
  m.CUST_CODEX    AS CUST_CODEX,
  m.CITIZENSHIP   AS CITIZENSHIP,
  m.CITIZENSHIPX  AS CITIZENSHIPX,
  m.MASCO2008     AS MASCO2008,
  m.MASCO2008X    AS MASCO2008X,
  m.EMPLOYMENT_SECTOR AS EMPLOYMENT_SECTOR,
  m.EMPLOYMENT_SECTORX AS EMPLOYMENT_SECTORX,
  m.EMPLOYMENT_TYPE AS EMPLOYMENT_TYPE,
  m.EMPLOYMENT_TYPEX AS EMPLOYMENT_TYPEX,
  m.EMPNAME       AS EMPNAME,
  m.EMPNAMEX      AS EMPNAMEX,
  m.PRCOUNTRY     AS PRCOUNTRY,
  m.PRCOUNTRYX    AS PRCOUNTRYX,
  m.RESIDENCY     AS RESIDENCY,
  m.RESDESC       AS RESDESC,
  m.RESDESCX      AS RESDESCX,
  m.CUSTMNTDATE   AS CUSTMNTDATE,
  m.CUSTMNTDATEX  AS CUSTMNTDATEX,
  m.CUSTLASTOPER  AS CUSTLASTOPER,
  m.CUSTLASTOPERX AS CUSTLASTOPERX,
  m.LAST_UPDATE_OPER AS LAST_UPDATE_OPER,
  m.LAST_UPDATE_OPERX AS LAST_UPDATE_OPERX,
  m.EMPLOYMENT_LAST_UPDATE AS EMPLOYMENT_LAST_UPDATE,
  m.EMPLOYMENT_LAST_UPDATEX AS EMPLOYMENT_LAST_UPDATEX,
  m.BNMID, m.ACCTNOC
FROM merge_a m
""")

# Now create each C_* table.
# We'll create a big union query that produces the same structure for each change condition.
# Note: Fields lengths are enforced at final file output. Here we just produce rows.

con.execute("""
CREATE TABLE temp_changes AS
SELECT
  CUSTNO,
  CASE WHEN CUSTMNTDATE IS NULL THEN NULL ELSE CUSTMNTDATE END AS UPDDATE,
  NULL::VARCHAR AS UPDOPER,
  'DATE'       AS FIELDS,
  CUSTMNTDATEX AS OLDVALUE,
  CUSTMNTDATE  AS NEWVALUE,
  ACCTNOC,
  EMPLOYMENT_LAST_UPDATE AS DATEUPD,
  LAST_UPDATE_OPER AS DATEOPER,
  CUSTNAME,
  CUSTLASTOPER
FROM merged_norm
WHERE CUSTMNTDATE IS DISTINCT FROM CUSTMNTDATEX

UNION ALL

SELECT
  CUSTNO,
  NULL AS UPDDATE,
  CUSTLASTOPER AS UPDOPER,
  'OPER' AS FIELDS,
  CUSTLASTOPERX AS OLDVALUE,
  CUSTLASTOPER  AS NEWVALUE,
  ACCTNOC,
  NULL AS DATEUPD,
  NULL AS DATEOPER,
  CUSTNAME,
  CUSTLASTOPER
FROM merged_norm
WHERE CUSTLASTOPER IS DISTINCT FROM CUSTLASTOPERX

UNION ALL

SELECT CUSTNO, NULL, NULL, 'ADDREF', ADDREFX, ADDREF, ACCTNOC, NULL, NULL, CUSTNAME, CUSTLASTOPER
FROM merged_norm
WHERE ADDREF IS DISTINCT FROM ADDREFX

UNION ALL

SELECT CUSTNO, NULL, NULL, 'NAME', CUSTNAMEX, CUSTNAME, ACCTNOC, NULL, NULL, CUSTNAME, CUSTLASTOPER
FROM merged_norm
WHERE CUSTNAME IS DISTINCT FROM CUSTNAMEX

UNION ALL

SELECT CUSTNO, NULL, NULL, 'CUSTOMER NAME', LONGNAMEX, LONGNAME, ACCTNOC, NULL, NULL, CUSTNAME, CUSTLASTOPER
FROM merged_norm
WHERE LONGNAME IS DISTINCT FROM LONGNAMEX

UNION ALL

SELECT CUSTNO, NULL, NULL,
  CASE WHEN INDORG = 'I' THEN 'DATE OF BIRTH' ELSE 'DATE OF REGISTRATION' END AS FIELDS,
  DOBDORX, DOBDOR, ACCTNOC, NULL, NULL, CUSTNAME, CUSTLASTOPER
FROM merged_norm
WHERE DOBDOR IS DISTINCT FROM DOBDORX

UNION ALL

SELECT CUSTNO, NULL, NULL, 'ENTITY TYPE', BASICGRPCODEX, BASICGRPCODE, ACCTNOC, NULL, NULL, CUSTNAME, CUSTLASTOPER
FROM merged_norm
WHERE BASICGRPCODE IS DISTINCT FROM BASICGRPCODEX

UNION ALL

SELECT CUSTNO, NULL, NULL, 'CORPORATE STATUS', CORPSTATUSX, CORPSTATUS, ACCTNOC, NULL, NULL, CUSTNAME, CUSTLASTOPER
FROM merged_norm
WHERE CORPSTATUS IS DISTINCT FROM CORPSTATUSX

UNION ALL

SELECT CUSTNO, NULL, NULL, 'MSIC 2008', MSICCODEX, MSICCODE, ACCTNOC, NULL, NULL, CUSTNAME, CUSTLASTOPER
FROM merged_norm
WHERE MSICCODE IS DISTINCT FROM MSICCODEX

UNION ALL

SELECT CUSTNO, NULL, NULL, 'CUSTOMER CODE', CUST_CODEX, CUST_CODE, ACCTNOC, NULL, NULL, CUSTNAME, CUSTLASTOPER
FROM merged_norm
WHERE CUST_CODE IS DISTINCT FROM CUST_CODEX

UNION ALL

SELECT CUSTNO, NULL, NULL, 'NATIONALITY', CITIZENSHIPX, CITIZENSHIP, ACCTNOC, NULL, NULL, CUSTNAME, CUSTLASTOPER
FROM merged_norm
WHERE CITIZENSHIP IS DISTINCT FROM CITIZENSHIPX

UNION ALL

SELECT CUSTNO, NULL, NULL, 'MASCO OCCUPATION', MASCO2008X, MASCO2008, ACCTNOC, NULL, NULL, CUSTNAME, CUSTLASTOPER
FROM merged_norm
WHERE MASCO2008 IS DISTINCT FROM MASCO2008X

UNION ALL

SELECT CUSTNO,
  CASE WHEN EMPLOYMENT_LAST_UPDATE IS DISTINCT FROM EMPLOYMENT_LAST_UPDATEX THEN EMPLOYMENT_LAST_UPDATE ELSE NULL END AS UPDDATE,
  CASE WHEN LAST_UPDATE_OPER IS DISTINCT FROM LAST_UPDATE_OPERX THEN LAST_UPDATE_OPER ELSE NULL END AS UPDOPER,
  'EMPLOYMENT SECTOR' AS FIELDS,
  EMPLOYMENT_SECTORX, EMPLOYMENT_SECTOR,
  ACCTNOC,
  EMPLOYMENT_LAST_UPDATE AS DATEUPD,
  LAST_UPDATE_OPER AS DATEOPER,
  CUSTNAME, CUSTLASTOPER
FROM merged_norm
WHERE EMPLOYMENT_SECTOR IS DISTINCT FROM EMPLOYMENT_SECTORX

UNION ALL

SELECT CUSTNO,
  CASE WHEN EMPLOYMENT_LAST_UPDATE IS DISTINCT FROM EMPLOYMENT_LAST_UPDATEX THEN EMPLOYMENT_LAST_UPDATE ELSE NULL END AS UPDDATE,
  CASE WHEN LAST_UPDATE_OPER IS DISTINCT FROM LAST_UPDATE_OPERX THEN LAST_UPDATE_OPER ELSE NULL END AS UPDOPER,
  'EMPLOYMENT TYPE' AS FIELDS,
  EMPLOYMENT_TYPEX, EMPLOYMENT_TYPE,
  ACCTNOC,
  EMPLOYMENT_LAST_UPDATE AS DATEUPD,
  LAST_UPDATE_OPER AS DATEOPER,
  CUSTNAME, CUSTLASTOPER
FROM merged_norm
WHERE EMPLOYMENT_TYPE IS DISTINCT FROM EMPLOYMENT_TYPEX

UNION ALL

SELECT CUSTNO, NULL, NULL, 'EMPLOYER NAME', EMPNAMEX, EMPNAME, ACCTNOC, NULL, NULL, CUSTNAME, CUSTLASTOPER
FROM merged_norm
WHERE EMPNAME IS DISTINCT FROM EMPNAMEX

UNION ALL

SELECT CUSTNO, NULL, NULL, 'PR COUNTRY', PRCOUNTRYX, PRCOUNTRY, ACCTNOC, NULL, NULL, CUSTNAME, CUSTLASTOPER
FROM merged_norm
WHERE PRCOUNTRY IS DISTINCT FROM PRCOUNTRYX

UNION ALL

SELECT CUSTNO, NULL, NULL, 'RESIDENCY', RESDESCX, RESDESC, ACCTNOC, NULL, NULL, CUSTNAME, CUSTLASTOPER
FROM merged_norm
WHERE RESDESC IS DISTINCT FROM RESDESCX
;
""")

# -----------------------
# 5) TEMPALL = union of all C_* produced above -> temp_changes is that table already
# -----------------------
con.execute("CREATE TABLE tempall AS SELECT * FROM temp_changes")
con.execute("CREATE TABLE tempall_sorted AS SELECT * FROM tempall ORDER BY CUSTNO")

# -----------------------
# 6) MRGCIS = Merge C_DATE(IN=D) C_OPER(IN=E) TEMPALL(IN=C) BY CUSTNO; IF C;
#    Then set UPDDATE/UPDOPER defaults if missing; remove rows with UPDOPER in list
# -----------------------
# We'll left-join C_DATE and C_OPER -> but we do not have separate C_DATE/C_OPER tables; instead use temp_changes to extract date/opers
# For simplicity, pick per CUSTNO: the first non-null UPDDATE and UPDOPER (if present). Then combine with tempall rows (we already have).
# We will produce MRGCIS rows by joining tempall records to computed UPDDATE/UPDOPER per CUSTNO.

con.execute("""
CREATE VIEW cust_date_oper AS
SELECT
  CUSTNO,
  MAX(UPDDATE) AS DATEUPD_COMPUTED,  -- max to get a value when present
  MAX(UPDOPER) AS OPERUPD_COMPUTED
FROM tempall
GROUP BY CUSTNO
""")

con.execute("""
CREATE TABLE mrgcis AS
SELECT
  t.*,
  co.DATEUPD_COMPUTED,
  co.OPERUPD_COMPUTED
FROM tempall_sorted t
LEFT JOIN cust_date_oper co USING (CUSTNO)
""")

# Apply SAS rules:
# IF DATEUPD NOT = ' ' THEN UPDDATE = DATEUPD;
# IF OPERUPD NOT = ' ' THEN UPDOPER = OPERUPD;
# IF UPDOPER = ' ' THEN UPDOPER = CUSTLASTOPER;
# IF UPDDATE = ' ' THEN UPDDATE = CUSTMNTDATE ;
# IF UPDOPER IN ('ELNBATCH','AMLBATCH','HRCBATCH','CTRBATCH','CIFLPRCE','CISUPDEC','CIUPDMSX','CIUPDMS9','MAPLOANS','CRIS') THEN DELETE;

# We'll implement this transformation via DuckDB SQL into final_mrgcis
con.execute("""
CREATE TABLE final_mrgcis AS
SELECT
  CUSTNO,
  COALESCE(NULLIF(DATEUPD_COMPUTED, ''), NULLIF(UPDDATE, ''), CUSTMNTDATE) AS UPDDATE,  -- prefer computed, then row UPDDATE, then CUSTMNTDATE
  COALESCE(NULLIF(OPERUPD_COMPUTED, ''), NULLIF(UPDOPER, ''), NULLIF(CUSTLASTOPER, '')) AS UPDOPER,
  FIELDS,
  OLDVALUE,
  NEWVALUE,
  ACCTNOC,
  DATEUPD_COMPUTED,
  OPERUPD_COMPUTED,
  CUSTNAME,
  CUSTLASTOPER
FROM mrgcis
WHERE
  -- Exclude rows where final UPDOPER is in the exclude list
  (COALESCE(NULLIF(OPERUPD_COMPUTED, ''), NULLIF(UPDOPER, ''), NULLIF(CUSTLASTOPER, '')) NOT IN
    ('ELNBATCH','AMLBATCH','HRCBATCH','CTRBATCH','CIFLPRCE','CISUPDEC','CIUPDMSX','CIUPDMS9','MAPLOANS','CRIS')
   OR COALESCE(NULLIF(OPERUPD_COMPUTED, ''), NULLIF(UPDOPER, ''), NULLIF(CUSTLASTOPER, '')) IS NULL)
""")
