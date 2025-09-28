import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# ================================================================
# Part 1: Setup DuckDB connection
# ================================================================
con = duckdb.connect()

# Assuming all inputs are already converted to Parquet
DEMOFILE   = "BANKCTRL_DEMOCODE.parquet"
RLENCAFILE = "BANKCTRL_RLENCODE_CA.parquet"
PBBRANCH   = "PBB_BRANCH.parquet"
FOREXGIA   = "SAP_PBB_FOREIGN_RATE.parquet"
SIGNATOR   = [f"SNGLVIEW_SIGN_FD{i}.parquet" for i in range(10,20)]
DORMFILE   = "FROZEN_INACTIVE_ACCT.parquet"
DPTRBALS   = "DPTRBLGS.parquet"
CUSTFILE   = "CIS_CUST_DAILY.parquet"
INDFILE    = "CIS_IDIC_DAILY_INDV.parquet"
IDICFILE   = "UNLOAD_CIBCCIST_FB.parquet"

# ================================================================
# OCCUPATION FILE
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE OCCUPATION AS
    SELECT
        substr(DEMOCATEGORY,1,5) AS DEMOCATEGORY,
        substr(OCCUPNUM,1,4)     AS OCCUPNUM,
        substr(OCCUPDESC,1,20)   AS OCCUPDESC,
        DEMOCATEGORY || OCCUPNUM AS OCCUPCD
    FROM parquet_scan('{DEMOFILE}')
    WHERE DEMOCATEGORY IN ('OCCUP','BUSIN')
""")

print("OCCUPATION FILE (first 5 rows):")
print(con.execute("SELECT * FROM OCCUPATION LIMIT 5").fetchdf())

# ================================================================
# RELATIONSHIP CODES FILE
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE RLENCA AS
    SELECT
        substr(RLENCATEGORY,1,2) AS RLENCATEGORY,
        CAST(RLENCODE AS INTEGER) AS RLENCODE,
        substr(RELATIONDESC,1,20) AS RELATIONDESC
    FROM parquet_scan('{RLENCAFILE}')
""")

print("RLENCA FILE (first 5 rows):")
print(con.execute("SELECT * FROM RLENCA LIMIT 5").fetchdf())

# ================================================================
# INDIVIDUAL FILE
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE INDV AS
    SELECT DISTINCT
        CUSTNO,
        CUSTNO AS CUSTNOX,
        EMPLOYMENT_TYPE,
        LAST_UPDATE_DATE
    FROM parquet_scan('{INDFILE}')
    WHERE EMPLOYMENT_TYPE IS NOT NULL AND EMPLOYMENT_TYPE <> ''
""")

print("INDV FILE (first 15 rows):")
print(con.execute("SELECT * FROM INDV LIMIT 15").fetchdf())

# ================================================================
# MISC CODE FILE
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE MSCO AS
    SELECT
        FIELDNAME,
        substr(MASCO2008,1,5) AS MASCO2008,
        substr(MSCDESC,1,150) AS MSCDESC
    FROM parquet_scan('{IDICFILE}')
    WHERE FIELDNAME = 'MASCO2008'
""")

con.execute(f"""
    CREATE OR REPLACE TABLE MSIC AS
    SELECT
        FIELDNAME,
        substr(MSICCODE,1,5) AS MSICCODE,
        substr(MSCDESC,1,150) AS MSCDESC
    FROM parquet_scan('{IDICFILE}')
    WHERE FIELDNAME = 'MSIC2008'
""")

# sort like PROC SORT
con.execute("CREATE OR REPLACE TABLE MSCO AS SELECT * FROM MSCO ORDER BY MASCO2008")
con.execute("CREATE OR REPLACE TABLE MSIC AS SELECT * FROM MSIC ORDER BY MSICCODE")

# ================================================================
# BRANCH FILE
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE PBBBRCH AS
    SELECT
        CAST(BRANCHNO AS INTEGER) AS BRANCHNO,
        substr(ACCTBRABBR,1,3) AS ACCTBRABBR
    FROM parquet_scan('{PBBRANCH}')
""")

print("PBB BRANCH FILE (first 5 rows):")
print(con.execute("SELECT * FROM PBBBRCH LIMIT 5").fetchdf())

# ================================================================
# FOREX CONTROL FILE
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE FOREX AS
    SELECT
        CURRCODE,
        CAST(FOREXRATE AS DOUBLE) AS FOREXRATE
    FROM parquet_scan('{FOREXGIA}')
    WHERE FOREXRATE IS NOT NULL
""")

# NOTE: SAS deletes first row (_N_=1). If needed, you can filter it explicitly.
#       DuckDB has no rownum concept, but we can filter later.

con.execute("CREATE OR REPLACE TABLE FOREX AS SELECT * FROM FOREX ORDER BY CURRCODE")

print("FOREX FILE (first 5 rows):")
print(con.execute("SELECT * FROM FOREX LIMIT 5").fetchdf())

# ================================================================
# SIGNATORY FILE
# ================================================================
# Read all signatory parquet files (FD10–FD19)
signator_files = [f"SNGLVIEW_SIGN_FD{i}.parquet" for i in range(10,20)]

con.execute(f"""
    CREATE OR REPLACE TABLE SIGNATORY5 AS
    SELECT
        CAST(BANKNO AS INTEGER) AS BANKNO,
        CAST(ACCTNO AS BIGINT)  AS ACCTNO,
        SIGNATORY_NAME,
        ALIAS,
        SIGN_STAT,
        CONCAT(CAST(ACCTNO AS VARCHAR), SIGNATORY_NAME, ALIAS) AS NOM_IDX
    FROM read_parquet({signator_files})
    WHERE ACCTNO > 1000000000 AND ACCTNO < 1999999999
      AND COALESCE(ALIAS,'') <> ''
      AND COALESCE(SIGNATORY_NAME,'') <> ''
""")

print("SIGNATORY5 (first 5 rows):")
print(con.execute("SELECT * FROM SIGNATORY5 LIMIT 5").fetchdf())

# Deduplicate by NOM_IDX (PROC SORT NODUPKEY)
con.execute("""
    CREATE OR REPLACE TABLE SIGNATORY AS
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY NOM_IDX ORDER BY ACCTNO) AS rn
        FROM SIGNATORY5
    ) t
    WHERE rn = 1
""")

print("SIGNATORY (deduped, first 5 rows):")
print(con.execute("SELECT * FROM SIGNATORY LIMIT 5").fetchdf())


# ================================================================
# FROZEN / INACTIVE ACCOUNTS FILE
# ================================================================
DORMFILE = "FROZEN_INACTIVE_ACCT.parquet"

con.execute(f"""
    CREATE OR REPLACE TABLE FRZ AS
    WITH raw AS (
        SELECT
            CAST(ACCTNO AS BIGINT) AS ACCTNO,
            CAST(LCUSTDATEM AS INTEGER) AS LCUSTDATEM,
            CAST(LCUSTDATED AS INTEGER) AS LCUSTDATED,
            CAST(LCUSTDATEY AS INTEGER) AS LCUSTDATEY,
            CURRENCY,
            OPENINDC,
            DORM1,
            POST1,
            POSTDATE,
            POSTREASON,
            POSTINSTRUCTION
        FROM parquet_scan('{DORMFILE}')
        WHERE ACCTNO > 1000000000 AND ACCTNO < 1999999999
    ),
    dated AS (
        SELECT
            ACCTNO,
            MAKE_DATE(2000+LCUSTDATEY, LCUSTDATEM, LCUSTDATED) AS DATE1,
            POSTDATE AS DATE2,
            DORM1,
            POST1
        FROM raw
    ),
    status AS (
        SELECT
            ACCTNO,
            DATE1,
            DATE2,
            CASE
                WHEN POST1 IS NOT NULL AND POST1 <> '' THEN 'FROZEN'
                WHEN DORM1 = 'D' THEN 'DORMANT'
                WHEN DORM1 = 'N' THEN 'INACTIVE'
                ELSE 'UNKNOWN'
            END AS ACCTSTATUS,
            CASE
                WHEN POST1 IS NOT NULL AND POST1 <> '' THEN DATE2
                ELSE DATE1
            END AS DATE3
        FROM dated
    )
    SELECT
        ACCTNO,
        ACCTSTATUS,
        DATE1,
        DATE2,
        DATE3,
        DATE3 AS DATECLSE
    FROM status
    ORDER BY ACCTNO
""")

print("FRZ (first 5 rows):")
print(con.execute("SELECT * FROM FRZ LIMIT 5").fetchdf())

# ================================================================
# Deposit Trial Balance (split MYR / non-MYR)
# ================================================================
DPTRBALS = "DPTRBLGS.parquet"

con.execute(f"""
    CREATE OR REPLACE TABLE DP_ALL AS
    SELECT
        BANKNO,
        REPTNO,
        FMTCODE,
        BRANCHNO,
        ACCTNO,
        ACCTX,
        CLSEDATE,
        OPENDATE,
        HOLDAMT1/100.0 AS HOLDAMT,
        LEDGERBAL1/100.0 AS LEDGERBAL,
        ODLIMIT,
        CURRCODE,
        OPENIND,
        DORMIND,
        COSTCTR,
        POSTIND,
        CASE 
            WHEN COSTCTR > 3000 AND COSTCTR < 3999 THEN 'I'
            ELSE 'C'
        END AS BANKINDC,
        CASE 
            WHEN CURRCODE <> 'MYR' THEN LEDGERBAL1/100.0
            ELSE 0
        END AS FOREXAMT,
        CASE 
            WHEN OPENIND = ''  THEN 'ACTIVE'
            WHEN OPENIND IN ('B','C','P') THEN 'CLOSED'
            WHEN OPENIND = 'Z' THEN 'ZERO BALANCE'
            ELSE ''
        END AS ACCTSTATUS,
        substr(OPENDATE,4,2) AS OPENDD,
        substr(OPENDATE,2,2) AS OPENMM,
        substr(OPENDATE,6,4) AS OPENYY,
        substr(CLSEDATE,4,2) AS CLSEDD,
        substr(CLSEDATE,2,2) AS CLSEMM,
        substr(CLSEDATE,6,4) AS CLSEYY
    FROM parquet_scan('{DPTRBALS}')
    WHERE REPTNO = 1001
      AND FMTCODE IN (1,10,22,19,20,21)
      AND ACCTNO > 1000000000 AND ACCTNO < 1999999999
""")

# Split into MYR and non-MYR
con.execute("CREATE OR REPLACE TABLE DPMYR AS SELECT * FROM DP_ALL WHERE CURRCODE = 'MYR'")
con.execute("CREATE OR REPLACE TABLE DPOTH AS SELECT * FROM DP_ALL WHERE CURRCODE <> 'MYR'")

print("DPMYR (first 5 rows):")
print(con.execute("SELECT * FROM DPMYR LIMIT 5").fetchdf())

# ================================================================
# Merge DPOTH with FOREX
# ================================================================
con.execute("""
    CREATE OR REPLACE TABLE FOREXMRG AS
    SELECT a.*,
           b.FOREXRATE,
           ROUND(((a.FOREXAMT * b.FOREXRATE)/0.01)::INT * 0.01, 2) AS LEDGERBAL
    FROM DPOTH a
    LEFT JOIN FOREX b
    ON a.CURRCODE = b.CURRCODE
""")

print("FOREXMRG (first 5 rows):")
print(con.execute("SELECT * FROM FOREXMRG LIMIT 5").fetchdf())

# ================================================================
# Deposit master (MYR + FOREX)
# ================================================================
con.execute("""
    CREATE OR REPLACE TABLE DEPOSIT AS
    SELECT * FROM DPMYR
    UNION ALL
    SELECT * FROM FOREXMRG
""")

# Attach Frozen/Inactive (FRZ from previous step)
con.execute("""
    CREATE OR REPLACE TABLE DEPOSIT2 AS
    SELECT d.*, f.ACCTSTATUS AS FRZ_STATUS, f.DATECLSE
    FROM DEPOSIT d
    LEFT JOIN FRZ f
    ON d.ACCTNO = f.ACCTNO
""")

# ================================================================
# Customer File (CIS_CUST_DAILY)
# ================================================================
CUSTFILE = "CIS_CUST_DAILY.parquet"

con.execute(f"""
    CREATE OR REPLACE TABLE CUST AS
    SELECT
        *,
        CASE WHEN PRISEC = 901 THEN 'P'
             WHEN PRISEC = 902 THEN 'S'
             ELSE NULL END AS PRIMSEC,
        CASE WHEN INDORG='O' THEN 'BUSIN' || COALESCE(SICCODE,'')
             WHEN INDORG='I' THEN 'OCCUP' || COALESCE(OCCUP,'')
             ELSE NULL END AS OCCUPCD,
        CASE WHEN JOINTACC = '' OR JOINTACC IS NULL THEN 'N'
             ELSE JOINTACC END AS JOINTACC_NEW,
        CASE WHEN ACCTNO > 1590000000 AND ACCTNO < 1599999999
             THEN 'FCYFD' ELSE 'FD' END AS ACCTCODE_NEW
    FROM parquet_scan('{CUSTFILE}')
    WHERE ACCTCODE = 'DP'
      AND ACCTNO > 1000000000 AND ACCTNO < 1999999999
      AND NOT (CUSTNAME = '' AND ALIAS = '')
""")

print("CUST (first 5 rows):")
print(con.execute("SELECT * FROM CUST LIMIT 5").fetchdf())

# ================================================================
# Merge with MSCO / MSIC
# ================================================================
con.execute("""
    CREATE OR REPLACE TABLE CUSTMSCA AS
    SELECT c.*, m.MASCO2008, m.MSCDESC
    FROM CUST c
    LEFT JOIN MSCO m
    ON c.MASCO2008 = m.MASCO2008
    WHERE m.MSICCODE IS NULL OR m.MSICCODE = ''
""")

con.execute("""
    CREATE OR REPLACE TABLE CUSTMSCB AS
    SELECT c.*, m.MSICCODE, m.MSCDESC
    FROM CUST c
    LEFT JOIN MSIC m
    ON c.MSICCODE = m.MSICCODE
""")

con.execute("""
    CREATE OR REPLACE TABLE CUSTMSC AS
    SELECT DISTINCT * FROM (
        SELECT * FROM CUSTMSCA
        UNION ALL
        SELECT * FROM CUSTMSCB
    )
""")

# Attach INDV (from earlier)
con.execute("""
    CREATE OR REPLACE TABLE CUSTA AS
    SELECT c.*, i.EMPLOYMENT_TYPE, i.LAST_UPDATE_DATE
    FROM CUSTMSC c
    LEFT JOIN INDV i
    ON c.CUSTNO = i.CUSTNO
""")

print("CUSTA (first 5 rows):")
print(con.execute("SELECT * FROM CUSTA LIMIT 5").fetchdf())

# ================================================================
# CISDP = Merge CUSTA with DEPOSIT2
# ================================================================
con.execute("""
    CREATE OR REPLACE TABLE CISDP AS
    SELECT c.*, d.*
    FROM CUSTA c
    INNER JOIN DEPOSIT2 d
    ON c.ACCTNO = d.ACCTNO
""")

print("CISDP (first 5 rows):")
print(con.execute("SELECT * FROM CISDP LIMIT 5").fetchdf())

# ==============================================================
# 1. Open DuckDB connection
# ==============================================================
con = duckdb.connect()

# Assume CISDP and lookup parquet files already exist
con.sql("""
    CREATE OR REPLACE TABLE idx1 AS
    SELECT a.*, b.OCCUPDESC
    FROM CISDP a
    LEFT JOIN OCCUPATION b
    ON a.OCCUPCD = b.OCCUPCD
""")

con.sql("""
    CREATE OR REPLACE TABLE idx2 AS
    SELECT a.*, b.RELATIONDESC
    FROM idx1 a
    LEFT JOIN RLENCA b
    ON a.RLENCODE = b.RLENCODE
""")

con.sql("""
    CREATE OR REPLACE TABLE idx3 AS
    SELECT a.*, b.ACCTBRABBR
    FROM idx2 a
    LEFT JOIN PBBBRCH b
    ON a.BRANCHNO = b.BRANCHNO
""")

# NOM_IDX = ACCTNO || CUSTNAME || ALIAS
con.sql("""
    CREATE OR REPLACE TABLE idx3_mod AS
    SELECT *, CAST(ACCTNO AS VARCHAR) || CUSTNAME || ALIAS AS NOM_IDX
    FROM idx3
""")

con.sql("""
    CREATE OR REPLACE TABLE idx4 AS
    SELECT a.*, 
           CASE WHEN b.NOM_IDX IS NOT NULL THEN 'Y' ELSE 'N' END AS SIGNATORY
    FROM idx3_mod a
    LEFT JOIN SIGNATORY b
    ON a.NOM_IDX = b.NOM_IDX
""")

# ==============================================================
# 2. Transform fields for output
# ==============================================================
tbl = con.sql("""
    SELECT 
        '033' AS BANKNO,
        CUSTNO,
        INDORG,
        REPLACE(CUSTNAME, '\\', '\\\\') AS CUSTNAME,  -- escape backslashes
        ALIASKEY,
        ALIAS,
        SUBSTR(OCCUPCD, 6, 5) AS OCCUPCD1,
        OCCUPDESC,
        ACCTBRABBR,
        COALESCE(BRANCHNO, 0) AS BRANCHNO,
        ACCTCODE,
        ACCTNO,
        BANKINDC,
        PRIMSEC,
        RLENCODE,
        RELATIONDESC,
        ACCTSTATUS,
        DATEOPEN,
        CASE 
            WHEN ACCTSTATUS IN ('ACTIVE','ZERO BALANCE') THEN NULL 
            ELSE DATECLSE 
        END AS DATECLSE,
        SIGNATORY,
        CASE WHEN CURRCODE = '' THEN '' ELSE 'LEDGB' END AS BAL1INDC,
        CASE 
            WHEN CURRCODE='XAU' THEN 'GM '
            WHEN CURRCODE='MYR' THEN ''
            ELSE CURRCODE
        END AS CURRCODE,
        CASE 
            WHEN CURRCODE='XAU' THEN 'GIA'
            ELSE ACCTCODE
        END AS ACCTCODE_NEW,
        COALESCE(LEDGERBAL,0) AS LEDGERBAL,
        COALESCE(FOREXAMT,0) AS FOREXAMT,
        COALESCE(SIGNATORY,'N') AS SIGNATORY_FINAL,
        COALESCE(JOINTACC,'N') AS JOINTACC,
        'N' AS COLLINDC,
        -- Employment logic
        CASE 
            WHEN INDORG='O' AND MSICCODE <> '' THEN MSICCODE
            WHEN INDORG='I' AND (EMPLOYMENT_TYPE <> '' AND MASCO2008 <> '') 
                THEN EMPLOYMENT_TYPE || MASCO2008
            ELSE SUBSTR(OCCUPCD,6,5)
        END AS EMPLMASCO,
        CASE 
            WHEN INDORG='O' AND MSICCODE='' THEN OCCUPDESC
            WHEN INDORG='I' AND (EMPLOYMENT_TYPE='' OR MASCO2008='') THEN OCCUPDESC
            ELSE OCCUPDESC
        END AS MSCDESC,
        DOBDOR
    FROM idx4
""").arrow()

# ==============================================================
# 3. Write to CSV (like SAS PUT statement)
# ==============================================================
pv.write_csv(tbl, "outfile.csv")

import duckdb
import pyarrow.parquet as pq

# Load all records (assume ALLREC.parquet already exists)
con = duckdb.connect()

# Split into PBBREC (flag = 'C') and PIBBREC (flag = 'I')
pbbrec = con.execute("""
    SELECT *
    FROM allrec
    WHERE SUBSTR(record, 249, 1) = 'C'
""").arrow()

pibbrec = con.execute("""
    SELECT *
    FROM allrec
    WHERE SUBSTR(record, 249, 1) = 'I'
""").arrow()

# Save outputs
pq.write_table(pbbrec, "RBP2.B033.SNGLVIEW.DEPOSIT.DP01.parquet")
pq.write_table(pibbrec, "RBP2.B051.SNGLVIEW.DEPOSIT.DP01.parquet")
