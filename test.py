import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ================================================================
# Setup DuckDB connection
# ================================================================
con = duckdb.connect()

# ================================================================
# OCCUPATION FILE
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE OCCUPATION AS
    SELECT
        DEMOCATEGORY,
        DEMOCODE    AS OCCUPNUM,
        RLENDESC    AS OCCUPDESC,
        DEMOCATEGORY || DEMOCODE AS OCCUPCD
    FROM '{host_parquet_path("BANKCTRL_DEMOCODE.parquet")}'
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
        RLENTYPE,
        RLENCODE,
        RLENDESC AS RELATIONDESC
    FROM '{host_parquet_path("BANKCTRL_RLENCODE_CA.parquet")}'
""")
#CAST(RLENCODE AS INTEGER) AS RLENCODE,

print("RLENCA FILE (first 5 rows):")
print(con.execute("SELECT * FROM RLENCA LIMIT 5").fetchdf())

# ================================================================
# INDIVIDUAL FILE
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE INDV AS
    SELECT DISTINCT
        CISNO AS CUSTNO,
        CISNO AS CUSTNOX,
        EMPLOYMENT_TYPE,
        LAST_UPDATE_DATE
    FROM read_parquet('/host/cis/parquet/CIS_IDIC_DAILY_INDVDLY/year=2025/month=9/day=11/data_0.parquet')
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
        substr(BC_FIELD_CODE,1,5) AS MASCO2008,
        MSCDESC
    FROM '{host_parquet_path("UNLOAD_CIBCCIST_FB.parquet")}'
    WHERE FIELDNAME = 'MASCO2008'
""")

con.execute(f"""
    CREATE OR REPLACE TABLE MSIC AS
    SELECT
        FIELDNAME,
        substr(BC_FIELD_CODE,1,5) AS MSICCODE,
        MSCDESC
    FROM '{host_parquet_path("UNLOAD_CIBCCIST_FB.parquet")}'
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
        BRANCHNO,
        ACCTBRABBR
    FROM '{host_parquet_path("PBBBRCH.parquet")}'
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
        FOREXRATE
    FROM '{host_parquet_path("SAP_PBB_FOREIGN_RATE.parquet")}'
    WHERE FOREXRATE IS NOT NULL
""")

# NOTE: SAS deletes first row (_N_=1). If needed, you can filter it explicitly.
#       DuckDB has no rownum concept, but we can filter later.

con.execute("CREATE OR REPLACE TABLE FOREX AS SELECT * FROM FOREX ORDER BY CURRCODE")

print("FOREX FILE (first 5 rows):")
print(con.execute("SELECT * FROM FOREX LIMIT 5").fetchdf())

# ================================================================
# FROZEN / INACTIVE ACCOUNTS FILE
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE FRZ AS
    WITH raw AS (
        SELECT
            CAST(ACCTNO AS VARCHAR) AS ACCTNO,
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
        FROM '{host_parquet_path("FROZEN_INACTIVE_ACCT.parquet")}'
        WHERE ACCTNO > '04000000000' AND ACCTNO < '04999999999'
    ),
    dated AS (
        SELECT
            ACCTNO,
            CONCAT(LPAD(CAST(LCUSTDATED AS VARCHAR), 2, '0'), 
                   LPAD(CAST(LCUSTDATEM AS VARCHAR), 2, '0'), 
                   CAST(2000 + LCUSTDATEY AS VARCHAR)) AS DATE1,
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
con.execute(f"""
    CREATE OR REPLACE TABLE DP_ALL AS
    SELECT
        LPAD(CAST(CAST(BANKNO AS BIGINT) AS VARCHAR), 3, '0') AS BANKNO,
        CAST(REPTNO AS BIGINT) AS REPTNO,
        CAST(FMTCODE AS BIGINT) AS FMTCODE,
        LPAD(CAST(CAST(ACCTBRCH1 AS BIGINT) AS VARCHAR), 3, '0') AS BRANCHNO,
        LPAD(CAST(CAST(ACCTNO AS BIGINT) AS VARCHAR), 11, '0') AS ACCTNO,
        ACCTX,
        CLSEDATE,
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
            WHEN COSTCTR > '3000' AND COSTCTR < '3999' THEN 'I'
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
        substr(CAST(OPENDATE AS VARCHAR), 4, 2) AS OPENDD,
        substr(CAST(OPENDATE AS VARCHAR), 2, 2) AS OPENMM,
        substr(CAST(OPENDATE AS VARCHAR), 6, 4) AS OPENYY,
        substr(CAST(CLSEDATE AS VARCHAR), 4, 2) AS CLSEDD,
        substr(CAST(CLSEDATE AS VARCHAR), 2, 2) AS CLSEMM,
        substr(CAST(CLSEDATE AS VARCHAR), 6, 4) AS CLSEYY,
        CONCAT(OPENMM, OPENMM, OPENYY) AS DATEOPEN,
        CONCAT(CLSEMM, CLSEDD, CLSEYY) AS DATECLSE
        
    FROM '{host_parquet_path("DPTRBLGS.parquet")}'
    WHERE REPTNO = '1001'
      AND FMTCODE IN ('1', '10', '22', '19', '20', '21')
      AND ACCTNO > '04000000000' AND ACCTNO < '04999999999'
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
    SELECT 
        a.*,
        b.FOREXRATE,
        ROUND(
            CAST((a.FOREXAMT * COALESCE(CAST(b.FOREXRATE AS DOUBLE), 0.0)) / 0.01 AS INTEGER) * 0.01,
            2
        ) AS LEDGERBAL
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
    SELECT
        BANKNO,
        REPTNO,
        FMTCODE,
        BRANCHNO,
        ACCTNO,
        ACCTX,
        CLSEDATE,
        OPENDATE,
        HOLDAMT,
        LEDGERBAL,
        ODLIMIT,
        CURRCODE,
        OPENIND,
        DORMIND,
        COSTCTR,
        POSTIND,
        BANKINDC,
        FOREXAMT,
        ACCTSTATUS,
        OPENDD,
        OPENMM,
        OPENYY,
        CLSEDD,
        CLSEMM,
        CLSEYY,
        DATEOPEN,
        DATECLSE
    FROM DPMYR

    UNION ALL

    SELECT
        BANKNO,
        REPTNO,
        FMTCODE,
        BRANCHNO,
        ACCTNO,
        ACCTX,
        CLSEDATE,
        OPENDATE,
        HOLDAMT,
        LEDGERBAL,
        ODLIMIT,
        CURRCODE,
        OPENIND,
        DORMIND,
        COSTCTR,
        POSTIND,
        BANKINDC,
        FOREXAMT,
        ACCTSTATUS,
        OPENDD,
        OPENMM,
        OPENYY,
        CLSEDD,
        CLSEMM,
        CLSEYY,
        DATEOPEN,
        DATECLSE
    FROM FOREXMRG;
""")

print("DEPOSIT (first 5 rows):")
print(con.execute("SELECT * FROM DEPOSIT LIMIT 5").fetchdf())

# Attach Frozen/Inactive (FRZ from previous step)
con.execute("""
    CREATE OR REPLACE TABLE DEPOSIT2 AS
    SELECT
        d.*,
        f.ACCTSTATUS AS FRZ_STATUS,
        f.DATECLSE
    FROM DEPOSIT d
    LEFT JOIN FRZ f
    ON d.ACCTNO = f.ACCTNO;
""")

print("DEPOSIT2 (first 5 rows):")
print(con.execute("SELECT * FROM DEPOSIT2 LIMIT 5").fetchdf())

# ================================================================
# Customer File (CIS_CUST_DAILY)
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE CUST AS
    SELECT
        *,
        CASE WHEN PRISEC = 901 THEN 'P'
             WHEN PRISEC = 902 THEN 'S'
             ELSE NULL END AS PRIMSEC,
        CASE WHEN INDORG='O' THEN 'BUSIN' || COALESCE(SICCODE)
             WHEN INDORG='I' THEN 'OCCUP' || COALESCE(OCCUP)
             ELSE NULL END AS OCCUPCD,
        CASE WHEN JOINTACC = '' OR JOINTACC IS NULL THEN 'N'
             ELSE JOINTACC END AS JOINTACC_NEW,
        CASE WHEN ACCTNO > '01590000000' AND ACCTNO < '01599999999'
             THEN 'FCYFD' ELSE 'SA' END AS ACCTCODE_NEW
    FROM read_parquet('/host/cis/parquet/CIS_CUST_DAILY/year=2025/month=9/day=29/data_0.parquet')
    WHERE ACCTCODE = 'DP'
      AND ACCTNO > '04000000000' AND ACCTNO < '04999999999'
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
    WHERE c.MSICCODE IS NULL OR c.MSICCODE = ''
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
# LOOKUP CONTROL FILES FOR DESCRIPTIONS
# ==============================================================
# Assume CISDP and lookup parquet files already exist
con.execute("""
    CREATE OR REPLACE TABLE idx1 AS
    SELECT e.*, f.*, f.OCCUPDESC
    FROM CISDP e
    LEFT JOIN OCCUPATION f
    ON e.OCCUPCD = f.OCCUPCD
""")

print("idx1 (first 5 rows):")
print(con.execute("SELECT * FROM idx1 LIMIT 5").fetchdf())

con.execute("""
    CREATE OR REPLACE TABLE idx2 AS
    SELECT g.*, h.*, h.RELATIONDESC
    FROM idx1 g
    LEFT JOIN RLENCA h
    ON g.RLENCODE = h.RLENCODE
""")

print("idx2 (first 5 rows):")
print(con.execute("SELECT * FROM idx2 LIMIT 5").fetchdf())

con.execute("""
    CREATE OR REPLACE TABLE idx3 AS
    SELECT i.*, j.*, j.ACCTBRABBR
    FROM idx2 i
    LEFT JOIN PBBBRCH j
    ON i.BRANCHNO = j.BRANCHNO
""")

print("idx3 (first 5 rows):")
print(con.execute("SELECT * FROM idx3 LIMIT 5").fetchdf())

# NOM_IDX = ACCTNO || CUSTNAME || ALIAS
con.execute("""
    CREATE OR REPLACE TABLE idx3_mod AS
    SELECT *, CAST(ACCTNO AS VARCHAR) || CUSTNAME || ALIAS AS NOM_IDX
    FROM idx3
""")

print("idx3_mod (first 5 rows):")
print(con.execute("SELECT * FROM idx3_mod LIMIT 5").fetchdf())

# ==============================================================
# Transform fields for output
# ==============================================================
tbl = con.execute("""
    SELECT 
        '033' AS BANKNO,
        CUSTNO,
        INDORG,
        CUSTNAME,
        ALIASKEY,
        ALIAS,
        SUBSTR(OCCUPCD, 6, 5) AS OCCUPCD1,
        OCCUPDESC,
        ACCTBRABBR,
        COALESCE(CAST(BRANCHNO AS VARCHAR), '0') AS BRANCHNO,
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
        COALESCE(LEDGERBAL,0.0) AS LEDGERBAL,
        COALESCE(FOREXAMT,0.0) AS FOREXAMT,
        COALESCE(JOINTACC,'N') AS JOINTACC,
        'Y' as SIGNATORY,
        0 as AMT2,
        'N' as COLLINDC,
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
    FROM idx3_mod
""").arrow()

# ==============================================================
# Write to CSV (like SAS PUT statement)
# ==============================================================
#pv.write_csv(tbl, "outfile.csv")

# Split into PBBREC (flag = 'C') and PIBBREC (flag = 'I')
pbbrec = """
    SELECT DISTINCT *,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM tbl
    WHERE BANKINDC = 'C'
""".format(year=year,month=month,day=day)

pibbrec = """
    SELECT DISTINCT *,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM tbl
    WHERE BANKINDC = 'I'
""".format(year=year,month=month,day=day)
# ======================================================
# Export with PyArrow
# ======================================================
queries = {
    "B033_SNGLVIEW_DEPOSIT_DP04"            : pbbrec,
    "B051_SNGLVIEW_DEPOSIT_DP04"            : pibbrec,
}

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    con.execute(f"""
    COPY ({query})
    TO '{parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);  
     """)
    
    con.execute(f"""
    COPY ({query})
    TO '{csv_path}'
    (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);  
     """)
