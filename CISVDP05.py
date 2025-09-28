import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# Connect DuckDB (in-memory)
con = duckdb.connect()

# ================================================================
# OCCUPATION FILE
# ================================================================
occupation = con.execute("""
    SELECT 
        substr(demofile.DEMOCATEGORY, 1, 5) AS DEMOCATEGORY,
        substr(demofile.OCCUPNUM, 1, 4) AS OCCUPNUM,
        substr(demofile.OCCUPDESC, 1, 20) AS OCCUPDESC,
        demofile.DEMOCATEGORY || demofile.OCCUPNUM AS OCCUPCD
    FROM read_parquet('DEMOFILE.parquet') AS demofile
    WHERE DEMOCATEGORY IN ('OCCUP','BUSIN')
""").arrow()

print("OCCUPATION FILE (first 5 rows):")
print(occupation.to_pandas().head())

pq.write_table(occupation, "OCCUPATION.parquet")


# ================================================================
# RELATIONSHIP CODES FILE
# ================================================================
rlenca = con.execute("""
    SELECT 
        substr(r.RLENCATEGORY,1,2) AS RLENCATEGORY,
        CAST(substr(r.RLENCODE,1,3) AS INTEGER) AS RLENCODE,
        substr(r.RELATIONDESC,1,20) AS RELATIONDESC
    FROM read_parquet('RLENCA.parquet') AS r
""").arrow()

print("CA RELATIONSHIP FILE (first 5 rows):")
print(rlenca.to_pandas().head())

pq.write_table(rlenca, "RLENCA.parquet")


# ================================================================
# IDIC INDIVIDUAL FILE
# ================================================================
indv = con.execute("""
    SELECT DISTINCT
        i.CUSTNO AS CUSTNO,
        i.CUSTNO AS CUSTNOX,
        i.EMPLOYMENT_TYPE,
        i.LAST_UPDATE_DATE
    FROM read_parquet('INDFILE.parquet') AS i
    WHERE i.EMPLOYMENT_TYPE <> ''
""").arrow()

print("INDV IDIC (first 15 rows):")
print(indv.to_pandas().head(15))

pq.write_table(indv, "INDV.parquet")


# ================================================================
# MISC CODE FILE (split into MSCO & MSIC)
# ================================================================
msco = con.execute("""
    SELECT 
        FIELDNAME,
        substr(MASCO2008,1,5) AS MASCO2008,
        substr(MSCDESC,1,150) AS MSCDESC
    FROM read_parquet('IDICFILE.parquet')
    WHERE FIELDNAME = 'MASCO2008'
    ORDER BY MASCO2008
""").arrow()

pq.write_table(msco, "MSCO.parquet")

msic = con.execute("""
    SELECT 
        FIELDNAME,
        substr(MSICCODE,1,5) AS MSICCODE,
        substr(MSCDESC,1,150) AS MSCDESC
    FROM read_parquet('IDICFILE.parquet')
    WHERE FIELDNAME = 'MSIC2008'
    ORDER BY MSICCODE
""").arrow()

pq.write_table(msic, "MSIC.parquet")


# ================================================================
# BRANCH FILE PBBRANCH
# ================================================================
pbbbrch = con.execute("""
    SELECT 
        CAST(substr(p.BRANCHNO,1,3) AS INTEGER) AS BRANCHNO,
        substr(p.ACCTBRABBR,1,3) AS ACCTBRABBR
    FROM read_parquet('PBBRANCH.parquet') AS p
""").arrow()

print("PBB BRANCH FILE (first 5 rows):")
print(pbbbrch.to_pandas().head())

pq.write_table(pbbbrch, "PBBBRCH.parquet")


# ================================================================
# FOREX CONTROL FILE
# ================================================================
forex = con.execute("""
    SELECT 
        substr(f.CURRCODE,1,3) AS CURRCODE,
        TRY_CAST(f.FOREXRATE AS DOUBLE) AS FOREXRATE
    FROM read_parquet('FOREXGIA.parquet') f
    WHERE row_number() OVER () > 1  -- skip first row (_N_=1)
""").arrow()

pq.write_table(forex, "FOREX.parquet")
print("FOREX (first 5 rows):")
print(forex.to_pandas().head())


# ================================================================
# FROZEN & INACTIVE DATES
# ================================================================
frz = con.execute("""
    WITH base AS (
        SELECT 
            CAST(substr(d.ACCTNO,1,11) AS BIGINT) AS ACCTNO,
            CAST(substr(d.LCUSTDATEM,1,2) AS INTEGER) AS LCUSTDATEM,
            CAST(substr(d.LCUSTDATED,1,2) AS INTEGER) AS LCUSTDATED,
            CAST(substr(d.LCUSTDATEY,1,2) AS INTEGER) AS LCUSTDATEY,
            substr(d.CURRENCY,1,3) AS CURRENCY,
            substr(d.OPENINDC,1,1) AS OPENINDC,
            substr(d.DORM1,1,1) AS DORM1,
            substr(d.POST1,1,1) AS POST1,
            try_cast(d.POSTDATE as DATE) AS POSTDATE
        FROM read_parquet('DORMFILE.parquet') d
        WHERE CAST(substr(d.ACCTNO,1,11) AS BIGINT) BETWEEN 5000000000 AND 5999999999
    )
    SELECT *,
           MAKE_DATE(2000+LCUSTDATEY, LCUSTDATEM, LCUSTDATED) AS DATE1,
           POSTDATE AS DATE2,
           CASE 
               WHEN POST1 <> '' THEN 'FROZEN'
               WHEN DORM1 = 'D' THEN 'DORMANT'
               WHEN DORM1 = 'N' THEN 'INACTIVE'
               ELSE NULL
           END AS ACCTSTATUS,
           CASE 
               WHEN POST1 <> '' THEN POSTDATE
               WHEN DORM1 IN ('D','N') THEN MAKE_DATE(2000+LCUSTDATEY, LCUSTDATEM, LCUSTDATED)
               ELSE NULL
           END AS DATE3,
           CASE 
               WHEN POST1 <> '' THEN POSTDATE
               WHEN DORM1 IN ('D','N') THEN MAKE_DATE(2000+LCUSTDATEY, LCUSTDATEM, LCUSTDATED)
               ELSE NULL
           END AS DATECLSE
    FROM base
""").arrow()

pq.write_table(frz, "FRZ.parquet")
print("FRZ (first 5 rows):")
print(frz.to_pandas().head())


# ================================================================
# DEPOSIT TRIAL BALANCE
# ================================================================
dptrbals = con.execute("""
    WITH raw AS (
        SELECT *
        FROM read_parquet('DPTRBALS.parquet')
    ),
    filtered AS (
        SELECT
            CAST(BANKNO AS INTEGER) AS BANKNO,
            CAST(REPTNO AS INTEGER) AS REPTNO,
            CAST(FMTCODE AS INTEGER) AS FMTCODE,
            CAST(BRANCHNO AS INTEGER) AS BRANCHNO,
            CAST(ACCTNO AS BIGINT) AS ACCTNO,
            substr(ACCTX,1,1) AS ACCTX,
            CAST(CLSEDATE AS BIGINT) AS CLSEDATE,
            CAST(OPENDATE AS BIGINT) AS OPENDATE,
            CAST(HOLDAMT1 AS DOUBLE)/100.0 AS HOLDAMT,
            CAST(LEDGERBAL1 AS DOUBLE)/100.0 AS LEDGERBAL,
            substr(BALHOLD,1,1) AS BALHOLD,
            CAST(ODLIMIT AS DOUBLE) AS ODLIMIT,
            substr(CURRCODE,1,3) AS CURRCODE,
            substr(OPENIND,1,1) AS OPENIND,
            substr(DORMIND,1,1) AS DORMIND,
            CAST(COSTCTR AS INTEGER) AS COSTCTR,
            substr(POSTIND,1,1) AS POSTIND
        FROM raw
        WHERE REPTNO = 1001 AND FMTCODE IN (1,10,22,19,20,21)
          AND ACCTNO BETWEEN 5000000000 AND 5999999999
    )
    SELECT *,
           CASE WHEN COSTCTR BETWEEN 3000 AND 3999 THEN 'I' ELSE 'C' END AS BANKINDC,
           CASE WHEN CURRCODE <> 'MYR' THEN LEDGERBAL ELSE 0 END AS FOREXAMT,
           -- DATE parsing simplified, assuming OPENDATE/CLSEDATE in YYYYMMDD
           TRY_CAST(CAST(OPENDATE AS VARCHAR) AS DATE) AS DATEOPEN,
           TRY_CAST(CAST(CLSEDATE AS VARCHAR) AS DATE) AS DATECLSE,
           CASE 
              WHEN OPENIND = '' THEN 'ACTIVE'
              WHEN OPENIND IN ('B','C','P') THEN 'CLOSED'
              WHEN OPENIND = 'Z' THEN 'ZERO BALANCE'
              ELSE NULL
           END AS ACCTSTATUS,
           CASE 
              WHEN DORMIND = 'D' THEN 'DORMANT'
              WHEN DORMIND = 'N' THEN 'INACTIVE'
              ELSE NULL
           END AS DORMSTATUS,
           CASE WHEN POSTIND <> '' THEN 'FROZEN' ELSE NULL END AS POSTSTATUS
    FROM filtered
""").arrow()

# Split DPMYR vs DPOTH
dpmry = con.execute("SELECT * FROM dptrbals WHERE CURRCODE = 'MYR'").arrow()
dpoth = con.execute("SELECT * FROM dptrbals WHERE CURRCODE <> 'MYR'").arrow()

pq.write_table(dpmry, "DPMYR.parquet")
pq.write_table(dpoth, "DPOTH.parquet")
print("DPMYR (first 5 rows):")
print(dpmry.to_pandas().head())


# ================================================================
# MERGE WITH FOREX
# ================================================================
forexmrg = con.execute("""
    SELECT 
        d.*,
        f.FOREXRATE,
        ROUND((d.FOREXAMT * f.FOREXRATE), 2) AS LEDGERBAL
    FROM read_parquet('DPOTH.parquet') d
    LEFT JOIN read_parquet('FOREX.parquet') f
    USING (CURRCODE)
""").arrow()

pq.write_table(forexmrg, "FOREXMRG.parquet")
print("FOREXMRG (first 5 rows):")
print(forexmrg.to_pandas().head())


# ================================================================
# DEPOSIT + MERGE FRZ
# ================================================================
deposit = con.execute("""
    SELECT * FROM read_parquet('DPMYR.parquet')
    UNION ALL
    SELECT * FROM read_parquet('FOREXMRG.parquet')
""").arrow()
pq.write_table(deposit, "DEPOSIT.parquet")

deposit2 = con.execute("""
    SELECT d.*, f.ACCTSTATUS AS FRZ_STATUS, f.DATECLSE AS FRZ_DATECLSE
    FROM read_parquet('DEPOSIT.parquet') d
    LEFT JOIN read_parquet('FRZ.parquet') f
    USING (ACCTNO)
""").arrow()

pq.write_table(deposit2, "DEPOSIT2.parquet")
print("DEPOSIT2 (first 5 rows):")
print(deposit2.to_pandas().head())

# ==========================================================
# 2. Process CUST
# ==========================================================
con = duckdb.connect()

con.register("custdly", cust)
con.register("msco", msco)
con.register("msic", msic)
con.register("indv", indv)
con.register("deposit2", deposit2)
con.register("occupation", occupation)
con.register("rlenca", rlenca)
con.register("pbbbrch", pbbbrch)

# Build cust with filters & transformations
con.execute("""
    CREATE OR REPLACE TABLE cust AS
    SELECT *,
           CASE WHEN PRISEC=901 THEN 'P'
                WHEN PRISEC=902 THEN 'S' END AS PRIMSEC,
           CASE WHEN INDORG='O' THEN 'BUSIN' || SICCODE
                WHEN INDORG='I' THEN 'OCCUP' || OCCUP END AS OCCUPCD,
           COALESCE(NULLIF(JOINTACC,''),'N') AS JOINTACC,
           'SA' AS ACCTCODE
    FROM custdly
    WHERE ACCTCODE='DP'
      AND ACCTNO BETWEEN 5000000000 AND 5999999999
      AND NOT (CUSTNAME='' AND ALIAS='')
""")

# ==========================================================
# 3. Merge chain
# ==========================================================
con.execute("""
    CREATE OR REPLACE TABLE custmsca AS
    SELECT c.*
    FROM cust c
    LEFT JOIN msco m ON c.masco2008=m.masco2008
    WHERE m.msiccode IS NULL
""")

con.execute("""
    CREATE OR REPLACE TABLE custmscb AS
    SELECT c.*
    FROM cust c
    LEFT JOIN msic m ON c.msiccode=m.msiccode
""")

con.execute("""
    CREATE OR REPLACE TABLE custmsc AS
    SELECT * FROM custmsca
    UNION
    SELECT * FROM custmscb
""")

# Deduplicate by custno+acctno
con.execute("""
    CREATE OR REPLACE TABLE custmsc_dedup AS
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY custno,acctno ORDER BY custno) rn
        FROM custmsc
    ) WHERE rn=1
""")

con.execute("""
    CREATE OR REPLACE TABLE custa AS
    SELECT c.* 
    FROM custmsc_dedup c
    LEFT JOIN indv i ON c.custno=i.custno
""")

con.execute("""
    CREATE OR REPLACE TABLE cisdp AS
    SELECT a.* 
    FROM custa a
    INNER JOIN deposit2 b ON a.acctno=b.acctno
""")

# ==========================================================
# 4. Lookup joins
# ==========================================================
con.execute("""
    CREATE OR REPLACE TABLE idx1 AS
    SELECT c.*, o.occupdesc
    FROM cisdp c
    LEFT JOIN occupation o ON c.occupcd=o.occupcd
""")

con.execute("""
    CREATE OR REPLACE TABLE idx2 AS
    SELECT c.*, r.relationdesc
    FROM idx1 c
    LEFT JOIN rlenca r ON c.rlencode=r.rlencode
""")

con.execute("""
    CREATE OR REPLACE TABLE idx3 AS
    SELECT c.*, p.branchabbr AS acctbrabbr
    FROM idx2 c
    LEFT JOIN pbbbrch p ON c.branchno=p.branchno
""")

# ================
# JCL
# ================
# ==========================================================
# 2. Split by column 249 (SAS is 1-based indexing!)
#    → In DuckDB, use SUBSTR(string, start, length) with 1-based index.
# ==========================================================
# If col249 is in a fixed-width text field:
con.execute("""
    CREATE OR REPLACE TABLE pbbrec AS
    SELECT * FROM allrec
    WHERE SUBSTR(record, 249, 1) = 'C'
""")

con.execute("""
    CREATE OR REPLACE TABLE pibbrec AS
    SELECT * FROM allrec
    WHERE SUBSTR(record, 249, 1) = 'I'
""")

# ==========================================================
# 3. Export to Parquet using PyArrow
# ==========================================================
pbb_arrow = con.execute("SELECT * FROM pbbrec").arrow()
pib_arrow = con.execute("SELECT * FROM pibbrec").arrow()

pq.write_table(pbb_arrow, "SNGLVIEW_DEPOSIT_DP05.parquet")      # Conventional
pq.write_table(pib_arrow, "RBP2_B051_SNGLVIEW_DEPOSIT_DP05.parquet")  # Islamic

# ================
# JCL Part 2
# ================
# ==========================================================
# 2. Add a row_number() to split evenly
#    (SPLIT in DFSORT distributes records round-robin)
# ==========================================================
# Use DuckDB's row_number() over() to number rows
deposit_arrow = con.execute("""
    SELECT *, 
           row_number() OVER () AS rn
    FROM deposit
""").arrow()

# Convert to Pandas for easier partitioning
df = deposit_arrow.to_pandas()

# ==========================================================
# 3. Split round-robin into 10 parts
# ==========================================================
num_parts = 10
for i in range(num_parts):
    part_df = df[df['rn'] % num_parts == i+1]   # rn mod 10 = 1..10
    table = pa.Table.from_pandas(part_df.drop(columns=['rn']))
    pq.write_table(table, f"SNGLVIEW_DEPOSIT_DP05{i+1:02}.parquet")
