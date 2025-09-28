import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# ================================================================
# 1. Setup DuckDB connection
# ================================================================
con = duckdb.connect()

# Assume parquet files (adjust paths to your environment)
DEMOFILE = "parquet/DEMOFILE.parquet"
RLENCA = "parquet/RLENCA.parquet"
INDFILE = "parquet/INDFILE.parquet"
IDICFILE = "parquet/IDICFILE.parquet"
PBBRANCH = "parquet/PBBRANCH.parquet"
FOREXGIA = "parquet/FOREXGIA.parquet"
SIGNATOR = [
    "parquet/SIGN_FD70.parquet", "parquet/SIGN_FD71.parquet",
    "parquet/SIGN_FD72.parquet", "parquet/SIGN_FD73.parquet",
    "parquet/SIGN_FD74.parquet", "parquet/SIGN_FD75.parquet",
    "parquet/SIGN_FD76.parquet", "parquet/SIGN_FD77.parquet",
    "parquet/SIGN_FD78.parquet", "parquet/SIGN_FD79.parquet"
]
DORMFILE = "parquet/DORMFILE.parquet"
DPTRBALS = "parquet/DPTRBALS.parquet"

# ================================================================
# 2. OCCUPATION FILE
# ================================================================
con.execute(f"""
    CREATE TABLE OCCUPATION AS
    SELECT 
        DEMOCATEGORY,
        OCCUPNUM,
        OCCUPDESC,
        DEMOCATEGORY || OCCUPNUM AS OCCUPCD
    FROM read_parquet('{DEMOFILE}')
    WHERE DEMOCATEGORY IN ('OCCUP','BUSIN')
""")

print("OCCUPATION SAMPLE:")
print(con.execute("SELECT * FROM OCCUPATION LIMIT 5").fetchdf())

# ================================================================
# 3. RELATIONSHIP CODES FILE
# ================================================================
con.execute(f"""
    CREATE TABLE RLENCA AS
    SELECT 
        RLENCATEGORY,
        RLENCODE,
        RELATIONDESC
    FROM read_parquet('{RLENCA}')
""")

print("RLENCA SAMPLE:")
print(con.execute("SELECT * FROM RLENCA LIMIT 5").fetchdf())

# ================================================================
# 4. IDIC INDIVIDUAL FILE
# ================================================================
con.execute(f"""
    CREATE TABLE INDV AS
    SELECT DISTINCT 
        CUSTNO AS CUSTNO,
        CUSTNO AS CUSTNOX,
        EMPLOYMENT_TYPE,
        LAST_UPDATE_DATE
    FROM read_parquet('{INDFILE}')
    WHERE EMPLOYMENT_TYPE <> ''
""")

print("INDV SAMPLE:")
print(con.execute("SELECT * FROM INDV LIMIT 15").fetchdf())

# ================================================================
# 5. MISC CODE FILE
# ================================================================
# Split into MSCO and MSIC depending on FIELDNAME
con.execute(f"""
    CREATE TABLE MSCO AS
    SELECT 
        FIELDNAME,
        MASCO2008,
        MSCDESC
    FROM read_parquet('{IDICFILE}')
    WHERE FIELDNAME = 'MASCO2008'
""")

con.execute(f"""
    CREATE TABLE MSIC AS
    SELECT 
        FIELDNAME,
        MSICCODE,
        MSCDESC
    FROM read_parquet('{IDICFILE}')
    WHERE FIELDNAME = 'MSIC2008'
""")

print("MSCO SAMPLE:")
print(con.execute("SELECT * FROM MSCO LIMIT 5").fetchdf())
print("MSIC SAMPLE:")
print(con.execute("SELECT * FROM MSIC LIMIT 5").fetchdf())

# ================================================================
# 6. BRANCH FILE
# ================================================================
con.execute(f"""
    CREATE TABLE PBBBRCH AS
    SELECT 
        BRANCHNO,
        ACCTBRABBR
    FROM read_parquet('{PBBRANCH}')
""")

print("PBBBRCH SAMPLE:")
print(con.execute("SELECT * FROM PBBBRCH LIMIT 5").fetchdf())

# ================================================================
# 1. FOREX CONTROL FILE
# ================================================================
con.execute(f"""
    CREATE TABLE FOREX AS
    SELECT 
        CURRCODE,
        CASE 
            WHEN row_number() OVER () = 1 THEN NULL
            WHEN FOREXRATE IS NULL THEN 0
            ELSE FOREXRATE
        END AS FOREXRATE
    FROM read_parquet('{FOREXGIA}')
""")

print("FOREX SAMPLE:")
print(con.execute("SELECT * FROM FOREX ORDER BY CURRCODE LIMIT 5").fetchdf())

# ================================================================
# 2. SIGNATORY FILE
# ================================================================
# Union all signatory datasets
union_query = " UNION ALL ".join([f"SELECT * FROM read_parquet('{f}')" for f in SIGNATOR])
con.execute(f"CREATE TABLE SIGNRAW AS {union_query}")

# Apply SAS-like filtering and computed column
con.execute("""
    CREATE TABLE SIGNATORY5 AS
    SELECT *,
           ACCTNO || SIGNATORY_NAME || ALIAS AS NOM_IDX
    FROM SIGNRAW
    WHERE ACCTNO BETWEEN 7000000000 AND 7999999999
      AND COALESCE(ALIAS,'') <> ''
      AND COALESCE(SIGNATORY_NAME,'') <> ''
""")

# Deduplicate
con.execute("""
    CREATE TABLE SIGNATORY AS
    SELECT DISTINCT * FROM SIGNATORY5
""")

print("SIGNATORY SAMPLE:")
print(con.execute("SELECT * FROM SIGNATORY LIMIT 5").fetchdf())

# ================================================================
# 3. FROZEN/INACTIVE FILE
# ================================================================
con.execute(f"""
    CREATE TABLE FRZ AS
    SELECT
        ACCTNO,
        CASE
            WHEN POST1 <> '' THEN 'FROZEN'
            WHEN DORM1 = 'D' THEN 'DORMANT'
            WHEN DORM1 = 'N' THEN 'INACTIVE'
        END AS ACCTSTATUS,
        -- Date conversions
        make_date(2000+LCUSTDATEY, LCUSTDATEM, LCUSTDATED) AS DATE1,
        POSTDATE AS DATE2,
        CASE
            WHEN POST1 <> '' THEN POSTDATE
            WHEN DORM1 IN ('D','N') THEN make_date(2000+LCUSTDATEY, LCUSTDATEM, LCUSTDATED)
        END AS DATE3,
        CASE
            WHEN POST1 <> '' THEN POSTDATE
            WHEN DORM1 IN ('D','N') THEN make_date(2000+LCUSTDATEY, LCUSTDATEM, LCUSTDATED)
        END AS DATECLSE
    FROM read_parquet('{DORMFILE}')
    WHERE ACCTNO BETWEEN 7000000000 AND 7999999999
""")

print("FRZ SAMPLE:")
print(con.execute("SELECT * FROM FRZ LIMIT 5").fetchdf())

# ================================================================
# 4. DEPOSIT TRIAL BALANCE
# ================================================================
# Only REPTNO=1001 + FMTCODE in (1,10,22,19,20,21)
con.execute(f"""
    CREATE TABLE DPTRBALS_FILTER AS
    SELECT *
    FROM read_parquet('{DPTRBALS}')
    WHERE REPTNO = 1001
      AND FMTCODE IN (1,10,22,19,20,21)
      AND ACCTNO BETWEEN 7000000000 AND 7999999999
""")

# Apply calculations
con.execute("""
    CREATE TABLE DPTRBALS_TRANS AS
    SELECT *,
        CASE WHEN COSTCTR BETWEEN 3000 AND 3999 THEN 'I' ELSE 'C' END AS BANKINDC,
        HOLDAMT1/100 AS HOLDAMT,
        LEDGERBAL1/100 AS LEDGERBAL,
        CASE WHEN CURRCODE <> 'MYR' THEN LEDGERBAL END AS FOREXAMT,
        CASE WHEN CURRCODE <> 'MYR' THEN 0 ELSE LEDGERBAL1/100 END AS LEDGERBAL_FINAL,
        -- Dates (simplified - assuming YYYYMMDD integers in source)
        CASE 
            WHEN OPENIND = '' THEN 'ACTIVE'
            WHEN OPENIND IN ('B','C','P') THEN 'CLOSED'
            WHEN OPENIND = 'Z' THEN 'ZERO BALANCE'
        END AS ACCTSTATUS,
        CASE 
            WHEN DORMIND='D' THEN 'DORMANT'
            WHEN DORMIND='N' THEN 'INACTIVE'
            WHEN POSTIND <> '' THEN 'FROZEN'
        END AS ACCTSTATUS_FINAL
    FROM DPTRBALS_FILTER
""")

# Split MYR vs non-MYR
con.execute("CREATE TABLE DPMYR AS SELECT * FROM DPTRBALS_TRANS WHERE CURRCODE='MYR'")
con.execute("CREATE TABLE DPOTH AS SELECT * FROM DPTRBALS_TRANS WHERE CURRCODE<>'MYR'")

print("DPMYR SAMPLE:")
print(con.execute("SELECT * FROM DPMYR LIMIT 5").fetchdf())

# ================================================================
# 5. FOREX MERGE (DPOTH + FOREX)
# ================================================================
con.execute("""
    CREATE TABLE FOREXMRG AS
    SELECT d.*,
           f.FOREXRATE,
           ROUND(((d.FOREXAMT * f.FOREXRATE)/0.01) * 0.01, 2) AS LEDGERBAL
    FROM DPOTH d
    LEFT JOIN FOREX f USING (CURRCODE)
""")

print("FOREXMRG SAMPLE:")
print(con.execute("SELECT * FROM FOREXMRG LIMIT 5").fetchdf())

# ================================================================
# 6. DEPOSIT FINAL MERGES
# ================================================================
# Merge DPMYR + FOREXMRG
con.execute("""
    CREATE TABLE DEPOSIT AS
    SELECT * FROM DPMYR
    UNION ALL
    SELECT * FROM FOREXMRG
""")

# Merge DEPOSIT with FRZ (left join on ACCTNO)
con.execute("""
    CREATE TABLE DEPOSIT2 AS
    SELECT d.*, f.ACCTSTATUS AS FRZ_STATUS, f.DATECLSE AS FRZ_DATECLSE
    FROM DEPOSIT d
    LEFT JOIN FRZ f USING (ACCTNO)
""")

print("DEPOSIT2 SAMPLE:")
print(con.execute("SELECT * FROM DEPOSIT2 LIMIT 5").fetchdf())

# ================================================================
# 2. Build CUST dataset (apply filters and transformations)
# ================================================================
cust = con.sql("""
    SELECT *,
           CASE WHEN PRISEC = 901 THEN 'P'
                WHEN PRISEC = 902 THEN 'S'
                ELSE PRIMSEC END AS PRIMSEC,
           CASE WHEN INDORG='O' THEN 'BUSIN' || SICCODE
                WHEN INDORG='I' THEN 'OCCUP' || OCCUP
                ELSE OCCUPCD END AS OCCUPCD,
           COALESCE(NULLIF(JOINTACC, ''), 'N') AS JOINTACC,
           'FD' AS ACCTCODE
    FROM custdly
    WHERE ACCTCODE = 'DP'
      AND ACCTNO BETWEEN 7000000000 AND 7999999999
      AND NOT (CUSTNAME = '' AND ALIAS = '')
""").arrow()

# ================================================================
# 3. Merge with MSCO (CUSTMSCA) and MSIC (CUSTMSCB)
# ================================================================
custmsca = con.sql("""
    SELECT c.*
    FROM cust c
    LEFT JOIN msco m ON c.masco2008 = m.masco2008
    WHERE m.msiccode IS NULL
""").arrow()

custmscb = con.sql("""
    SELECT c.*
    FROM cust c
    LEFT JOIN msic m ON c.msiccode = m.msiccode
    WHERE m.msiccode IS NOT NULL
""").arrow()

# Union both
custmsc = pa.concat_tables([custmsca, custmscb])

# Deduplicate by (custno, acctno)
custmsc = custmsc.drop_duplicates(['custno', 'acctno'])

# ================================================================
# 4. Merge with INDV and DEPOSIT2
# ================================================================
custa = con.sql("""
    SELECT c.*, i.*
    FROM custmsc c
    LEFT JOIN indv i ON c.custno = i.custno
""").arrow()

cisdp = con.sql("""
    SELECT a.*, d.*
    FROM custa a
    JOIN deposit2 d ON a.acctno = d.acctno
""").arrow()

# ================================================================
# 5. Lookup codes (Occupation, Relationship, Branch, Signatory)
# ================================================================
idx1 = con.sql("""
    SELECT c.*, o.occupdesc
    FROM cisdp c
    LEFT JOIN occupation o ON c.occupcd = o.occupcd
""").arrow()

idx2 = con.sql("""
    SELECT i.*, r.relationdesc
    FROM idx1 i
    LEFT JOIN rlenca r ON i.rlencode = r.rlencode
""").arrow()

idx3 = con.sql("""
    SELECT i.*, b.acctbrabbr
    FROM idx2 i
    LEFT JOIN pbbbrch b ON i.branchno = b.branchno
""").arrow()

idx4 = con.sql("""
    SELECT i.*, 
           CASE WHEN s.nom_idx IS NOT NULL THEN 'Y' ELSE 'N' END AS signatory
    FROM (
        SELECT *, acctno || custname || alias AS nom_idx FROM idx3
    ) i
    LEFT JOIN signatory s ON i.nom_idx = s.nom_idx
""").arrow()

# ================================================================
# 6. Final Output Transformations (example subset)
# ================================================================
# Convert Arrow table → Pandas for transformations not easy in SQL
df = idx4.to_pandas()

# Example: replace currency codes
df['BAL1INDC'] = df['currcode'].apply(lambda x: 'LEDGB' if x != '' else '     ')
df['CURRCODE'] = df['currcode'].replace({'XAU': 'GM ', 'MYR': ''})
df['ACCTCODE'] = df.apply(lambda r: 'GIA' if r['currcode'] == 'GM ' else r['acctcode'], axis=1)

# Example: joint account default
df['JOINTACC'] = df['jointacc'].replace('', 'N')

# ================================================================
# 7. Save Output (CSV or Parquet)
# ================================================================
final_table = pa.Table.from_pandas(df)

# Save as CSV (like SAS DATA _NULL_ with PUT)
pc.write_csv(final_table, "output_detail.csv")

# Or save as parquet
pq.write_table(final_table, "output_detail.parquet")

# ============
# JCL
# ============
import duckdb
import pyarrow.parquet as pq

# ================================================================
# 1. Connect to DuckDB
# ================================================================
con = duckdb.connect()

# Assume ALLREC is already in parquet
con.sql("CREATE VIEW allrec AS SELECT * FROM 'ALLREC.parquet'")

# ================================================================
# 2. Split into two outputs based on column 249
#    - SAS position-based filtering must be mapped to a column.
#    - If original file was fixed-length 1000 chars, load as text.
# ================================================================
# Option 1: If ALLREC has one column 'record' (string of length 1000)
pbbrec = con.sql("""
    SELECT * FROM allrec
    WHERE substr(record, 249, 1) = 'C'
""").arrow()

pibbrecord = con.sql("""
    SELECT * FROM allrec
    WHERE substr(record, 249, 1) = 'I'
""").arrow()

# ================================================================
# 3. Save to Parquet (like DD PBBREC & PIBBREC)
# ================================================================
pq.write_table(pbbrec, "SNGLVIEW.DEPOSIT.DP07.parquet")
pq.write_table(pibbrecord, "RBP2.B051.SNGLVIEW.DEPOSIT.DP07.parquet")
