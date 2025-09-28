import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# ================================================================
# Setup: open a DuckDB connection
# ================================================================
con = duckdb.connect()

# Assume all DSNs already converted into parquet files and available in local paths
DEMOFILE = "BANKCTRL_DEMOCODE.parquet"
RLENCA = "BANKCTRL_RLENCODE_CA.parquet"
PBBRANCH = "PBB_BRANCH.parquet"
FOREXGIA = "SAP_PBB_FOREIGN_RATE.parquet"
DORMFILE = "FROZEN_INACTIVE_ACCT.parquet"
DPTRBALS = "DPTRBLGS.parquet"
CUSTFILE = "CIS_CUST_DAILY.parquet"
INDFILE = "CIS_IDIC_DAILY_INDV.parquet"
IDICFILE = "UNLOAD_CIBCCIST_FB.parquet"

# ================================================================
# OCCUPATION FILE
# ================================================================
occupation_query = f"""
    SELECT 
        substr(d.DEMOCATEGORY, 1, 5) as DEMOCATEGORY,
        substr(d.OCCUPNUM, 1, 4) as OCCUPNUM,
        substr(d.OCCUPDESC, 1, 20) as OCCUPDESC,
        concat(d.DEMOCATEGORY, d.OCCUPNUM) as OCCUPCD
    FROM read_parquet('{DEMOFILE}') d
    WHERE DEMOCATEGORY IN ('OCCUP','BUSIN')
"""
occupation_tbl = con.execute(occupation_query).arrow()
print("OCCUPATION FILE SAMPLE:")
print(occupation_tbl.to_pandas().head())

# ================================================================
# RELATIONSHIP CODES FILE
# ================================================================
relenca_query = f"""
    SELECT 
        substr(r.RLENCATEGORY, 1, 2) as RLENCATEGORY,
        cast(substr(r.RLENCODE, 1, 3) as INT) as RLENCODE,
        substr(r.RELATIONDESC, 1, 20) as RELATIONDESC
    FROM read_parquet('{RLENCA}') r
"""
relenca_tbl = con.execute(relenca_query).arrow()
print("RELATIONSHIP CODES FILE SAMPLE:")
print(relenca_tbl.to_pandas().head())

# ================================================================
# IDIC INDIVIDUAL FILE
# ================================================================
indv_query = f"""
    SELECT DISTINCT
        CUSTNO,
        CUSTNO as CUSTNOX,
        EMPLOYMENT_TYPE,
        LAST_UPDATE_DATE
    FROM read_parquet('{INDFILE}')
    WHERE EMPLOYMENT_TYPE IS NOT NULL AND EMPLOYMENT_TYPE <> ''
"""
indv_tbl = con.execute(indv_query).arrow()
print("INDV IDIC SAMPLE:")
print(indv_tbl.to_pandas().head(15))

# ================================================================
# MISC CODE FILE (split into MSCO and MSIC)
# ================================================================
msco_query = f"""
    SELECT FIELDNAME, MASCO2008, MSCDESC
    FROM read_parquet('{IDICFILE}')
    WHERE FIELDNAME = 'MASCO2008'
"""
msic_query = f"""
    SELECT FIELDNAME, MSICCODE, MSCDESC
    FROM read_parquet('{IDICFILE}')
    WHERE FIELDNAME = 'MSIC2008'
"""
msco_tbl = con.execute(msco_query).arrow()
msic_tbl = con.execute(msic_query).arrow()

# ================================================================
# BRANCH FILE PBBRANCH
# ================================================================
pbbranch_query = f"""
    SELECT 
        cast(substr(BRANCHNO, 1, 3) as INT) as BRANCHNO,
        substr(ACCTBRABBR, 1, 3) as ACCTBRABBR
    FROM read_parquet('{PBBRANCH}')
"""
pbbranch_tbl = con.execute(pbbranch_query).arrow()
print("PBB BRANCH SAMPLE:")
print(pbbranch_tbl.to_pandas().head())

# ================================================================
# FOREX CONTROL FILE
# ================================================================
forex_query = f"""
    SELECT 
        substr(CURRCODE, 1, 3) as CURRCODE,
        cast(FOREXRATE as DOUBLE) as FOREXRATE
    FROM read_parquet('{FOREXGIA}')
"""
forex_tbl = con.execute(forex_query).arrow()
print("FOREX SAMPLE:")
print(forex_tbl.to_pandas().head())

# ================================================================
# FROZEN AND INACTIVE DATES (FRZ)
# ================================================================
frz_query = f"""
    WITH base AS (
        SELECT 
            ACCTNO,
            LCUSTDATEM, LCUSTDATED, LCUSTDATEY,
            CURRENCY,
            OPENINDC,
            DORM1,
            POST1,
            POSTDATE,
            POSTREASON,
            POSTINSTRUCTION
        FROM read_parquet('FROZEN_INACTIVE_ACCT.parquet')
        WHERE ACCTNO > 4000000000 AND ACCTNO < 4999999999
    ),
    dated AS (
        SELECT *,
            make_date(2000 + LCUSTDATEY, LCUSTDATEM, LCUSTDATED) as DATE1,
            POSTDATE as DATE2
        FROM base
    ),
    status AS (
        SELECT *,
            CASE
                WHEN POST1 <> '' THEN 'FROZEN'
                WHEN DORM1 = 'D' THEN 'DORMANT'
                WHEN DORM1 = 'N' THEN 'INACTIVE'
                ELSE NULL
            END as ACCTSTATUS,
            CASE
                WHEN POST1 <> '' THEN DATE2
                WHEN DORM1 IN ('D','N') THEN DATE1
                ELSE NULL
            END as DATE3
        FROM dated
    )
    SELECT 
        ACCTNO, 
        ACCTSTATUS,
        DATE1, DATE2, DATE3,
        DATE3 as DATECLSE
    FROM status
    ORDER BY ACCTNO
"""
frz_tbl = con.execute(frz_query).arrow()
print("FRZ SAMPLE:")
print(frz_tbl.to_pandas().head())


# ================================================================
# DEPOSIT TRIAL BALANCE (split into DPMYR and DPOTH)
# ================================================================
dp_query = f"""
    WITH base AS (
        SELECT *
        FROM read_parquet('DPTRBLGS.parquet')
        WHERE REPTNO = 1001
          AND FMTCODE IN (1,10,22,19,20,21)
          AND ACCTNO > 4000000000 AND ACCTNO < 4999999999
    ),
    derived AS (
        SELECT *,
            CASE WHEN COSTCTR > 3000 AND COSTCTR < 3999 THEN 'I' ELSE 'C' END as BANKINDC,
            HOLDAMT1 / 100.0 as HOLDAMT,
            LEDGERBAL1 / 100.0 as LEDGERBAL,
            CASE WHEN CURRCODE <> 'MYR' THEN LEDGERBAL1 / 100.0 ELSE 0 END as FOREXAMT,
            CASE WHEN CURRCODE <> 'MYR' THEN 0 ELSE LEDGERBAL1 / 100.0 END as LEDGERBAL_FINAL,

            -- derive DATEOPEN / DATECLSE from numeric yyyymmdd
            TRY_CAST(strftime('%Y-%m-%d', OPENDATE) as DATE) as DATEOPEN,
            TRY_CAST(strftime('%Y-%m-%d', CLSEDATE) as DATE) as DATECLSE
        FROM base
    ),
    acct_status AS (
        SELECT *,
            CASE
                WHEN OPENIND = ''  THEN 'ACTIVE'
                WHEN OPENIND IN ('B','C','P') THEN 'CLOSED'
                WHEN OPENIND = 'Z' THEN 'ZERO BALANCE'
                ELSE NULL
            END as ACCTSTATUS0
        FROM derived
    ),
    dorm_status AS (
        SELECT *,
            CASE 
                WHEN DORMIND = 'D' THEN 'DORMANT'
                WHEN DORMIND = 'N' THEN 'INACTIVE'
                ELSE ACCTSTATUS0
            END as ACCTSTATUS1
        FROM acct_status
    ),
    frozen_status AS (
        SELECT *,
            CASE 
                WHEN POSTIND <> '' THEN 'FROZEN'
                ELSE ACCTSTATUS1
            END as ACCTSTATUS2
        FROM dorm_status
    ),
    hold_status AS (
        SELECT *,
            CASE 
                WHEN HOLDAMT > 0 THEN 'HOLD/EARMARK'
                ELSE ACCTSTATUS2
            END as ACCTSTATUS,
            CASE 
                WHEN HOLDAMT > 0 THEN HOLDAMT
                ELSE NULL
            END as AMT1
        FROM frozen_status
    )
    SELECT *
    FROM hold_status
"""
dp_tbl = con.execute(dp_query).arrow()

# Split into MYR vs OTH
dpmyr_tbl = con.execute("SELECT * FROM dp_tbl WHERE CURRCODE = 'MYR'").arrow()
dpoth_tbl = con.execute("SELECT * FROM dp_tbl WHERE CURRCODE <> 'MYR'").arrow()

print("DPMYR SAMPLE:")
print(dpmyr_tbl.to_pandas().head())
print("DPOTH SAMPLE:")
print(dpoth_tbl.to_pandas().head())


# ================================================================
# FOREX MERGE (DPOTH + FOREX)
# ================================================================
forexmrg_query = f"""
    SELECT 
        d.*,
        f.FOREXRATE,
        ROUND(CAST(((d.FOREXAMT * f.FOREXRATE)/.01) * .01 AS DOUBLE), 2) as LEDGERBAL
    FROM dpoth_tbl d
    LEFT JOIN forex_tbl f
    ON d.CURRCODE = f.CURRCODE
"""
forexmrg_tbl = con.execute(forexmrg_query).arrow()
print("FOREXMRG SAMPLE:")
print(forexmrg_tbl.to_pandas().head())


# ================================================================
# DEPOSIT & DEPOSIT2 (merge with FRZ)
# ================================================================
deposit_tbl = pa.concat_tables([dpmyr_tbl, forexmrg_tbl])

deposit2_query = """
    SELECT 
        d.*,
        f.ACCTSTATUS as FRZ_ACCTSTATUS,
        f.DATECLSE as FRZ_DATECLSE
    FROM deposit_tbl d
    LEFT JOIN frz_tbl f
    ON d.ACCTNO = f.ACCTNO
"""
deposit2_tbl = con.execute(deposit2_query).arrow()
print("DEPOSIT2 SAMPLE:")
print(deposit2_tbl.to_pandas().head())

# ================================================================
# STEP 1: Load and clean CUST
# ================================================================
cust_query = """
    WITH base AS (
        SELECT *
        FROM read_parquet('CUSTFILE_CUSTDLY.parquet')
        WHERE ACCTCODE = 'DP'
          AND ACCTNO > 4000000000 AND ACCTNO < 4999999999
          AND NOT (CUSTNAME = '' AND ALIAS = '')
    ),
    derived AS (
        SELECT *,
            CASE WHEN PRISEC = 901 THEN 'P'
                 WHEN PRISEC = 902 THEN 'S'
                 ELSE NULL END as PRIMSEC,
            CASE WHEN INDORG='O' THEN 'BUSIN' || SICCODE
                 WHEN INDORG='I' THEN 'OCCUP' || OCCUP
                 ELSE NULL END as OCCUPCD,
            COALESCE(NULLIF(JOINTACC,''),'N') as JOINTACC_NEW,
            'SA' as ACCTCODE_NEW
        FROM base
    )
    SELECT *, JOINTACC_NEW as JOINTACC, ACCTCODE_NEW as ACCTCODE
    FROM derived
    ORDER BY MASCO2008
"""
cust_tbl = con.execute(cust_query).arrow()


# ================================================================
# STEP 2: Merge with MSCO & MSIC
# ================================================================
custmsca_query = """
    SELECT c.*
    FROM cust_tbl c
    LEFT JOIN read_parquet('MSCO.parquet') m
    ON c.MASCO2008 = m.MASCO2008
    WHERE m.MSICCODE IS NULL OR m.MSICCODE = ''
"""
custmsca_tbl = con.execute(custmsca_query).arrow()

custmscb_query = """
    SELECT c.*
    FROM cust_tbl c
    LEFT JOIN read_parquet('MSIC.parquet') m
    ON c.MSICCODE = m.MSICCODE
"""
custmscb_tbl = con.execute(custmscb_query).arrow()

custmsc_tbl = pa.concat_tables([custmsca_tbl, custmscb_tbl])
custmsc_tbl = con.execute("SELECT DISTINCT * FROM custmsc_tbl").arrow()


# ================================================================
# STEP 3: Merge with INDV
# ================================================================
custa_query = """
    SELECT c.*, i.*
    FROM custmsc_tbl c
    LEFT JOIN read_parquet('INDV.parquet') i
    ON c.CUSTNO = i.CUSTNO
"""
custa_tbl = con.execute(custa_query).arrow()


# ================================================================
# STEP 4: Merge with DEPOSIT2
# ================================================================
cisdp_query = """
    SELECT c.*, d.*
    FROM custa_tbl c
    JOIN deposit2_tbl d
    ON c.ACCTNO = d.ACCTNO
"""
cisdp_tbl = con.execute(cisdp_query).arrow()


# ================================================================
# STEP 5: Lookup joins (Occupation, Relationship, Branch)
# ================================================================
idx1_query = """
    SELECT c.*, o.OCCUPDESC
    FROM cisdp_tbl c
    LEFT JOIN read_parquet('OCCUPATION.parquet') o
    ON c.OCCUPCD = o.OCCUPCD
"""
idx1_tbl = con.execute(idx1_query).arrow()

idx2_query = """
    SELECT c.*, r.RELATIONDESC
    FROM idx1_tbl c
    LEFT JOIN read_parquet('RLENCA.parquet') r
    ON c.RLENCODE = r.RLENCODE
"""
idx2_tbl = con.execute(idx2_query).arrow()

idx3_query = """
    SELECT c.*, b.ACCTBRABBR
    FROM idx2_tbl c
    LEFT JOIN read_parquet('PBBBRCH.parquet') b
    ON c.BRANCHNO = b.BRANCHNO
"""
idx3_tbl = con.execute(idx3_query).arrow()


# ================================================================
# STEP 6: Final transformations before output
# ================================================================
final_query = """
    SELECT *,
        CASE WHEN CURRCODE = '' THEN '     ' ELSE 'LEDGB' END as BAL1INDC,
        CASE WHEN CURRCODE = 'XAU' THEN 'GM ' ELSE CURRCODE END as CURRCODE_NEW,
        CASE WHEN CURRCODE = 'XAU' THEN 'GIA' ELSE ACCTCODE END as ACCTCODE_NEW,
        COALESCE(LEDGERBAL,0) as LEDGERBAL_CLEAN,
        COALESCE(FOREXAMT,0) as FOREXAMT_CLEAN,
        'Y' as SIGNATORY,
        0 as AMT2,
        'N' as COLLINDC
    FROM idx3_tbl
"""
final_tbl = con.execute(final_query).arrow()


# ================================================================
# STEP 7: Write Output File
# ================================================================
pq.write_table(final_tbl, "CISDP_DETAIL.parquet")
csv.write_csv(final_tbl, "CISDP_DETAIL.csv")

print("CISDP SAMPLE:")
print(final_tbl.to_pandas().head())

# ================================================================
# JCL
# ================================================================
# Split into Conventional (C) and Islamic (I)
pbbrec = duckdb.sql("""
    SELECT * FROM allrec
    WHERE SUBSTR(column249, 1, 1) = 'C'
""").arrow()

pibbrec = duckdb.sql("""
    SELECT * FROM allrec
    WHERE SUBSTR(column249, 1, 1) = 'I'
""").arrow()

# Save outputs as parquet
pq.write_table(pbbrec, "SNGLVIEW_DEPOSIT_DP04.parquet")
pq.write_table(pibbrec, "RBP2_B051_SNGLVIEW_DEPOSIT_DP04.parquet")

# ================
# JCL Part 2
# ================
# Number of parts
num_parts = 10

# Convert Arrow Table to batches
num_rows = deposit.num_rows

# Round-robin split (like DFSORT SPLIT)
for i in range(num_parts):
    # Select rows where row_number % num_parts == i
    part = duckdb.sql(f"""
        SELECT * 
        FROM deposit
        WHERE row_number() OVER () % {num_parts} = {i}
    """).arrow()
    
    # Save each split to parquet
    pq.write_table(part, f"SNGLVIEW_DEPOSIT_DP04{i+1:02}.parquet")
