import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# -------------------------------------------------------------------
# Setup DuckDB connection
# -------------------------------------------------------------------
con = duckdb.connect()

# ===================================================================
# OCCUPATION FILE
# ===================================================================
occupation = con.execute("""
    SELECT 
        substr(DEMOCATEGORY, 1, 5) AS DEMOCATEGORY,
        substr(OCCUPNUM, 1, 4) AS OCCUPNUM,
        substr(OCCUPDESC, 1, 20) AS OCCUPDESC,
        DEMOCATEGORY || OCCUPNUM AS OCCUPCD
    FROM 'BANKCTRL_DEMOCODE.parquet'
    WHERE DEMOCATEGORY IN ('OCCUP','BUSIN')
""").arrow()

pq.write_table(occupation, "OCCUPATION.parquet")

# ===================================================================
# RELATIONSHIP CODES FILE
# ===================================================================
rlenca = con.execute("""
    SELECT 
        substr(RLENCATEGORY, 1, 2) AS RLENCATEGORY,
        cast(substr(RLENCODE, 1, 3) AS INTEGER) AS RLENCODE,
        substr(RELATIONDESC, 1, 20) AS RELATIONDESC
    FROM 'BANKCTRL_RLENCODE_CA.parquet'
""").arrow()

pq.write_table(rlenca, "RLENCA.parquet")

# ===================================================================
# IDIC INDIVIDUAL FILE
# ===================================================================
indv = con.execute("""
    SELECT DISTINCT
        CUSTNO AS CUSTNO,
        CUSTNO AS CUSTNOX,
        EMPLOYMENT_TYPE,
        LAST_UPDATE_DATE
    FROM 'CIS_IDIC_DAILY_INDV.parquet'
    WHERE EMPLOYMENT_TYPE IS NOT NULL AND EMPLOYMENT_TYPE <> ''
""").arrow()

pq.write_table(indv, "INDV.parquet")

# ===================================================================
# MISC CODE FILE (MSCO / MSIC split)
# ===================================================================
msco = con.execute("""
    SELECT 
        substr(FIELDNAME, 1, 20) AS FIELDNAME,
        substr(MASCO2008, 1, 5) AS MASCO2008,
        substr(MSCDESC, 1, 150) AS MSCDESC
    FROM 'UNLOAD_CIBCCIST_FB.parquet'
    WHERE FIELDNAME = 'MASCO2008'
""").arrow()

pq.write_table(msco, "MSCO.parquet")

msic = con.execute("""
    SELECT 
        substr(FIELDNAME, 1, 20) AS FIELDNAME,
        substr(MSICCODE, 1, 5) AS MSICCODE,
        substr(MSCDESC, 1, 150) AS MSCDESC
    FROM 'UNLOAD_CIBCCIST_FB.parquet'
    WHERE FIELDNAME = 'MSIC2008'
""").arrow()

pq.write_table(msic, "MSIC.parquet")

# ===================================================================
# BRANCH FILE
# ===================================================================
pbbbranch = con.execute("""
    SELECT 
        cast(substr(BRANCHNO, 1, 3) AS INTEGER) AS BRANCHNO,
        substr(ACCTBRABBR, 1, 3) AS ACCTBRABBR
    FROM 'PBB_BRANCH.parquet'
""").arrow()

pq.write_table(pbbbranch, "PBBBRCH.parquet")

# ===================================================================
# 1. FOREX CONTROL FILE
# ===================================================================
forex = con.execute("""
    SELECT 
        substr(CURRCODE, 1, 3) AS CURRCODE,
        try_cast(FOREXRATE AS DOUBLE) AS FOREXRATE
    FROM 'SAP_PBB_FOREIGN_RATE.parquet'
    -- remove header (_N_=1) equivalent: skip first row
    OFFSET 1
""").arrow()

# Replace null FOREXRATE with 0 (SAS _N_=2 blank → 0)
forex = forex.set_column(
    forex.schema.get_field_index("FOREXRATE"),
    "FOREXRATE",
    pa.compute.fill_null(forex["FOREXRATE"], 0)
)

pq.write_table(forex, "FOREX.parquet")

# ===================================================================
# 2. FROZEN AND INACTIVE DATES
# ===================================================================
frz = con.execute("""
    SELECT 
        ACCTNO,
        CASE 
            WHEN POST1 <> '' THEN 'FROZEN'
            WHEN DORM1 = 'D' THEN 'DORMANT'
            WHEN DORM1 = 'N' THEN 'INACTIVE'
            ELSE NULL
        END AS ACCTSTATUS,
        -- build DATE1 from LCUSTDATEM, LCUSTDATED, LCUSTDATEY
        make_date(2000 + cast(LCUSTDATEY AS INTEGER),
                  cast(LCUSTDATEM AS INTEGER),
                  cast(LCUSTDATED AS INTEGER)) AS DATE1,
        POSTDATE AS DATE2,
        CASE 
            WHEN POST1 <> '' THEN POSTDATE
            WHEN DORM1 IN ('D','N') 
                THEN make_date(2000 + cast(LCUSTDATEY AS INTEGER),
                               cast(LCUSTDATEM AS INTEGER),
                               cast(LCUSTDATED AS INTEGER))
            ELSE NULL
        END AS DATE3
    FROM 'FROZEN_INACTIVE_ACCT.parquet'
    WHERE ACCTNO BETWEEN 6000000000 AND 6999999999
""").arrow()

# DATECLSE = DATE3
frz = frz.append_column("DATECLSE", frz["DATE3"])
pq.write_table(frz, "FRZ.parquet")

# ===================================================================
# 3. DEPOSIT TRIAL BALANCE
# ===================================================================
# Load and filter accounts
dptr = con.execute("""
    SELECT 
        BANKNO, REPTNO, FMTCODE,
        BRANCHNO, ACCTNO, ACCTX, CLSEDATE, OPENDATE,
        HOLDAMT1, LEDGERBAL1, BALHOLD, ODLIMIT,
        CURRCODE, OPENIND, DORMIND, COSTCTR, POSTIND
    FROM 'DPTRBLGS.parquet'
    WHERE REPTNO = 1001
      AND FMTCODE IN (1,10,22,19,20,21)
      AND ACCTNO BETWEEN 6000000000 AND 6999999999
""").arrow()

# Transform using DuckDB again
con.register("dptr", dptr)

dptr_t = con.execute("""
    SELECT *,
        CASE WHEN COSTCTR BETWEEN 3000 AND 3999 THEN 'I' ELSE 'C' END AS BANKINDC,
        HOLDAMT1/100.0 AS HOLDAMT,
        LEDGERBAL1/100.0 AS LEDGERBAL,
        CASE WHEN CURRCODE <> 'MYR' THEN LEDGERBAL1/100.0 ELSE 0 END AS FOREXAMT,
        CASE WHEN CURRCODE <> 'MYR' THEN 0 ELSE LEDGERBAL1/100.0 END AS LEDGERBAL_ADJ,
        -- account status rules
        CASE
            WHEN OPENIND = ''  THEN 'ACTIVE'
            WHEN OPENIND IN ('B','C','P') THEN 'CLOSED'
            WHEN OPENIND = 'Z' THEN 'ZERO BALANCE'
        END AS ACCTSTATUS_OPEN
    FROM dptr
""").arrow()

# Split MYR vs Non-MYR
dpmry = dptr_t.filter(dptr_t.column("CURRCODE") == "MYR")
dpoth = dptr_t.filter(dptr_t.column("CURRCODE") != "MYR")

pq.write_table(dpmry, "DPMYR.parquet")
pq.write_table(dpoth, "DPOTH.parquet")

# ===================================================================
# 4. MERGE WITH FOREX
# ===================================================================
con.register("dpoth", dpoth)
con.register("forex", forex)

forexmrg = con.execute("""
    SELECT 
        dpoth.*,
        forex.FOREXRATE,
        ROUND(((dpoth.FOREXAMT * forex.FOREXRATE)/0.01) * 0.01, 2) AS LEDGERBAL
    FROM dpoth
    LEFT JOIN forex USING (CURRCODE)
""").arrow()

pq.write_table(forexmrg, "FOREXMRG.parquet")

# ===================================================================
# 5. Combine all deposits + merge FRZ
# ===================================================================
deposit = pa.concat_tables([dpmry, forexmrg])
pq.write_table(deposit, "DEPOSIT.parquet")

con.register("deposit", deposit)
con.register("frz", frz)

deposit2 = con.execute("""
    SELECT deposit.*, frz.ACCTSTATUS AS FRZ_STATUS, frz.DATECLSE
    FROM deposit
    LEFT JOIN frz USING (ACCTNO)
""").arrow()

pq.write_table(deposit2, "DEPOSIT2.parquet")

print("FOREX, FRZ, DPMYR, DPOTH, FOREXMRG, DEPOSIT, DEPOSIT2 written as parquet.")

# ===================================================================
# CIS CUSTOMER FILE
# ===================================================================
import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.csv as pacsv

con = duckdb.connect()

# 1) Load base CUST and apply row-level filters + transformations
con.execute("""
CREATE OR REPLACE VIEW cust_raw AS
SELECT
    *, 
    -- start with the input ACCTCODE filter (SAS: IF ACCTCODE EQ 'DP'; then later set to 'SA')
    ACCTCODE AS ORIGINAL_ACCTCODE
FROM 'CIS_CUST_DAILY.parquet'
""")

con.execute("""
CREATE OR REPLACE VIEW cust AS
SELECT
    CUSTNO,
    ACCTNO,
    CUSTNAME,
    ALIAS,
    MASCO2008,
    MSICCODE,
    SICCODE,
    INDORG,
    PRISEC,
    EMPLOYMENT_TYPE,
    JOINTACC,
    ACCTCODE,
    OCCUP AS OCCUP,            -- used to build OCCUPCD for INDORG='I'
    SICCODE AS SICCODE_IN,
    -- apply filters (equivalent to SAS IF statements)
    CASE WHEN ACCTCODE = 'DP' THEN TRUE ELSE FALSE END AS keep_dp,
    CASE WHEN CAST(ACCTNO AS BIGINT) BETWEEN 6000000001 AND 6999999998 THEN TRUE ELSE FALSE END AS acct_in_range
FROM cust_raw
""")

# Create filtered and mutated CUST dataset (SAS DATA CUST; SET CUSTFILE.CUSTDLY; ... )
con.execute("""
CREATE OR REPLACE VIEW cust_step AS
SELECT
    CUSTNO,
    ACCTNO,
    CUSTNAME,
    ALIAS,
    MASCO2008,
    MSICCODE,
    SICCODE_in,
    INDORG,
    PRISEC,
    EMPLOYMENT_TYPE,
    COALESCE(NULLIF(JOINTACC, ''), 'N') as JOINTACC,
    -- Map PRISEC
    CASE WHEN PRISEC = 901 THEN 'P'
         WHEN PRISEC = 902 THEN 'S'
         ELSE NULL END as PRIMSEC,
    -- Build OCCUPCD
    CASE WHEN INDORG = 'O' THEN 'BUSIN' || COALESCE(SICCODE_in, '')
         WHEN INDORG = 'I' THEN 'OCCUP' || COALESCE(OCCUP, '')
         ELSE NULL END as OCCUPCD,
    -- Set ACCTCODE to 'SA' as per SAS (done after initial filter)
    'SA' as ACCTCODE_NEW
FROM cust
WHERE keep_dp = TRUE
  AND acct_in_range = TRUE
  AND NOT (COALESCE(CUSTNAME, '') = '' AND COALESCE(ALIAS, '') = '')
""")

# Persist CUST to Parquet for later joins
cust_table = con.execute("SELECT * FROM cust_step").arrow()
pq.write_table(cust_table, "CUST.parquet")
con.register("CUST", cust_table)

# 2) CUSTMSCA: merge CUST with MSCO by MASCO2008; keep only CUST rows; then SAS deletes when MSICCODE NOT = ' '
# We'll interpret "IF MSICCODE NOT = ' ' THEN DELETE" as: keep only those where MSICCODE IS NULL or blank
# Left join then filter
con.execute("""
CREATE OR REPLACE VIEW custmsca AS
SELECT c.*, m.MASCO2008 as MSCO_MASCO2008, m.MSCDESC as MSCO_MSCDESC, m.MSICCODE as MSCO_MSICCODE
FROM CUST c
LEFT JOIN 'MSCO.parquet' m USING (MASCO2008)
""")

con.execute("""
CREATE OR REPLACE VIEW custmsca_filt AS
SELECT * FROM custmsca
WHERE COALESCE(MSCO_MSICCODE, '') = ''
""")

custmsca_table = con.execute("SELECT * FROM custmsca_filt").arrow()
pq.write_table(custmsca_table, "CUSTMSCA.parquet")
con.register("CUSTMSCA", custmsca_table)

# 3) CUSTMSCB: merge CUST with MSIC by MSICCODE (keep all CUST rows)
con.execute("""
CREATE OR REPLACE VIEW custmscb AS
SELECT c.*, mi.MSICCODE as MSIC_MSICCODE, mi.MSCDESC as MSIC_MSCDESC
FROM CUST c
LEFT JOIN 'MSIC.parquet' mi ON c.MSICCODE = mi.MSICCODE
""")

custmscb_table = con.execute("SELECT * FROM custmscb").arrow()
pq.write_table(custmscb_table, "CUSTMSCB.parquet")
con.register("CUSTMSCB", custmscb_table)

# 4) CUSTMSC = union of CUSTMSCA + CUSTMSCB, dedup by CUSTNO, ACCTNO
con.execute("""
CREATE OR REPLACE VIEW custmsc_union AS
SELECT * FROM CUSTMSCA
UNION ALL
SELECT * FROM CUSTMSCB
""")

# dedup by CUSTNO, ACCTNO (keep first)
custmsc_table = con.execute("""
SELECT DISTINCT ON (CUSTNO, ACCTNO) *
FROM custmsc_union
ORDER BY CUSTNO, ACCTNO
""").arrow()
pq.write_table(custmsc_table, "CUSTMSC.parquet")
con.register("CUSTMSC", custmsc_table)

# 5) CUSTA: merge CUSTMSC (X) with INDV (Y) by CUSTNO; keep X
con.execute("""
CREATE OR REPLACE VIEW inda AS
SELECT i.CUSTNO as INDV_CUSTNO, i.EMPLOYMENT_TYPE as INDV_EMPLOYMENT_TYPE, i.LAST_UPDATE_DATE
FROM 'INDV.parquet' i
""")
con.execute("""
CREATE OR REPLACE VIEW custa AS
SELECT msc.*, indv.INDV_EMPLOYMENT_TYPE as EMPLOYMENT_TYPE_INDV
FROM CUSTMSC msc
LEFT JOIN inda indv USING (CUSTNO)
""")
custa_table = con.execute("SELECT * FROM custa").arrow()
pq.write_table(custa_table, "CUSTA.parquet")
con.register("CUSTA", custa_table)

# 6) CISDP: inner merge CUSTA and DEPOSIT2 by ACCTNO; only keep rows where both exist
con.execute("""
CREATE OR REPLACE VIEW deposit2 AS SELECT * FROM 'DEPOSIT2.parquet'
""")
con.execute("""
CREATE OR REPLACE VIEW cisdpt AS
SELECT a.*, d.*
FROM CUSTA a
INNER JOIN deposit2 d USING (ACCTNO)
""")
cisdpt_table = con.execute("SELECT * FROM cisdpt").arrow()
pq.write_table(cisdpt_table, "CISDP.parquet")
con.register("CISDP", cisdpt_table)

# 7) Lookups: OCCUPATION by OCCUPCD, RLENCA by RLENCODE, PBBBRCH by BRANCHNO
con.execute("""
CREATE OR REPLACE VIEW idx1 AS
SELECT c.*, occ.OCCUPDESC as OCCUPDESC
FROM CISDP c
LEFT JOIN 'OCCUPATION.parquet' occ USING (OCCUPCD)
""")

con.execute("""
CREATE OR REPLACE VIEW idx2 AS
SELECT i1.*, r.RELATIONDESC as RELATIONDESC
FROM idx1 i1
LEFT JOIN 'RLENCA.parquet' r USING (RLENCODE)
""")

con.execute("""
CREATE OR REPLACE VIEW idx3 AS
SELECT i2.*, b.ACCTBRABBR as ACCTBRABBR_LOOKUP, b.BRANCHNO as BRANCHNO_LOOKUP
FROM idx2 i2
LEFT JOIN 'PBBBRCH.parquet' b USING (BRANCHNO)
""")

idx3_table = con.execute("SELECT * FROM idx3").arrow()
pq.write_table(idx3_table, "IDX3.parquet")

# 8) Final formatting & detail output
# Build the output columns that mirror the SAS PUT order (as CSV columns)
# We'll create a CSV with the same logical fields in order; numeric formatting will be preserved as numeric columns

con.register("idx3", idx3_table)  # register arrow table

final = con.execute("""
SELECT
    '033' as bank_code_literal,
    CUSTNO,
    INDORG,
    CUSTNAME,
    -- aliaskey (3 chars) and ALIAS
    substr(coalesce(ALIAS,''),1,3) as ALIASKEY,
    ALIAS,
    -- OCCUPCD1 = substring of OCCUPCD starting at pos6 of length 5 (SAS: SUBSTR(OCCUPCD,6,5))
    CASE WHEN LENGTH(COALESCE(OCCUPCD,'')) >= 10 THEN SUBSTR(COAUPCD := OCCUPCD, 6, 5)
         ELSE SUBSTR(COALESCE(OCCUPCD,''), 6, 5) END AS OCCUPCD1,
    COALESCE(OCCUPDESC, '') as OCCUPDESC,
    COALESCE(ACCTBRABBR_LOOKUP, '') as ACCTBRABBR,
    COALESCE(COALESCE(BRANCHNO_LOOKUP, 0), 0) as BRANCHNO,
    -- ACCTCODE (the SAS sets to 'SA' earlier, but note special GIA case later)
    ACCTCODE_NEW as ACCTCODE,
    ACCTNO,
    BANKINDC,
    PRIMSEC,
    RLENCODE,
    COALESCE(RELATIONDESC,'') as RELATIONDESC,
    ACCTSTATUS,        -- may come from deposit/frz logic
    DATEOPEN,
    DATECLSE,
    'Y' as SIGNATORY,
    CASE WHEN COALESCE(CURRCODE,'') = '' THEN '     ' ELSE 'LEDGB' END as BAL1INDC,
    COALESCE(LEDGERBAL,0) as LEDGERBAL,
    CASE WHEN CURRCODE = 'XAU' THEN 'GM ' ELSE COALESCE(CURRCODE,'') END as CURRCODE_OUT,
    COALESCE(FOREXAMT,0) as FOREXAMT,
    COALESCE(AMT1INDC,'') as AMT1INDC,
    COALESCE(AMT1,0) as AMT1,
    'N' as COLLINDC,
    COALESCE(JOINTACC,'N') as JOINTACC,
    COALESCE(DOBDOR,'') as DOBDOR,
    -- EMPLMASCO logic: build using MSICCODE, OCCUPCD1, EMPLOYMENT_TYPE & MASCO2008
    CASE
        WHEN INDORG = 'O' THEN
            CASE WHEN COALESCE(MSIC_MSICCODE,'') <> '' THEN MSIC_MSICCODE ELSE SUBSTR(COALESCE(OCCUPCD, ''), 6, 5) END
        WHEN INDORG = 'I' THEN
            CASE
                WHEN COALESCE(EMPLOYMENT_TYPE,'') = '' OR COALESCE(MASCO2008,'') = '' THEN SUBSTR(COALESCE(OCCUPCD,''),6,5)
                ELSE TRIM(EMPLOYMENT_TYPE || MASCO2008)
            END
        ELSE NULL
    END as EMPLMASCO,
    COALESCE(MSCDESC, '') as MSCDESC
FROM idx3
""").arrow()

# NOTE: DuckDB SQL above may need small tweaks for substr when field lengths vary. If an error occurs, fallback to Python processing.

# Write final Parquet and CSV
pq.write_table(final, "CIS_DETAIL.parquet")

# For CSV: use pyarrow.csv to write table -> CSV
with pa.OSFile("OUTFILE_DETAIL.csv", "wb") as sink:
    pacsv.write_csv(final, sink)

print("Done. Outputs written:")
print(" - CUST.parquet, CUSTMSCA.parquet, CUSTMSCB.parquet, CUSTMSC.parquet, CUSTA.parquet, CISDP.parquet")
print(" - IDX3.parquet, CIS_DETAIL.parquet, OUTFILE_DETAIL.csv")

# ===========
# JCL
# ===========
# Paths
allrec_parquet = "ALLREC.parquet"   # assumed input parquet
pbbrec_parquet = "SNGLVIEW.DEPOSIT.DP06.parquet"
pibbrec_parquet = "RBP2.B051.SNGLVIEW.DEPOSIT.DP06.parquet"

# Register input
con.execute(f"CREATE TABLE allrec AS SELECT * FROM read_parquet('{allrec_parquet}')")

# Split PBB (BANKINDC = 'C')
pbb = con.execute("SELECT * FROM allrec WHERE BANKINDC = 'C'").fetch_arrow_table()
pq.write_table(pbb, pbbrec_parquet)

# Split PIBB (BANKINDC = 'I')
pibb = con.execute("SELECT * FROM allrec WHERE BANKINDC = 'I'").fetch_arrow_table()
pq.write_table(pibb, pibbrec_parquet)
