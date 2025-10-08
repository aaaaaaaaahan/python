import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path  # assume your helper

# ============================================================
# SETUP
# ============================================================
con = duckdb.connect()

sccust = host_parquet_path("SCCUST.parquet")
sccard = host_parquet_path("SCCARD.parquet")
cisfile = host_parquet_path("CIS_CUST_DAILY.parquet")

# ============================================================
# STEP 1: PROCESS CUST FILE
# ============================================================
con.execute(f"""
    CREATE TABLE cust AS
    SELECT
        ALIASKEY,
        ALIAS,
        TAXID,
        TAXNUM,
        ACCTNAME,
        TOTCOMBLIMIT,
        CASE WHEN SIGN1 = '-' THEN OUTSTNDAMT * -1
             WHEN SIGN1 = '+' THEN OUTSTNDAMT * 1 ELSE OUTSTNDAMT END AS OUTSTNDAMT,
        CASE WHEN SIGN2 = '-' THEN TOTMIN * -1
             WHEN SIGN2 = '+' THEN TOTMIN * 1 ELSE TOTMIN END AS TOTMIN,
        CASE WHEN SIGN3 = '-' THEN OUTSTANDDUE * -1
             WHEN SIGN3 = '+' THEN OUTSTANDDUE * 1 ELSE OUTSTANDDUE END AS OUTSTANDDUE
    FROM read_parquet('{sccust}')
    WHERE ALIAS IS NOT NULL AND ALIAS <> '' AND ALIASKEY <> 'CV '
""")

# ============================================================
# STEP 2: PROCESS CARD FILE
# ============================================================
con.execute(f"""
    CREATE TABLE card AS
    SELECT
        ALIASKEY,
        ALIAS,
        TAXID,
        TAXNUM,
        ACCTNOC,
        OPENDATE,
        STATUS,
        CLOSEDATE,
        MONITOR,
        DUEDAY,
        CASE WHEN SIGN4 = '-' THEN OVERDUEAMT * -1
             WHEN SIGN4 = '+' THEN OVERDUEAMT * 1 ELSE OVERDUEAMT END AS OVERDUEAMT,
        PRODTYPE AS ACCTCODE,
        -- DATE LOGIC
        CASE
            WHEN length(OPENDATE)=8 THEN substr(OPENDATE,1,4)||'-'||substr(OPENDATE,5,2)||'-'||substr(OPENDATE,7,2)
            ELSE '9999-01-01'
        END AS DATEOPEN,
        CASE
            WHEN length(CLOSEDATE)=8 THEN substr(CLOSEDATE,1,4)||'-'||substr(CLOSEDATE,5,2)||'-'||substr(CLOSEDATE,7,2)
            ELSE '9999-01-01'
        END AS DATECLOSE
    FROM read_parquet('{sccard}')
    WHERE ALIAS IS NOT NULL AND ALIAS <> '' AND ALIASKEY <> 'CV '
""")

# ============================================================
# STEP 3: MERGE CUST + CARD
# ============================================================
con.execute("""
    CREATE TABLE cardcust AS
    SELECT *
    FROM cust
    JOIN card USING (ALIAS, TAXID)
""")

# ============================================================
# STEP 4: CREATE NEWIC / OLDIC VERSIONS
# ============================================================
con.execute("""
    CREATE TABLE card_newic AS
    SELECT *, ALIASKEY || ALIAS AS CUSTID, '' AS TAXID, '' AS TAXNUM
    FROM cardcust
""")

con.execute("""
    CREATE TABLE card_oldic AS
    SELECT *, TAXID || TAXNUM AS CUSTID, '' AS ALIASKEY, '' AS ALIAS
    FROM cardcust
""")

con.execute("""
    CREATE TABLE card_custids AS
    SELECT * FROM card_newic
    UNION ALL
    SELECT * FROM card_oldic
    WHERE CUSTID <> ''
""")

# ============================================================
# STEP 5: PROCESS CIS FILE
# ============================================================
con.execute(f"""
    CREATE TABLE cis AS
    SELECT
        ALIAS,
        RLENCODE,
        printf('%03d', RLENCODE) AS RLENCD,
        ACCTNOC,
        CUSTNO,
        TAXID,
        ALIASKEY,
        CUSTNAME,
        CASE WHEN PRISEC = '901' THEN 'P' ELSE 'S' END AS PRIMSEC,
        RLENCATEGORY,
        RELATIONDESC,
        JOINTACC,
        ACCTCODE
    FROM read_parquet('{cisfile}')
    WHERE ACCTCODE NOT IN ('DP','LN','EQC')
      AND CUSTNO <> ''
      AND ACCTNOC <> ''
""")

# ============================================================
# STEP 6: CIS NEWIC / OLDIC / COMBINED
# ============================================================
con.execute("""
    CREATE TABLE cis_newic AS
    SELECT *, ALIASKEY || ALIAS AS CUSTID, '' AS TAXID
    FROM cis
    WHERE ALIAS <> ''
""")

con.execute("""
    CREATE TABLE cis_oldic AS
    SELECT *, 'OC' || TAXID AS CUSTID, '' AS ALIASKEY, '' AS ALIAS
    FROM cis
    WHERE TAXID <> ''
""")

con.execute("""
    CREATE TABLE cis_custids AS
    SELECT * FROM cis_newic
    UNION ALL
    SELECT * FROM cis_oldic
""")

# ============================================================
# STEP 7: MERGE MATCHED & UNMATCHED
# ============================================================
con.execute("""
    CREATE TABLE matched_rec AS
    SELECT a.*, b.CUSTNO, b.CUSTNAME, b.PRIMSEC, b.JOINTACC, b.RLENCD, b.RELATIONDESC
    FROM card_custids a
    JOIN cis_custids b USING (CUSTID, ACCTNOC)
""")

con.execute("""
    CREATE TABLE un_matched_rec AS
    SELECT a.*, a.ACCTNOC AS CUSTNO, a.ACCTNAME AS CUSTNAME
    FROM card_custids a
    WHERE NOT EXISTS (
        SELECT 1 FROM cis_custids b
        WHERE a.CUSTID = b.CUSTID AND a.ACCTNOC = b.ACCTNOC
    )
""")

# ============================================================
# STEP 8: FINAL OUTPUT
# ============================================================
con.execute("""
    CREATE TABLE final AS
    SELECT
        CUSTID,
        CUSTNO,
        'PBB' AS SOURCE,
        '001' AS BRANCHNO,
        'HOE' AS BRCABBRV,
        COALESCE(CUSTNAME, ACCTNAME) AS CUSTNAME,
        ACCTCODE,
        ACCTNOC,
        PRIMSEC,
        JOINTACC,
        RLENCD,
        RELATIONDESC,
        STATUS,
        MONITOR,
        DUEDAY,
        DATEOPEN,
        DATECLOSE,
        OVERDUEAMT,
        TOTMIN,
        TOTCOMBLIMIT
    FROM matched_rec
    UNION ALL
    SELECT
        CUSTID,
        CUSTNO,
        'PBB',
        '001',
        'HOE',
        CUSTNAME,
        ACCTCODE,
        ACCTNOC,
        PRIMSEC,
        JOINTACC,
        RLENCD,
        RELATIONDESC,
        STATUS,
        MONITOR,
        DUEDAY,
        DATEOPEN,
        DATECLOSE,
        OVERDUEAMT,
        TOTMIN,
        TOTCOMBLIMIT
    FROM un_matched_rec
""")

# ============================================================
# STEP 9: SAVE OUTPUT USING PYARROW
# ============================================================
table = con.execute("SELECT * FROM final").arrow()
output_path = parquet_output_path("CISCCARD_output.parquet")
pq.write_table(table, output_path)

print(f"✅ Output written to: {output_path}")
