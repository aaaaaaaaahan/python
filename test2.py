# ================================================================
# Part 10: CIS + merges → mergeall1
# ================================================================

# Step 1: Merge aliasfl with CIS on ALIASKEY + ALIAS
con.execute("""
    CREATE VIEW mergeals AS
    SELECT a.*, c.*
    FROM alias a
    INNER JOIN cisfile c
      ON a.ALIASKEY = c.ALIASKEY
     AND a.ALIAS    = c.ALIAS
    ORDER BY a.ALIASKEY, a.ALIAS
""")
print("\n=== Part 10: MERGE ALIAS ===")
print(con.execute("SELECT * FROM mergeals LIMIT 5").fetchdf())

# Step 2: Merge OCCUP code with occupation lookup table
con.execute("""
    CREATE VIEW mergeocc AS
    SELECT m.*, o.DEMODESC
    FROM mergeals m
    LEFT JOIN occupfl o
      ON m.OCCUP = o.DEMOCODE
""")
print("\n=== Part 10: MERGE OCCUP FILE ===")
print(con.execute("SELECT * FROM mergeocc LIMIT 5").fetchdf())

# Step 3: Merge MASCO
con.execute("""
    CREATE VIEW mergemsc AS
    SELECT mo.*, ms.MASCODESC
    FROM mergeocc mo
    LEFT JOIN mascofl ms
      ON mo.MASCO2008 = ms.MASCO2008
""")
print("\n=== Part 10: MERGE MASCO FILE ===")
print(con.execute("SELECT * FROM mergemsc LIMIT 5").fetchdf())

# Step 4: Merge MSIC
con.execute("""
    CREATE VIEW mergeall1 AS
    SELECT mm.*, msic.MSICDESC
    FROM mergemsc mm
    LEFT JOIN msicfl msic
      ON mm.MSICCODE = msic.MSICCODE
    ORDER BY mm.ALIAS
""")
print("\n=== Part 10: MERGE MSIC FILE ===")
print(con.execute("SELECT * FROM mergeall1 LIMIT 5").fetchdf())

# ================================================================
# Part 11: SAFE DEPOSIT BOX
# ================================================================

# Step 1: Create safebox2 with casts
con.execute(f"""
    CREATE VIEW safebox2 AS
    SELECT
        CAST(INDORG AS VARCHAR)        AS INDORG,
        CAST(CUSTNAME_SDB AS VARCHAR)  AS CUSTNAME_SDB,
        CAST(ALIASKEY_SDB AS VARCHAR)  AS ALIASKEY_SDB,
        CAST(ALIAS AS VARCHAR)         AS ALIAS,
        CAST(BRANCH_ABBR AS VARCHAR)   AS BRANCH_ABBR,
        CAST(BRANCHNO AS VARCHAR)      AS BRANCHNO,
        CAST(ACCTCODE AS VARCHAR)      AS ACCTCODE,
        CAST(ACCT_NO AS VARCHAR)       AS ACCT_NO,
        CAST(BANKINDC AS VARCHAR)      AS BANKINDC,
        CAST(ACCTSTATUS AS VARCHAR)    AS ACCTSTATUS,
        CAST(BAL1INDC AS VARCHAR)      AS BAL1INDC,
        TRY_CAST(BAL1 AS DOUBLE)       AS BAL1,
        CAST(AMT1INDC AS VARCHAR)      AS AMT1INDC,
        TRY_CAST(AMT1 AS DOUBLE)       AS AMT1,
        0                              AS LEDGERBAL,
        '3'                            AS CATEGORY,
        'SDB  '                        AS APPL_CODE
    FROM '{host_parquet_path("SAFEBOX_FILE.parquet")}'
    ORDER BY ALIAS
""")

print("\n=== Part 11: SAFEBOX2 ===")
print(con.execute("SELECT * FROM safebox2 LIMIT 10").fetchdf())

# ----------------------------------------------------------------
# NEW SDB MATCHING - MERGEALL
# ----------------------------------------------------------------
con.execute("""
    CREATE VIEW mergeall AS
    SELECT m.*,
           s.INDORG,
           s.CUSTNAME_SDB,
           s.ALIASKEY_SDB,
           s.BRANCH_ABBR,
           s.BRANCHNO,
           s.ACCTCODE,
           s.ACCT_NO,
           s.BANKINDC,
           s.ACCTSTATUS,
           s.BAL1INDC,
           s.BAL1,
           s.AMT1INDC,
           s.AMT1,
           s.LEDGERBAL,
           s.CATEGORY,
           s.APPL_CODE,
           CASE WHEN s.BRANCH_ABBR IS NOT NULL THEN 'YES' ELSE 'NIL' END AS SDBIND,
           COALESCE(s.BRANCH_ABBR, 'NIL') AS SDBBRH
    FROM mergeall1 m
    LEFT JOIN safebox2 s
      ON m.ALIAS = s.ALIAS
""")

-- Drop duplicates by ACCTNOC (keeping first)
con.execute("""
    CREATE VIEW mergeall_dedup AS
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY ACCTNOC ORDER BY ALIAS) AS rn
        FROM mergeall
    )
    WHERE rn = 1
""")

print("\n=== Part 11: SDB MATCHING ===")
print(con.execute("SELECT * FROM mergeall_dedup LIMIT 10").fetchdf())

# ================================================================
# Part 12: DEPOSIT TRIAL BALANCE
# ================================================================

# Step 1: Load and filter DPTRBALS
con.execute(f"""
    CREATE VIEW dptrbals AS
    SELECT
        LPAD(CAST(ACCTNO AS VARCHAR), 11, '0')       AS ACCTNOC,
        LPAD(CAST(ACCTBRCH1 AS VARCHAR), 3, '0')    AS ACCTBRCH,
        LPAD(CAST(PRODTYPE AS VARCHAR), 3, '0')     AS PRODTY,
        CAST(ACCTNAME AS VARCHAR)                   AS ACCTNAME40,
        CASE WHEN COSTCTR > 3000 AND COSTCTR < 3999 THEN 'I' ELSE 'C' END AS BANKINDC,
        LEDGERBAL1 / 100.0                          AS LEDGERBAL,
        ''                                          AS APPL_CODE,
        ''                                          AS ACCT_TYPE,
        CAST(OPENDATE AS VARCHAR)                   AS OPENDATE,
        CAST(CLSEDATE AS VARCHAR)                   AS CLSEDATE,
        CAST(OPENIND AS VARCHAR)                    AS OPENIND,
        CAST(PURPOSECD AS VARCHAR)                  AS PURPOSECD,
        *
    FROM '{host_parquet_path("DPTRBALS_FILE.parquet")}'
    WHERE REPTNO = 1001
      AND FMTCODE IN (1,10,22,19,20,21)
""")

# Step 2: Application code rules (ACCTNOC ranges)
con.execute("""
    CREATE VIEW dptrbals_appl AS
    SELECT *,
           CASE
               WHEN ACCTNOC > '03000000000' AND ACCTNOC < '03999999999' THEN 'CA   '
               WHEN ACCTNOC > '06200000000' AND ACCTNOC < '06299999999' THEN 'CA   '
               WHEN ACCTNOC > '06710000000' AND ACCTNOC < '06719999999' THEN 'CA   '
               WHEN ACCTNOC > '01000000000' AND ACCTNOC < '01999999999' THEN 'FD   '
               WHEN ACCTNOC > '07000000000' AND ACCTNOC < '07999999999' THEN 'FD   '
               WHEN ACCTNOC > '04000000000' AND ACCTNOC < '04999999999' THEN 'SA   '
               WHEN ACCTNOC > '05000000000' AND ACCTNOC < '05999999999' THEN 'SA   '
               WHEN ACCTNOC > '06000000000' AND ACCTNOC < '06199999999' THEN 'SA   '
               WHEN ACCTNOC > '06300000000' AND ACCTNOC < '06709999999' THEN 'SA   '
               WHEN ACCTNOC > '06720000000' AND ACCTNOC < '06999999999' THEN 'SA   '
               ELSE APPL_CODE
           END AS APPL_CODE
    FROM dptrbals
""")

# Step 3: Override APPL_CODE for special PRODTYPEs
con.execute("""
    CREATE VIEW dptrbals_appl2 AS
    SELECT *,
           CASE
               WHEN PRODTY IN ('371','350','351','352','353','354','355','356','357','358',
                               '359','360','361','362')
                   THEN 'FCYFD'
               WHEN PRODTY IN ('400','401','402','403','404','405','406','407','408','409',
                               '410','411','413','414','420','421','422','423','424','425',
                               '426','427','428','429','430','431','432','433','434','440',
                               '441','442','443','444','450','451','452','453','454','460',
                               '461','473','474','475','476')
                   THEN 'FCYCA'
               ELSE APPL_CODE
           END AS APPL_CODE
    FROM dptrbals_appl
    WHERE PURPOSECD IS NOT NULL AND PURPOSECD <> ''
""")

# Step 4: ACCT_TYPE from PURPOSECD
con.execute("""
    CREATE VIEW dptrbals_type AS
    SELECT *,
           PURPOSECD AS ACCT_TYPE
    FROM dptrbals_appl2
""")

# Step 5: ACCTSTATUS from OPENIND
con.execute("""
    CREATE VIEW dptrbals_stat AS
    SELECT *,
           CASE
               WHEN OPENIND = '' THEN 'ACTIVE'
               WHEN OPENIND IN ('B','C','P') THEN 'CLOSED'
               WHEN OPENIND = 'Z' THEN 'ZERO BALANCE'
               ELSE ''
           END AS ACCTSTATUS
    FROM dptrbals_type
""")

# Step 6: Date conversions
con.execute("""
    CREATE VIEW dptrbals_date AS
    SELECT *,
           SUBSTRING(OPENDATE,5,2) AS OPENDD,
           SUBSTRING(OPENDATE,3,2) AS OPENMM,
           SUBSTRING(OPENDATE,7,4) AS OPENYY,
           SUBSTRING(CLSEDATE,5,2) AS CLSEDD,
           SUBSTRING(CLSEDATE,3,2) AS CLSEMM,
           SUBSTRING(CLSEDATE,7,4) AS CLSEYY
    FROM dptrbals_stat
""")

# Step 7: Pad OPENMM/CLSEMM
con.execute("""
    CREATE VIEW dptrbals_date2 AS
    SELECT *,
           CASE WHEN TRY_CAST(OPENMM AS INT) < 10
                THEN '0' || RIGHT(OPENMM,1)
                ELSE OPENMM END AS OPENMM,
           CASE WHEN TRY_CAST(CLSEMM AS INT) < 10
                THEN '0' || RIGHT(CLSEMM,1)
                ELSE CLSEMM END AS CLSEMM,
           (OPENYY || OPENMM || OPENDD) AS DATEOPEN,
           (CLSEYY || CLSEMM || CLSEDD) AS DATECLSE
    FROM dptrbals_date
    ORDER BY ACCTBRCH
""")

print("DEPOSIT REC")
print(con.execute("SELECT * FROM dptrbals_date2 LIMIT 10").fetchdf())

# ----------------------------------------------------------------
# MERGEBRCH (DPTRBALS + PBBBRCH)
# ----------------------------------------------------------------
con.execute("""
    CREATE VIEW mergebrch AS
    SELECT d.*, b.BRANCH_ABBR
    FROM dptrbals_date2 d
    LEFT JOIN pbbrch b
      ON d.ACCTBRCH = b.ACCTBRCH
    ORDER BY ACCTNOC
""")
print("DEPOSIT MATCH REC")
print(con.execute("SELECT * FROM mergebrch LIMIT 10").fetchdf())

# ----------------------------------------------------------------
# MERGEDP (MERGEALL + MERGEBRCH)
# ----------------------------------------------------------------
con.execute("""
    CREATE VIEW mergedp AS
    SELECT *
    FROM mergeall_dedup m
    INNER JOIN mergebrch d
      ON m.ACCTNOC = d.ACCTNOC
    WHERE d.ACCTNOC <> ''
""")

con.execute("""
    CREATE VIEW mergedp_dedup AS
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY ACCTNOC ORDER BY ACCTNOC) AS rn
        FROM mergedp
    )
    WHERE rn = 1
    ORDER BY ACCTNOC
""")

print("DEPOSIT MATCH REC")
print(con.execute("SELECT * FROM mergedp_dedup LIMIT 10").fetchdf())

# ----------------------------------------------------------------
# MERGEDP1: MERGEDP + DPSTMT
# ----------------------------------------------------------------
con.execute("""
    CREATE VIEW mergedp1 AS
    SELECT m.*, c.*
    FROM mergedp_dedup m
    LEFT JOIN cyclefl c
      ON m.ACCTNOC = c.ACCTNOC
    ORDER BY m.ACCTNOC
""")
print("MERGE STMT FILE")
print(con.execute("SELECT * FROM mergedp1 LIMIT 10").fetchdf())

# ----------------------------------------------------------------
# MERGEDP2: MERGEDP1 + DPPOST
# ----------------------------------------------------------------
con.execute("""
    CREATE VIEW mergedp2 AS
    SELECT m.*, p.*
    FROM mergedp1 m
    LEFT JOIN postfl p
      ON m.ACCTNOC = p.ACCTNOC
    ORDER BY m.ACCTNOC
""")
print("MERGE POST IND FILE")
print(con.execute("SELECT * FROM mergedp2 LIMIT 10").fetchdf())

# ----------------------------------------------------------------
# MERGEDP3: MERGEDP2 + DEPOSIT1
# ----------------------------------------------------------------
con.execute("""
    CREATE VIEW mergedp3 AS
    SELECT m.*, d.*
    FROM mergedp2 m
    LEFT JOIN depofl d
      ON m.ACCTNOC = d.ACCTNOC
    ORDER BY m.ACCTNOC
""")
print("ADDITIONAL DP LIST")
print(con.execute("SELECT * FROM mergedp3 LIMIT 10").fetchdf())

# ================================================================
# Part 13: LOANS ACCOUNT FILE
# ================================================================

# Step 1: Build acctfile with transformations
con.execute(f"""
    CREATE VIEW acctfile AS
    SELECT
        TRY_CAST(ACCTNO AS BIGINT)                                     AS ACCTNO,
        LPAD(CAST(ACCTNO AS VARCHAR), 11, '0')                         AS ACCTNOC,
        TRY_CAST(NOTENO AS BIGINT)                                     AS NOTENO,
        LPAD(CAST(NOTENO AS VARCHAR), 5, '0')                          AS NOTENOC,
        LPAD(CAST(ACCTNO AS VARCHAR), 11, '0') || '-' ||
        LPAD(CAST(NOTENO AS VARCHAR), 5, '0')                          AS ACCTNOTE,

        CAST(ACCTNAME AS VARCHAR)                                      AS ACCTNAME40,
        CAST(ORGTYPE AS VARCHAR)                                       AS ACCT_TYPE,

        CASE
            WHEN LPAD(CAST(ACCTNO AS VARCHAR), 11, '0') > '02000000000'
             AND LPAD(CAST(ACCTNO AS VARCHAR), 11, '0') < '02999999999'
                THEN 'LN'
            WHEN LPAD(CAST(ACCTNO AS VARCHAR), 11, '0') > '08000000000'
             AND LPAD(CAST(ACCTNO AS VARCHAR), 11, '0') < '08999999999'
                THEN 'HP'
            ELSE NULL
        END                                                            AS APPL_CODE,

        CASE
            WHEN COSTCENTER BETWEEN 3000 AND 3999 THEN 'I'
            ELSE 'C'
        END                                                            AS BANKINDC,

        -- Pad COSTCENTER to 7 chars and derive branch
        LPAD(CAST(COSTCENTER AS VARCHAR), 7, '0')                      AS COSTCENTERX,
        SUBSTRING(LPAD(CAST(COSTCENTER AS VARCHAR), 7, '0'), 5, 3)     AS COSTCTR1,
        SUBSTRING(LPAD(CAST(COSTCENTER AS VARCHAR), 7, '0'), 5, 3)     AS ACCTBRCH,

        -- Account open date
        SUBSTRING(LPAD(CAST(ACCTOPENDATE AS VARCHAR), 11, '0'), 1, 8)  AS ACCTOPNDT,
        SUBSTRING(LPAD(CAST(ACCTOPENDATE AS VARCHAR), 11, '0'), 1, 2)  AS OPENMM,
        SUBSTRING(LPAD(CAST(ACCTOPENDATE AS VARCHAR), 11, '0'), 3, 2)  AS OPENDD,
        SUBSTRING(LPAD(CAST(ACCTOPENDATE AS VARCHAR), 11, '0'), 5, 4)  AS OPENYY,
        SUBSTRING(LPAD(CAST(ACCTOPENDATE AS VARCHAR), 11, '0'), 5, 4) ||
        SUBSTRING(LPAD(CAST(ACCTOPENDATE AS VARCHAR), 11, '0'), 1, 2) ||
        SUBSTRING(LPAD(CAST(ACCTOPENDATE AS VARCHAR), 11, '0'), 3, 2)  AS DATEOPEN,

        -- Last transaction date
        SUBSTRING(LPAD(CAST(LASTTRANDATE AS VARCHAR), 11, '0'), 1, 8)  AS LASTTRNDT,
        SUBSTRING(LPAD(CAST(LASTTRANDATE AS VARCHAR), 11, '0'), 1, 2)  AS LTRNMM,
        SUBSTRING(LPAD(CAST(LASTTRANDATE AS VARCHAR), 11, '0'), 3, 2)  AS LTRNDD,
        SUBSTRING(LPAD(CAST(LASTTRANDATE AS VARCHAR), 11, '0'), 5, 4)  AS LTRNYY,
        SUBSTRING(LPAD(CAST(LASTTRANDATE AS VARCHAR), 11, '0'), 5, 4) ||
        SUBSTRING(LPAD(CAST(LASTTRANDATE AS VARCHAR), 11, '0'), 1, 2) ||
        SUBSTRING(LPAD(CAST(LASTTRANDATE AS VARCHAR), 11, '0'), 3, 2)  AS DATECLSE,

        -- Ledger balance
        (CAST(NOTECURBAL AS DOUBLE) / 100.0)                           AS LEDGERBAL,

        -- Status (multi-condition cascade)
        CASE
            WHEN NPLINDC = '3' OR ARREARDAY > 92
                THEN 'NPL'
            WHEN ARREARDAY > 1 AND ARREARDAY < 92
                THEN 'ACCOUNT IN ARREARS'
            WHEN NOTEPAID = 'P'
                THEN 'PAID-OFF'
            ELSE ''
        END                                                            AS ACCTSTATUS_pre,

        CASE
            WHEN (NOTECURBAL > 0) AND (ACCTSTATUS_pre = '' OR ACCTSTATUS_pre IS NULL)
                THEN 'ACTIVE'
            ELSE ACCTSTATUS_pre
        END                                                            AS ACCTSTATUS

    FROM '{host_parquet_path("ACCTFILE.parquet")}'
    ORDER BY ACCTBRCH
""")

# Step 2: Join acctfile + pbbrch → mergelnbrch
con.execute("""
    CREATE VIEW mergelnbrch AS
    SELECT a.*, b.*
    FROM acctfile a
    LEFT JOIN pbbrch b
      ON a.ACCTBRCH = b.ACCTBRCH
    ORDER BY a.ACCTNOC
""")

print("\n=== Part 13: LOANS BRCH ===")
print(con.execute("SELECT * FROM mergelnbrch LIMIT 5").fetchdf())

# Step 3: MERGEALL + MERGELNBRCH → MERGELN
con.execute("""
    CREATE VIEW mergeln AS
    SELECT *
    FROM (
        SELECT m.*, l.*,
               ROW_NUMBER() OVER (PARTITION BY m.ACCTNOC ORDER BY m.ACCTNOC) AS rn
        FROM mergeall_dedup m
        INNER JOIN mergelnbrch l
          ON m.ACCTNOC = l.ACCTNOC
    )
    WHERE rn = 1
""")

print("\n=== Part 13: LOANS MATCH REC ===")
print(con.execute("SELECT * FROM mergeln LIMIT 5").fetchdf())

# ================================================================
# Part 14: SAFE DEPOSIT BOX
# ================================================================

# Step 1: Create safebox view
con.execute(f"""
    CREATE VIEW safebox_v14 AS
    SELECT
        CAST(CUSTNO AS VARCHAR)        AS CUSTNO,
        CAST(ACCTNAME40 AS VARCHAR)    AS ACCTNAME40,
        CAST(BRANCH_ABBR AS VARCHAR)   AS BRANCH_ABBR,
        CAST(ACCTNOC AS VARCHAR)       AS ACCTNOC,
        CAST(BANKINDC AS VARCHAR)      AS BANKINDC,
        CAST(ACCTSTATUS AS VARCHAR)    AS ACCTSTATUS,
        0.0                            AS LEDGERBAL,
        '3'                            AS CATEGORY,
        'SDB'                          AS APPL_CODE
    FROM '{host_parquet_path("SAFEBOX.parquet")}'
    ORDER BY ACCTNOC
""")

# Step 2: Inner join with mergeall_dedup (keep unique ACCTNOC)
con.execute("""
    CREATE VIEW mergesdb AS
    SELECT *
    FROM (
        SELECT m.*, s.*,
               ROW_NUMBER() OVER (PARTITION BY m.ACCTNOC ORDER BY m.ACCTNOC) AS rn
        FROM mergeall_dedup m
        INNER JOIN safebox_v14 s
          ON m.ACCTNOC = s.ACCTNOC
    )
    WHERE rn = 1
""")

print("\n=== Part 14: SDB MATCH REC ===")
print(con.execute("SELECT * FROM mergesdb LIMIT 5").fetchdf())

# ================================================================
# Part 14: UNICARD processing + merge with MERGEALL
# ================================================================

# Step 1: Create unicard view
con.execute(f"""
    CREATE VIEW unicard_v14 AS
    SELECT
        CAST(BRANCH_ABBR AS VARCHAR)   AS BRANCH_ABBR,
        CAST(ACCTNOC AS VARCHAR)       AS ACCTNOC,
        CAST(ACCTSTATUS AS VARCHAR)    AS ACCTSTATUS,
        CAST(DATEOPEN AS VARCHAR)      AS DATEOPEN,
        CAST(DATECLSE AS VARCHAR)      AS DATECLSE
    FROM '{host_parquet_path("UNICARD.parquet")}'
    ORDER BY ACCTNOC
""")

# Step 2: Inner join with mergeall_dedup (keep unique ACCTNOC)
con.execute("""
    CREATE VIEW mergeuni AS
    SELECT *
    FROM (
        SELECT m.*, u.*,
               ROW_NUMBER() OVER (PARTITION BY m.ACCTNOC ORDER BY m.ACCTNOC) AS rn
        FROM mergeall_dedup m
        INNER JOIN unicard_v14 u
          ON m.ACCTNOC = u.ACCTNOC
    )
    WHERE rn = 1
""")

print("\n=== Part 14: UNICARD MATCH REC ===")
print(con.execute("SELECT * FROM mergeuni LIMIT 5").fetchdf())

# ================================================================
# Part 15: COMCARD processing + merge with MERGEALL
# ================================================================
con.execute("""
    CREATE OR REPLACE TABLE comcard_clean AS
    SELECT
        CAST(BRANCH_ABBR AS VARCHAR) AS BRANCH_ABBR,
        CAST(ACCTNOC AS VARCHAR)      AS ACCTNOC,
        CAST(ACCTSTATUS AS VARCHAR)   AS ACCTSTATUS,
        CAST(DATEOPEN AS VARCHAR)     AS DATEOPEN,
        CAST(DATECLSE AS VARCHAR)     AS DATECLSE
    FROM comcard
    ORDER BY ACCTNOC
""")

# Inner join COMCARD with mergeall, keep unique ACCTNOC
con.execute("""
    CREATE OR REPLACE TABLE mergecom AS
    SELECT DISTINCT ON (ACCTNOC) m.*
    FROM mergeall m
    INNER JOIN comcard_clean c
    ON m.ACCTNOC = c.ACCTNOC
""")

print("\n=== Part 15: COMCARD MATCH REC ===")
print(con.execute("SELECT * FROM mergecom LIMIT 5").fetchdf())

# ================================================================
# Part 16: Combine all merged dataframes into final output
# ================================================================

# Union all tables (diagonal relaxed → different schemas allowed)
con.execute("""
    CREATE OR REPLACE TABLE output AS
    SELECT * FROM mergedp
    UNION BY NAME
    SELECT * FROM mergeln
    UNION BY NAME
    SELECT * FROM mergesdb
    UNION BY NAME
    SELECT * FROM mergeuni
    UNION BY NAME
    SELECT * FROM mergecom
""")

# Ensure all required columns exist (add if missing)
required_cols = [
    "CUSTNO","ACCTNOC","OCCUP","MASCO2008","ALIASKEY","ALIAS",
    "CUSTNAME","DATEOPEN","DATECLSE","LEDGERBAL","BANKINDC","CITIZENSHIP",
    "APPL_CODE","PRODTY","DEMODESC","MASCODESC","JOINTACC","MSICCODE",
    "ACCTBRCH","BRANCH_ABBR","ACCTSTATUS","SICCODE"
]

for col in required_cols:
    con.execute(f"""
        ALTER TABLE output ADD COLUMN IF NOT EXISTS {col} VARCHAR
    """)

# Reorder columns
output_df = con.execute(f"""
    SELECT {",".join(required_cols)} FROM output
    LIMIT 5
""").fetchdf()

print("\n=== Part 16: Combined output preview ===")
print(output_df)


# ================================================================
# Part 17.1: Generate semicolon-delimited customer report
# ================================================================

con.execute("""
    CREATE OR REPLACE TABLE report_base AS
    SELECT * FROM mergedp3
    UNION BY NAME
    SELECT * FROM mergeln
    UNION BY NAME
    SELECT * FROM mergesdb
    UNION BY NAME
    SELECT * FROM mergeuni
    UNION BY NAME
    SELECT * FROM mergecom
""")

# Fill missing with NIL or 0 per logic
con.execute("""
    CREATE OR REPLACE TABLE report_df AS
    SELECT
        ROW_NUMBER() OVER() AS NO,
        ALIASKEY, ALIAS, CUSTNAME, CUSTNO,
        COALESCE(NULLIF(DEMODESC,''),'NIL') AS DEMODESC,
        COALESCE(NULLIF(MASCODESC,''),'NIL') AS MASCODESC,
        COALESCE(NULLIF(SICCODE,''),'NIL')   AS SICCODE,
        COALESCE(NULLIF(MSICDESC,''),'NIL') AS MSICDESC,
        ACCTNOC, BRANCH_ABBR, ACCTSTATUS, DATEOPEN, DATECLSE,
        SDBIND, SDBBRH,

        CASE WHEN APPL_CODE IN ('FD','CA','SA')
             THEN COALESCE(CURBAL,0) ELSE 0 END AS TEMP_CURBAL,

        CASE WHEN APPL_CODE IN ('FD','CA','SA') AND CURR_AMT_DR > 0
             THEN CURR_AMT_DR ELSE 0 END AS TEMP_CURR_AMT_DR,

        CASE WHEN APPL_CODE IN ('FD','CA','SA') AND CURR_AMT_CR > 0
             THEN CURR_AMT_CR ELSE 0 END AS TEMP_CURR_AMT_CR,

        CASE WHEN APPL_CODE IN ('FD','CA','SA') AND PREV_AMT_DR > 0
             THEN PREV_AMT_DR ELSE 0 END AS TEMP_PREV_AMT_DR,

        CASE WHEN APPL_CODE IN ('FD','CA','SA') AND PREV_AMT_CR > 0
             THEN PREV_AMT_CR ELSE 0 END AS TEMP_PREV_AMT_CR,

        CASE WHEN APPL_CODE IN ('FD','CA','SA') AND CURR_CYC_DR > 0
             THEN CURR_CYC_DR ELSE 0 END AS TEMP_CURR_CYC_DR,

        CASE WHEN APPL_CODE IN ('FD','CA','SA') AND CURR_CYC_CR > 0
             THEN CURR_CYC_CR ELSE 0 END AS TEMP_CURR_CYC_CR,

        CASE WHEN APPL_CODE IN ('FD','CA','SA') AND PREV_CYC_DR > 0
             THEN PREV_CYC_DR ELSE 0 END AS TEMP_PREV_CYC_DR,

        CASE WHEN APPL_CODE IN ('FD','CA','SA') AND PREV_CYC_CR > 0
             THEN PREV_CYC_CR ELSE 0 END AS TEMP_PREV_CYC_CR,

        CASE WHEN AMT_1 > 0 THEN AMT_1 ELSE 0 END AS TEMP_AMT_1,
        CASE WHEN AMT_2 > 0 THEN AMT_2 ELSE 0 END AS TEMP_AMT_2,
        CASE WHEN AMT_3 > 0 THEN AMT_3 ELSE 0 END AS TEMP_AMT_3,

        ACCT_PST_IND, ACCT_PST_REASON, TOT_HOLD,
        SEQID_1, DESC_1, SOURCE_1,
        SEQID_2, DESC_2, SOURCE_2,
        SEQID_3, DESC_3, SOURCE_3
    FROM report_base
""")

# Save parquet
report_arrow = con.table("report_df").to_arrow()
report_arrow.to_parquet("CMD_REPORT.parquet")

# Save CSV with semicolon separator
report_df = con.execute("SELECT * FROM report_df").fetchdf()
with open("CMDREPORT.csv", "w", encoding="utf-8") as f:
    f.write("LIST OF CUSTOMERS INFORMATION\n")
    f.write(";".join(report_df.columns) + "\n")
    for row in report_df.itertuples(index=False):
        f.write(";".join("" if v is None else str(v) for v in row) + "\n")
