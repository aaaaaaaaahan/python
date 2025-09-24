import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

#--------------------------------#
# Open DuckDB in-memory database #
#--------------------------------#
con = duckdb.connect()

# ================================================================
# Part 1: DATE PROCESSING
# ================================================================
#today = datetime.now()
#LOADDATE = today.strftime("%Y-%m-%d")  # equivalent to SAS YYMMDD10.
#print(f"Load date: {LOADDATE}")

# ================================================================
# Part 2: ALIAS FILE INPUT
# ================================================================
con.execute(f"""
    CREATE VIEW alias AS
    SELECT 
        CAST(ALIASKEY AS VARCHAR) AS ALIASKEY,
        CAST(ALIAS AS VARCHAR)    AS ALIAS
    FROM '{host_parquet_path("CMD_ALIAS.parquet")}'
    ORDER BY ALIASKEY, ALIAS
""")

# ================================================================
# Part 3: BRANCH FILE PBBRANCH
# ================================================================
con.execute(f"""
    CREATE VIEW pbbrch AS
    SELECT DISTINCT ON (ACCTBRCH)
        CAST(ACCTBRCH AS VARCHAR)     AS ACCTBRCH,
        CAST(BRANCH_ABBR AS VARCHAR)  AS BRANCH_ABBR
    FROM '{host_parquet_path("PBBBRCH.parquet")}'
    ORDER BY ACCTBRCH
""")

# ================================================================
# Part 4: OCCUP FILE
# ================================================================
con.execute(f"""
    CREATE VIEW occupfl AS
    SELECT 
        CAST(TYPE AS VARCHAR)      AS TYPE,
        CAST(DEMOCODE AS VARCHAR)  AS DEMOCODE,
        CAST(DEMODESC AS VARCHAR)  AS DEMODESC
    FROM '{host_parquet_path("BANKCTRL_DEMOCODE.parquet")}'
    WHERE TYPE = 'OCCUP'
    ORDER BY DEMOCODE
""")

# ================================================================
# Part 5: MASCO FILE
# ================================================================
con.execute(f"""
    CREATE VIEW mascofl AS
    SELECT 
        CAST(MASCO2008 AS VARCHAR) AS MASCO2008,
        CAST(MASCODESC AS VARCHAR) AS MASCODESC
    FROM '{host_parquet_path("BANKCTRL_MISC10.parquet")}'
    ORDER BY MASCO2008
""")

# ================================================================
# Part 6: MSIC FILE
# ================================================================
con.execute(f"""
    CREATE VIEW msicfl AS
    SELECT 
        CAST(MSICCODE AS VARCHAR) AS MSICCODE,
        CAST(MSICDESC AS VARCHAR) AS MSICDESC
    FROM '{host_parquet_path("BANKCTRL_MISC9.parquet")}'
    ORDER BY MSICCODE
""")

# ================================================================
# Part 7: STATEMENT CYCLE FILE
# ================================================================
con.execute(f"""
    CREATE VIEW cyclefl AS
    SELECT 
        LPAD(CAST(ACCTNO AS VARCHAR), 11, '0') AS ACCTNOC,
        CAST(ACCTNAME AS VARCHAR)              AS ACCTNAME,
        TRY_CAST(CURR_CYC_DR AS BIGINT)        AS CURR_CYC_DR,
        TRY_CAST(CURR_AMT_DR AS DOUBLE)        AS CURR_AMT_DR,
        TRY_CAST(CURR_CYC_CR AS BIGINT)        AS CURR_CYC_CR,
        TRY_CAST(CURR_AMT_CR AS DOUBLE)        AS CURR_AMT_CR,
        TRY_CAST(PREV_CYC_DR AS BIGINT)        AS PREV_CYC_DR,
        TRY_CAST(PREV_AMT_DR AS DOUBLE)        AS PREV_AMT_DR,
        TRY_CAST(PREV_CYC_CR AS BIGINT)        AS PREV_CYC_CR,
        TRY_CAST(PREV_AMT_CR AS DOUBLE)        AS PREV_AMT_CR
    FROM '{host_parquet_path("STMT_INQ_FILE.parquet")}'
    ORDER BY ACCTNOC
""")

# ================================================================
# Part 8: POST INDICATOR FILE
# ================================================================
con.execute(f"""
    CREATE VIEW postfl AS
    SELECT 
        CAST(ACCTNOC AS VARCHAR)         AS ACCTNOC,
        CAST(ACCT_PST_IND AS VARCHAR)    AS ACCT_PST_IND,
        CAST(ACCT_PST_REASON AS VARCHAR) AS ACCT_PST_REASON
    FROM '{host_parquet_path("POST_IND_EXT.parquet")}'
    ORDER BY ACCTNOC
""")

# ================================================================
# Part 9: DEPOFL
# ================================================================
con.execute(f"""
    CREATE VIEW depofl AS
    SELECT 
        LPAD(CAST(ACCTNO AS VARCHAR), 11, '0') AS ACCTNOC,
        CAST(SEQID_1 AS VARCHAR)  AS SEQID_1,
        CAST(SEQID_2 AS VARCHAR)  AS SEQID_2,
        CAST(SEQID_3 AS VARCHAR)  AS SEQID_3,
        TRY_CAST(AMT_1 AS DOUBLE) AS AMT_1,
        TRY_CAST(AMT_2 AS DOUBLE) AS AMT_2,
        TRY_CAST(AMT_3 AS DOUBLE) AS AMT_3,
        CAST(DESC_1 AS VARCHAR)   AS DESC_1,
        CAST(DESC_2 AS VARCHAR)   AS DESC_2,
        CAST(DESC_3 AS VARCHAR)   AS DESC_3,
        CAST(SOURCE_1 AS VARCHAR) AS SOURCE_1,
        CAST(SOURCE_2 AS VARCHAR) AS SOURCE_2,
        CAST(SOURCE_3 AS VARCHAR) AS SOURCE_3,
        CAST(TOT_HOLD AS VARCHAR) AS TOT_HOLD
    FROM '{host_parquet_path("CNTMAX3_TRANSPO.parquet")}'
    ORDER BY ACCTNOC
""")
