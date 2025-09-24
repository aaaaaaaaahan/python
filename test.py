import duckdb
import pyarrow as pa
from datetime import datetime

con = duckdb.connect(database=":memory:")

# ================================================================
# Part 1: DATE PROCESSING
# ================================================================
today = datetime.now()
LOADDATE = today.strftime("%Y-%m-%d")  # equivalent to SAS YYMMDD10.
print(f"Load date: {LOADDATE}")

# ================================================================
# Part 2: ALIAS FILE INPUT
# ================================================================
alias_arrow = con.execute("""
    SELECT 
        CAST(ALIASKEY AS VARCHAR) AS ALIASKEY,
        CAST(ALIAS AS VARCHAR)    AS ALIAS
    FROM 'ALIAS.parquet'
    ORDER BY ALIASKEY, ALIAS
""").arrow()
print("\n=== Part 2: ALIASFL ===")
print(alias_arrow.to_pandas().head(5))

# ================================================================
# Part 3: BRANCH FILE PBBRANCH
# ================================================================
pbbrch_arrow = con.execute("""
    SELECT DISTINCT ON (ACCTBRCH)
        CAST(ACCTBRCH AS VARCHAR)     AS ACCTBRCH,
        CAST(BRANCH_ABBR AS VARCHAR)  AS BRANCH_ABBR
    FROM 'BRANCH.parquet'
    ORDER BY ACCTBRCH
""").arrow()
print("\n=== Part 3: BRANCH ===")
print(pbbrch_arrow.to_pandas().head(5))

# ================================================================
# Part 4: OCCUP FILE
# ================================================================
occupfl_arrow = con.execute("""
    SELECT 
        CAST(TYPE AS VARCHAR)      AS TYPE,
        CAST(DEMOCODE AS VARCHAR)  AS DEMOCODE,
        CAST(DEMODESC AS VARCHAR)  AS DEMODESC
    FROM 'BANKCTRL_DEMOCODE.parquet'
    WHERE TYPE = 'OCCUP'
    ORDER BY DEMOCODE
""").arrow()
print("\n=== Part 4: OCCUPAT ===")
print(occupfl_arrow.to_pandas().head(5))

# ================================================================
# Part 5: MASCO FILE
# ================================================================
mascofl_arrow = con.execute("""
    SELECT 
        CAST(MASCO2008 AS VARCHAR) AS MASCO2008,
        CAST(MASCODESC AS VARCHAR) AS MASCODESC
    FROM 'MASCOFL.parquet'
    ORDER BY MASCO2008
""").arrow()
print("\n=== Part 5: MASCO ===")
print(mascofl_arrow.to_pandas().head(5))

# ================================================================
# Part 6: MSIC FILE
# ================================================================
msicfl_arrow = con.execute("""
    SELECT 
        CAST(MSICCODE AS VARCHAR) AS MSICCODE,
        CAST(MSICDESC AS VARCHAR) AS MSICDESC
    FROM 'MSICFL.parquet'
    ORDER BY MSICCODE
""").arrow()
print("\n=== Part 6: MSIC ===")
print(msicfl_arrow.to_pandas().head(5))

# ================================================================
# Part 7: STATEMENT CYCLE FILE
# ================================================================
cyclefl_arrow = con.execute("""
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
    FROM 'CYCLEFL.parquet'
    ORDER BY ACCTNOC
""").arrow()
print("\n=== Part 7: STATEMENT CYCLE FILE ===")
print(cyclefl_arrow.to_pandas().head(5))

# ================================================================
# Part 8: POST INDICATOR FILE
# ================================================================
postfl_arrow = con.execute("""
    SELECT 
        CAST(ACCTNOC AS VARCHAR)         AS ACCTNOC,
        CAST(ACCT_PST_IND AS VARCHAR)    AS ACCT_PST_IND,
        CAST(ACCT_PST_REASON AS VARCHAR) AS ACCT_PST_REASON
    FROM 'POSTFL.parquet'
    ORDER BY ACCTNOC
""").arrow()
print("\n=== Part 8: POST INDICATOR FILE ===")
print(postfl_arrow.to_pandas().head(5))

# ================================================================
# Part 9: DEPOFL
# ================================================================
depofl_arrow = con.execute("""
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
    FROM 'DEPOFL.parquet'
    ORDER BY ACCTNOC
""").arrow()
print("\n=== Part 9: ADDITIONAL DP INPUT ===")
print(depofl_arrow.to_pandas().head(5))
