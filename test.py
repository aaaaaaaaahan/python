import duckdb
import pyarrow as pa
from datetime import datetime

# ================================================================
# Part 1: DATE PROCESSING
# ================================================================
today = datetime.now()
LOADDATE = today.strftime("%Y-%m-%d")  # equivalent to SAS YYMMDD10.
print(f"Load date: {LOADDATE}")

con = duckdb.connect(database=":memory:")

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
