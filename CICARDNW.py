import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# ================================================================
# 1. Open DuckDB connection
# ================================================================
con = duckdb.connect()

# ================================================================
# 2. Load input parquet (instead of INFILE UNIFILE)
# ================================================================
# Replace with your parquet path for CISNACCT_OCM
ufile = con.execute("""
    SELECT 
        CAST(SUBSTR(BRANCH, 1, 5) AS INTEGER)        AS BRANCH,
        CAST(ACCTCODE AS VARCHAR)                    AS ACCTCODE,
        CAST(ACCTNO AS VARCHAR)                      AS ACCTNO,
        CAST(OLDIC AS VARCHAR)                       AS OLDIC,
        CAST(INDORG AS VARCHAR)                      AS INDORG,
        CAST(DATEOPEN AS VARCHAR)                    AS DATEOPEN,
        CAST(ALIASKEY AS VARCHAR)                    AS ALIASKEY,
        CAST(ALIAS AS VARCHAR)                       AS ALIAS,
        CAST(NAME AS VARCHAR)                        AS NAME
    FROM read_parquet('CISNACCT_OCM.parquet')
""").arrow()

# ================================================================
# 3. Sort (PROC SORT BY ACCTNO NAME DATEOPEN ACCTCODE)
# ================================================================
ufile_sorted = con.execute("""
    SELECT * 
    FROM ufile
    ORDER BY ACCTNO, NAME, DATEOPEN, ACCTCODE
""").arrow()

# Show first 20 rows (PROC PRINT)
print(ufile_sorted.to_pandas().head(20))

# ================================================================
# 4. OUTPUT DATA (equivalent to DATA TEMPOUT + PUT)
# ================================================================
# In SAS: formatted fixed-width; here: structured columns
# Add constant + rearrange columns
tempo_out = con.execute("""
    SELECT
        '033' AS COMPANY,
        DATEOPEN,
        LPAD(CAST(BRANCH AS VARCHAR), 5, '0') AS BRANCH,
        ACCTCODE,
        ACCTNO,
        INDORG,
        NAME,
        ALIASKEY,
        ALIAS,
        OLDIC
    FROM ufile
""").arrow()

# ================================================================
# 5. Write output with PyArrow (OUTFILE in SAS)
# ================================================================
pq.write_table(tempo_out, "CIS_CARDNEW.parquet")
