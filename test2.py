import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# ==================================
# Input and Output paths
# ==================================
cisfile_path = "CIS_CUST_DAILY.parquet"
rmkcafle_path = "CCRIS_CISRMRK_ACC.parquet"

enhfile_path = "CCRIS_CISRMRK_ACC_ENH.parquet"
rricust_path = "CCRIS_CISRMRK_ACC_RRICUST.parquet"
dup_path = "CCRIS_CISRMRK_ACC_DUP.parquet"

# ==================================
# Connect to DuckDB
# ==================================
con = duckdb.connect()

# ==================================
# Step 1 - Load CIS file
# ==================================
con.execute(f"""
    CREATE OR REPLACE TABLE CIS AS
    SELECT 
        CUSTNO,
        ACCTNOC,
        ACCTCODE,
        PRISEC
    FROM read_parquet('{cisfile_path}')
    WHERE ACCTNO > 1000000000
""")

# Deduplicate
con.execute("""
    CREATE OR REPLACE TABLE CIS AS
    SELECT DISTINCT * 
    FROM CIS
""")

print("Sample CIS:")
print(con.execute("SELECT * FROM CIS LIMIT 20").fetchdf())

# ==================================
# Step 2 - Load Remark CA file
# ==================================
con.execute(f"""
    CREATE OR REPLACE TABLE RMK_CA AS
    SELECT 
        BANKNO,
        ACCTCODE,
        ACCTNOC,
        EFF_DATE,
        RMK_KEYWORD,
        RMK_LINE_1,
        RMK_LINE_2,
        RMK_LINE_3,
        RMK_LINE_4,
        RMK_LINE_5,
        RMK_OPERATOR,
        EXPIRE_DATE,
        LAST_MNT_DATE
    FROM read_parquet('{rmkcafle_path}')
""")

print("Sample RMK_CA:")
print(con.execute("SELECT * FROM RMK_CA LIMIT 20").fetchdf())

# ==================================
# Step 3 - Merge RMK_CA with CIS
# ==================================
con.execute("""
    CREATE OR REPLACE TABLE ENH_RMRK AS
    SELECT 
        r.*,
        c.CUSTNO,
        c.PRISEC
    FROM RMK_CA r
    LEFT JOIN CIS c
    ON r.ACCTCODE = c.ACCTCODE
   AND r.ACCTNOC = c.ACCTNOC
""")

# Save ENHFILE
enh_arrow = con.execute("SELECT * FROM ENH_RMRK").arrow()
pq.write_table(enh_arrow, enhfile_path)

print("Enhanced remark file written:", enhfile_path)

# ==================================
# Step 4 - Segregate RRICUST (keyword filter + sort)
# ==================================
con.execute("""
    CREATE OR REPLACE TABLE RRICUST AS
    SELECT *
    FROM ENH_RMRK
    WHERE RMK_KEYWORD = 'RRICUST'
    ORDER BY ACCTCODE, ACCTNOC, EFF_DATE
""")

rricust_arrow = con.execute("SELECT * FROM RRICUST").arrow()
pq.write_table(rricust_arrow, rricust_path)

print("RRICUST file written:", rricust_path)

# ==================================
# Step 5 - Get duplicate remarks (based on first 25 chars + 308 chars block)
# Simulating ICETOOL SELECT ALLDUPS
# ==================================
con.execute("""
    CREATE OR REPLACE TABLE DUP AS
    SELECT *
    FROM ENH_RMRK
    WHERE (ACCTCODE || ACCTNOC || RMK_KEYWORD || RMK_LINE_1 || RMK_LINE_2 || RMK_LINE_3 || RMK_LINE_4 || RMK_LINE_5)
          IN (
              SELECT (ACCTCODE || ACCTNOC || RMK_KEYWORD || RMK_LINE_1 || RMK_LINE_2 || RMK_LINE_3 || RMK_LINE_4 || RMK_LINE_5)
              FROM ENH_RMRK
              GROUP BY 1
              HAVING COUNT(*) > 1
          )
""")

dup_arrow = con.execute("SELECT * FROM DUP").arrow()
pq.write_table(dup_arrow, dup_path)

print("Duplicate remark file written:", dup_path)

