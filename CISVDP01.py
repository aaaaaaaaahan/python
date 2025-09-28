import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# ================================================================
# Part 1: Setup DuckDB connection
# ================================================================
con = duckdb.connect()

# Assuming all inputs are already converted to Parquet
DEMOFILE   = "BANKCTRL_DEMOCODE.parquet"
RLENCAFILE = "BANKCTRL_RLENCODE_CA.parquet"
PBBRANCH   = "PBB_BRANCH.parquet"
FOREXGIA   = "SAP_PBB_FOREIGN_RATE.parquet"
SIGNATOR   = [f"SNGLVIEW_SIGN_FD{i}.parquet" for i in range(10,20)]
DORMFILE   = "FROZEN_INACTIVE_ACCT.parquet"
DPTRBALS   = "DPTRBLGS.parquet"
CUSTFILE   = "CIS_CUST_DAILY.parquet"
INDFILE    = "CIS_IDIC_DAILY_INDV.parquet"
IDICFILE   = "UNLOAD_CIBCCIST_FB.parquet"

# ================================================================
# OCCUPATION FILE
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE OCCUPATION AS
    SELECT
        substr(DEMOCATEGORY,1,5) AS DEMOCATEGORY,
        substr(OCCUPNUM,1,4)     AS OCCUPNUM,
        substr(OCCUPDESC,1,20)   AS OCCUPDESC,
        DEMOCATEGORY || OCCUPNUM AS OCCUPCD
    FROM parquet_scan('{DEMOFILE}')
    WHERE DEMOCATEGORY IN ('OCCUP','BUSIN')
""")

print("OCCUPATION FILE (first 5 rows):")
print(con.execute("SELECT * FROM OCCUPATION LIMIT 5").fetchdf())

# ================================================================
# RELATIONSHIP CODES FILE
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE RLENCA AS
    SELECT
        substr(RLENCATEGORY,1,2) AS RLENCATEGORY,
        CAST(RLENCODE AS INTEGER) AS RLENCODE,
        substr(RELATIONDESC,1,20) AS RELATIONDESC
    FROM parquet_scan('{RLENCAFILE}')
""")

print("RLENCA FILE (first 5 rows):")
print(con.execute("SELECT * FROM RLENCA LIMIT 5").fetchdf())

# ================================================================
# INDIVIDUAL FILE
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE INDV AS
    SELECT DISTINCT
        CUSTNO,
        CUSTNO AS CUSTNOX,
        EMPLOYMENT_TYPE,
        LAST_UPDATE_DATE
    FROM parquet_scan('{INDFILE}')
    WHERE EMPLOYMENT_TYPE IS NOT NULL AND EMPLOYMENT_TYPE <> ''
""")

print("INDV FILE (first 15 rows):")
print(con.execute("SELECT * FROM INDV LIMIT 15").fetchdf())

# ================================================================
# MISC CODE FILE
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE MSCO AS
    SELECT
        FIELDNAME,
        substr(MASCO2008,1,5) AS MASCO2008,
        substr(MSCDESC,1,150) AS MSCDESC
    FROM parquet_scan('{IDICFILE}')
    WHERE FIELDNAME = 'MASCO2008'
""")

con.execute(f"""
    CREATE OR REPLACE TABLE MSIC AS
    SELECT
        FIELDNAME,
        substr(MSICCODE,1,5) AS MSICCODE,
        substr(MSCDESC,1,150) AS MSCDESC
    FROM parquet_scan('{IDICFILE}')
    WHERE FIELDNAME = 'MSIC2008'
""")

# sort like PROC SORT
con.execute("CREATE OR REPLACE TABLE MSCO AS SELECT * FROM MSCO ORDER BY MASCO2008")
con.execute("CREATE OR REPLACE TABLE MSIC AS SELECT * FROM MSIC ORDER BY MSICCODE")

# ================================================================
# BRANCH FILE
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE PBBBRCH AS
    SELECT
        CAST(BRANCHNO AS INTEGER) AS BRANCHNO,
        substr(ACCTBRABBR,1,3) AS ACCTBRABBR
    FROM parquet_scan('{PBBRANCH}')
""")

print("PBB BRANCH FILE (first 5 rows):")
print(con.execute("SELECT * FROM PBBBRCH LIMIT 5").fetchdf())

# =
