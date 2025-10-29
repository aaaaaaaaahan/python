import duckdb
from CIS_PY_READER import host_parquet_path, get_hive_parquet, parquet_output_path, csv_output_path
import datetime

# ============================================================
#  DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()
print("✅ Connected to DuckDB")

# ============================================================
#  LOAD INPUTS
# ============================================================
print("\n📂 Loading input parquet files...")

con.execute(f"""
    CREATE OR REPLACE TABLE TBL_EMAIL AS
    SELECT CUSTNO
    FROM read_parquet('{host_parquet_path("CIEMLDBT_FB.parquet")}')
""")
print("\n🔹 TBL_EMAIL sample:")
print(con.execute("SELECT * FROM TBL_EMAIL LIMIT 10").fetchdf())

con.execute(f"""
    CREATE OR REPLACE TABLE RMK AS
    SELECT 
        CUSTNO,
        RMK_LINE_1 AS REMARKS
    FROM read_parquet('{host_parquet_path("CCRIS_CISRMRK_EMAIL_FIRST.parquet")}')
""")
print("\n🔹 RMK sample:")
print(con.execute("SELECT * FROM RMK LIMIT 10").fetchdf())

cis_paths = get_hive_parquet('CIS_CUST_DAILY')
print("\n🔹 Using CIS_CUST_DAILY parquet(s):")
for path in cis_paths:
    print(f"   - {path}")

con.execute(f"""
    CREATE OR REPLACE TABLE CUS AS
    SELECT 
        CUSTNO, ALIAS, ALIASKEY, INDORG, CUSTNAME, ACCTCODE
    FROM read_parquet('{cis_paths[0]}')
    WHERE INDORG = 'I'
      AND CUSTNAME <> ''
      AND ALIAS <> ''
      AND ACCTCODE <> ''
""")
print("\n🔹 CUS after filter (INDORG='I', no blanks):")
print(con.execute("SELECT * FROM CUS LIMIT 10").fetchdf())

# Remove duplicates
con.execute("CREATE OR REPLACE TABLE CUS AS SELECT DISTINCT ON (CUSTNO) * FROM CUS")
print("\n🔹 CUS (distinct CUSTNO):")
print(con.execute("SELECT * FROM CUS LIMIT 10").fetchdf())

# ============================================================
#  STEP 1: Identify INSERT1 / DELETE1
# ============================================================
print("\n🧩 Step 1: Identify INSERT1 and DELETE1 (merge RMK + CUS)")
con.execute("""
    CREATE OR REPLACE TABLE INSERT1 AS
    SELECT B.* FROM RMK A
    RIGHT JOIN CUS B ON A.CUSTNO = B.CUSTNO
    WHERE A.CUSTNO IS NULL
""")
print("\n🔹 INSERT1 sample:")
print(con.execute("SELECT * FROM INSERT1 LIMIT 10").fetchdf())

con.execute("""
    CREATE OR REPLACE TABLE DELETE1 AS
    SELECT B.*, A.REMARKS FROM RMK A
    INNER JOIN CUS B ON A.CUSTNO = B.CUSTNO
""")
print("\n🔹 DELETE1 sample:")
print(con.execute("SELECT * FROM DELETE1 LIMIT 10").fetchdf())

# ============================================================
#  STEP 2: Compare against TBL_EMAIL
# ============================================================
print("\n📊 Step 2: Compare against TBL_EMAIL")

con.execute("""
    CREATE OR REPLACE TABLE INSERT2 AS
    SELECT C.*
    FROM INSERT1 C
    LEFT JOIN TBL_EMAIL D ON C.CUSTNO = D.CUSTNO
    WHERE D.CUSTNO IS NULL
""")
print("\n🔹 INSERT2 (Confirm insert):")
print(con.execute("SELECT * FROM INSERT2 LIMIT 10").fetchdf())

con.execute("""
    CREATE OR REPLACE TABLE DELETE2 AS
    SELECT E.*
    FROM DELETE1 E
    INNER JOIN TBL_EMAIL F ON E.CUSTNO = F.CUSTNO
""")
print("\n🔹 DELETE2 (Confirm delete):")
print(con.execute("SELECT * FROM DELETE2 LIMIT 10").fetchdf())

# ============================================================
#  STEP 3: Assign PROMPT_DATE and Output
# ============================================================
print("\n🧾 Step 3: Finalize outputs")

con.execute("""
    CREATE OR REPLACE TABLE OUT_INSERT AS
    SELECT 
        CUSTNO,
        ALIAS,
        ALIASKEY,
        '2001-01-01' AS PROMPT_DATE,
        'INIT' AS TELLER_ID,
        'CIEMLFIL' AS REASON
    FROM INSERT2
""")
print("\n🔹 OUT_INSERT sample:")
print(con.execute("SELECT * FROM OUT_INSERT LIMIT 10").fetchdf())

con.execute("""
    CREATE OR REPLACE TABLE OUT_DELETE AS
    SELECT 
        CUSTNO,
        ALIAS,
        ALIASKEY,
        '2001-01-01' AS PROMPT_DATE,
        TELLER_ID,
        REASON
    FROM DELETE2
""")
print("\n🔹 OUT_DELETE sample:")
print(con.execute("SELECT * FROM OUT_DELETE LIMIT 10").fetchdf())
