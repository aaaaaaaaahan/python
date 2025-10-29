error:
duckdb.duckdb.BinderException: Binder Error: Column "TELLER_ID" referenced that exists in the SELECT clause - but this column cannot be referenced before it is defined

program:
import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path, get_hive_parquet
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ============================================================
#  DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()
cis = get_hive_parquet('CIS_CUST_DAILY')

# ============================================================
#  LOAD INPUT TABLES
# ============================================================
con.execute(f"""
    CREATE OR REPLACE TABLE TBL_EMAIL AS
    SELECT CUSTNO
    FROM '{host_parquet_path("CIEMLDBT_FB.parquet")}'
""")

con.execute(f"""
    CREATE OR REPLACE TABLE RMK AS
    SELECT 
        CUSTNO,
        RMK_LINE_1 AS REMARKS
    FROM '{host_parquet_path("CCRIS_CISRMRK_EMAIL_FIRST.parquet")}'
""")

con.execute(f"""
    CREATE OR REPLACE TABLE CUS AS
    SELECT 
        CUSTNO, ALIAS, ALIASKEY, INDORG, CUSTNAME, ACCTCODE
    FROM read_parquet('{cis[0]}')
    WHERE INDORG = 'I'
      AND CUSTNAME <> ''
      AND ALIAS <> ''
      AND ACCTCODE <> ''
""")

# Remove duplicate CUSTNO
con.execute("CREATE OR REPLACE TABLE CUS AS SELECT DISTINCT ON (CUSTNO) * FROM CUS")

# ============================================================
#  STEP 1 - IDENTIFY CUSTOMER WITH/WITHOUT EMAIL
# ============================================================
# INSERT1: Customer exists in CUS but not in RMK
# DELETE1: Customer exists in both
con.execute("""
    CREATE OR REPLACE TABLE INSERT1 AS
    SELECT B.*
    FROM CUS B
    LEFT JOIN RMK A USING (CUSTNO)
    WHERE A.CUSTNO IS NULL
""")

con.execute("""
    CREATE OR REPLACE TABLE DELETE1 AS
    SELECT B.*
    FROM CUS B
    INNER JOIN RMK A USING (CUSTNO)
""")

# ============================================================
#  STEP 2 - COMPARE AGAINST TABLE CIEMLDBT (TBL_EMAIL)
# ============================================================
# INSERT2: in INSERT1 but not in TBL_EMAIL
# DELETE2: in DELETE1 and in TBL_EMAIL
con.execute("""
    CREATE OR REPLACE TABLE INSERT2 AS
    SELECT C.*
    FROM INSERT1 C
    LEFT JOIN TBL_EMAIL D USING (CUSTNO)
    WHERE D.CUSTNO IS NULL
""")

con.execute("""
    CREATE OR REPLACE TABLE DELETE2 AS
    SELECT E.*
    FROM DELETE1 E
    INNER JOIN TBL_EMAIL F USING (CUSTNO)
""")

# ============================================================
#  STEP 3 - ADD OUTPUT COLUMNS & EXPORT
# ============================================================
prompt_date = datetime.date(2001, 1, 1).strftime("%Y-%m-%d")

out1 = f"""
    SELECT 
        CUSTNO,
        ALIAS,
        ALIASKEY,
        '{prompt_date}' AS PROMPT_DATE,
        'INIT' AS TELLER_ID,
        'CIEMLFIL' AS REASON
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM INSERT2
""".format(year=year,month=month,day=day)

out2 = f"""
    SELECT 
        CUSTNO,
        ALIAS,
        ALIASKEY,
        '{prompt_date}' AS PROMPT_DATE,
        COALESCE(TELLER_ID, ' ') AS TELLER_ID,
        COALESCE(REASON, ' ') AS REASON
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM DELETE2
""".format(year=year,month=month,day=day)

# ============================================================
#  EXPORT RESULTS TO PARQUET & CSV
# ============================================================
queries = {
    "CIEMLFIL_INSERT"                 : out1,
    "CIEMLFIL_DELETE"                 : out2
}

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    con.execute(f"""
    COPY ({query})
    TO '{parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);  
     """)
    
    con.execute(f"""
    COPY ({query})
    TO '{csv_path}'
    (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);  
     """)
