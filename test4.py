import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path, get_hive_parquet
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year1, month1, day1 = batch_date.year, batch_date.month, batch_date.day

# ============================================================
# SETUP
# ============================================================
con = duckdb.connect()

# ============================================================
# STEP 1: Read NEWCHG, OLDCHG, NOCHG into DuckDB tables
# (assume parquet columns correspond to SAS positions; adapt names as necessary)
# ============================================================
con.execute(f"""
    CREATE TABLE newchg AS
    SELECT
        -- expected parquet columns (adapt if different names)
        CUSTNOX          AS CUSTNO,
        CUSTMNTDATE      AS CUSTMNTDATE,
        CUSTLASTOPER     AS CUSTLASTOPER,
        CUST_CODE        AS CUST_CODE,
        MSICCODE         AS MSICCODE
    FROM '{host_parquet_path("CIS_IDIC_DAILY_INEW.parquet")}'
    WHERE CUSTNO IS NOT NULL
""")

con.execute(f"""
    CREATE TABLE oldchg AS
    SELECT
        CUSTNOX          AS CUSTNO,
        CUSTMNTDATE      AS CUSTMNTDATEX,
        CUSTLASTOPER     AS CUSTLASTOPERX,
        CUST_CODE        AS CUST_CODEX,
        MSICCODE         AS MSICCODEX
    FROM '{host_parquet_path("CIS_IDIC_DAILY_IOLD.parquet")}'
    WHERE CUSTNO IS NOT NULL
""")

# NOCHG -> NEWCUST: include runtime timestamp, select only rows matching a given DATESTAMP
# We assume nochg has RUNTIMESTAMP, CUSTNO, CUSTMNTDATEX, CUSTLASTOPERX, CUST_CODE, MSICCODE
# The SAS used macro vars YEAR/MONTH/DAY to form DATESTAMP; here we'll use RDATE from control date.
con.execute(f"""
    CREATE TABLE nochg_raw AS
    SELECT
        RUNTIMESTAMP,
        CUSTNOX          AS CUSTNO,
        CUSTMNTDATE      AS CUSTMNTDATEX,
        CUSTLASTOPER     AS CUSTLASTOPERX,
        CUST_CODE,
        MSICCODE
    FROM '{host_parquet_path("CIS_IDIC_DAILY_NOCHG.parquet")}'
    WHERE CUSTNO IS NOT NULL
""")

# Make NEWCUST by filtering rows where substr(RUNTIMESTAMP,1,8) == RDATE
con.execute(f"""
    CREATE TABLE newcust AS
    SELECT
        RUNTIMESTAMP,
        CUSTNO,
        CUSTMNTDATEX,
        CUSTLASTOPERX,
        CUST_CODE,
        MSICCODE,
        '{batch_date}' AS DATESTAMP,
        SUBSTR(RUNTIMESTAMP,1,8) AS DATEREC,
        CASE WHEN COALESCE(CUST_CODE,'') <> '' THEN 'Y' ELSE 'N' END AS UPDMSIC,
        CASE WHEN COALESCE(MSICCODE,'') <> '' THEN 'Y' ELSE 'N' END AS UPDCCDE
    FROM nochg_raw
    WHERE SUBSTR(RUNTIMESTAMP,1,8) = '{batch_date}'
""")

# ============================================================
# STEP 2: Read RLEN file and filter ACCTCODE in ('DP','LN')
# Note: SAS used positional + PD2. for RLENCODE; expect numeric RLENCODE column.
# ============================================================
con.execute(f"""
    CREATE TABLE rlen AS
    SELECT
        ACCTNOC,
        TRIM(ACCTCODE) AS ACCTCODE,
        CUSTNO,
        CAST(RLENCODE AS INTEGER) AS RLENCODE,
        CAST(PRISEC AS INTEGER) AS PRISEC,
        LPAD(CAST(CAST(RLENCODE AS INTEGER) AS VARCHAR),3,'0') AS RLENCD
    FROM '{host_parquet_path("RLENCA.parquet")}'
    WHERE TRIM(ACCTCODE) IN ('DP','LN')
""")

# ============================================================
# STEP 3: Merge NEWCHG + OLDCHG (inner join on CUSTNO) => MERGE_A
# ============================================================
con.execute("""
    CREATE TABLE merge_a AS
    SELECT a.*, b.CUSTMNTDATEX, b.CUSTLASTOPERX, b.CUST_CODEX, b.MSICCODEX
    FROM newchg a
    JOIN oldchg b USING (CUSTNO)
""")

# ============================================================
# STEP 4: DTCHG -> find rows where MSIC or CUST_CODE changed
# ============================================================
con.execute("""
    CREATE TABLE dtchg AS
    SELECT
        CUSTNO,
        CUSTMNTDATE,
        CUSTLASTOPER,
        CUST_CODE,
        MSICCODE,
        CUSTMNTDATEX,
        CUSTLASTOPERX,
        CUST_CODEX,
        MSICCODEX,
        CASE WHEN COALESCE(MSICCODE,'') <> COALESCE(MSICCODEX,'') THEN 'Y' ELSE 'N' END AS UPDMSIC,
        CASE WHEN COALESCE(CUST_CODE,'') <> COALESCE(CUST_CODEX,'') THEN 'Y' ELSE 'N' END AS UPDCCDE
    FROM merge_a
    WHERE COALESCE(MSICCODE,'') <> COALESCE(MSICCODEX,'') OR COALESCE(CUST_CODE,'') <> COALESCE(CUST_CODEX,'')
""")

# ============================================================
# STEP 5: MIXALL = DTCHG UNION ALL NEWCUST
# ============================================================
con.execute("""
    CREATE TABLE mixall AS
    SELECT *, UPDMSIC, UPDCCDE FROM dtchg
    UNION ALL
    SELECT
      RUNTIMESTAMP, CUSTNO, CUSTMNTDATEX, CUSTLASTOPERX, CUST_CODE, MSICCODE,
      NULL, NULL, NULL, NULL, UPDMSIC, UPDCCDE
    FROM newcust
""")

# Normalize column names in mixall for subsequent join with rlen:
# We'll create a view-like table with expected columns:
con.execute("""
    CREATE TABLE mixall_norm AS
    SELECT
      CUSTNO,
      COALESCE(CUSTMNTDATE, CUSTMNTDATEX) AS CUSTMNTDATE,
      COALESCE(CUSTLASTOPER, CUSTLASTOPERX) AS CUSTLASTOPER,
      COALESCE(CUST_CODE, CUST_CODEX) AS CUST_CODE,
      COALESCE(MSICCODE, MSICCODEX) AS MSICCODE,
      UPDMSIC,
      UPDCCDE
    FROM mixall
""")

# ============================================================
# STEP 6: Merge MIXALL with RLEN by CUSTNO -> create DPLIST, BTLIST, LNALL, DPALL
# Logic implemented per SAS:
#   - if F and G (i.e. match in both)
#   - BTRADE = 'Y' if ACCTNOC startswith '025' or startswith '0285'
#   - DPALL: ACCTCODE == 'DP'
#   - BTLIST: ACCTCODE == 'LN' AND BTRADE == 'Y'
#   - LNALL: ACCTCODE == 'LN' AND BTRADE != 'Y'
#   - DPLIST: RLENCD == '020' and ACCTCODE == 'DP'
# ============================================================
con.execute("""
    CREATE TABLE merged_mix_rlen AS
    SELECT
        m.CUSTNO,
        r.ACCTNOC,
        TRIM(r.ACCTCODE) AS ACCTCODE,
        m.CUST_CODE,
        m.MSICCODE,
        m.UPDMSIC,
        m.UPDCCDE,
        r.RLENCD,
        CASE
            WHEN SUBSTR(coalesce(r.ACCTNOC,''),1,3) = '025' THEN 'Y'
            WHEN SUBSTR(coalesce(r.ACCTNOC,''),1,4) = '0285' THEN 'Y'
            ELSE 'N'
        END AS BTRADE
    FROM mixall_norm m
    JOIN rlen r USING (CUSTNO)
""")

# DPLIST: RLENCD == '020' AND ACCTCODE == 'DP'
dplist = """
    SELECT *
           ,{year1} AS year
           ,{month1} AS month
           ,{day1} AS day
    FROM merged_mix_rlen
    WHERE RLENCD = '020' AND ACCTCODE = 'DP'
""".format(year1=year1,month1=month1,day1=day1)

# DPALL: ACCTCODE == 'DP' (all RLEN values)
dpall = """
    SELECT *
           ,{year1} AS year
           ,{month1} AS month
           ,{day1} AS day
    FROM merged_mix_rlen
    WHERE ACCTCODE = 'DP'
""".format(year1=year1,month1=month1,day1=day1)

# BTLIST: ACCTCODE == 'LN' AND BTRADE == 'Y'
btlist = """
    SELECT *
           ,{year1} AS year
           ,{month1} AS month
           ,{day1} AS day
    FROM merged_mix_rlen
    WHERE ACCTCODE = 'LN' AND BTRADE = 'Y'
""".format(year1=year1,month1=month1,day1=day1)

# LNALL: ACCTCODE == 'LN' AND BTRADE != 'Y'
lnall = """
    SELECT *
           ,{year1} AS year
           ,{month1} AS month
           ,{day1} AS day
    FROM merged_mix_rlen
    WHERE ACCTCODE = 'LN' AND BTRADE = 'N'
""".format(year1=year1,month1=month1,day1=day1)

# ====================================
# STEP =7 - Export to CSV & parquet
# ====================================
queries = {
    "CBD_DPFILE"            : dplist,
    "CBD_LNFILE"            : dpall,
    "CBD_BTFILE"            : btlist,
    "BPM_DPFILE"            : lnall
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
