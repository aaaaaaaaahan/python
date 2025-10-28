import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ============================================================
# DATE SETUP (Equivalent to SAS SYMPUT for &CURRDT)
# ============================================================
today = datetime.date.today()

# ============================================================
# DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()

# ============================================================
# PROCESSING: Extract YYYY, MM, DD from EXPIRE_DATE and build REPDT
# ============================================================
con.execute(f"""
    WITH tmp AS (
        SELECT
            BANK_NO,
            APPL_CODE,
            APPL_NO,
            RMK_KEYWORD,
            RMK_LINE_1,
            RMK_LINE_2,
            RMK_LINE_3,
            RMK_LINE_4,
            RMK_LINE_5,
            EXPIRE_DATE,
            TRY_CAST(SUBSTR(EXPIRE_DATE, 1, 4) AS INTEGER) AS YYYY,
            TRY_CAST(SUBSTR(EXPIRE_DATE, 5, 2) AS INTEGER) AS MM,
            TRY_CAST(SUBSTR(EXPIRE_DATE, 7, 2) AS INTEGER) AS DD
        FROM '{host_parquet_path("CIRMRKS_FB.parquet")}'
    )
    SELECT 
        *,
        MAKE_DATE(YYYY, MM, DD) AS REPDT
    FROM tmp
    WHERE RMK_KEYWORD IN ('VALID', 'PASSPORT', 'MMTOH')
      AND MAKE_DATE(YYYY, MM, DD) >= '{today}'
""")

# ============================================================
# OUTPUT TO PARQUET & CSV
# ============================================================
out = """
    SELECT
        BANK_NO,
        APPL_CODE,
        APPL_NO,
        RMK_KEYWORD,
        EXPIRE_DATE,
        RMK_LINE_1,
        RMK_LINE_2,
        RMK_LINE_3,
        RMK_LINE_4,
        RMK_LINE_5
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM OUT_DELETE
""".format(year=year,month=month,day=day)

queries = {
    "REMARKS_VALID_EXPIRE"                 : out,
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
