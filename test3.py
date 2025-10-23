import duckdb
from CIS_PY_READER_copy import host_parquet_path,parquet_output_path,csv_output_path, get_hive_parquet
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year1, month1, day1 = batch_date.year, batch_date.month, batch_date.day

#---------------------------------------------------------------------#
# Original Program: CCRSRACE                                          #
#---------------------------------------------------------------------#
# ESMR2015-707 CIS CUSTOMER DEMOGRAPHIC ETHNIC CODE                   #
# INITIALIZE DATASETS *                                               #
#---------------------------------------------------------------------#

# ==================================
# CONNECT TO DUCKDB
# ==================================
con = duckdb.connect()
cis = get_hive_parquet('CIS_CUST_DAILY', generations=3)

# ====================================
# STEP 1 - Load and filter CUST File
# ====================================
con.execute(f"""
    CREATE OR REPLACE TABLE CIS AS
    SELECT DISTINCT ON (CUSTNO)
        ALIASKEY,
        ALIAS,   
        CUSTNAME,
        CUSTNO,  
        LPAD(CAST(CAST(CUSTBRCH AS INTEGER) AS VARCHAR), 3, '0') AS CUSTBRCH
    FROM read_parquet('{cis[3]}')
    WHERE CUSTNAME <> ''
      AND ALIASKEY = 'IC'
      AND INDORG = 'I'
      AND CITIZENSHIP = 'MY'
      AND RACE = 'O'
    ORDER BY CUSTNO
""")

# ==================================
# STEP 4 - PRINT SAMPLE (OBS=5)
# ==================================
out1 = """
    SELECT *
           ,{year1} AS year
           ,{month1} AS month
           ,{day1} AS day
    FROM CIS
""".format(year1=year1,month1=month1,day1=day1)

queries = {
    "CIS_RACE"            : out1
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
