import duckdb
from CIS_PY_READER_JKH import host_parquet_path,parquet_output_path,csv_output_path, get_hive_parquet
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ==================================
# CONNECT TO DUCKDB
# ==================================
con = duckdb.connect()
CIS, year, month, day = get_hive_parquet('AMLHRC_EXTRACT_MASSCLS')

# ====================================
# STEP 1 - Load and filter CUST File
# ====================================
con.execute(f"""
    CREATE OR REPLACE TABLE CIS AS
    SELECT *
    FROM read_parquet({CIS})
""")

# ==================================
# STEP 4 - PRINT SAMPLE (OBS=5)
# ==================================
out1 = """
    SELECT *
    FROM CIS
"""

queries = {
    "test_jkh"            : out1
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
