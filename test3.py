import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow.compute as pc

from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

# ==================================
# CONNECT TO DUCKDB
# ==================================
con = duckdb.connect()

# ====================================
# STEP 1 - Load and filter CUST File
# ====================================
con.execute(f"""
    CREATE OR REPLACE TABLE CIS AS
    SELECT 
        ALIASKEY,
        ALIAS,   
        CUSTNAME,
        CUSTNO,  
        CUSTBRCH
    FROM cis
    WHERE CUSTNAME <> ''
      AND ALIASKEY = 'IC'
      AND INDORG = 'I'
      AND CITIZENSHIP = 'MY'
      AND RACE = 'O'
""")

# ==================================
# STEP 2 - REMOVE DUPLICATES (BY CUSTNO)
# ==================================
out1 = CIS.drop_duplicates(subset=["CUSTNO"])

# ==================================
# STEP 4 - PRINT SAMPLE (OBS=5)
# ==================================
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
