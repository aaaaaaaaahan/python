import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
import datetime
import os

# ============================================================
# PATH CONFIGURATION
# ============================================================
# (Modify these to your actual paths)
host_parquet_path = '/host/cis/parquet/sas_parquet'
parquet_output_path = '/host/cis/parquet'
csv_output_path = '/host/cis/output'

# ============================================================
# DATE CONFIGURATION
# ============================================================
batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ============================================================
# DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()

# ============================================================
# INPUT PARQUET FILES
# ============================================================
rlen_files = [
    f"{host_parquet_path}/rlen_ca_ln02.parquet",
    f"{host_parquet_path}/rlen_ca_ln08.parquet"
]
borwgtor_parquet = f"{host_parquet_path}/sas_b033_liabfile_borwguar.parquet"

# ============================================================
# STEP 1: DUPLICATE REMOVAL (simulate ICETOOL SELECT FIRST)
# ============================================================
# Keep first occurrence based on key (ACCTNO + IND)
con.execute(f"""
    CREATE OR REPLACE TABLE borwguar_unq AS
    SELECT *
    FROM read_parquet('{borwgtor_parquet}')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY substr(ACCTNO,1,11), substr(IND,1,1) ORDER BY ACCTNO) = 1
""")

# ============================================================
# STEP 2: READ RLEN FILES & FILTER
# ============================================================
con.execute(f"""
    CREATE OR REPLACE TABLE rlen AS
    SELECT 
        substr(ACCTNO,1,11) AS ACCTNO,
        substr(ACCTCODE,1,5) AS ACCTCODE,
        substr(CUSTNO,1,11) AS CUSTNO,
        RLENCODE,
        PRISEC
    FROM read_parquet({rlen_files})
    WHERE PRISEC = 901
      AND RLENCODE IN (3,11,12,13,14,16,17,18,19,21,22,23,27,28)
""")

# ============================================================
# STEP 3: READ BORWGUAR.UNQ AS LOAN AND MAP STAT TO CODE
# ============================================================
con.execute(f"""
    CREATE OR REPLACE TABLE loan AS
    SELECT 
        substr(ACCTNO,1,11) AS ACCTNO,
        substr(IND,1,3) AS STAT,
        CASE 
            WHEN trim(IND) = 'B' THEN 20
            WHEN trim(IND) = 'G' THEN 17
            WHEN trim(IND) = 'B/G' THEN 28
            ELSE NULL
        END AS CODE
    FROM borwguar_unq
    WHERE IND IN ('B','G','B/G')
""")

# ============================================================
# STEP 4: MERGE LOGIC
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE merge1 AS
    SELECT 
        l.ACCTNO,
        r.ACCTCODE,
        r.CUSTNO,
        r.RLENCODE,
        l.CODE,
        l.STAT
    FROM loan l
    JOIN rlen r ON l.ACCTNO = r.ACCTNO
    WHERE NOT (
        l.CODE = r.RLENCODE
        OR (r.RLENCODE = 21 AND l.CODE = 17)
    )
""")

# ============================================================
# STEP 5: OUTPUT (SHOW BEFORE & AFTER EFFECT)
# ============================================================
outfile_table = con.execute("""
    SELECT 
        ACCTCODE,
        ACCTNO,
        CUSTNO,
        LPAD(CAST(RLENCODE AS VARCHAR),3,'0') AS RLENCODE,
        LPAD(CAST(CODE AS VARCHAR),3,'0') AS CODE,
        STAT
    FROM merge1
""").arrow()

outfile_path_parquet = f"{parquet_output_path}/cis_updrlen_borwgtor_{batch_date}.parquet"
outfile_path_csv = f"{csv_output_path}/cis_updrlen_borwgtor_{batch_date}.csv"
pq.write_table(outfile_table, outfile_path_parquet)
outfile_table.to_pandas().to_csv(outfile_path_csv, index=False)

# ============================================================
# STEP 6: OUTPUT FOR UPDATE (CIUPDRLN)
# ============================================================
updf_table = con.execute("""
    SELECT 
        '033' AS CONST1,
        ACCTCODE,
        ACCTNO,
        'CUST ' AS CONST2,
        CUSTNO,
        '901' AS CONST3,
        LPAD(CAST(CODE AS VARCHAR),3,'0') AS CODE
    FROM merge1
""").arrow()

updf_path_parquet = f"{parquet_output_path}/cis_updrlen_update_{batch_date}.parquet"
updf_path_csv = f"{csv_output_path}/cis_updrlen_update_{batch_date}.csv"
pq.write_table(updf_table, updf_path_parquet)
updf_table.to_pandas().to_csv(updf_path_csv, index=False)

# ============================================================
# COMPLETION LOG
# ============================================================
print("✅ CCRSRLEB Python Conversion Completed")
print(f"Output 1 (Before/After): {outfile_path_parquet}")
print(f"Output 2 (For Update): {updf_path_parquet}")
