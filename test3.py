import duckdb
import datetime
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
CURDD = day
CURMM = month
CURYY = year

# -----------------------------
# CONNECT DUCKDB
# -----------------------------
con = duckdb.connect()

# -----------------------------
# CREATE DUCKDB TABLES FROM PARQUET
# -----------------------------
con.execute(f"""
CREATE OR REPLACE TABLE PHONE AS
SELECT *,
       MAKE_DATE(CAST(UPDTYY AS INTEGER), CAST(UPDTMM AS INTEGER), CAST(UPDTDD AS INTEGER)) AS RECDT
FROM '{host_parquet_path("UNLOAD_CIPHONET_FB.parquet")}'
""")

# -----------------------------
# FILTER LOGIC (SAS IF &CURDD = UPDTDD AND &CURMM = UPDTMM THEN &CURYY > UPDTYY)
# -----------------------------
con.execute(f"""
CREATE OR REPLACE TABLE PHONE_FILTERED AS
SELECT 
    BANKNO,
    APPLCODE,
    CUSTNO,
    PHONETYPE,
    PHONEPAC,
    PHONEPREV,
    INDORG,
    FIRSTDATE,
    '0' AS PROMPTNO,
    PROMTSOURCE,
    PROMPTDATE, 
    PROMPTTIME, 
    UPDSOURCE,  
    RECDT AS UPDDATE,    
    UPDTIME,    
    UPDOPER,    
    TRXAPPLCODE,
    TRXAPPLNO,  
    NEWPHONE AS PHONENEW   
FROM PHONE
WHERE EXTRACT(DAY FROM RECDT) = {CURDD}
  AND EXTRACT(MONTH FROM RECDT) = {CURMM}
  AND EXTRACT(YEAR FROM RECDT) < {CURYY}
ORDER BY CUSTNO
""")
