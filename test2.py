import duckdb
import datetime
from pathlib import Path

# -----------------------------
# CONFIG & FILE PATHS
# -----------------------------
CTRLDATE_PARQUET = "SRSCTRL1.parquet"  # Not really used, just kept for reference
CIPHONET_PARQUET = "CIPHONET.parquet"

OUTPUT_PARQUET = "CIPHONET_RESET.parquet"
OUTPUT_TXT = "CIPHONET_RESET.txt"

# -----------------------------
# GET CURRENT DATE
# -----------------------------
today = datetime.date.today()
CURDD = today.day
CURMM = today.month
CURYY = today.year

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
       MAKEDATE(UPDTYY, UPDTMM, UPDTDD) AS RECDT
FROM read_parquet('{CIPHONET_PARQUET}')
""")

# -----------------------------
# FILTER LOGIC (SAS IF &CURDD = UPDTDD AND &CURMM = UPDTMM THEN &CURYY > UPDTYY)
# -----------------------------
con.execute(f"""
CREATE OR REPLACE TABLE PHONE_FILTERED AS
SELECT *
FROM PHONE
WHERE UPDTDD = {CURDD}
  AND UPDTMM = {CURMM}
  AND {CURYY} > UPDTYY
ORDER BY CUSTNO
""")

# -----------------------------
# WRITE PARQUET OUTPUT
# -----------------------------
con.execute(f"""
COPY PHONE_FILTERED TO '{OUTPUT_PARQUET}' (FORMAT PARQUET)
""")

# -----------------------------
# WRITE FIXED-WIDTH TXT OUTPUT
# -----------------------------
# DuckDB can use string formatting inside SQL
# Note: LPAD/RPAD is used to match SAS column widths
query_txt = f"""
SELECT
    RPAD(BANKNO, 3, ' ') ||
    RPAD(APPLCODE, 5, ' ') ||
    RPAD(CUSTNO, 11, ' ') ||
    RPAD(PHONETYPE, 15, ' ') ||
    LPAD(CAST(PHONEPAC AS VARCHAR), 8, '0') ||
    LPAD(CAST(PHONEPREV AS VARCHAR), 8, '0') ||
    RPAD(INDORG, 1, ' ') ||
    RPAD(FIRSTDATE, 10, ' ') ||
    '0' ||  -- PROMPTNO reset
    RPAD(PROMTSOURCE, 5, ' ') ||
    RPAD(PROMPTDATE, 10, ' ') ||
    RPAD(PROMPTTIME, 10, ' ') ||
    RPAD(UPDSOURCE, 5, ' ') ||
    LPAD(CAST(UPDTYY AS VARCHAR), 4, '0') ||
    LPAD(CAST(UPDTMM AS VARCHAR), 2, '0') ||
    LPAD(CAST(UPDTDD AS VARCHAR), 2, '0') ||
    RPAD(UPDTIME, 8, ' ') ||
    RPAD(UPDOPER, 8, ' ') ||
    RPAD(TRXAPPLCODE, 5, ' ') ||
    RPAD(TRXAPPLNO, 20, ' ') ||
    LPAD(CAST(PHONENEW AS VARCHAR), 8, '0') AS line
FROM PHONE_FILTERED
"""

# Export fixed-width TXT
con.execute(f"COPY ({query_txt}) TO '{OUTPUT_TXT}' (FORMAT CSV, DELIMITER '', HEADER FALSE)")

print("DuckDB processing completed. Parquet & TXT outputs generated.")
