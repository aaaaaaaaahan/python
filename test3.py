import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

#---------------------------------------------------------------------#
# Original Program: CIHRCPUR                                          #
#---------------------------------------------------------------------#
# ESMR 2023-0862 PURGE HRC RECORDS MORE THAN 60 DAYS FROM DB2         #
#                WITH APPROVAL STATUS '05' OR '06'                    #
#---------------------------------------------------------------------#

# ---------------------------
# Connect to DuckDB
# ---------------------------
con = duckdb.connect()

# ---------------------------------------------------------------------
# STEP 1: FILTER DATA (equivalent to SAS DATA step)
# ---------------------------------------------------------------------
# Apply the same filters and computed columns
con.execute(f"""
    CREATE TABLE hrc_filtered AS
    SELECT
        *,
        substring(UPDATEDATE, 1, 4) AS UPDDATE,
        CASE 
            WHEN ACCTNO != ' ' AND POSITION('Noted by' IN HOVERIFYREMARKS) <= 0 THEN 1 
            ELSE 0 
        END AS HOEPDNOTE,
        CASE 
            WHEN ACCTNO != ' ' AND POSITION('Noted by' IN HOVERIFYREMARKS) > 0 THEN 1 
            ELSE 0 
        END AS HOENOTED
    FROM '{host_parquet_path("UNLOAD_CIHRCAPT_FB.parquet")}'
    WHERE UPDDATE = '{year}'
      AND ACCTTYPE IN ('CA','SA','SDB','FD','FC','FCI','O','FDF')
      AND APPROVALSTATUS = '08'
""")

print("STEP 1:")
print(con.execute("SELECT * FROM hrc_filtered LIMIT 500").fetchdf())

# ---------------------------------------------------------------------
# STEP 2: SUMMARY BY BRANCH
# ---------------------------------------------------------------------
con.execute(f"""
    CREATE TABLE SUMMARY AS
    SELECT 
        BRCHCODE,
        SUM(HOEPDNOTE) AS HOEPDNOTE,
        SUM(HOENOTED) AS HOENOTED,
        SUM(HOEPDNOTE + HOENOTED) AS TOTALX
    FROM hrc_filtered
    GROUP BY BRCHCODE
    ORDER BY BRCHCODE
""")

print("STEP 2:")
print(con.execute("SELECT * FROM SUMMARY LIMIT 500").fetchdf())

# ---------------------------------------------------------------------
# STEP 3: WRITE OUTPUT TO TXT (SAS-LIKE FORMAT)
# ---------------------------------------------------------------------
# output "name" used by helper functions
OUT_NAME = "CISHRC_STATUS_YEARLY"
# Decide txt file path:
csv_path = csv_output_path(OUT_NAME)
txt_path = csv_path
if txt_path.lower().endswith('.csv'):
    txt_path = txt_path[:-4] + '.txt'
else:
    txt_path = txt_path + '.txt'

# Fetch rows from SUMMARY (we used the same SELECT as out1 but without year/month/day)
rows = con.execute("""
    SELECT BRCHCODE, HOEPDNOTE, HOENOTED, TOTALX
    FROM SUMMARY
    ORDER BY BRCHCODE
""").fetchall()

header = (
    f"{'BRANCH':<7}"
    "HOE PEND NOTE  HOE NOTED  TOTAL"
)

def z8(val):
    try:
        ival = int(val)
    except Exception:
        ival = 0
    return f"{ival:0>8d}"  # zero-padded width 8

with open(txt_path, 'w', encoding='utf-8') as f:
    # write header line (SAS prints header only once at _N_=1)
    f.write(header + "\n")
    for r in rows:
        brchcode = (r[0] or "").ljust(7)[:7]
        parts = [
            brchcode, ", ",
            z8(r[1]), ", ",
            z8(r[2]), ", ",
            z8(r[3])
        ]
        f.write("".join(parts) + "\n")
