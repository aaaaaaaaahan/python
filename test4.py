import duckdb
import datetime
from pathlib import Path

# ---------------------------------------------------------------------
# Job: CIHRCYRP  -  YEARLY HRC SUMMARY REPORT BY STATUS (NOTED & PENDING NOTE)
# ---------------------------------------------------------------------

# === Output name and paths ===
OUT_NAME = "CISHRC_STATUS_YEARLY"
base_output = Path("output")
base_output.mkdir(parents=True, exist_ok=True)

def csv_output_path(name):
    return base_output / f"{name}.csv"

csv_path = csv_output_path(OUT_NAME)
txt_path = csv_path.with_suffix('.txt')

# === Input Parquet Files ===
input_parquet = Path("input/UNLOAD_CIHRCAPT_FB.parquet")
ctrl_file = Path("input/SRSCTRL1.parquet")

# ---------------------------------------------------------------------
# STEP 1: GET TODAY’S REPORT DATE (simulate SAS CTRLDATE)
# ---------------------------------------------------------------------
ctrl_df = duckdb.query(f"SELECT * FROM read_parquet('{ctrl_file}')").to_df()
srs_year = int(ctrl_df.iloc[0]['SRSYY'])
srs_month = int(ctrl_df.iloc[0]['SRSMM'])
srs_day = int(ctrl_df.iloc[0]['SRSDD'])

today = datetime.date(srs_year, srs_month, srs_day)
yyyy = today.strftime("%Y")
yyyymm = today.strftime("%Y%m")
today_str = today.strftime("%Y-%m-%d")

print(f"Reporting Date: {today_str}  (YYYY={yyyy}, YYYYMM={yyyymm})")

# ---------------------------------------------------------------------
# STEP 2: Load data into DuckDB
# ---------------------------------------------------------------------
con = duckdb.connect()
con.execute(f"""
    CREATE TABLE hrc AS
    SELECT * FROM read_parquet('{input_parquet}');
""")

# ---------------------------------------------------------------------
# STEP 3: Apply filters and derive HOEPDNOTE / HOENOTED
# ---------------------------------------------------------------------
query_filtered = f"""
    SELECT
        BRCHCODE,
        APPROVALSTATUS,
        ACCTNO,
        HOVERIFYREMARKS,
        ACCTTYPE,
        UPDATEDATE,
        substring(UPDATEDATE, 1, 4) AS UPDDATE,
        CASE 
            WHEN ACCTNO != ' ' AND POSITION('Noted by' IN HOVERIFYREMARKS) <= 0 THEN 1 
            ELSE 0 
        END AS HOEPDNOTE,
        CASE 
            WHEN ACCTNO != ' ' AND POSITION('Noted by' IN HOVERIFYREMARKS) > 0 THEN 1 
            ELSE 0 
        END AS HOENOTED
    FROM hrc
    WHERE substring(UPDATEDATE, 1, 4) = '{yyyy}'
      AND ACCTTYPE IN ('CA','SA','SDB','FD','FC','FCI','O','FDF')
      AND APPROVALSTATUS = '08'
"""

con.execute(f"CREATE TABLE hrc_filtered AS {query_filtered}")

# ---------------------------------------------------------------------
# STEP 4: SUMMARY BY BRANCH
# ---------------------------------------------------------------------
query_summary = """
    SELECT 
        BRCHCODE,
        SUM(HOEPDNOTE) AS HOEPDNOTE,
        SUM(HOENOTED) AS HOENOTED,
        SUM(HOEPDNOTE + HOENOTED) AS TOTAL
    FROM hrc_filtered
    GROUP BY BRCHCODE
    ORDER BY BRCHCODE
"""
con.execute("CREATE TABLE SUMMARY AS " + query_summary)

rows = con.execute("""
    SELECT BRCHCODE, HOEPDNOTE, HOENOTED, TOTAL
    FROM SUMMARY
    ORDER BY BRCHCODE
""").fetchall()

# ---------------------------------------------------------------------
# STEP 5: WRITE OUTPUT TO TXT (SAS-LIKE FORMAT)
# ---------------------------------------------------------------------
header = (
    f"{'BRANCH':<7}" +
    "HOE PEND NOTE, HOE NOTED, TOTAL"
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

print(f"\n✅ TXT output generated at: {txt_path}")
print("Sample Preview:")
with open(txt_path, 'r', encoding='utf-8') as preview:
    for _ in range(5):
        line = preview.readline()
        if not line:
            break
        print(line.strip())
