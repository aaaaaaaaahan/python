import duckdb
import datetime
from pathlib import Path

# ---------------------------------------------------------------------
# Job: CIHRCYRP  -  YEARLY HRC SUMMARY REPORT BY STATUS
# ---------------------------------------------------------------------

# Define file paths (update these paths as needed)
input_parquet = Path("input/UNLOAD_CIHRCAPT_FB.parquet")
ctrl_file = Path("input/SRSCTRL1.parquet")
output_parquet = Path("output/CISHRC_STATUS_YEARLY.parquet")
output_csv = Path("output/CISHRC_STATUS_YEARLY.csv")

# ---------------------------------------------------------------------
# STEP 1: GET TODAY’S REPORT DATE
# ---------------------------------------------------------------------
ctrl_df = duckdb.query(f"SELECT * FROM read_parquet('{ctrl_file}')").to_df()
# Assuming ctrl_file has columns SRSYY, SRSMM, SRSDD
srs_year = int(ctrl_df.iloc[0]['SRSYY'])
srs_month = int(ctrl_df.iloc[0]['SRSMM'])
srs_day = int(ctrl_df.iloc[0]['SRSDD'])

today = datetime.date(srs_year, srs_month, srs_day)
yyyy = today.strftime("%Y")
yyyymm = today.strftime("%Y%m")
today_str = today.strftime("%Y-%m-%d")

print(f"Reporting Date: {today_str}  (YYYY={yyyy}, YYYYMM={yyyymm})")

# ---------------------------------------------------------------------
# STEP 2: CONNECT DUCKDB AND LOAD INPUT FILE
# ---------------------------------------------------------------------
con = duckdb.connect()

con.execute(f"""
    CREATE TABLE hrc AS
    SELECT * FROM read_parquet('{input_parquet}');
""")

# ---------------------------------------------------------------------
# STEP 3: FILTER DATA (equivalent to SAS DATA step)
# ---------------------------------------------------------------------
# Apply the same filters and computed columns
query_filtered = f"""
    SELECT
        *,
        substring(UPDATEDATE, 1, 4) AS UPDDATE,
        CASE WHEN APPROVALSTATUS != '08' THEN 0 ELSE 1 END AS VALID_STATUS,
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

print("Filtered data preview:")
print(con.execute("SELECT * FROM hrc_filtered LIMIT 5").fetchdf())

# ---------------------------------------------------------------------
# STEP 4: SUMMARY BY BRANCH
# ---------------------------------------------------------------------
query_summary = """
    SELECT 
        BRCHCODE,
        SUM(HOEPDNOTE) AS HOEPDNOTE,
        SUM(HOENOTED) AS HOENOTED,
        SUM(HOEPDNOTE + HOENOTED) AS TOTALX
    FROM hrc_filtered
    GROUP BY BRCHCODE
    ORDER BY BRCHCODE
"""

summary_df = con.execute(query_summary).fetchdf()

print("Report Summary Preview:")
print(summary_df.head())

# ---------------------------------------------------------------------
# STEP 5: OUTPUT TO PARQUET AND CSV
# ---------------------------------------------------------------------
# Save outputs
summary_table = con.execute(query_summary).arrow()
import pyarrow.parquet as pq

pq.write_table(summary_table, output_parquet)
summary_df.to_csv(output_csv, index=False)

print(f"\n✅ Output files generated:")
print(f"- Parquet: {output_parquet}")
print(f"- CSV: {output_csv}")

# Optional: print sample formatted text output
print("\nSample Output Format:")
print("BRANCH, HOE PEND NOTE, HOE NOTED, TOTAL")
for _, row in summary_df.head(5).iterrows():
    print(f"{row['BRCHCODE']}, {int(row['HOEPDNOTE'])}, {int(row['HOENOTED'])}, {int(row['TOTALX'])}")
