import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path

# Paths for parquet input and txt output
input_dowj = "MDOWJS.parquet"
input_rhold = "MRHOLD.parquet"

output_dir = Path("./output_txt")
output_dir.mkdir(exist_ok=True)

# Load parquet files into DuckDB tables
con = duckdb.connect()

con.execute(f"""
CREATE OR REPLACE TABLE dowj AS
SELECT *,
       'DOWJONES' AS reason,
       dreason AS remarks,
       dept AS details,
       mdnic AS m_nic,
       mdnid AS m_nid,
       mdic AS m_ic,
       mdid AS m_id,
       mddob AS m_dob
FROM read_parquet('{input_dowj}')
WHERE NOT (matchname='Y' AND matchind='     ')
  AND NOT (mdnic='N' AND mdnid='N' AND mdic='N' AND mdid='N' AND mddob='N')
""")

con.execute(f"""
CREATE OR REPLACE TABLE rhold AS
SELECT *,
       'RHOLD' AS reason,
       rremark AS remarks,
       key_describe AS details,
       mrnic AS m_nic,
       mrnid AS m_nid,
       mric AS m_ic,
       mrid AS m_id,
       mrdob AS m_dob
FROM read_parquet('{input_rhold}')
WHERE NOT (matchname='Y' AND matchind='     ')
  AND NOT (mrnic='N' AND mrnid='N' AND mric='N' AND mrid='N' AND mrdob='N')
""")

# Combine both datasets
con.execute("""
CREATE OR REPLACE TABLE combined AS
SELECT * FROM dowj
UNION ALL
SELECT * FROM rhold
""")

# Split by COMPCODE
comp_codes = ["PBB", "PIB", "PNSB", "PTS", "PHSB"]
for code in comp_codes:
    table_name = f"mp{code.lower()}"
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM combined WHERE compcode='{code}'")

# MOVERSEA: everything else
con.execute("""
CREATE OR REPLACE TABLE moversea AS
SELECT * FROM combined
WHERE compcode NOT IN ('PBB','PIB','PNSB','PTS','PHSB')
""")

# Function to write fixed-width txt file
def write_fixed_width_txt(table_name, filename, title):
    df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
    
    if df.empty:
        with open(filename, "w") as f:
            f.write(f"{' '*55}EXCEPTION REPORT ON VALIDATION OF {title}\n")
            f.write(f"{' '*55}       NO MATCHING RECORDS\n")
        return

    with open(filename, "w") as f:
        # Header
        f.write(f"{' '*55}EXCEPTION REPORT ON VALIDATION OF {title}\n")
        f.write(f"{'STAFF ID':<8};{'NAME':<40};{'OLD IC':<15};{'NEW IC':<12};{'DATE OF BIRTH':<12};"
                f"{'BASE':<24};{'DESIGNATION':<24};{'REASON':<10};{'REMARKS':<150};{'DETAILS':<150}\n")
        # Data
        for idx, row in df.iterrows():
            f.write(f"{row['staffid']:<5};{row['hcmname']:<40};{row['oldid']:<15};{row['ic']:<12};"
                    f"{row['dobdt']:<10};{row['base']:<20};{row['designation']:<20};{row['reason']:<10};"
                    f"{row['remarks']:<150};{row['details']:<150}\n")

# Write all output files
outputs = {
    "MPBB": "OUTPBB.txt",
    "MPIB": "OUTPIB.txt",
    "MPNSB": "OUTPNSB.txt",
    "MPTS": "OUTPTS.txt",
    "MPHSB": "OUTPHSB.txt",
    "MOVERSEA": "OUTOVER.txt"
}

for table, filename in outputs.items():
    write_fixed_width_txt(table, output_dir / filename, title=table)

print("All reports generated successfully!")
