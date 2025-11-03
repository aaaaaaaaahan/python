import duckdb
from CIS_PY_READER import get_hive_parquet, csv_output_path

# --- Input parquet files (assume already converted) ---
dowj_parquet = get_hive_parquet('HCM_DOWJONES_MATCH')
rhld_parquet = get_hive_parquet('HCM_RHOLD_MATCH')

# --- Connect DuckDB ---
con = duckdb.connect(database=':memory:')

# --- Load data ---
con.execute(f"""
    CREATE TABLE DOWJ AS
    SELECT *,
           'DOWJONES' AS REASON,
           REMARKS,
           DEPT AS DETAILS,
           M_NIC,
           M_NID,
           M_IC,
           M_ID,
           M_DOB
    FROM read_parquet('{dowj_parquet[0]}')
    WHERE NOT (M_NAME = 'Y' AND M_NID = '     ')
      AND NOT (M_NIC='N' AND M_NID='N' AND M_IC='N' AND M_ID='N' AND M_DOB='N')
""")

# --- Merge DOWJ + RHOLD ---
con.execute("""
    CREATE TABLE ALL_MATCH AS
    SELECT * FROM DOWJ
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
    "MPBB": "HCM_MATCH_PBB_RPT.txt",
    "MPIB": "HCM_MATCH_PIB_RPT.txt",
    "MPNSB": "HCM_MATCH_PNSB_RPT.txt",
    "MPTS": "HCM_MATCH_PTS_RPT.txt",
    "MPHSB": "HCM_MATCH_PHSB_RPT.txt",
    "MOVERSEA": "HCM_MATCH_OVERSEA_RPT.txt"
}

for table, filename in outputs.items():
    write_fixed_width_txt(table, csv_output_path / filename, title=table)
