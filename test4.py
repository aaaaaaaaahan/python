import duckdb
from CIS_PY_READER import get_hive_parquet, csv_output_path
from pathlib import Path

# --- Ensure output folder exists ---
csv_output_path.mkdir(parents=True, exist_ok=True)

# --- Input parquet files (assume already converted) ---
dowj_parquet = get_hive_parquet('HCM_DOWJONES_MATCH')
rhld_parquet = get_hive_parquet('HCM_RHOLD_MATCH')

# --- Connect DuckDB ---
con = duckdb.connect(database=':memory:')

# --- Load DOWJ table ---
con.execute(f"""
CREATE OR REPLACE TABLE DOWJ AS
SELECT *,
       'DOWJONES' AS REASON,
       DREMARK AS REMARKS,
       DEPT AS DETAILS,
       MDNIC AS M_NIC,
       MDNID AS M_NID,
       MDIC AS M_IC,
       MDID AS M_ID,
       MDDOB AS M_DOB
FROM read_parquet('{dowj_parquet[0]}')
WHERE NOT (MATCHNAME='Y' AND MATCHIND='     ')
  AND NOT (MDNIC='N' AND MDNID='N' AND MDIC='N' AND MDID='N' AND MDDOB='N')
""")

# --- Load RHOLD table ---
con.execute(f"""
CREATE OR REPLACE TABLE RHOLD AS
SELECT *,
       'RHOLD' AS REASON,
       RREMARK AS REMARKS,
       KEY_DESCRIBE AS DETAILS,
       MRNIC AS M_NIC,
       MRNID AS M_NID,
       MRIC AS M_IC,
       MRID AS M_ID,
       MRDOB AS M_DOB
FROM read_parquet('{rhld_parquet[0]}')
WHERE NOT (MATCHNAME='Y' AND MATCHIND='     ')
  AND NOT (MRNIC='N' AND MRNID='N' AND MRIC='N' AND MRID='N' AND MRDOB='N')
""")

# --- Merge both tables ---
con.execute("""
CREATE OR REPLACE TABLE ALL_MATCH AS
SELECT * FROM DOWJ
UNION ALL
SELECT * FROM RHOLD
""")

# --- Split by COMPCODE ---
comp_codes = ["PBB", "PIB", "PNSB", "PTS", "PHSB"]
for code in comp_codes:
    table_name = f"mp{code.lower()}"
    con.execute(f"""
    CREATE OR REPLACE TABLE {table_name} AS
    SELECT * FROM ALL_MATCH WHERE COMPCODE='{code}'
    """)

# --- MOVERSEA: everything else ---
con.execute("""
CREATE OR REPLACE TABLE moversea AS
SELECT * FROM ALL_MATCH
WHERE COMPCODE NOT IN ('PBB','PIB','PNSB','PTS','PHSB')
""")

# --- Function to write fixed-width txt file ---
def write_fixed_width_txt(table_name, filename, title):
    df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
    
    with open(filename, "w") as f:  # Overwrite if exists
        # Handle empty table
        if df.empty:
            f.write(f"{' '*55}EXCEPTION REPORT ON VALIDATION OF {title}\n")
            f.write(f"{' '*55}       NO MATCHING RECORDS\n")
            return

        # Header
        f.write(f"{' '*55}EXCEPTION REPORT ON VALIDATION OF {title}\n")
        f.write(f"{'STAFF ID':<8};{'NAME':<40};{'OLD IC':<15};{'NEW IC':<12};{'DATE OF BIRTH':<12};"
                f"{'BASE':<24};{'DESIGNATION':<24};{'REASON':<10};{'REMARKS':<150};{'DETAILS':<150}\n")
        
        # Data rows
        for idx, row in df.iterrows():
            f.write(f"{str(row.get('STAFFID','')):<5};"
                    f"{str(row.get('HCMNAME','')):<40};"
                    f"{str(row.get('OLDID','')):<15};"
                    f"{str(row.get('IC','')):<12};"
                    f"{str(row.get('DOBDT','')):<10};"
                    f"{str(row.get('BASE','')):<20};"
                    f"{str(row.get('DESIGNATION','')):<20};"
                    f"{str(row.get('REASON','')):<10};"
                    f"{str(row.get('REMARKS','')):<150};"
                    f"{str(row.get('DETAILS','')):<150}\n")

# --- Write all output files ---
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

print("All 6 reports generated successfully!")
