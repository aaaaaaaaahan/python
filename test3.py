import duckdb
import datetime
from pathlib import Path
from CIS_PY_READER import get_hive_parquet, csv_output_path

# --- Input parquet files (assume already converted) ---
dowj_parquet = get_hive_parquet('HCM_DOWJONES_MATCH')
rhld_parquet = get_hive_parquet('HCM_RHOLD_MATCH')

# --- Connect DuckDB ---
con = duckdb.connect()

# --- Get current date for filenames ---
today = datetime.date.today()
# For Windows, replace %-d/%-m with %#d/%#m if needed
date_str = today.strftime("%-d-%-m-%Y")  

# --- Load DOWJ data ---
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

# --- Merge DOWJ + RHOLD into ALL_MATCH ---
con.execute("""
    CREATE TABLE ALL_MATCH AS
    SELECT * FROM DOWJ
""")

# --- Split by COMPCODE ---
comp_codes = ["PBB", "PIB", "PNSB", "PTS", "PHSB"]
for code in comp_codes:
    table_name = f"M{code}"
    con.execute(f"""
    CREATE OR REPLACE TABLE {table_name} AS
    SELECT * FROM ALL_MATCH WHERE COMPCODE='{code}'
    """)

# --- MOVERSEA: everything else ---
con.execute("""
CREATE OR REPLACE TABLE MOVERSEA AS
SELECT * FROM ALL_MATCH
WHERE COMPCODE NOT IN ('PBB','PIB','PNSB','PTS','PHSB')
""")

# --- Function to write fixed-width TXT file using csv_output_path ---
def write_fixed_width_txt(table_name, title, report_code):
    # Create the full filename in the same CSV folder
    base_folder = Path(csv_output_path("hcm_reports"))
    txt_path = base_folder / f"HCM_MATCH_{report_code}_RPT_{date_str}.txt"

    df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
    
    with open(txt_path, "w") as f:
        # Handle empty table
        if df.empty:
            f.write(f"{' '*55}EXCEPTION REPORT ON VALIDATION OF {title}\n")
            f.write(f"{' '*55}       NO MATCHING RECORDS\n")
            return

        # Header
        f.write(f"{' '*55}EXCEPTION REPORT ON VALIDATION OF {title}\n")
        f.write(f"{'STAFF ID':<8};{'NAME':<40};{'OLD IC':<15};{'NEW IC':<12};{'DATE OF BIRTH':<12};"
                f"{'BASE':<24};{'DESIGNATION':<24};{'REASON':<17};{'REMARKS':<150};{'DETAILS':<150}\n")
        
        # Data rows
        for _, row in df.iterrows():
            f.write(f"{str(row.get('STAFFID','')):<8};"
                    f"{str(row.get('HCMNAME','')):<40};"
                    f"{str(row.get('OLDID','')):<15};"
                    f"{str(row.get('IC','')):<12};"
                    f"{str(row.get('DOB','')):<13};"
                    f"{str(row.get('BASE','')):<24};"
                    f"{str(row.get('DESIGNATION','')):<24};"
                    f"{str(row.get('REASON','')):<10};"
                    f"{str(row.get('REMARKS','')):<150};"
                    f"{str(row.get('DETAILS','')):<150}\n")

    print(f"✅ TXT report generated: {txt_path}")

# --- Generate all 6 reports ---
outputs = {
    "MPBB": ("RHOLD AND DJWD (PBB)", "PBB"),
    "MPIB": ("RHOLD AND DJWD (PIB)", "PIB"),
    "MPNSB": ("RHOLD AND DJWD (PNSB)", "PNSB"),
    "MPTS": ("RHOLD AND DJWD (PTS)", "PTS"),
    "MPHSB": ("RHOLD AND DJWD (PHSB)", "PHSB"),
    "MOVERSEA": ("RHOLD AND DJWD (OVERSEA)", "OVERSEA")
}

for table, (title, report_code) in outputs.items():
    write_fixed_width_txt(table, title, report_code)

print("All 6 dated TXT reports generated successfully!")
