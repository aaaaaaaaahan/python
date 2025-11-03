import duckdb
from CIS_PY_READER import get_hive_parquet

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

# --- Split by COMPCODE with flexible field selection ---
con.execute("""
    CREATE TABLE MPBB AS
    SELECT STAFFID,HCMNAME,OLDID,IC,DOB,BASE,DESIGNATION,REASON,REMARKS,DETAILS,M_NIC,M_NID,M_IC,M_ID,M_DOB
    FROM ALL_MATCH
    WHERE COMPCODE='PBB';
""")

con.execute("""
    CREATE TABLE MPIB AS
    SELECT STAFFID,HCMNAME,OLDID,IC,DOB,BASE,DESIGNATION,REASON,REMARKS,DETAILS,M_NIC,M_NID,M_IC,M_ID,M_DOB
    FROM ALL_MATCH
    WHERE COMPCODE='PIB';
""")

con.execute("""
    CREATE TABLE MPNSB AS
    SELECT STAFFID,HCMNAME,OLDID,IC,DOB,BASE,DESIGNATION,REASON,REMARKS,DETAILS,M_NIC,M_NID,M_IC,M_ID,M_DOB
    FROM ALL_MATCH
    WHERE COMPCODE='PNSB';
""")

con.execute("""
    CREATE TABLE MPTS AS
    SELECT STAFFID,HCMNAME,OLDID,IC,DOB,BASE,DESIGNATION,REASON,REMARKS,DETAILS,M_NIC,M_NID,M_IC,M_ID,M_DOB
    FROM ALL_MATCH
    WHERE COMPCODE='PTS';
""")

con.execute("""
    CREATE TABLE MPHSB AS
    SELECT STAFFID,HCMNAME,OLDID,IC,DOB,BASE,DESIGNATION,REASON,REMARKS,DETAILS,M_NIC,M_NID,M_IC,M_ID,M_DOB
    FROM ALL_MATCH
    WHERE COMPCODE='PHSB';
""")

con.execute("""
    CREATE TABLE MOVERSEA AS
    SELECT STAFFID,HCMNAME,OLDID,IC,DOB,BASE,DESIGNATION,REASON,REMARKS,DETAILS,M_NIC,M_NID,M_IC,M_ID,M_DOB
    FROM ALL_MATCH
    WHERE COMPCODE NOT IN ('PBB','PIB','PNSB','PTS','PHSB');
""")

# --- Function to write TXT report with flexible fields using tuple indices ---
def write_report(table_name, filename, title, fields):
    # Fetch rows as tuples
    res = con.execute(f"SELECT {', '.join(fields)} FROM {table_name} ORDER BY HCMNAME, OLDID, IC").fetchall()
    
    with open(filename, "w", encoding="utf-8") as f:
        # No records
        if len(res) == 0:
            f.write(f"{' ' * 55}EXCEPTION REPORT ON VALIDATION OF {title}\n")
            f.write(f"{' ' * 55}       NO MATCHING RECORDS\n")
        else:
            # Header
            f.write(';'.join(fields) + '\n')
            # Data rows
            for row in res:
                f.write(';'.join([str(row[i]) if row[i] is not None else '' for i in range(len(fields))]) + '\n')

# --- Define flexible field list for each output ---
fields_mpbb = ['STAFFID','HCMNAME','OLDID','IC','DOB','BASE','DESIGNATION','REASON','REMARKS','DETAILS','M_NIC','M_NID','M_IC','M_ID','M_DOB']
fields_mpib = fields_mpbb.copy()  # Could customize if needed
fields_mpnsb = fields_mpbb.copy()
fields_mpts = fields_mpbb.copy()
fields_mphsb = fields_mpbb.copy()
fields_moversea = fields_mpbb.copy()

# --- Write TXT reports ---
write_report("MPBB", "/host/cis/output/HCM_MATCH_PBB_RPT.txt", "RHOLD AND DJWD (PBB)", fields_mpbb)
write_report("MPIB", "/host/cis/output/HCM_MATCH_PIB_RPT.txt", "RHOLD AND DJWD (PIB)", fields_mpib)
write_report("MPNSB", "/host/cis/output/HCM_MATCH_PNSB_RPT.txt", "RHOLD AND DJWD (PNSB)", fields_mpnsb)
write_report("MPTS", "/host/cis/output/HCM_MATCH_PTS_RPT.txt", "RHOLD AND DJWD (PTS)", fields_mpts)
write_report("MPHSB", "/host/cis/output/HCM_MATCH_PHSB_RPT.txt", "RHOLD AND DJWD (PHSB)", fields_mphsb)
write_report("MOVERSEA", "/host/cis/output/HCM_MATCH_OVERSEA_RPT.txt", "RHOLD AND DJWD (PTL/PBL/OVERSEA)", fields_moversea)

print("All TXT reports generated successfully!")
