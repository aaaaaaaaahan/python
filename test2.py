import duckdb

# --- Input parquet files (already converted from SAS) ---
dowj_parquet = "HCM_DOWJONES_MATCH.parquet"
rhld_parquet = "HCM_RHOLD_MATCH.parquet"

# --- Connect DuckDB in memory ---
con = duckdb.connect(database=':memory:')

# --- Load DOWJONES data ---
con.execute(f"""
    CREATE TABLE DOWJ AS
    SELECT *,
           'DOWJONES' AS REASON,
           DREMARK AS REMARKS,
           DEPT AS DETAILS,
           MDNIC AS M_NIC,
           MDNID AS M_NID,
           MDIC AS M_IC,
           MDID AS M_ID,
           MDDOB AS M_DOB
    FROM read_parquet('{dowj_parquet}')
    WHERE NOT (MATCHNAME = 'Y' AND MATCHIND = '     ')
      AND NOT (MDNIC='N' AND MDNID='N' AND MDIC='N' AND M_ID='N' AND MDDOB='N')
""")

# --- Load RHOLD data ---
con.execute(f"""
    CREATE TABLE RHOLD AS
    SELECT *,
           'RHOLD' AS REASON,
           RREMARK AS REMARKS,
           KEY_DESCRIBE AS DETAILS,
           MRNIC AS M_NIC,
           MRNID AS M_NID,
           MRIC AS M_IC,
           MRID AS M_ID,
           MRDOB AS M_DOB
    FROM read_parquet('{rhld_parquet}')
    WHERE NOT (MATCHNAME = 'Y' AND MATCHIND = '     ')
      AND NOT (MRNIC='N' AND MRNID='N' AND MRIC='N' AND MRID='N' AND MRDOB='N')
""")

# --- Merge DOWJ + RHOLD ---
con.execute("""
    CREATE TABLE ALL_MATCH AS
    SELECT * FROM DOWJ
    UNION ALL
    SELECT * FROM RHOLD
""")

# --- Split by COMPCODE with flexible field selection ---
con.execute("""
    CREATE TABLE MPBB AS
    SELECT STAFFID,HCMNAME,OLDID,IC,DOBDT,BASE,DESIGNATION,REASON,REMARKS,DETAILS,M_NIC,M_NID,M_IC,M_ID,M_DOB
    FROM ALL_MATCH
    WHERE COMPCODE='PBB';
""")

con.execute("""
    CREATE TABLE MPIB AS
    SELECT STAFFID,HCMNAME,OLDID,IC,DOBDT,BASE,DESIGNATION,REASON,REMARKS,DETAILS,M_NIC,M_NID,M_IC,M_ID,M_DOB
    FROM ALL_MATCH
    WHERE COMPCODE='PIB';
""")

con.execute("""
    CREATE TABLE MPNSB AS
    SELECT STAFFID,HCMNAME,OLDID,IC,DOBDT,BASE,DESIGNATION,REASON,REMARKS,DETAILS,M_NIC,M_NID,M_IC,M_ID,M_DOB
    FROM ALL_MATCH
    WHERE COMPCODE='PNSB';
""")

con.execute("""
    CREATE TABLE MPTS AS
    SELECT STAFFID,HCMNAME,OLDID,IC,DOBDT,BASE,DESIGNATION,REASON,REMARKS,DETAILS,M_NIC,M_NID,M_IC,M_ID,M_DOB
    FROM ALL_MATCH
    WHERE COMPCODE='PTS';
""")

con.execute("""
    CREATE TABLE MPHSB AS
    SELECT STAFFID,HCMNAME,OLDID,IC,DOBDT,BASE,DESIGNATION,REASON,REMARKS,DETAILS,M_NIC,M_NID,M_IC,M_ID,M_DOB
    FROM ALL_MATCH
    WHERE COMPCODE='PHSB';
""")

con.execute("""
    CREATE TABLE MOVERSEA AS
    SELECT STAFFID,HCMNAME,OLDID,IC,DOBDT,BASE,DESIGNATION,REASON,REMARKS,DETAILS,M_NIC,M_NID,M_IC,M_ID,M_DOB
    FROM ALL_MATCH
    WHERE COMPCODE NOT IN ('PBB','PIB','PNSB','PTS','PHSB');
""")

# --- Function to write TXT report with flexible fields ---
def write_report(table_name, filename, title, fields):
    # Fetch rows from DuckDB
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
                f.write(';'.join([str(row[field]) if row[field] is not None else '' for field in fields]) + '\n')

# --- Define flexible field list for each output ---
fields_mpbb = ['STAFFID','HCMNAME','OLDID','IC','DOBDT','BASE','DESIGNATION','REASON','REMARKS','DETAILS','M_NIC','M_NID','M_IC','M_ID','M_DOB']
fields_mpib = fields_mpbb.copy()  # Could customize if needed
fields_mpnsb = fields_mpbb.copy()
fields_mpts = fields_mpbb.copy()
fields_mphsb = fields_mpbb.copy()
fields_moversea = fields_mpbb.copy()

# --- Write TXT reports ---
write_report("MPBB", "OUTPBB.txt", "RHOLD AND DJWD (PBB)", fields_mpbb)
write_report("MPIB", "OUTPIB.txt", "RHOLD AND DJWD (PIB)", fields_mpib)
write_report("MPNSB", "OUTPNSB.txt", "RHOLD AND DJWD (PNSB)", fields_mpnsb)
write_report("MPTS", "OUTPTS.txt", "RHOLD AND DJWD (PTS)", fields_mpts)
write_report("MPHSB", "OUTPHSB.txt", "RHOLD AND DJWD (PHSB)", fields_mphsb)
write_report("MOVERSEA", "OUTOVER.txt", "RHOLD AND DJWD (PTL/PBL/OVERSEA)", fields_moversea)

print("All TXT reports generated successfully!")
