import duckdb

# --- Input parquet files (assume already converted) ---
dowj_parquet = "HCM_DOWJONES_MATCH.parquet"
rhld_parquet = "HCM_RHOLD_MATCH.parquet"

# --- Connect DuckDB ---
con = duckdb.connect(database=':memory:')

# --- Load data ---
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
      AND NOT (MDNIC='N' AND MDNID='N' AND MDIC='N' AND MDID='N' AND MDDOB='N')
""")

con.execute("""
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

# --- Split by COMPCODE ---
con.execute("""
    CREATE TABLE MPBB AS SELECT * FROM ALL_MATCH WHERE COMPCODE='PBB';
    CREATE TABLE MPIB AS SELECT * FROM ALL_MATCH WHERE COMPCODE='PIB';
    CREATE TABLE MPNSB AS SELECT * FROM ALL_MATCH WHERE COMPCODE='PNSB';
    CREATE TABLE MPTS AS SELECT * FROM ALL_MATCH WHERE COMPCODE='PTS';
    CREATE TABLE MPHSB AS SELECT * FROM ALL_MATCH WHERE COMPCODE='PHSB';
    CREATE TABLE MOVERSEA AS SELECT * FROM ALL_MATCH WHERE COMPCODE NOT IN ('PBB','PIB','PNSB','PTS','PHSB');
""")

# --- Function to write formatted report ---
def write_report(table_name, filename, title):
    res = con.execute(f"SELECT * FROM {table_name} ORDER BY HCMNAME, OLDID, IC").fetchall()
    
    with open(filename, "w") as f:
        if len(res) == 0:
            f.write(f"{' ' * 55}EXCEPTION REPORT ON VALIDATION OF {title}\n")
            f.write(f"{' ' * 55}       NO MATCHING RECORDS\n")
        else:
            # Header
            f.write(f"{' ' * 55}EXCEPTION REPORT ON VALIDATION OF {title}\n")
            f.write(f"{'STAFF ID':<8};{'NAME':<40};{'OLD IC':<15};{'NEW IC':<12};"
                    f"{'DATE OF BIRTH':<10};{'BASE':<20};{'DESIGNATION':<20};"
                    f"{'REASON':<10};{'REMARKS':<150};{'DETAILS':<150}\n")
            # Data rows
            for row in res:
                f.write(f"{row['STAFFID']:<5};{row['HCMNAME']:<40};{row['OLDID']:<15};{row['IC']:<12};"
                        f"{row['DOBDT']:<10};{row['BASE']:<20};{row['DESIGNATION']:<20};"
                        f"{row['REASON']:<10};{row['REMARKS']:<150};{row['DETAILS']:<150}\n")

# --- Write reports ---
write_report("MPBB", "OUTPBB.txt", "RHOLD AND DJWD (PBB)")
write_report("MPIB", "OUTPIB.txt", "RHOLD AND DJWD (PIB)")
write_report("MPNSB", "OUTPNSB.txt", "RHOLD AND DJWD (PNSB)")
write_report("MPTS", "OUTPTS.txt", "RHOLD AND DJWD (PTS)")
write_report("MPHSB", "OUTPHSB.txt", "RHOLD AND DJWD (PHSB)")
write_report("MOVERSEA", "OUTOVER.txt", "RHOLD AND DJWD (PTL/PBL/OVERSEA)")
