import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import os

# ======================================================
# Setup Connection
# ======================================================
con = duckdb.connect(database=':memory:')

# ======================================================
# Load Input Files (Assumed Already Parquet)
# ======================================================
con.execute("""
    CREATE TABLE hcm_raw AS
    SELECT * FROM read_parquet('HCM_STAFF_LIST.parquet')
""")

con.execute("""
    CREATE TABLE dowj_raw AS
    SELECT * FROM read_parquet('UNLOAD_CIDOWJ1T_FB.parquet')
""")

# ======================================================
# Detect Delimiter in HCM file automatically
# ======================================================
delimiter = con.execute("""
    SELECT 
        CASE
            WHEN rawdata LIKE '%;%' THEN ';'
            WHEN rawdata LIKE '%,%' THEN ','
            WHEN rawdata LIKE '%|%' THEN '|'
            WHEN rawdata LIKE '%\t%' THEN '\t'
            ELSE ''
        END AS delim
    FROM hcm_raw LIMIT 1
""").fetchone()[0]

print(f"Detected delimiter in HCM file: {repr(delimiter)}")

# ======================================================
# Split HCM fields dynamically based on delimiter
# ======================================================
if delimiter:
    con.execute(f"""
        CREATE TABLE hcm AS
        SELECT 
            split_part(rawdata, '{delimiter}', 1) AS STAFFID,
            split_part(rawdata, '{delimiter}', 2) AS HCMNAME,
            split_part(rawdata, '{delimiter}', 3) AS OLDID,
            split_part(rawdata, '{delimiter}', 4) AS IC,
            split_part(rawdata, '{delimiter}', 5) AS DOB,
            split_part(rawdata, '{delimiter}', 6) AS BASE,
            split_part(rawdata, '{delimiter}', 7) AS COMPCODE,
            split_part(rawdata, '{delimiter}', 8) AS DESIGNATION,
            split_part(rawdata, '{delimiter}', 9) AS STATUS
        FROM hcm_raw
    """)
else:
    raise ValueError("No valid delimiter detected in HCM file.")

# ======================================================
# Extract DowJones fields (fixed-width logic already done in parquet conversion)
# ======================================================
con.execute("""
    CREATE TABLE dowj AS
    SELECT 
        DJ_NAME,
        DJ_ID_NO,
        DJ_PERSON_ID,
        DJ_IND_ORG,
        DJ_DESC1,
        DJ_DOB_DOR,
        DJ_NAME_TYPE,
        DJ_ID_TYPE,
        DJ_DATE_TYPE,
        DJ_GENDER,
        DJ_SANCTION_INDC,
        DJ_OCCUP_INDC,
        DJ_RLENSHIP_INDC,
        DJ_OTHER_LIST_INDC,
        DJ_ACTIVE_STATUS,
        DJ_CITIZENSHIP
    FROM dowj_raw
""")

# ======================================================
# Derived logic - DowJones split datasets (as per SAS)
# ======================================================
con.execute("""
    CREATE TABLE dname AS
    SELECT DJ_NAME AS NAME, DJ_DOB_DOR AS DOBDOR, DJ_ID_NO AS ALIAS
    FROM dowj WHERE DJ_NAME IS NOT NULL
""")

con.execute("""
    CREATE TABLE dic AS
    SELECT DJ_ID_NO AS NEWIC FROM dowj WHERE length(DJ_ID_NO)=12
""")

con.execute("""
    CREATE TABLE did AS
    SELECT DJ_ID_NO AS OTHID FROM dowj WHERE length(DJ_ID_NO)!=12
""")

con.execute("""
    CREATE TABLE ndob AS
    SELECT DJ_NAME AS NAME, DJ_DOB_DOR AS DOBDOR
    FROM dowj WHERE DJ_NAME IS NOT NULL AND DJ_DOB_DOR IS NOT NULL
""")

con.execute("""
    CREATE TABLE nnew AS
    SELECT DJ_NAME AS NAME, DJ_ID_NO AS NEWIC
    FROM dowj WHERE DJ_NAME IS NOT NULL AND length(DJ_ID_NO)=12
""")

con.execute("""
    CREATE TABLE nold AS
    SELECT DJ_NAME AS NAME, DJ_ID_NO AS OTHID
    FROM dowj WHERE DJ_NAME IS NOT NULL AND length(DJ_ID_NO)!=12
""")

# ======================================================
# Derived logic - HCM split datasets (as per SAS)
# ======================================================
con.execute("""
    CREATE TABLE hcmold AS
    SELECT * FROM hcm WHERE OLDID IS NOT NULL
""")

con.execute("""
    CREATE TABLE hcmnew AS
    SELECT * FROM hcm WHERE IC IS NOT NULL
""")

con.execute("""
    CREATE TABLE hcmall AS
    SELECT * FROM hcm WHERE HCMNAME IS NOT NULL
""")

con.execute("""
    CREATE TABLE hcmndob AS
    SELECT * FROM hcm WHERE HCMNAME IS NOT NULL AND DOB IS NOT NULL
""")

con.execute("""
    CREATE TABLE hcmnnew AS
    SELECT * FROM hcm WHERE HCMNAME IS NOT NULL AND IC IS NOT NULL
""")

con.execute("""
    CREATE TABLE hcmnold AS
    SELECT * FROM hcm WHERE HCMNAME IS NOT NULL AND OLDID IS NOT NULL
""")

# ======================================================
# Matching Logic (All Merge Steps)
# ======================================================
con.execute("""
    CREATE TABLE mrgname AS
    SELECT b.*, '6' AS MATCH_IND, 'DOWJONES NAME MATCH' AS REASON, 'Y' AS M_NAME
    FROM dname a JOIN hcmall b ON a.NAME = b.HCMNAME
""")

con.execute("""
    CREATE TABLE mrgid AS
    SELECT b.*, '4' AS MATCH_IND, 'DOWJONES ID MATCH' AS REASON, 'Y' AS M_ID
    FROM did a JOIN hcmold b ON a.OTHID = b.OLDID
""")

con.execute("""
    CREATE TABLE mrgic AS
    SELECT b.*, '3' AS MATCH_IND, 'DOWJONES IC MATCH' AS REASON, 'Y' AS M_IC
    FROM dic a JOIN hcmnew b ON a.NEWIC = b.IC
""")

con.execute("""
    CREATE TABLE mrgndob AS
    SELECT b.*, '5' AS MATCH_IND, 'DOWJONES NAME AND DOB MATCH' AS REASON, 'Y' AS M_DOB
    FROM ndob a JOIN hcmndob b ON a.NAME = b.HCMNAME AND a.DOBDOR = REPLACE(b.DOB, '-', '')
""")

con.execute("""
    CREATE TABLE mrgnid AS
    SELECT b.*, '2' AS MATCH_IND, 'DOWJONES NAME AND ID MATCH' AS REASON, 'Y' AS M_NID
    FROM nold a JOIN hcmnold b ON a.NAME = b.HCMNAME AND a.OTHID = b.OLDID
""")

con.execute("""
    CREATE TABLE mrgnic AS
    SELECT b.*, '1' AS MATCH_IND, 'DOWJONES NAME AND IC MATCH' AS REASON, 'Y' AS M_NIC
    FROM nnew a JOIN hcmnnew b ON a.NAME = b.HCMNAME AND a.NEWIC = b.IC
""")

# ======================================================
# Combine all match results
# ======================================================
con.execute("""
    CREATE TABLE allmatch AS
    SELECT DISTINCT *
    FROM (
        SELECT * FROM mrgname
        UNION ALL
        SELECT * FROM mrgid
        UNION ALL
        SELECT * FROM mrgic
        UNION ALL
        SELECT * FROM mrgndob
        UNION ALL
        SELECT * FROM mrgnid
        UNION ALL
        SELECT * FROM mrgnic
    )
""")

# ======================================================
# Final Output: replace null match flags with 'N'
# ======================================================
con.execute("""
    CREATE TABLE output AS
    SELECT
        HCMNAME,
        OLDID,
        IC,
        MATCH_IND,
        DOB,
        BASE,
        DESIGNATION,
        COALESCE(REASON, '') AS REASON,
        COALESCE(M_NAME, 'N') AS M_NAME,
        COALESCE(M_NIC, 'N') AS M_NIC,
        COALESCE(M_NID, 'N') AS M_NID,
        COALESCE(M_IC, 'N') AS M_IC,
        COALESCE(M_ID, 'N') AS M_ID,
        COALESCE(M_DOB, 'N') AS M_DOB,
        COMPCODE,
        STAFFID,
        'AML/CFT' AS DEPT,
        'MS NG MEE WUN 03-21767651; MS WONG LAI SAN 03-21763005' AS CONTACT
    FROM allmatch
""")

# ======================================================
# Save final output
# ======================================================
os.makedirs("output", exist_ok=True)
con.execute("COPY output TO 'output/HCM_DOWJONES_MATCH.csv' (HEADER, DELIMITER ',')")
con.execute("COPY output TO 'output/HCM_DOWJONES_MATCH.parquet' (FORMAT PARQUET)")

print("✅ Processing complete. Output files generated in ./output/")
