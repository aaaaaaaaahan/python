import duckdb
import datetime
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

# ======================================================
# Set Batch Date
# ======================================================
batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ======================================================
# Setup DuckDB Connection
# ======================================================
con = duckdb.connect()

# ======================================================
# Load Input Files (Converted from EBCDIC to Parquet)
# ======================================================
con.execute(f"""
    CREATE TABLE HCMFILE AS
    SELECT * FROM '{host_parquet_path("HCM_STAFF_LIST.parquet")}'
""")

con.execute(f"""
    CREATE TABLE DOWJONES AS
    SELECT * FROM '{host_parquet_path("UNLOAD_CIDOWJ1T_FB.parquet")}'
""")

# ======================================================
# Detect Delimiter in HCM file (simulate INFILE DSD logic)
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
    FROM HCMFILE LIMIT 1
""").fetchone()[0]

# ======================================================
# Split HCM fields dynamically by detected delimiter
# (equivalent to SAS INPUT with DSD DELIMITER)
# ======================================================
if delimiter:
    con.execute(f"""
        CREATE TABLE HCM AS
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
        FROM HCMFILE
    """)
else:
    raise ValueError("No valid delimiter detected in HCM file.")

# ======================================================
# Split DowJones file into multiple datasets
# (equivalent to SAS DATA DNAME / DIC / DID / NDOB / NNEW / NOLD)
# ======================================================
con.execute("""
    CREATE TABLE DNAME AS
    SELECT DJ_NAME AS NAME, DJ_DOB_DOR AS DOBDOR, DJ_ID_NO AS ALIAS
    FROM DOWJONES WHERE DJ_NAME IS NOT NULL
""")

con.execute("""
    CREATE TABLE DIC AS
    SELECT DJ_ID_NO AS NEWIC FROM DOWJONES WHERE length(DJ_ID_NO)=12
""")

con.execute("""
    CREATE TABLE DID AS
    SELECT DJ_ID_NO AS OTHID FROM DOWJONES WHERE length(DJ_ID_NO)!=12
""")

con.execute("""
    CREATE TABLE NDOB AS
    SELECT DJ_NAME AS NAME, DJ_DOB_DOR AS DOBDOR
    FROM DOWJONES WHERE DJ_NAME IS NOT NULL AND DJ_DOB_DOR IS NOT NULL
""")

con.execute("""
    CREATE TABLE NNEW AS
    SELECT DJ_NAME AS NAME, DJ_ID_NO AS NEWIC
    FROM DOWJONES WHERE DJ_NAME IS NOT NULL AND length(DJ_ID_NO)=12
""")

con.execute("""
    CREATE TABLE NOLD AS
    SELECT DJ_NAME AS NAME, DJ_ID_NO AS OTHID
    FROM DOWJONES WHERE DJ_NAME IS NOT NULL AND length(DJ_ID_NO)!=12
""")

# ======================================================
# Split HCM file into datasets
# (equivalent to SAS DATA HCMOLD / HCMNEW / HCMALL / HCMNDOB / HCMNNEW / HCMNOLD)
# ======================================================
con.execute("CREATE TABLE HCMOLD AS SELECT * FROM HCM WHERE OLDID IS NOT NULL")
con.execute("CREATE TABLE HCMNEW AS SELECT * FROM HCM WHERE IC IS NOT NULL")
con.execute("CREATE TABLE HCMALL AS SELECT * FROM HCM WHERE HCMNAME IS NOT NULL")
con.execute("CREATE TABLE HCMNDOB AS SELECT * FROM HCM WHERE HCMNAME IS NOT NULL AND DOB IS NOT NULL")
con.execute("CREATE TABLE HCMNNEW AS SELECT * FROM HCM WHERE HCMNAME IS NOT NULL AND IC IS NOT NULL")
con.execute("CREATE TABLE HCMNOLD AS SELECT * FROM HCM WHERE HCMNAME IS NOT NULL AND OLDID IS NOT NULL")

# ======================================================
# Merge Logic (Equivalent to SAS MATCH SECTIONS)
# ======================================================

# (1) NAME MATCH
con.execute("""
    CREATE TABLE MRGNAME AS
    SELECT b.*, '6' AS MATCH_IND, 'DOWJONES NAME MATCH' AS REASON, 'Y' AS M_NAME
    FROM DNAME a JOIN HCMALL b ON a.NAME = b.HCMNAME
""")

# (2) ID MATCH
con.execute("""
    CREATE TABLE MRGID AS
    SELECT b.*, '4' AS MATCH_IND, 'DOWJONES ID MATCH' AS REASON, 'Y' AS M_ID
    FROM DID a JOIN HCMOLD b ON a.OTHID = b.OLDID
""")

# (3) IC MATCH
con.execute("""
    CREATE TABLE MRGIC AS
    SELECT b.*, '3' AS MATCH_IND, 'DOWJONES IC MATCH' AS REASON, 'Y' AS M_IC
    FROM DIC a JOIN HCMNEW b ON a.NEWIC = b.IC
""")

# (4) NAME & DOB MATCH
con.execute("""
    CREATE TABLE MRGNDOB AS
    SELECT b.*, '5' AS MATCH_IND, 'DOWJONES NAME AND DOB MATCH' AS REASON, 'Y' AS M_DOB
    FROM NDOB a JOIN HCMNDOB b ON a.NAME = b.HCMNAME AND a.DOBDOR = REPLACE(b.DOB, '-', '')
""")

# (5) NAME & ID MATCH
con.execute("""
    CREATE TABLE MRGNID AS
    SELECT b.*, '2' AS MATCH_IND, 'DOWJONES NAME AND ID MATCH' AS REASON, 'Y' AS M_NID
    FROM NOLD a JOIN HCMNOLD b ON a.NAME = b.HCMNAME AND a.OTHID = b.OLDID
""")

# (6) NAME & IC MATCH
con.execute("""
    CREATE TABLE MRGNIC AS
    SELECT b.*, '1' AS MATCH_IND, 'DOWJONES NAME AND IC MATCH' AS REASON, 'Y' AS M_NIC
    FROM NNEW a JOIN HCMNNEW b ON a.NAME = b.HCMNAME AND a.NEWIC = b.IC
""")

# ======================================================
# Combine all match results (Equivalent to DATA ALLMATCH)
# ======================================================
con.execute("""
    CREATE TABLE ALLMATCH AS
    SELECT DISTINCT * FROM (
        SELECT HCMNAME, OLDID, IC, DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON,
               'Y' AS M_NAME, NULL AS M_NIC, NULL AS M_NID, NULL AS M_IC, NULL AS M_ID, NULL AS M_DOB
        FROM MRGNAME
        UNION ALL
        SELECT HCMNAME, OLDID, IC, DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON,
               NULL AS M_NAME, NULL AS M_NIC, NULL AS M_NID, NULL AS M_IC, 'Y' AS M_ID, NULL AS M_DOB
        FROM MRGID
        UNION ALL
        SELECT HCMNAME, OLDID, IC, DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON,
               NULL AS M_NAME, NULL AS M_NIC, NULL AS M_NID, 'Y' AS M_IC, NULL AS M_ID, NULL AS M_DOB
        FROM MRGIC
        UNION ALL
        SELECT HCMNAME, OLDID, IC, DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON,
               NULL AS M_NAME, NULL AS M_NIC, NULL AS M_NID, NULL AS M_IC, NULL AS M_ID, 'Y' AS M_DOB
        FROM MRGNDOB
        UNION ALL
        SELECT HCMNAME, OLDID, IC, DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON,
               NULL AS M_NAME, NULL AS M_NIC, 'Y' AS M_NID, NULL AS M_IC, NULL AS M_ID, NULL AS M_DOB
        FROM MRGNID
        UNION ALL
        SELECT HCMNAME, OLDID, IC, DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON,
               NULL AS M_NAME, 'Y' AS M_NIC, NULL AS M_NID, NULL AS M_IC, NULL AS M_ID, NULL AS M_DOB
        FROM MRGNIC
    )
""")


# ======================================================
# Final Output (Equivalent to DATA OUTPUT)
# Replace missing flags with 'N' and add fixed text fields
# ======================================================
con.execute("""
    CREATE TABLE OUTPUT AS
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
    FROM ALLMATCH
""")

out = """
    SELECT 
        *
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM OUTPUT
""".format(year=year,month=month,day=day)

queries = {
    "HCM_DOWJONES_MATCH"                 : out
}

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    con.execute(f"""
    COPY ({query})
    TO '{parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);  
     """)
    
    con.execute(f"""
    COPY ({query})
    TO '{csv_path}'
    (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);  
     """)
