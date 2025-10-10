import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pcsv
import datetime
import os

#------------------------------------------------------------#
#  Configuration
#------------------------------------------------------------#
host_parquet_path = "/host/cis/parquet/sas_parquet/"
parquet_output_path = "/host/cis/parquet/output/"
csv_output_path = "/host/cis/output_csv/"

merchant_file = os.path.join(host_parquet_path, "UNICARD.MERCHANT.parquet")
visa_file = os.path.join(host_parquet_path, "UNICARD.VISA.parquet")

os.makedirs(parquet_output_path, exist_ok=True)
os.makedirs(csv_output_path, exist_ok=True)

#------------------------------------------------------------#
#  Initialize DuckDB connection
#------------------------------------------------------------#
con = duckdb.connect(database=':memory:')

#------------------------------------------------------------#
#  Load Input Tables
#------------------------------------------------------------#
con.execute(f"""
    CREATE TABLE merchant AS 
    SELECT * FROM read_parquet('{merchant_file}');
""")

con.execute(f"""
    CREATE TABLE visa AS 
    SELECT * FROM read_parquet('{visa_file}');
""")

#------------------------------------------------------------#
#  Process MERCHANT data
#------------------------------------------------------------#
con.execute("""
    CREATE TABLE merchant_proc AS
    SELECT
        ACCTNO,
        CUSTNAME1 AS CUSTNAME,
        DATEOPEN,
        DATECLSE,
        MERCHTYPE,
        ALIAS,
        'MERCH' AS ACCTCODE,
        'N' AS COLLINDC,
        'C' AS BANKINDC,
        'P' AS PRIMSEC,
        'MERCHANT CODE : ' || MERCHTYPE AS OCCUPDESC,
        CASE 
            WHEN DATECLSE IS NULL THEN 'ACTIVE'
            ELSE 'CLOSED'
        END AS ACCTSTATUS,
        'O' AS INDORG
    FROM merchant
    ORDER BY ACCTNO;
""")

#------------------------------------------------------------#
#  Process VISA data
#------------------------------------------------------------#
con.execute("""
    CREATE TABLE visa_proc AS
    SELECT
        ACCTNO,
        CARDNOGTOR,
        CUSTNAME,
        ALIASKEY,
        COALESCE(NULLIF(ALIAS1,''), ALIAS2, '') AS ALIAS,
        ALIAS1, ALIAS2,
        EMPLNAME,
        OCCUPDESC,
        DATEOPEN,
        DATECLSE,
        CREDITLIMIT,
        ACCTTYPE,
        ACCTCLSECODE,
        ACCTCLSEDESC,
        CCELCODE,
        CCELCODEDESC,
        COLLNO,
        CURRENTBAL,
        CURRENTBALSIGN,
        AUTHCHARGE,
        AUTHCHARGESIGN,
        PRODDESC,
        DOBDOR,
        CASE 
            WHEN CRINDC = 'C' THEN 'CREDT'
            WHEN CRINDC = 'D' THEN 'DEBIT'
            ELSE ''
        END AS ACCTCODE,
        'C' AS BANKINDC,
        CASE 
            WHEN SUBSTR(ACCTNO, 14, 1) = '1' THEN 'P'
            ELSE 'S'
        END AS PRIMSEC,
        CASE 
            WHEN COLLNO <> '00000' THEN 'Y'
            ELSE 'N'
        END AS COLLINDC,
        CASE 
            WHEN COLLNO <> '00000' THEN 'FIXED DEPOSIT'
            ELSE ''
        END AS COLLDESC,
        CASE 
            WHEN CCELCODE = '' AND CCELCODEDESC = '' THEN 'ACTIVE'
            WHEN CCELCODE <> '' AND CCELCODEDESC <> '' THEN CCELCODEDESC
            WHEN CCELCODE <> '' AND CCELCODEDESC = '' THEN 'INACTIVE'
            ELSE 'CLOSED'
        END AS ACCTSTATUS,
        CASE 
            WHEN CURRENTBALSIGN = '-' THEN -CURRENTBAL
            ELSE CURRENTBAL
        END AS CURRENTBAL2,
        (-1) * (CURRENTBAL - AUTHCHARGE) AS BAL1,
        'O/B' AS BAL1INDC,
        'C/L' AS AMT1INDC,
        CREDITLIMIT AS AMT1,
        CASE 
            WHEN ACCTTYPE = 'I ' THEN 'PRINCIPAL CARD '
            WHEN ACCTTYPE = 'IA' THEN 'PRINC + SUPP   '
            WHEN ACCTTYPE = 'IS' THEN 'SUPP SEPARATE  '
            WHEN ACCTTYPE = 'A ' THEN 'SUPP COMBINE   '
            ELSE 'UNKNOWN        '
        END AS RELATIONDESC,
        'I' AS INDORG
    FROM visa
    ORDER BY ACCTNO;
""")

#------------------------------------------------------------#
#  Merge MERCHANT + VISA datasets
#------------------------------------------------------------#
con.execute("""
    CREATE TABLE mrgcard AS
    SELECT * FROM merchant_proc
    UNION ALL
    SELECT * FROM visa_proc;
""")

#------------------------------------------------------------#
#  Additional fields as per SAS
#------------------------------------------------------------#
con.execute("""
    ALTER TABLE mrgcard ADD COLUMN ACCTBRABBR VARCHAR;
    UPDATE mrgcard SET ACCTBRABBR = 'PBCSS';

    ALTER TABLE mrgcard ADD COLUMN JOINTACC VARCHAR;
    UPDATE mrgcard SET JOINTACC = 'N';

    UPDATE mrgcard SET DOBDOR = NULL WHERE DOBDOR = '00000000';
""")

#------------------------------------------------------------#
#  Output: Save merged table as Parquet + CSV
#------------------------------------------------------------#
output_parquet = os.path.join(parquet_output_path, "SNGLVIEW_PBCS.parquet")
output_csv = os.path.join(csv_output_path, "SNGLVIEW_PBCS.csv")

# Save to Parquet
con.execute(f"COPY (SELECT * FROM mrgcard) TO '{output_parquet}' (FORMAT PARQUET);")

# Save to CSV
con.execute(f"COPY (SELECT * FROM mrgcard) TO '{output_csv}' (HEADER, DELIMITER ',');")

#------------------------------------------------------------#
#  Split output into 10 smaller Parquet files (like OUTFIL SPLIT)
#------------------------------------------------------------#
table = pq.read_table(output_parquet)
num_rows = table.num_rows
split_size = num_rows // 10 + 1

for i in range(10):
    start = i * split_size
    end = min((i + 1) * split_size, num_rows)
    chunk = table.slice(start, end - start)
    pq.write_table(chunk, os.path.join(parquet_output_path, f"SNGLVIEW_PBCS{i+1:02}.parquet"))

print("✅ CISVPBCS Conversion Completed Successfully.")
print(f"Main output: {output_parquet}")
print(f"Split outputs: {parquet_output_path}SNGLVIEW_PBCS01–10.parquet")
