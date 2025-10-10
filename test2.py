import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path, get_hive_parquet
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year1, month1, day1 = batch_date.year, batch_date.month, batch_date.day

#------------------------------------------------------------#
#  Initialize DuckDB connection
#------------------------------------------------------------#
con = duckdb.connect()

#------------------------------------------------------------#
#  Load Input Tables
#------------------------------------------------------------#
con.execute(f"""
    CREATE TABLE merchant AS 
    SELECT * 
    FROM '{host_parquet_path("UNICARD_MERCHANT.parquet")}'
""")

con.execute(f"""
    CREATE TABLE visa AS 
    SELECT * 
    FROM {host_parquet_path("UNICARD_VISA.parquet")}'
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


#------------------------------------------------------------#
#  Split output into 10 smaller Parquet files (like OUTFIL SPLIT)
#------------------------------------------------------------#
print("✅ CISVPBCS Conversion Completed Successfully.")
