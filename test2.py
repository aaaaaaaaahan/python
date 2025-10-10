import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet
import datetime

#------------------------------------------------------------#
#  Batch Date Setup
#------------------------------------------------------------#
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
    FROM '{host_parquet_path("UNICARD_VISA.parquet")}'
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
        ALIAS1,
        ALIAS2,
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

        -- Safely handle numeric operations
        CASE
            WHEN CURRENTBALSIGN = '-' THEN 
                -TRY_CAST(REPLACE(REPLACE(REPLACE(TRIM(CURRENTBAL), ',', ''), '(', '-'), ')', '') AS DOUBLE)
            ELSE 
                TRY_CAST(REPLACE(REPLACE(REPLACE(TRIM(CURRENTBAL), ',', ''), '(', '-'), ')', '') AS DOUBLE)
        END AS CURRENTBAL2,

        (-1) * (
            TRY_CAST(REPLACE(REPLACE(REPLACE(TRIM(CURRENTBAL), ',', ''), '(', '-'), ')', '') AS DOUBLE)
            - COALESCE(
                TRY_CAST(REPLACE(REPLACE(REPLACE(TRIM(AUTHCHARGE), ',', ''), '(', '-'), ')', '') AS DOUBLE),
                0
            )
        ) AS BAL1,

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
#  Merge MERCHANT + VISA datasets (align columns)
#------------------------------------------------------------#
con.execute("""
    CREATE TABLE mrgcard AS
    SELECT
        ACCTNO,
        NULL AS CARDNOGTOR,
        CUSTNAME,
        NULL AS ALIASKEY,
        ALIAS,
        NULL AS ALIAS1,
        NULL AS ALIAS2,
        NULL AS EMPLNAME,
        OCCUPDESC,
        DATEOPEN,
        DATECLSE,
        NULL AS CREDITLIMIT,
        NULL AS ACCTTYPE,
        NULL AS ACCTCLSECODE,
        NULL AS ACCTCLSEDESC,
        NULL AS CCELCODE,
        NULL AS CCELCODEDESC,
        NULL AS COLLNO,
        NULL AS CURRENTBAL,
        NULL AS CURRENTBALSIGN,
        NULL AS AUTHCHARGE,
        NULL AS AUTHCHARGESIGN,
        NULL AS PRODDESC,
        NULL AS DOBDOR,
        ACCTCODE,
        BANKINDC,
        PRIMSEC,
        COLLINDC,
        NULL AS COLLDESC,
        ACCTSTATUS,
        NULL AS CURRENTBAL2,
        NULL AS BAL1,
        NULL AS BAL1INDC,
        NULL AS AMT1INDC,
        NULL AS AMT1,
        NULL AS RELATIONDESC,
        INDORG
    FROM merchant_proc

    UNION ALL

    SELECT
        ACCTNO,
        CARDNOGTOR,
        CUSTNAME,
        ALIASKEY,
        ALIAS,
        ALIAS1,
        ALIAS2,
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
        ACCTCODE,
        BANKINDC,
        PRIMSEC,
        COLLINDC,
        COLLDESC,
        ACCTSTATUS,
        CURRENTBAL2,
        BAL1,
        BAL1INDC,
        AMT1INDC,
        AMT1,
        RELATIONDESC,
        INDORG
    FROM visa_proc;
""")

#------------------------------------------------------------#
#  Additional fields as per SAS logic
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
final = """
    SELECT *
           ,{year1} AS year
           ,{month1} AS month
           ,{day1} AS day
    FROM mrgcard
""".format(year1=year1,month1=month1,day1=day1)

queries = {
    "SNGLVIEW_PBCS"            : final
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

#------------------------------------------------------------#
#  Split output into 10 smaller Parquet files (optional)
#------------------------------------------------------------#
# Example: Split into 10 partitions
for i in range(1, 11):
    query = f"""
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER () AS rn
            FROM ({final})
        )
        WHERE MOD(rn, 10) = {i-1}
    """
    
    # Parquet output
    con.execute(f"""
    COPY ({query})
    TO '{parquet_output_path(f"SNGLVIEW_PBCS{i:02d}")}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true)
    """)

    # CSV output
    con.execute(f"""
    COPY ({query})
    TO '{csv_output_path(f"SNGLVIEW_PBCS{i:02d}")}'
    (HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true)
    """)
