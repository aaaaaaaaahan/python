ERROR:
duckdb.duckdb.ConversionException: Conversion Error: Type DOUBLE with value 60133333333.0 can't be cast because the value is out of range for the destination type INT32 when casting from source column MOBILEPH

LINE 36:         CAST(MOBILEPH AS INTEGER) AS TELEXPHONE,

PROGRAM:
import duckdb
import pyarrow.parquet as pq
import datetime
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

# ============================================================
# DATE SETUP
# ============================================================
batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ============================================================
# DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()

# ============================================================
# LOAD
# ============================================================
con.execute(f"""
    CREATE OR REPLACE TABLE inname_sorted AS
    SELECT
        CAST(HOLDCONO AS INTEGER) AS HOLDCONO,
        LPAD(CAST(CAST(BANKNO AS INTEGER) AS VARCHAR), 3, '0') AS BANKNO,
        CUSTNO,
        CAST(RECTYPE AS INTEGER) AS RECTYPE,
        CAST(RECSEQ AS INTEGER) AS RECSEQ,
        EFFDATE,
        PROCESSTIME,
        CAST(ADRHOLDCONO AS INTEGER) AS ADRHOLDCONO,
        CAST(ADRBANKNO AS INTEGER) AS ADRBANKNO,
        CAST(ADRREFNO AS INTEGER) AS ADRREFNO,
        INDORG AS CUSTTYPE,
        KEYFIELD1,
        KEYFIELD2,
        KEYFIELD3,
        KEYFIELD4,
        LINECODE,
        CUSTNAME AS NAMELINE,
        LINECODE1,
        NAMETITLE1,
        LINECODE2,
        NAMETITLE2,
        SALUTATION,
        CAST(TITLECODE AS INTEGER) AS TITLECODE,
        FIRSTMID,
        SURNAME,
        SURNAMEKEY,
        CAST(SUFFIXCODE AS INTEGER) AS SUFFIXCODE,
        CAST(APPENDCODE AS INTEGER) AS APPENDCODE,
        CAST(PRIMPHONE AS INTEGER) AS PRIMPHONE,
        CAST(PPHONELTH AS INTEGER) AS PPHONELTH,
        CAST(SECPHONE AS INTEGER) AS SECPHONE,
        CAST(SPHONELTH AS INTEGER) AS SPHONELTH,
        CAST(MOBILEPH AS INTEGER) AS TELEXPHONE,
        CAST(TPHONELTH AS INTEGER) AS TPHONELTH,
        CAST(FAX AS INTEGER) AS FAXPHONE,
        CAST(FPHONELTH AS INTEGER) AS FPHONELTH,
        LASTCHANGE,
        NAMEFMT,
        regexp_extract(NAMELINE, '^[^ ]+ +([^ ]+)', 1) AS SECND_WORD
    FROM '{host_parquet_path("PRIMNAME_OUT.parquet")}'
    WHERE CUSTTYPE = 'I'
      AND NAMELINE != ''
      AND KEYFIELD1 IS NULL
      AND SECND_WORD = ''
    ORDER BY CUSTNO
""")
# ============================================================
# OUTPUT 1: OUTDEL (TO DELETE)
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE tempout AS
    SELECT 
        HOLDCONO,
        BANKNO,
        CUSTNO,
        RECTYPE,
        RECSEQ,
        EFFDATE,
        PROCESSTIME,
        ADRHOLDCONO,
        ADRBANKNO,
        ADRREFNO,
        CUSTTYPE,
        KEYFIELD1,
        KEYFIELD2,
        KEYFIELD3,
        KEYFIELD4,
        LINECODE,
        NAMELINE,
        LINECODE1,
        NAMETITLE1,
        LINECODE2,
        NAMETITLE2,
        SALUTATION,
        TITLECODE,
        FIRSTMID,
        SURNAME,
        SURNAMEKEY,
        SUFFIXCODE,
        APPENDCODE,
        PRIMPHONE,
        PPHONELTH,
        SECPHONE,
        SPHONELTH,
        TELEXPHONE,
        TPHONELTH,
        FAXPHONE,
        FPHONELTH,
        LASTCHANGE,
        NAMEFMT
    FROM inname_sorted
""")

# ============================================================
# OUTPUT 2: OUTINS (TO INSERT)
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE tempout1 AS
    SELECT
        HOLDCONO,
        BANKNO,
        CUSTNO,
        RECTYPE,
        RECSEQ,
        EFFDATE,
        PROCESSTIME,
        ADRHOLDCONO,
        ADRBANKNO,
        ADRREFNO,
        CUSTTYPE,
        NAMELINE AS KEYFIELD1,
        KEYFIELD2,
        KEYFIELD3,
        KEYFIELD4,
        LINECODE,
        NAMELINE,
        LINECODE1,
        NAMETITLE1,
        LINECODE2,
        NAMETITLE2,
        SALUTATION,
        TITLECODE,
        FIRSTMID,
        SURNAME,
        SURNAMEKEY,
        SUFFIXCODE,
        APPENDCODE,
        PRIMPHONE,
        PPHONELTH,
        SECPHONE,
        SPHONELTH,
        TELEXPHONE,
        TPHONELTH,
        FAXPHONE,
        FPHONELTH,
        LASTCHANGE,
        'M' AS NAMEFMT
    FROM inname_sorted
""")

# ============================================================
# OUTPUT
# ============================================================
out1 = """
    SELECT 
        *
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM tempout
""".format(year=year,month=month,day=day)

out2 = """
    SELECT 
        *
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM tempout1
""".format(year=year,month=month,day=day)

queries = {
    "CIS_NAMEKEY1_TODELETE"                 : out1,
    "CIS_NAMEKEY1_TOINSERT"                 : out2
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
