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
# INPUT PARQUET FILE (Already Converted)
# ============================================================
input_file = f"{host_parquet_path}/RBP2_B033_UNLOAD_PRIMNAME_OUT.parquet"

con.execute(f"""
    CREATE OR REPLACE TABLE inname AS 
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
    FROM read_parquet('{input_file}')
""")

# ============================================================
# FILTER RECORDS BASED ON SAS LOGIC
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE filtered AS
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
        NAMEFMT,
        regexp_extract(NAMELINE, '^[^ ]+ +([^ ]+)', 1) AS SECND_WORD
    FROM inname
    WHERE CUSTTYPE = 'I'
      AND NAMELINE IS NOT NULL
      AND trim(NAMELINE) != ''
      AND (KEYFIELD1 IS NULL OR trim(KEYFIELD1) = '')
      AND (regexp_extract(NAMELINE, '^[^ ]+ +([^ ]+)', 1) IS NULL OR trim(regexp_extract(NAMELINE, '^[^ ]+ +([^ ]+)', 1)) = '')
""")

# ============================================================
# SORT BY CUSTNO (Like PROC SORT)
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE inname_sorted AS
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
    FROM filtered
    ORDER BY CUSTNO
""")

# ============================================================
# OUTPUT 1: OUTDEL (TO DELETE)
# ============================================================
outdel_query = """
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
"""
outdel_df = con.execute(outdel_query).fetch_arrow_table()

outdel_parquet = f"{parquet_output_path}/RBP2_B033_CIS_NAMEKEY1_TODELETE_{year}{month:02}{day:02}.parquet"
outdel_csv = f"{csv_output_path}/RBP2_B033_CIS_NAMEKEY1_TODELETE_{year}{month:02}{day:02}.csv"

pq.write_table(outdel_df, outdel_parquet)
con.execute(f"COPY ({outdel_query}) TO '{outdel_csv}' (HEADER, DELIMITER ',')")

# ============================================================
# OUTPUT 2: OUTINS (TO INSERT)
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE tempoout1 AS
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

outins_query = "SELECT * FROM tempoout1"
outins_df = con.execute(outins_query).fetch_arrow_table()

outins_parquet = f"{parquet_output_path}/RBP2_B033_CIS_NAMEKEY1_TOINSERT_{year}{month:02}{day:02}.parquet"
outins_csv = f"{csv_output_path}/RBP2_B033_CIS_NAMEKEY1_TOINSERT_{year}{month:02}{day:02}.csv"

pq.write_table(outins_df, outins_parquet)
con.execute(f"COPY ({outins_query}) TO '{outins_csv}' (HEADER, DELIMITER ',')")

# ============================================================
# LOG OUTPUT
# ============================================================
print(f"[INFO] OUTDEL written: {outdel_parquet} and {outdel_csv}")
print(f"[INFO] OUTINS written: {outins_parquet} and {outins_csv}")
print(f"[INFO] Total records deleted: {len(outdel_df)}")
print(f"[INFO] Total records inserted: {len(outins_df)}")

con.close()
