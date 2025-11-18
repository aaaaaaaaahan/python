import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")

#---------------------------------------------------------------------#
# Original Program: CINMELON                                          #
#---------------------------------------------------------------------#
# A2016-22256                                                         #
# TO DETECT CUSTOMER WITH NO REMARKS = 'LONGNAME'                     #
# POTENTIAL UPDATES FILE TO REMARKS TABLE                             #
#---------------------------------------------------------------------#

# ---------------------------------------------------------------------
# DUCKDB PROCESSING
# ---------------------------------------------------------------------
con = duckdb.connect()

# ---------------------------------------------------------------------
# Read input parquet files into DuckDB tables
# ---------------------------------------------------------------------
con.execute(f"""
    CREATE TABLE NAME AS 
    SELECT DISTINCT
        CAST(HOLDCONO AS INTEGER) AS HOLDCONO,
        LPAD(CAST(CAST(BANKNO AS INTEGER) AS VARCHAR), 3, '0') AS BANKNO,
        CUSTNO,
        CAST(RECTYPE AS INTEGER) AS RECTYPE,
        CAST(RECSEQ AS INTEGER) AS RECSEQ,
        EFFDATE,
        PROCESSTIME,
        CAST(ADRHOLDCONO AS INTEGER) AS ADRHOLDCONO,
        CAST(ADRBANKNO AS INTEGER) AS ADRBANKNO,
        CAST(ADDREF AS INTEGER) AS ADDREF,
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
        CAST(PRIPHONE AS BIGINT) AS PRIPHONE,
        CAST(PPHONELTH AS BIGINT) AS PPHONELTH,
        CAST(SECPHONE AS BIGINT) AS SECPHONE,
        CAST(SPHONELTH AS BIGINT) AS SPHONELTH,
        CAST(MOBILEPH AS BIGINT) AS TELEXPHONE,
        CAST(TPHONELTH AS BIGINT) AS TPHONELTH,
        CAST(FAX AS BIGINT) AS FAXPHONE,
        CAST(FPHONELTH AS BIGINT) AS FPHONELTH,
        LASTCHANGE,
        NAMEFMT
    FROM read_parquet('{host_parquet_path("PRIMNAME_OUT.parquet")}')
    ORDER BY CUSTNO
""")

con.execute(f"""
    CREATE TABLE RMRK AS 
    SELECT DISTINCT * 
    FROM read_parquet('{host_parquet_path("CCRIS_CISRMRK_LONGNAME.parquet")}')
    WHERE LONGNAME IS NOT NULL AND LONGNAME != ''
    ORDER BY CUSTNO
""")

# Merge NAME and RMRK by CUSTNO, keep NAME only if no matching RMRK
con.execute("""
    CREATE TABLE MERGE AS
    SELECT n.*
    FROM NAME n
    LEFT JOIN RMRK r
    ON n.CUSTNO = r.CUSTNO
    WHERE r.CUSTNO IS NULL
    ORDER BY n.CUSTNO
""")

# ---------------------------------------------------------------------
# Output as Parquet and CSV
# ---------------------------------------------------------------------
out = """
    SELECT
        *
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM MERGE
""".format(year=year,month=month,day=day)

queries = {
    "CIS_LONGNAME_NONE"                      : out
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

# ---------------------------------------------------------------------
# Output as TXT
# ---------------------------------------------------------------------
txt_queries = {
    "CIS_LONGNAME_NONE"                      : out
}

for txt_name, txt_query in txt_queries.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(row.get('HOLDCONO','')).rjust(2)}"
                f"{str(row.get('BANKNO','')).rjust(2)}"
                f"{str(row.get('CUSTNO','')).ljust(20)}"
                f"{str(row.get('RECTYPE','')).rjust(2)}"
                f"{str(row.get('RECSEQ','')).rjust(2)}"
                f"{str(row.get('EFFDATE','')).rjust(5)}"
                f"{str(row.get('PROCESSTIME','')).ljust(8)}"
                f"{str(row.get('ADRHOLDCONO','')).rjust(2)}"
                f"{str(row.get('ADRBANKNO','')).rjust(2)}"
                f"{str(row.get('ADRREFNO','')).rjust(6)}"
                f"{str(row.get('CUSTTYPE','')).ljust(1)}"
                f"{str(row.get('KEYFIELD1','')).ljust(15)}"
                f"{str(row.get('KEYFIELD2','')).ljust(10)}"
                f"{str(row.get('KEYFIELD3','')).ljust(5)}"
                f"{str(row.get('KEYFIELD4','')).ljust(5)}"
                f"{str(row.get('LINECODE','')).ljust(1)}"
                f"{str(row.get('NAMELINE','')).ljust(40)}"
                f"{str(row.get('LINECODE1','')).ljust(1)}"
                f"{str(row.get('NAMETITLE1','')).ljust(40)}"
                f"{str(row.get('LINECODE2','')).ljust(1)}"
                f"{str(row.get('NAMETITLE2','')).ljust(40)}"
                f"{str(row.get('SALUTATION','')).ljust(40)}"
                f"{str(row.get('TITLECODE','')).rjust(4)}"
                f"{str(row.get('FIRSTMID','')).ljust(30)}"
                f"{str(row.get('SURNAME','')).ljust(20)}"
                f"{str(row.get('SURNAMEKEY','')).ljust(3)}"
                f"{str(row.get('SUFFIXCODE','')).ljust(2)}"
                f"{str(row.get('APPENDCODE','')).ljust(2)}"
                f"{str(row.get('PRIMPHONE','')).rjust(6)}"
                f"{str(row.get('PPHONELTH','')).rjust(2)}"
                f"{str(row.get('SECPHONE','')).rjust(6)}"
                f"{str(row.get('SPHONELTH','')).rjust(2)}"
                f"{str(row.get('TELEXPHONE','')).rjust(6)}"
                f"{str(row.get('TPHONELTH','')).rjust(2)}"
                f"{str(row.get('FAXPHONE','')).rjust(6)}"
                f"{str(row.get('FPHONELTH','')).rjust(2)}"
                f"{str(row.get('LASTCHANGE','')).ljust(10)}"
                f"{str(row.get('PARSEIND','')).ljust(1)}"
            )
            f.write(line + "\n")

"""
def safe_str(val):
    #Convert None, NaN, literal 'NULL', or NUL bytes to empty string
    import math
    if val is None:
        return ""
    if isinstance(val, float) and math.isnan(val):
        return ""
    if isinstance(val, str):
        # Replace literal 'NULL', 'NaN', and NUL bytes
        return val.replace("\x00", "").replace("NULL", "").replace("NaN", "")
    return str(val)

txt_queries = {
    "CIS_LONGNAME_NONE": out
}

for txt_name, txt_query in txt_queries.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{safe_str(row.get('HOLDCONO')).rjust(2)}"
                f"{safe_str(row.get('BANKNO')).rjust(2)}"
                f"{safe_str(row.get('CUSTNO')).ljust(20)}"
                f"{safe_str(row.get('RECTYPE')).rjust(2)}"
                f"{safe_str(row.get('RECSEQ')).rjust(2)}"
                f"{safe_str(row.get('EFFDATE')).rjust(5)}"
                f"{safe_str(row.get('PROCESSTIME')).ljust(8)}"
                f"{safe_str(row.get('ADRHOLDCONO')).rjust(2)}"
                f"{safe_str(row.get('ADRBANKNO')).rjust(2)}"
                f"{safe_str(row.get('ADRREFNO')).rjust(6)}"
                f"{safe_str(row.get('CUSTTYPE')).ljust(1)}"
                f"{safe_str(row.get('KEYFIELD1')).ljust(15)}"
                f"{safe_str(row.get('KEYFIELD2')).ljust(10)}"
                f"{safe_str(row.get('KEYFIELD3')).ljust(5)}"
                f"{safe_str(row.get('KEYFIELD4')).ljust(5)}"
                f"{safe_str(row.get('LINECODE')).ljust(1)}"
                f"{safe_str(row.get('NAMELINE')).ljust(40)}"
                f"{safe_str(row.get('LINECODE1')).ljust(1)}"
                f"{safe_str(row.get('NAMETITLE1')).ljust(40)}"
                f"{safe_str(row.get('LINECODE2')).ljust(1)}"
                f"{safe_str(row.get('NAMETITLE2')).ljust(40)}"
                f"{safe_str(row.get('SALUTATION')).ljust(40)}"
                f"{safe_str(row.get('TITLECODE')).rjust(4)}"
                f"{safe_str(row.get('FIRSTMID')).ljust(30)}"
                f"{safe_str(row.get('SURNAME')).ljust(20)}"
                f"{safe_str(row.get('SURNAMEKEY')).ljust(3)}"
                f"{safe_str(row.get('SUFFIXCODE')).ljust(2)}"
                f"{safe_str(row.get('APPENDCODE')).ljust(2)}"
                f"{safe_str(row.get('PRIMPHONE')).rjust(6)}"
                f"{safe_str(row.get('PPHONELTH')).rjust(2)}"
                f"{safe_str(row.get('SECPHONE')).rjust(6)}"
                f"{safe_str(row.get('SPHONELTH')).rjust(2)}"
                f"{safe_str(row.get('TELEXPHONE')).rjust(6)}"
                f"{safe_str(row.get('TPHONELTH')).rjust(2)}"
                f"{safe_str(row.get('FAXPHONE')).rjust(6)}"
                f"{safe_str(row.get('FPHONELTH')).rjust(2)}"
                f"{safe_str(row.get('LASTCHANGE')).ljust(10)}"
                f"{safe_str(row.get('PARSEIND')).ljust(1)}"
            )
            f.write(line + "\n")
"""
