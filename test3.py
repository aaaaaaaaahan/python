import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")

# ---------------------------------------------------------------------
# DUCKDB PROCESSING
# ---------------------------------------------------------------------
con = duckdb.connect()

# Load input parquet into DuckDB
con.execute(f"""
    CREATE OR REPLACE TABLE CISEOD AS
    SELECT *
    FROM '{host_parquet_path("CIDARPGS.parquet")}'
""")

# ---------------------------------------------------------------------
# Transform logic (convert SAS logic into SQL)
# ---------------------------------------------------------------------
# This will handle:
#  - Filtering REPORTNO=8106 and SORTSETNO=1
#  - Deriving CONSENTX from MISC(A–J)
#  - Mapping 001→Y, 002→N
#  - Formatting date from UPDDATE (PD6 numeric to YYYY-MM-DD)
#  - Add fixed constant fields
# ---------------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE CONSENT AS
    WITH BASE AS (
        SELECT 
            BANKNO,
            REPORTNO,
            SORTSETNO,
            UPDATEOPERATOR,
            UPDDATE,
            CUSTNO,
            CASE 
                WHEN MISCA IN ('07A','07C','07D') THEN CONSENTA
                WHEN MISCB IN ('07A','07C','07D') THEN CONSENTB
                WHEN MISCC IN ('07A','07C','07D') THEN CONSENTC
                WHEN MISCD IN ('07A','07C','07D') THEN CONSENTD
                WHEN MISCE IN ('07A','07C','07D') THEN CONSENTE
                WHEN MISCF IN ('07A','07C','07D') THEN CONSENTF
                WHEN MISCG IN ('07A','07C','07D') THEN CONSENTG
                WHEN MISCH IN ('07A','07C','07D') THEN CONSENTH
                WHEN MISCI IN ('07A','07C','07D') THEN CONSENTI
                WHEN MISCJ IN ('07A','07C','07D') THEN CONSENTJ
                ELSE ''
            END AS CONSENTX
        FROM CISEOD
        WHERE REPORTNO = 8106 AND SORTSETNO = 1
    ),
    FILTERED AS (
        SELECT *
        FROM BASE
        WHERE CONSENTX <> ''
    ),
    FINAL AS (
        SELECT DISTINCT
            BANKNO,
            CUSTNO AS CUSTNOX,
            'CUST' AS APPLCODE,
            'BATCH' AS UPDATESOURCE,
            CASE 
                WHEN CONSENTX = '001' THEN 'Y'
                WHEN CONSENTX = '002' THEN 'N'
                ELSE ''
            END AS CONSENT,
            -- Convert numeric date to YYYY-MM-DD
            CASE 
                WHEN length(cast(UPDDATE AS VARCHAR)) = 6 THEN 
                    substr(lpad(cast(UPDDATE AS VARCHAR),8,'0'),5,4) || '-' ||
                    substr(lpad(cast(UPDDATE AS VARCHAR),8,'0'),3,2) || '-' ||
                    substr(lpad(cast(UPDDATE AS VARCHAR),8,'0'),1,2)
                ELSE ''
            END AS UPDATEDATE,
            '00000000' AS UPDATETIME,
            UPDATEOPERATOR
        FROM FILTERED
    )
    SELECT * FROM FINAL
""")

# ---------------------------------------------------------------------
# OUTPUT PARQUET
# ---------------------------------------------------------------------
out = """
    SELECT 
        BANKNO,
        'CUST' AS INDICATOR1,
        CUSTNOX,
        CONSENT,
        UPDATEDATE,
        'X' AS INDICATOR2,
        UPDATESOURCE,
        UPDATEDATE,
        UPDATETIME,
        UPDATESOURCE,
        UPDATEDATE,
        UPDATETIME,
        UPDATEOPERATOR,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM CONSENT
""".format(year=year,month=month,day=day)

queries = {
    "CIDARPGS_CONSENT"                      : out
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
# OUTPUT FIXED-WIDTH TEXT
# ---------------------------------------------------------------------
txt_queries = {
        "CIDARPGS_CONSENT"                      : out
}

for txt_name, txt_query in txt_queries.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(row['BANKNO']).ljust(3)}"
                f"{str(row['INDICATOR1']).ljust(5)}"
                f"{str(row['CUSTNOX']).ljust(11)}"
                f"{str(row['CONSENT']).ljust(1)}"
                f"{str(row['UPDATEDATE']).ljust(10)}"
                f"{str(row['INDICATOR2']).ljust(1)}"
                f"{str(row['UPDATESOURCE']).ljust(5)}"
                f"{str(row['UPDATEDATE']).ljust(10)}"
                f"{str(row['UPDATETIME']).ljust(8)}"
                f"{str(row['UPDATESOURCE']).ljust(5)}"
                f"{str(row['UPDATEDATE']).ljust(10)}"
                f"{str(row['UPDATETIME']).ljust(8)}"
            )
            f.write(line + "\n")
