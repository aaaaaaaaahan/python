import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")
DATE1 = batch_date.strftime('%Y%m%d')
DATE2 = batch_date.strftime('%Y%m%d')

# -------------------------------
# CONNECT TO DUCKDB
# -------------------------------
con = duckdb.connect()
cis = get_hive_parquet('CIS_CUST_DAILY')

# -------------------------------
# LOAD CONSENT DATA
# -------------------------------
# Convert EFFTIMESTAMP to inverted value like in SAS: 100000000000000 - EFFTIMESTAMP
con.execute(f"""
    CREATE OR REPLACE VIEW consent AS
    SELECT
        CUSTNO,
        100000000000000 - CAST(EFFTIMESTAMP AS BIGINT) AS EFFDATETIME,
        CAST(100000000000000 - CAST(EFFTIMESTAMP AS BIGINT) AS VARCHAR) AS EFFDATETIMEX,
        SUBSTR(CAST(100000000000000 - CAST(EFFTIMESTAMP AS BIGINT) AS VARCHAR), 1, 8) AS EFFDATE,
        SUBSTR(CAST(100000000000000 - CAST(EFFTIMESTAMP AS BIGINT) AS VARCHAR), 9, 6) AS EFFTIME,
        KEYWORD,
        CHANNEL,
        CONSENT
    FROM '{host_parquet_path("REMARKS_CONSENT_ALL.parquet")}'
""")

# -------------------------------
# LOAD CUSTOMER DATA
# -------------------------------
# Filter out specific ACCTCODE, invalid ACCTNOC, and empty ALIASKEY & TAXID
con.execute(f"""
    CREATE OR REPLACE VIEW cust AS
    SELECT DISTINCT *
    FROM read_parquet('{cis[0]}')
    WHERE ACCTCODE NOT IN ('DP   ','LN   ','EQC  ','FSF  ')
      AND ACCTNOC > '1000000000000000'
      AND ACCTNOC < '9999999999999999'
      AND NOT (ALIASKEY = '' AND TAXID = '')
""")

# -------------------------------
# MERGE CONSENT AND CUSTOMER DATA
# -------------------------------
con.execute("""
    CREATE OR REPLACE VIEW merge_data AS
    SELECT c.*, co.CONSENT, co.CHANNEL, co.EFFDATETIME, co.EFFDATE, co.EFFTIME
    FROM cust c
    INNER JOIN consent co
    ON c.CUSTNO = co.CUSTNO
""")

# -------------------------------
# ALLFILE
# -------------------------------
con.execute("""
    CREATE OR REPLACE TABLE all AS
    SELECT ACCTNOC, ALIASKEY, ALIAS, TAXID, CONSENT, CHANNEL
    FROM merge_data
    ORDER BY ACCTNOC
""")

# -------------------------------
# DAILY FILE (DAY)
# -------------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE daily AS
    SELECT ACCTNOC, ALIASKEY, ALIAS, TAXID, CONSENT, CHANNEL
    FROM merge_data
    WHERE EFFDATE = '{DATE2}' AND CHANNEL != 'UNIBATCH'
    ORDER BY ACCTNOC
""")

# ---------------------------------------------------------------------
# OUTPUT PARQUET
# ---------------------------------------------------------------------
out1 = """
    SELECT 
        *,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM all
""".format(year=year,month=month,day=day)

out2 = """
    SELECT 
        *,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM daily
""".format(year=year,month=month,day=day)

queries = {
    "UNICARD_MAILFLAG_ALL"                      : out1,
    "UNICARD_MAILFLAG_DLY"                      : out2
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
    "UNICARD_MAILFLAG_ALL"                      : out1,
    "UNICARD_MAILFLAG_DLY"                      : out2
}

for txt_name, txt_query in txt_queries.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(row['ACCTNOC']).ljust(16)}"
                f"{str(row['ALIASKEY']).ljust(3)}"
                f"{str(row['ALIAS']).ljust(12)}"
                f"{str(row['TAXID']).ljust(12)}"
                f"{str(row['CONSENT']).ljust(1)}"
                f"{str(row['CHANNEL']).ljust(8)}"
            )
            f.write(line + "\n")
