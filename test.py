import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

#--------------------------------#
# Open DuckDB in-memory database #
#--------------------------------#
#con = duckdb.connect(database=":memory:")
con = duckdb.connect()

#-----------------------------------#
# Load parquet datasets into DuckDB #
#-----------------------------------#
con.execute(f"""
    CREATE VIEW primary1 AS 
    SELECT 
        CAST(ACCTNO AS VARCHAR) AS ACCTNO,
        CAST(ACCTCODE AS VARCHAR) AS ACCTCODE,
        CAST(CUSTNO AS VARCHAR) AS CUSTNO
    FROM '{host_parquet_path("RLENCA_NONJOINT.parquet")}';

    CREATE VIEW ccr_all AS
    SELECT 
        CUSTNO1, INDORG1 AS CUSTTYPE1, CODE1 AS RLENCODE1, DESC1,
        CUSTNO2 AS CUSTNO, INDORG2 AS CUSTTYPE, CODE2 AS RLENCODE, DESC2 AS "DESC",
        CUSTNAME1, ALIAS1, CUSTNAME2 AS CUSTNAME, ALIAS2 AS ALIAS
    FROM read_parquet('/host/cis/parquet/RLNSHIP_SRCH/year=2025/month=9/day=22/data_0.parquet');
""")

# Single source file (RLNSHIP), then split into IND / ORG
#con.execute(f"""
#    CREATE VIEW ccr_all AS
#    SELECT 
#        CUSTNO1, INDORG1 AS CUSTTYPE1, CODE1 AS RLENCODE1, DESC1,
#        CUSTNO2 AS CUSTNO, INDORG2 AS CUSTTYPE, CODE2 AS RLENCODE, DESC2 AS DESC,
#        CUSTNAME1, ALIAS1, CUSTNAME2 AS CUSTNAME, ALIAS2 AS ALIAS
#    FROM '{host_parquet_path("RLNSHIP_SRCH.parquet")}'
#""")

# Split into ORG (O) and IND (I)
con.execute("""
    CREATE VIEW ccrlen AS
    SELECT * FROM ccr_all WHERE CUSTTYPE = 'O';

    CREATE VIEW ccrlen1 AS
    SELECT * FROM ccr_all WHERE CUSTTYPE = 'I';
""")

#con.execute("""
#    CREATE VIEW ccrlen1 AS
#    SELECT * FROM ccr_all WHERE CUSTTYPE = 'I'
#""")

#------------------------------------------------------#
# Merge organisation CCRLEN with PRIMARY accounts      #
#------------------------------------------------------#
con.execute("""
    CREATE VIEW cc_primary AS
    SELECT
        c.CUSTNO1, c.CUSTTYPE1, c.RLENCODE1, c.DESC1,
        c.CUSTNO,  c.CUSTTYPE,  c.RLENCODE,  c."DESC",
        c.CUSTNAME1, c.ALIAS1, c.CUSTNAME, c.ALIAS,
        p.ACCTNO, p.ACCTCODE
    FROM ccrlen c
    INNER JOIN primary1 p
        ON c.CUSTNO = p.CUSTNO
""")

#------------------------------------------------------#
# Union ORG+PRIMARY with IND relationship (ccrlen1)    #
#------------------------------------------------------#
con.execute("""
    CREATE VIEW out1 AS
    SELECT
        CUSTNO1, CUSTTYPE1, RLENCODE1, DESC1,
        CUSTNO, CUSTTYPE, RLENCODE, "DESC",
        ACCTCODE, ACCTNO, CUSTNAME1, ALIAS1,
        CUSTNAME, ALIAS
    FROM cc_primary

    UNION ALL

    SELECT
        CUSTNO1, CUSTTYPE1, RLENCODE1, DESC1,
        CUSTNO, CUSTTYPE, RLENCODE, "DESC",
        NULL AS ACCTCODE, NULL AS ACCTNO,
        CUSTNAME1, ALIAS1, CUSTNAME, ALIAS
    FROM ccrlen1
    ORDER BY CUSTNO1
""")

#-----------------------------------#
# Export using PyArrow              #
#-----------------------------------#
# Convert to Arrow Table
out_table = """
    SELECT * ,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM out1
""".format(year=year,month=month,day=day)

queries = {
    "CCRIS_CC_RLNSHIP_PARTIES"            : out_table,
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
    
queries = {
    "CCRIS_CC_RLNSHIP_PARTIES_IMIS"            : out_table,
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
    (FORMAT CSV, HEADER, QUOTE '"', DELIMITER ',', OVERWRITE_OR_IGNORE true);  
     """)
