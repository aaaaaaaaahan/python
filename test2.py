import duckdb
import pyarrow.parquet as pq
import pyarrow.csv as pc
from reader import host_parquet_path, parquet_output_path, csv_output_path

#--------------------------------#
# Open DuckDB in-memory database #
#--------------------------------#
con = duckdb.connect(database=":memory:")

#-----------------------------------#
# Load parquet datasets into DuckDB #
#-----------------------------------#
con.execute(f"""
    CREATE VIEW primary AS 
    SELECT 
        CAST(ACCTNO AS VARCHAR) AS ACCTNO,
        CAST(ACCTCODE AS VARCHAR) AS ACCTCODE,
        CAST(CUSTNO AS VARCHAR) AS CUSTNO
    FROM read_parquet('{host_parquet_path("RLENCA_NONJOINT")}')
""")

# Single source file (RLNSHIP), then split into IND / ORG
con.execute(f"""
    CREATE VIEW ccr_all AS
    SELECT 
        CUSTNO1, INDORG1 AS CUSTTYPE1, CODE1 AS RLENCODE1, DESC1,
        CUSTNO2 AS CUSTNO, INDORG2 AS CUSTTYPE, CODE2 AS RLENCODE, DESC2 AS DESC,
        CUSTNAME1, ALIAS1, CUSTNAME2 AS CUSTNAME, ALIAS2 AS ALIAS
    FROM read_parquet('{host_parquet_path("RLNSHIP")}')
""")

# Split into ORG (O) and IND (I)
con.execute("""
    CREATE VIEW ccrlen AS
    SELECT * FROM ccr_all WHERE CUSTTYPE = 'O'
""")

con.execute("""
    CREATE VIEW ccrlen1 AS
    SELECT * FROM ccr_all WHERE CUSTTYPE = 'I'
""")

#------------------------------------------------------#
# Merge organisation CCRLEN with PRIMARY accounts      #
#------------------------------------------------------#
con.execute("""
    CREATE VIEW cc_primary AS
    SELECT
        c.CUSTNO1, c.CUSTTYPE1, c.RLENCODE1, c.DESC1,
        c.CUSTNO,  c.CUSTTYPE,  c.RLENCODE,  c.DESC,
        c.CUSTNAME1, c.ALIAS1, c.CUSTNAME, c.ALIAS,
        p.ACCTNO, p.ACCTCODE
    FROM ccrlen c
    INNER JOIN primary p
        ON c.CUSTNO = p.CUSTNO
""")

#------------------------------------------------------#
# Union ORG+PRIMARY with IND relationship (ccrlen1)    #
#------------------------------------------------------#
con.execute("""
    CREATE VIEW out1 AS
    SELECT
        CUSTNO1, CUSTTYPE1, RLENCODE1, DESC1,
        CUSTNO, CUSTTYPE, RLENCODE, DESC,
        ACCTNO, ACCTCODE, CUSTNAME1, ALIAS1,
        CUSTNAME, ALIAS
    FROM cc_primary

    UNION ALL

    SELECT
        CUSTNO1, CUSTTYPE1, RLENCODE1, DESC1,
        CUSTNO, CUSTTYPE, RLENCODE, DESC,
        NULL AS ACCTNO, NULL AS ACCTCODE,
        CUSTNAME1, ALIAS1, CUSTNAME, ALIAS
    FROM ccrlen1
""")

#-----------------------------------#
# Export using PyArrow              #
#-----------------------------------#
# Convert to Arrow Table
out_table = con.execute("SELECT * FROM out1").arrow()

# Write Parquet
pq.write_table(out_table, parquet_output_path("PARTIES.parquet"))

# Write CSV with quotes + \N for nulls
pc.write_csv(
    out_table,
    csv_output_path("IMIS.csv"),
    write_options=pc.WriteOptions(
        include_header=True,
        delimiter=",",
        quoting_style="all",
        null_value="\\N"
    )
)
