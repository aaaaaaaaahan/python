import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

#---------------------------------------------------------------------#
# Original Program: CIHRCPUR                                          #
#---------------------------------------------------------------------#
# ESMR 2023-0862 PURGE HRC RECORDS MORE THAN 60 DAYS FROM DB2         #
#                WITH APPROVAL STATUS '05' OR '06'                    #
#---------------------------------------------------------------------#

# ---------------------------
# Connect to DuckDB
# ---------------------------
con = duckdb.connect()

# ---------------------------------------------------------------------
# Step 1: Define cutoff date (today - 60 days)
# ---------------------------------------------------------------------
start_date = (datetime.date.today() - datetime.timedelta(days=60)).strftime("%Y-%m-%d")

# ---------------------------------------------------------------------
# Step 2: Load input parquet (already converted from FB file)
# ---------------------------------------------------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE HRC AS
    SELECT 
        ALIAS,
        BRCHCODE,
        ACCTTYPE,
        APPROVALSTATUS,
        ACCTNO,
        CISNO,
        CREATIONDATE,
        PRIMARYJOINT,
        CISJOINTID1,
        CISJOINTID2,
        CISJOINTID3,
        CISJOINTID4,
        CISJOINTID5,
        CUSTTYPE,
        CUSTNAME,
        CUSTGENDER,
        CUSTDOBDOR,
        CUSTEMPLOYER,
        CUSTADDR1,
        CUSTADDR2,
        CUSTADDR3,
        CUSTADDR4,
        CUSTADDR5,
        CUSTPHONE,
        CUSTPEP,
        DTCORGUNIT,
        DTCINDUSTRY,
        DTCNATION,
        DTCOCCUP,
        DTCACCTTYPE,
        DTCCOMPFORM AS TEMPCOMPFORM,
        DTCWEIGHTAGE,
        DTCTOTAL,
        DTCSCORE1,
        DTCSCORE2,
        DTCSCORE3,
        DTCSCORE4,
        DTCSCORE5,
        DTCSCORE6,
        ACCTPURPOSE,
        ACCTREMARKS,
        SOURCEFUND,
        SOURCEDETAILS,
        PEPINFO,
        PEPWEALTH,
        PEPFUNDS,
        BRCHRECOMDETAILS,
        BRCHREWORK,
        HOVERIFYDATE,
        HOAPPROVEDATE,
        UPDATEDATE,
        UPDATETIME
    FROM '{host_parquet_path("UNLOAD_CIHRCAPT_FB.parquet")}'
    WHERE APPROVALSTATUS IN ('05','06')
""")

# ---------------------------------------------------------------------
# Step 3: Convert CREATIONDATE to date type and filter older than 60 days
# ---------------------------------------------------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE TODEL AS
    SELECT *
    FROM HRC
    WHERE try_cast(CREATIONDATE AS DATE) < DATE '{start_date}'
""")

# ---------------------------------------------------------------------
# Step 4: Select only required output fields
# ---------------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE OUT AS
    SELECT
        ALIAS,
        BRCHCODE,
        ACCTTYPE,
        APPROVALSTATUS,
        ACCTNO,
        CISNO,
        CREATIONDATE,
        PRIMARYJOINT,
        CISJOINTID1,
        CISJOINTID2,
        CISJOINTID3,
        CISJOINTID4,
        CISJOINTID5
    FROM TODEL
    ORDER BY ALIAS
""")

# ---------------------------------------------------------------------
# Step 5: Export results to Parquet and CSV
# ---------------------------------------------------------------------
out = """
    SELECT
        *
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM OUT
""".format(year=year,month=month,day=day)

queries = {
    "HRC_DELETE_MORE60D"                      : out
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
