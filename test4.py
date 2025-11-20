import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")
DATE3 = batch_date.strftime("%Y%m%d")     # format YYYYMMDD

# =============================================================================
# DuckDB connection
# =============================================================================
con = duckdb.connect()
cis = get_hive_parquet('CIS_CUST_DAILY')

# =============================================================================
# LOAD CIS (CISFILE.CUSTDLY)
# =============================================================================
con.execute(f"""
    CREATE TABLE CIS AS
    SELECT *
    EXCLUDE (ALIAS,ALIASKEY)
    FROM read_parquet('{cis[0]}')
    WHERE (
         ACCTNO BETWEEN '01000000000' AND '01999999999' OR
         ACCTNO BETWEEN '03000000000' AND '03999999999' OR
         ACCTNO BETWEEN '04000000000' AND '04999999999' OR
         ACCTNO BETWEEN '05000000000' AND '05999999999' OR
         ACCTNO BETWEEN '06000000000' AND '06999999999' OR
         ACCTNO BETWEEN '07000000000' AND '07999999999'
    )
    ORDER BY CUSTNO
""")


# =============================================================================
# LOAD HR FILE (WITH VALIDATION)
# =============================================================================
con.execute(f"""
    CREATE TABLE HR_RAW AS
    SELECT *
    FROM '{host_parquet_path("HCMS_STAFF_RESIGN.parquet")}'
""")

# Validate HEADER date (DATAINDC=0, HEADERDATE must match DATE3)
hdr = con.execute("""
    SELECT HEADERDATE 
    FROM HR_RAW 
    WHERE DATAINDC = '0'
""").fetchone()

if hdr and hdr[0] != DATE3:
    raise Exception(f"ABORT 77: HEADERDATE {hdr[0]} != REPORT DATE {DATE3}")

# Extract HR + OLD_IC
con.execute("""
    CREATE TABLE HR AS
    SELECT *
    FROM HR_RAW
    WHERE DATAINDC = '1' AND REGEXP_MATCHES(ALIAS, '^[0-9]{12}$')
""")

con.execute("""
    CREATE TABLE OLD_IC AS
    SELECT *, '003 IC NOT 12 DIGIT      ' AS remarks
    FROM HR_RAW
    WHERE DATAINDC = '1' AND NOT REGEXP_MATCHES(ALIAS, '^[0-9]{12}$')
""")

# Validate TRAILER (DATAINDC=9)
trailer = con.execute("""
    SELECT total_rec
    FROM HR_RAW WHERE DATAINDC='9'
""").fetchone()

count_hr = con.execute("SELECT COUNT(*) FROM HR").fetchone()[0]

if trailer and int(trailer[0]) != count_hr:
    raise Exception(f"ABORT 88: trailer count {trailer[0]} != HR count {count_hr}")


# =============================================================================
# LOAD ALS FILE
# =============================================================================
con.execute(f"""
    CREATE TABLE ALS AS
    SELECT CUSTNO, ALIAS, ALIASKEY
    FROM '{host_parquet_path("ALLALIAS_FIX.parquet")}'
    WHERE ALIASKEY = 'IC'
""")


# =============================================================================
# MATCH 1: HR + ALS → RESULT1, NO_IC
# =============================================================================
con.execute("""
    CREATE TABLE RESULT1 AS
    SELECT hr.*, als.*
    FROM HR hr
    JOIN ALS als USING (ALIAS)
""")

con.execute("""
    CREATE TABLE NO_IC AS
    SELECT hr.*, als.*, '001 STAFF IC NOT FOUND   ' AS REMARKS
    FROM HR hr
    LEFT JOIN ALS als USING (ALIAS)
    WHERE als.ALIAS IS NULL
""")


# =============================================================================
# MATCH 2: RESULT1 + CIS → MATCH2, NO_ACCT
# =============================================================================
con.execute("""
    CREATE TABLE MATCH2 AS
    SELECT r.*, c.CUSTNAME, c.ACCTCODE, c.ACCTNOC, c.PRISEC
    FROM RESULT1 r
    JOIN CIS c
    ON c.CUSTNO = r.CUSTNO
""")

con.execute("""
    CREATE TABLE NO_ACCT AS
    SELECT r.*, '002 CIS WITH NO ACCOUNT  ' AS REMARKS
    FROM RESULT1 r
    LEFT JOIN CIS c USING (CUSTNO)
    WHERE c.CUSTNO IS NULL
""")

con.execute("""
    CREATE TABLE notfound AS
    SELECT REMARKS, ORGID, STAFFID, ALIAS, HRNAME, CUSTNO
    FROM NO_IC
    UNION ALL
    SELECT REMARKS, ORGID, STAFFID, ALIAS, HRNAME, CUSTNO
    FROM NO_ACCT
    UNION ALL
    SELECT REMARKS, ORGID, STAFFID, ALIAS, HRNAME, ' ' AS CUSTNO
    FROM OLD_IC
    ORDER BY REMARKS, STAFFID
""")

# ----------------------------
# OUTPUT TO PARQUET, CSV, TXT
# ----------------------------
out1 = """
    SELECT 
        *, 
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM notfound
""".format(year=year,month=month,day=day)

out2 = """
    SELECT 
        STAFFID,
        CUSTNO,
        HRNAME,
        CUSTNAME,
        ALIASKEY,
        ALIAS,
        CASE WHEN PRISEC=901 THEN 'P'
             WHEN PRISEC=902 THEN 'S'
        ELSE '' END AS PRIMSEC,
        ACCTCODE,
        ACCTNOC, 
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM MATCH2
""".format(year=year,month=month,day=day)

# Dictionary of outputs for parquet & CSV
queries = {
    "CIS_EMPLOYEE_RESIGN_NOTFOUND":              out1,
    "CIS_EMPLOYEE_RESIGN":                       out2
    }

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    # COPY to Parquet with partitioning
    con.execute(f"""
        COPY ({query})
        TO '{parquet_path}'
        (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE TRUE)
    """)

    # COPY to CSV with header
    con.execute(f"""
        COPY ({query})
        TO '{csv_path}'
        (FORMAT CSV, HEADER, DELIMITER ';', OVERWRITE_OR_IGNORE TRUE)
    """)

# Dictionary for fixed-width TXT
txt_queries1 = {
        "CIS_EMPLOYEE_RESIGN_NOTFOUND":              out1
    }

for txt_name, txt_query in txt_queries1.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(row['REMARKS']).ljust(25)}"
                f"{str(row['ORGID']).ljust(13)}"
                f"{str(row['STAFFID']).ljust(9)}"
                f"{str(row['ALIAS']).ljust(15)}"
                f"{str(row['HRNAME']).zfill(40)}"
                f"{str(row['CUSTNO']).zfill(11)}"
            )
            f.write(line + "\n")

txt_queries2 = {
        "CIS_EMPLOYEE_RESIGN":              out2
    }

for txt_name, txt_query in txt_queries1.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(row['STAFFID']).ljust(10)}"
                f"{str(row['CUSTNO']).ljust(11)}"
                f"{str(row['HRNAME']).ljust(40)}"
                f"{str(row['CUSTNAME']).ljust(40)}"
                f"{str(row['ALIASKEY']).zfill(3)}"
                f"{str(row['ALIAS']).zfill(15)}"
                f"{str(row['PRIMSEC']).zfill(1)}"
                f"{str(row['ACCTCODE']).zfill(5)}"
                f"{str(row['ALIAS']).zfill(20)}"
            )
            f.write(line + "\n")
