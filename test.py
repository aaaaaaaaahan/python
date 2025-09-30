import duckdb
from CIS_PY_READER_test import host_parquet_path,parquet_output_path,csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# -----------------------------
# Step 1: Connect DuckDB
# -----------------------------
con = duckdb.connect()

# -----------------------------
# Step 2: Clean & prepare NAMEFILE
# -----------------------------
con.execute(f"""
    CREATE VIEW name_clean AS
    SELECT DISTINCT
        LPAD(CAST(CUSTNO AS VARCHAR), 11, '0') AS CUSTNO, CUSTNAME,
        LPAD(CAST(ADREFNO AS VARCHAR), 11, '0') AS ADREFNO,
        LPAD(CAST(CAST(PRIPHONE AS BIGINT) AS VARCHAR), 11, '0') AS PRIPHONE,
        LPAD(CAST(CAST(SECPHONE AS BIGINT) AS VARCHAR), 11, '0') AS SECPHONE, CUSTTYPE, CUSTNAME1,
        LPAD(CAST(CAST(MOBILEPHONE AS BIGINT) AS VARCHAR), 11, '0') AS MOBILEPHONE,
    FROM '{host_parquet_path("CCRIS_CISNAME_TEMP.parquet")}'
""")

# -----------------------------
# Step 3: Clean & prepare RMRKFILE
# -----------------------------
con.execute(f"""
    CREATE VIEW rmrk_clean AS
    SELECT DISTINCT
        BANKNO, APPLCODE, 
        LPAD(CAST(CAST(CUSTNO AS BIGINT) AS VARCHAR), 11, '0') AS CUSTNO,
        EFFDATE, RMKKEYWORD, LONGNAME, RMKOPERATOR, EXPIREDATE, LASTMNTDATE
    FROM '{host_parquet_path("CCRIS_CISRMRK_LONGNAME.parquet")}'
""")

# -----------------------------
# Step 4: Merge (LEFT JOIN like SAS "IF A;")
# -----------------------------
merge = f"""
    SELECT 
        n.CUSTNO,
        n.CUSTNAME,
        n.ADREFNO,
        n.PRIPHONE,
        n.SECPHONE,
        n.CUSTTYPE,
        n.CUSTNAME1,
        n.MOBILEPHONE,
        r.LONGNAME
        ,{year} AS year
        ,{month} AS month
        ,{day} AS day
    FROM name_clean n
    LEFT JOIN rmrk_clean r
    ON n.CUSTNO = r.CUSTNO
    ORDER BY n.CUSTNO
""".format(year=year,month=month,day=day)

#arrow_result = con.execute(query).arrow()  # PyArrow Table

# -----------------------------
# Step 5: Write Output with PyArrow
# -----------------------------
queries = {
    "CCRIS_CISNAME_OUT"            : merge
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
