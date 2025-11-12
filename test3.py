import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")

# ---------------------------------------------------------------------
# DUCKDB PROCESSING
# ---------------------------------------------------------------------
con = duckdb.connect()

# ---------------------------------------------------------------------
# Read input parquet files into DuckDB tables
# ---------------------------------------------------------------------
con.execute(f"""
    CREATE TABLE NAME AS 
    SELECT DISTINCT * 
    FROM read_parquet('{host_parquet_path("PRIMNAME_OUT.parquet")}')
    ORDER BY CUSTNO
""")

con.execute(f"""
    CREATE TABLE RMRK AS 
    SELECT DISTINCT * 
    FROM read_parquet('{host_parquet_path("CCRIS_CISRMRK_LONGNAME.parquet")}')
    WHERE INDORG IS NOT NULL AND INDORG != ''
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
    ORDER BY CUSTNO
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

df = con.execute(out).fetchdf()

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
txt_path = csv_output_path(f"CIS_DJW_DPACCT_{report_date}").replace(".csv", ".txt")

res = con.execute(out)
columns = [desc[0] for desc in res.description]
rows = res.fetchall()

with open(txt_path, "w", encoding="utf-8") as f:
    for _, row in df.iterrows():
        line = (
            f"{str(row['CUSTNO']).ljust(20)}"
            f"{str(row['ACCTCODE']).ljust(5)}"
            f"{str(row['ACCTNOX']).ljust(20)}"
            f"{str(row['OPENDX']).ljust(10)}"
        )
        f.write(line + "\n")
