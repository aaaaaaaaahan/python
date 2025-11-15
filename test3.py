import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")

# ---------------------------------------------------------
# 1. Load CIHRCRVT parquet (already converted AS400 FB file)
# ---------------------------------------------------------
con = duckdb.connect()

con.execute(f"""
    CREATE VIEW cihrcrvt AS
    SELECT
        HRV_MONTH,
        HRV_BRCH_CODE,
        HRV_ACCT_TYPE,
        HRV_ACCT_NO,
        HRV_CUSTNO,
        HRV_CUSTID,
        HRV_CUST_NAME,
        HRV_NATIONALITY,
        HRV_ACCT_OPENDATE,
        HRV_OVERRIDING_INDC,
        HRV_OVERRIDING_OFFCR,
        HRV_OVERRIDING_REASON,
        HRV_DOWJONES_INDC,
        HRV_FUZZY_INDC,
        HRV_FUZZY_SCORE,
        HRV_NOTED_BY,
        HRV_RETURNED_BY,
        HRV_ASSIGNED_TO,
        HRV_NOTED_DATE,
        HRV_RETURNED_DATE,
        HRV_ASSIGNED_DATE,
        HRV_COMMENT_BY,
        HRV_COMMENT_DATE,
        HRV_SAMPLING_INDC,
        HRV_RETURN_STATUS,
        HRV_RECORD_STATUS,
        HRV_FUZZY_SCREEN_DATE
    FROM read_parquet({host_parquet_path("UNLOAD_CIHRCRVT_FB")})
    ORDER BY
        HRV_MONTH,
        HRV_BRCH_CODE,
        HRV_ACCT_TYPE,
        HRV_ACCT_NO,
        HRV_CUSTNO
""")

# ----------------------------
# OUTPUT TO PARQUET, CSV, TXT
# ----------------------------
out = """
    SELECT 
        *,  
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM cihrcrvt
""".format(year=year,month=month,day=day)

# Dictionary of outputs for parquet & CSV
queries = {
    "UNLOAD_CIHRCRVT_EXCEL":              out
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
        (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE TRUE)
    """)

# ---------------------------------------------------------
# 3. Write TXT output (with header + | delimiter)
# ---------------------------------------------------------
txt_queries = {
        "CIS_ALIAS_CHANGE_RPT":              out
    }

header = [
    "DETAIL LISTING FOR CIHRCRVT",
    "MONTH|BRCH_CODE|ACCT_TYPE|ACCT_NO|CUSTNO|CUSTID|CUST_NAME|"
    "NATIONALITY|ACCT_OPENDATE|OVERRIDING_INDC|OVERRIDING_OFFCR|"
    "OVERRIDING_REASON|DOWJONES_INDC|FUZZY_INDC|FUZZY_SCORE|"
    "NOTED_BY|RETURNED_BY|ASSIGNED_TO|NOTED_DATE|RETURNED_DATE|"
    "ASSIGNED_DATE|COMMENT_BY|COMMENT_DATE|SAMPLING_INDC|RETURN_STATUS|"
    "RECORD_STATUS|FUZZY_SCREEN_DATE"
]

for txt_name, txt_query in txt_queries.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(row['UPDOPER']).ljust(10)}"
                f"{str(row['CUSTNO']).ljust(20)}"
                f"{str(row['ACCTNOC']).ljust(20)}"
                f"{str(row['CUSTNAME']).ljust(40)}"
                f"{str(row['FIELDS']).ljust(20)}"
                f"{str(row['OLDVALUE']).ljust(150)}"
                f"{str(row['NEWVALUE']).ljust(150)}"
                f"{str(row['UPDDATX']).ljust(10)}"
            )
            f.write(line + "\n")
