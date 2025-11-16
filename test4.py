import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet
import datetime

batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")

# ============================
# CONFIG
# ============================
con = duckdb.connect()
cis = get_hive_parquet('CIS_CUST_DAILY')

# ============================
# STEP 1: LOAD PARQUET
# ============================
con.execute(f"""
    CREATE TABLE CUST AS
    SELECT * FROM read_parquet('{cis[0]}')
""")
print("STEP 1: CUST table preview (5 rows)")
print(con.execute("SELECT * FROM CUST LIMIT 5").fetchdf())

# ============================
# STEP 2: PROCESS HRC FIELDS
# ============================
hrc_list = [f"HRC{str(i).zfill(2)}" for i in range(1, 21)]
processed_hrc = ",\n".join([
    f"CASE WHEN LPAD(CAST({h} AS VARCHAR),3,'0')='002' THEN '   ' ELSE LPAD(CAST({h} AS VARCHAR),3,'0') END AS {h}C"
    for h in hrc_list
])
filter_condition = " OR ".join([f"LPAD(CAST({h} AS VARCHAR),3,'0')='002'" for h in hrc_list])

con.execute(f"""
    CREATE TABLE CIS AS
    SELECT
        CUSTNO,
        INDORG,
        CUSTBRCH,
        CUSTNAME,
        'A' AS FILECODE,
        {processed_hrc},
        '000' AS CODEFILLER
    FROM CUST
    WHERE {filter_condition}
""")
print("STEP 2: CIS table preview (5 rows)")
print(con.execute("SELECT * FROM CIS LIMIT 5").fetchdf())

# ============================
# STEP 4: CREATE CUSTCODEALL
# ============================
concat_fields = "||".join([f"{h}C" for h in hrc_list] + ["CODEFILLER"])
con.execute(f"""
    CREATE TABLE CIS2 AS
    SELECT *,
           REPLACE({concat_fields}, ' ', '') AS CUSTCODEALL
    FROM CIS
""")
print("STEP 4: CIS2 table preview (5 rows)")
print(con.execute("SELECT * FROM CIS2 LIMIT 5").fetchdf())

# ============================
# STEP 5: REMOVE DUPLICATES BY CUSTNO
# ============================
con.execute("""
    CREATE TABLE FINAL AS
    SELECT *
    FROM CIS2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) = 1
""")
print("STEP 5: FINAL table preview (5 rows)")
print(con.execute("SELECT * FROM FINAL LIMIT 5").fetchdf())

# -----------------------------
# 9. Write Parquet and CSV
# -----------------------------
out = """
SELECT 
    CUSTNO,
    INDORG,
    CUSTBRCH,
    CUSTCODEALL,
    FILECODE,
    ' ' AS STAFFID,
    CUSTNAME,  
    {year} AS year,
    {month} AS month,
    {day} AS day
FROM FINAL
""".format(year=year,month=month,day=day)

print("STEP 9: Output query preview (5 rows)")
print(con.execute(out + " LIMIT 5").fetchdf())

queries = {
    "CICUSCD4_STAF002_INIT": out
}

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    con.execute(f"""
        COPY ({query})
        TO '{parquet_path}'
        (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE TRUE)
    """)

    con.execute(f"""
        COPY ({query})
        TO '{csv_path}'
        (FORMAT CSV, HEADER, DELIMITER ';', OVERWRITE_OR_IGNORE TRUE)
    """)

# -----------------------------
# 10. Write Fixed-width TXT (matching SAS positions)
# -----------------------------
txt_queries = {
    "CICUSCD4_STAF002_INIT": out
}

for txt_name, txt_query in txt_queries.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    print(f"STEP 10: Fixed-width TXT preview for {txt_name} (5 rows)")
    print(df_txt.head(5))  # preview first 5 rows

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(row['CUSTNO']).ljust(20)}"
                f"{str(row['INDORG']).ljust(1)}"
                f"{str(row['CUSTBRCH']).ljust(7)}"
                f"{str(row['CUSTCODEALL']).ljust(60)}"
                f"{str(row['FILECODE']).ljust(1)}"
                f"{str(row['STAFFID']).ljust(9)}" 
                f"{str(row['CUSTNAME']).ljust(40)}"
            )
            f.write(line + "\n")
