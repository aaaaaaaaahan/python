# -----------------------------
# STEP 5: Write Parquet and CSV
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
ORDER BY CUSTNO
""".format(year=year,month=month,day=day)

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
# STEP 6: Write Fixed-width TXT
# -----------------------------
txt_queries = {
    "CICUSCD4_STAF002_INIT": out
}

for txt_name, txt_query in txt_queries.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(row['CUSTNO']).ljust(20)}"
                f"{str(row['INDORG']).ljust(1)}"
                f"{str(int(row['CUSTBRCH'])).zfill(7)}"
                f"{str(row['CUSTCODEALL']).ljust(60)}"
                f"{str(row['FILECODE']).ljust(1)}"
                f"{str(row['STAFFID']).ljust(9)}" 
                f"{str(row['CUSTNAME']).ljust(40)}"
            )
            f.write(line + "\n")
