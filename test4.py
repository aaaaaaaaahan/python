# ----------------------------
# OUTPUT TO PARQUET, CSV, TXT
# ----------------------------
out = """
    SELECT 
        CUSTNO,
        ACCTCODE,
        ACCTNOC,
        JOINTACC,
        CUSTNAME, 
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM OUT
""".format(year=year,month=month,day=day)

# Dictionary of outputs for parquet & CSV
queries = {
    "CUSTCODE_BNKEMP":              out
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
txt_queries = {
        "CUSTCODE_BNKEMP":              out
    }

for txt_name, txt_query in txt_queries.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(row['CUSTNO']).ljust(20)}"
                f"{str(row['ACCTCODE']).ljust(5)}"
                f"{str(row['ACCTNOC']).ljust(11)}"
                f"{str(row['JOINTACC']).ljust(1)}"
                f"{str(row['CUSTNAME']).ljust(40)}"
            )
            f.write(line + "\n")
