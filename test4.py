out = """
    SELECT 
        *, 
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM shirt1
""".format(year=year,month=month,day=day)

# Dictionary of outputs for parquet & CSV
queries = {
    "CIS_EMPLOYEE_RESIGN_NOTFOUND":              out
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
    "CIS_EMPLOYEE_RESIGN_NOTFOUND": out
}

# Loop through all TXT outputs
for txt_name, txt_query in txt_queries.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
                line = (
                    f"{str(row['REMARKS']).ljust(25)}"
                    f"{str(row['ORGID']).ljust(13)}"
                    f"{str(row['STAFFID']).ljust(9)}"
                    f"{str(row['ALIAS']).ljust(15)}"
                    f"{str(row['HRNAME']).ljust(40)}"
                    f"{str(row['CUSTNO']).ljust(11)}"
                )
            f.write(line + "\n")
