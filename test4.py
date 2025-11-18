# ----------------------------
# OUTPUT TO PARQUET, CSV, TXT
# ----------------------------

# Dictionary of tables to output
output_tables = {
    "GOODDP": "GOODDP",
    "BADDP": "BADDP",
    "GOOD_PBB": "GOOD_PBB",
    "GOOD_PIBB": "GOOD_PIBB"
}

for name, table in output_tables.items():
    # Query
    query = f"""
        SELECT *,
            {year} AS year,
            {month} AS month,
            {day} AS day
        FROM {table}
    """
    
    # Paths
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)
    txt_path = csv_output_path(f"{name}_{batch_date}").replace(".csv", ".txt")
    
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
    
    # Fixed-width TXT
    df_txt = con.execute(query).fetchdf()
    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(row.get('LOAD_DATE','')).ljust(10)}"
                f"{str(row.get('BANKNUM','')).ljust(5)}"
                f"{str(row.get('CUSTBRCH','')).ljust(3)}"
                f"{str(row.get('ACCTNO','')).ljust(11)}"
                f"{str(row.get('ACCSTAT','')).ljust(1)}"
                f"{str(row.get('LEDBAL','')).rjust(12)}"
                f"{str(row.get('COSTCTR','')).ljust(3)}"
                f"{str(row.get('year')).rjust(4)}"
                f"{str(row.get('month')).rjust(2,'0')}"
                f"{str(row.get('day')).rjust(2,'0')}"
            )
            f.write(line + "\n")
