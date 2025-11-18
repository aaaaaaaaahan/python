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
    # ----------------------------
    # Query with explicit fields + year/month/day
    # ----------------------------
    query = f"""
        SELECT
            BANKNUM,
            CUSTBRCH,
            CUSTNO,
            CUSTNAME,
            RACE,
            CITIZENSHIP,
            INDORG,
            PRIMSEC,
            CUSTLASTDATECC,
            CUSTLASTDATEYY,
            CUSTLASTDATEMM,
            CUSTLASTDATEDD,
            ALIASKEY,
            ALIAS,
            HRCCODES,
            BRANCH,
            ACCTCODE,
            ACCTNO,
            OPENDATE,
            LEDBAL,
            ACCSTAT,
            COSTCTR,
            {year} AS year,
            {month} AS month,
            {day} AS day
        FROM {table}
    """
    
    # Paths
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)
    txt_path = csv_output_path(f"{name}_{batch_date}").replace(".csv", ".txt")
    
    # ----------------------------
    # COPY to Parquet with partitioning
    # ----------------------------
    con.execute(f"""
        COPY ({query})
        TO '{parquet_path}'
        (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE TRUE)
    """)
    
    # ----------------------------
    # COPY to CSV with header
    # ----------------------------
    con.execute(f"""
        COPY ({query})
        TO '{csv_path}'
        (FORMAT CSV, HEADER, DELIMITER ';', OVERWRITE_OR_IGNORE TRUE)
    """)
    
    # ----------------------------
    # Fixed-width TXT following SAS PUT layout
    # ----------------------------
    df_txt = con.execute(query).fetchdf()
    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(row.get('BANKNUM','')).rjust(3,'0')}"                  # @01 BANKNUM Z3.
                f"{str(row.get('CUSTBRCH','')).rjust(5,'0')}"                # @04 CUSTBRCH Z5.
                f"{str(row.get('CUSTNO','')).ljust(11)}"                     # @09 CUSTNO $11.
                f"{str(row.get('CUSTNAME','')).ljust(40)}"                   # @20 CUSTNAME $40.
                f"{str(row.get('RACE','')).ljust(1)}"
