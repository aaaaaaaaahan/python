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
    # Query with date columns
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
                f"{str(row.get('RACE','')).ljust(1)}"                        # @60 RACE $1.
                f"{str(row.get('CITIZENSHIP','')).ljust(2)}"                 # @61 CITIZENSHIP $2.
                f"{str(row.get('INDORG','')).ljust(1)}"                      # @63 INDORG $1.
                f"{str(row.get('PRIMSEC','')).ljust(1)}"                     # @64 PRIMSEC $1.
                f"{str(row.get('CUSTLASTDATECC','')).rjust(2,'0')}"          # @65 CUSTLASTDATECC Z2.
                f"{str(row.get('CUSTLASTDATEYY','')).rjust(2,'0')}"          # @67 CUSTLASTDATEYY Z2.
                f"{str(row.get('CUSTLASTDATEMM','')).rjust(2,'0')}"          # @69 CUSTLASTDATEMM Z2.
                f"{str(row.get('CUSTLASTDATEDD','')).rjust(2,'0')}"          # @71 CUSTLASTDATEDD Z2.
                f"{str(row.get('ALIASKEY','')).rjust(3,'0')}"                # @73 ALIASKEY Z3.
                f"{str(row.get('ALIAS','')).ljust(20)}"                      # @76 ALIAS $20.
                f"{str(row.get('HRCCODES','')).ljust(60)}"                   # @96 HRCCODES $60.
                f"{str(row.get('BRANCH','')).rjust(7,'0')}"                  # @156 BRANCH Z7.
                f"{str(row.get('ACCTCODE','')).ljust(5)}"                    # @163 ACCTCODE $5.
                f"{str(row.get('ACCTNO','')).ljust(20)}"                     # @168 ACCTNO 20.
                f"{str(row.get('OPENDATE','')).rjust(8,'0')}"                # @188 OPDATE 8.
                f"{str(row.get('LEDBAL','')).rjust(13,'0')}"                 # @196 LEDBAL Z13.
                f"{str(row.get('ACCSTAT','')).ljust(1)}"                     # @209 ACCSTAT $1.
                f"{str(row.get('COSTCTR','')).rjust(4,'0')}"                 # @210 COSTCTR Z4.
            )
            f.write(line + "\n")
