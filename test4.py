# -------------------------------
# ALLFILE
# -------------------------------
con.execute("""
    CREATE OR REPLACE TABLE allfile AS
    SELECT ACCTNOC, ALIASKEY, ALIAS, TAXID, CONSENT, CHANNEL
    FROM merge_data
    ORDER BY ACCTNOC
""")

# -------------------------------
# DAILY FILE (DAY)
# -------------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE dailyfile AS
    SELECT ACCTNOC, ALIASKEY, ALIAS, TAXID, CONSENT, CHANNEL
    FROM merge_data
    WHERE EFFDATE = '{DATE2}' AND CHANNEL != 'UNIBATCH'
    ORDER BY ACCTNOC
""")

# ---------------------------------------------------------------------
# OUTPUT PARQUET
# ---------------------------------------------------------------------
out1 = """
    SELECT 
        *,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM allfile
""".format(year=year,month=month,day=day)

out2 = """
    SELECT 
        *,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM dailyfile
""".format(year=year,month=month,day=day)

queries = {
    "UNICARD_MAILFLAG_ALL"                      : out1,
    "UNICARD_MAILFLAG_DLY"                      : out2
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
# OUTPUT FIXED-WIDTH TEXT
# ---------------------------------------------------------------------
txt_queries = {
    "UNICARD_MAILFLAG_ALL"                      : out1,
    "UNICARD_MAILFLAG_DLY"                      : out2
}

for txt_name, txt_query in txt_queries.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(row['ACCTNOC']).ljust(16)}"
                f"{str(row['ALIASKEY']).ljust(3)}"
                f"{str(row['ALIAS']).ljust(12)}"
                f"{str(row['TAXID']).ljust(12)}"
                f"{str(row['CONSENT']).ljust(1)}"
                f"{str(row['CHANNEL']).ljust(8)}"
            )
            f.write(line + "\n")
