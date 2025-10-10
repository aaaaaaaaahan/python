#------------------------------------------------------------#
#  Output: Save merged table as Parquet + CSV (SAS layout order)
#------------------------------------------------------------#
final = f"""
    SELECT
         '033' AS BANKNO,                  -- BANK NO
         INDORG,
         CUSTNAME,
         ALIASKEY,
         ALIAS,
         OCCUPDESC,
         EMPLNAME,
         ACCTBRABBR,
         ACCTCODE,
         ACCTNO,
         BANKINDC,
         PRIMSEC,
         RELATIONDESC,
         ACCTSTATUS,
         DATEOPEN,
         DATECLSE,
         BAL1INDC,
         BAL1,
         AMT1INDC,
         AMT1,
         COLLDESC,
         COLLINDC,
         COLLNO,
         JOINTACC,
         PRODDESC,
         DOBDOR,
         {year1} AS year,
         {month1} AS month,
         {day1} AS day
    FROM mrgcard
    ORDER BY ACCTNO
"""

queries = {
    "SNGLVIEW_PBCS": final
}

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    # Write to Parquet (partition by year/month/day)
    con.execute(f"""
    COPY ({query})
    TO '{parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
    """)

    # Write to CSV
    con.execute(f"""
    COPY ({query})
    TO '{csv_path}'
    (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);
    """)

#------------------------------------------------------------#
#  Split output into 10 smaller Parquet + CSV files (optional)
#------------------------------------------------------------#
for i in range(1, 11):
    query = f"""
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER () AS rn
            FROM ({final})
        )
        WHERE MOD(rn, 10) = {i-1}
    """

    # Parquet output
    con.execute(f"""
    COPY ({query})
    TO '{parquet_output_path(f"SNGLVIEW_PBCS{i:02d}")}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
    """)

    # CSV output
    con.execute(f"""
    COPY ({query})
    TO '{csv_output_path(f"SNGLVIEW_PBCS{i:02d}")}'
    (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);
    """)
