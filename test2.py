for i in range(1, 11):
    query = f"""
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER () AS rn
            FROM ({pbbrec})
        )
        WHERE MOD(rn, 10) = {i-1}
    """
    
    # Parquet output
    con.execute(f"""
    COPY ({query})
    TO '{parquet_output_path(f"B033_SNGLVIEW_DEPOSIT_DP04{i:02d}")}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true)
    """)

    # CSV output
    con.execute(f"""
    COPY ({query})
    TO '{csv_output_path(f"B033_SNGLVIEW_DEPOSIT_DP04{i:02d}.csv")}'
    (HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true)
    """)
