# Build the list of parquet paths using your host_parquet_path function
signator_files = [
    f"'{host_parquet_path(f'SNGLVIEW_SIGN_FD{i}.parquet')}'"
    for i in range(10, 20)
]

# Join them into a comma-separated list for DuckDB's read_parquet
signator_files_str = ", ".join(signator_files)

# Now use it inside the query
con.execute(f"""
    CREATE OR REPLACE TABLE SIGNATORY5 AS
    SELECT
        CAST(BANKNO AS INTEGER) AS BANKNO,
        CAST(ACCTNO AS BIGINT)  AS ACCTNO,
        SIGNATORY_NAME,
        ALIAS,
        SIGN_STAT,
        CONCAT(CAST(ACCTNO AS VARCHAR), SIGNATORY_NAME, ALIAS) AS NOM_IDX
    FROM read_parquet({signator_files_str})
    WHERE ACCTNO > 1000000000 AND ACCTNO < 1999999999
      AND COALESCE(ALIAS,'') <> ''
      AND COALESCE(SIGNATORY_NAME,'') <> ''
""")
