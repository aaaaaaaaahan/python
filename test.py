# ================================================================
# SIGNATORY FILE
# ================================================================
# Read all signatory parquet files (FD10–FD19)
con.execute(f"""
    CREATE VIEW signator_all AS
    SELECT * FROM '{host_parquet_path("SNGLVIEW_SIGN_FD10.parquet")}'
    UNION ALL
    SELECT * FROM '{host_parquet_path("SNGLVIEW_SIGN_FD11.parquet")}'
    UNION ALL
    SELECT * FROM '{host_parquet_path("SNGLVIEW_SIGN_FD12.parquet")}'
    UNION ALL
    SELECT * FROM '{host_parquet_path("SNGLVIEW_SIGN_FD13.parquet")}'
    UNION ALL
    SELECT * FROM '{host_parquet_path("SNGLVIEW_SIGN_FD14.parquet")}'
    UNION ALL
    SELECT * FROM '{host_parquet_path("SNGLVIEW_SIGN_FD15.parquet")}'
    UNION ALL
    SELECT * FROM '{host_parquet_path("SNGLVIEW_SIGN_FD16.parquet")}'
    UNION ALL
    SELECT * FROM '{host_parquet_path("SNGLVIEW_SIGN_FD17.parquet")}'
    UNION ALL
    SELECT * FROM '{host_parquet_path("SNGLVIEW_SIGN_FD18.parquet")}'
    UNION ALL
    SELECT * FROM '{host_parquet_path("SNGLVIEW_SIGN_FD19.parquet")}';
""")

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
    FROM signator_all
    WHERE ACCTNO > '1000000000' AND ACCTNO < '1999999999'
      AND COALESCE(ALIAS,'') <> ''
      AND COALESCE(SIGNATORY_NAME,'') <> ''
""")

print("SIGNATORY5 (first 5 rows):")
print(con.execute("SELECT * FROM SIGNATORY5 LIMIT 5").fetchdf())

# Deduplicate by NOM_IDX (PROC SORT NODUPKEY)
con.execute("""
    CREATE OR REPLACE TABLE SIGNATORY AS
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY NOM_IDX ORDER BY ACCTNO) AS rn
        FROM SIGNATORY5
    ) t
    WHERE rn = 1
""")

print("SIGNATORY (deduped, first 5 rows):")
print(con.execute("SELECT * FROM SIGNATORY LIMIT 5").fetchdf())
