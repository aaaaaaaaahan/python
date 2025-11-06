con.execute(f"""
    CREATE OR REPLACE TABLE INDATA1 AS
    SELECT *
    FROM (
        SELECT *,
               substring(CREATIONDATE, 1, 7) AS TCREATE
        FROM '{host_parquet_path("UNLOAD_CIHRCAPT_FB.parquet")}'
    )
    WHERE TCREATE = '{date}'
""")
