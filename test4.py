batch_yyyymmdd = batch_date.strftime("%Y-%m-%d")

con.execute(f"""
    CREATE TABLE DEPOSIT AS
    WITH RAW AS (
        SELECT
            BANKNO,
            REPTNO,
            FMTCODE,
            BRANCH,
            ACCTNO,
            OPENDATEX
        FROM parquet_scan('{host_parquet_path("DPTRBLGS_CIS.parquet")}')
    )
    SELECT
        LPAD(CAST(ACCTNO AS VARCHAR), 10, '0') AS ACCTNOC,
        -- SAS reconstructs date from OPENDATEX (PD6.)
        CONCAT(
            SUBSTR(LPAD(CAST(OPENDATEX AS VARCHAR), 11, '0'), 5, 4), '-',
            SUBSTR(LPAD(CAST(OPENDATEX AS VARCHAR), 11, '0'), 1, 2), '-',
            SUBSTR(LPAD(CAST(OPENDATEX AS VARCHAR), 11, '0'), 3, 2)
        ) AS OPENDATE,
        '' AS NOTENOC
    FROM RAW
    WHERE CAST(REPTNO AS INTEGER) = 1001
      AND CAST(FMTCODE AS INTEGER) IN (1,10,22)
      AND CONCAT(
            SUBSTR(LPAD(CAST(OPENDATEX AS VARCHAR), 11, '0'), 5, 4), '-',
            SUBSTR(LPAD(CAST(OPENDATEX AS VARCHAR), 11, '0'), 1, 2), '-',
            SUBSTR(LPAD(CAST(OPENDATEX AS VARCHAR), 11, '0'), 3, 2)
          ) = '{batch_yyyymmdd}'
""")
