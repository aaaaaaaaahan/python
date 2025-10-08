con.execute(f"""
    CREATE TABLE cust AS
    SELECT
        ALIASKEY,
        ALIAS,
        TAXID,
        TAXNUM,
        ACCTNAME,
        CAST(TOTCOMBLIMIT AS DOUBLE) AS TOTCOMBLIMIT,
        CASE
            WHEN SIGN1 = '-' THEN CAST(OUTSTNDAMT AS DOUBLE) * -1
            WHEN SIGN1 = '+' THEN CAST(OUTSTNDAMT AS DOUBLE)
            ELSE CAST(OUTSTNDAMT AS DOUBLE)
        END AS OUTSTNDAMT,
        CASE
            WHEN SIGN2 = '-' THEN CAST(TOTMIN AS DOUBLE) * -1
            WHEN SIGN2 = '+' THEN CAST(TOTMIN AS DOUBLE)
            ELSE CAST(TOTMIN AS DOUBLE)
        END AS TOTMIN,
        CASE
            WHEN SIGN3 = '-' THEN CAST(OUTSTANDDUE AS DOUBLE) * -1
            WHEN SIGN3 = '+' THEN CAST(OUTSTANDDUE AS DOUBLE)
            ELSE CAST(OUTSTANDDUE AS DOUBLE)
        END AS OUTSTANDDUE
    FROM read_parquet('{sccust}')
    WHERE ALIAS IS NOT NULL AND ALIAS <> '' AND ALIASKEY <> 'CV '
""")


con.execute(f"""
    CREATE TABLE card AS
    SELECT
        ALIASKEY,
        ALIAS,
        TAXID,
        TAXNUM,
        ACCTNOC,
        OPENDATE,
        STATUS,
        CLOSEDATE,
        MONITOR,
        DUEDAY,
        CASE
            WHEN SIGN4 = '-' THEN CAST(OVERDUEAMT AS DOUBLE) * -1
            WHEN SIGN4 = '+' THEN CAST(OVERDUEAMT AS DOUBLE)
            ELSE CAST(OVERDUEAMT AS DOUBLE)
        END AS OVERDUEAMT,
        PRODTYPE AS ACCTCODE,
        CASE
            WHEN length(OPENDATE)=8 THEN substr(OPENDATE,1,4)||'-'||substr(OPENDATE,5,2)||'-'||substr(OPENDATE,7,2)
            ELSE '9999-01-01'
        END AS DATEOPEN,
        CASE
            WHEN length(CLOSEDATE)=8 THEN substr(CLOSEDATE,1,4)||'-'||substr(CLOSEDATE,5,2)||'-'||substr(CLOSEDATE,7,2)
            ELSE '9999-01-01'
        END AS DATECLOSE
    FROM read_parquet('{sccard}')
    WHERE ALIAS IS NOT NULL AND ALIAS <> '' AND ALIASKEY <> 'CV '
""")
