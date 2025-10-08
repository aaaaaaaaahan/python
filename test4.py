# ============================================================
# STEP 7 – MATCHED AND UNMATCHED
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE MATCHED_REC AS
    SELECT 
        A.*,
        B.CUSTNO,
        B.CUSTNAME,
        B.PRIMSEC,
        B.JOINTACC,
        B.RLENCD,
        B.RELATIONDESC
    FROM CARD_CUSTIDS A
    JOIN CIS_CUSTIDS B
    ON A.CUSTID = B.CUSTID AND A.ACCTNOC = B.ACCTNOC
""")

con.execute("""
    CREATE OR REPLACE TABLE UN_MATCHED_REC AS
    SELECT 
        A.*,
        A.ACCTNOC AS CUSTNO,
        A.ACCTNAME AS CUSTNAME,
        NULL AS PRIMSEC,
        NULL AS JOINTACC,
        NULL AS RLENCD,
        NULL AS RELATIONDESC
    FROM CARD_CUSTIDS A
    LEFT JOIN CIS_CUSTIDS B
    ON A.CUSTID = B.CUSTID AND A.ACCTNOC = B.ACCTNOC
    WHERE B.CUSTID IS NULL
""")

# ============================================================
# STEP 8 – FINAL OUTPUT TABLE
# ============================================================
con.execute("""
    CREATE OR REPLACE TABLE FINAL AS
    SELECT
        A.CUSTID,
        COALESCE(A.CUSTNO, '') AS CUSTNO,
        '' AS SORTKEY,
        'PBB' AS SOURCE,
        '' AS BANKINDC,
        COALESCE(A.CUSTNAME, '') AS CUSTNAME,
        COALESCE(A.ACCTCODE, '') AS ACCTCODE,
        COALESCE(A.ACCTNOC, '') AS ACCTNOC,
        '' AS NOTE_NO,
        '' AS MULTINOTEIND,
        '' AS REFKEY1,
        '' AS REF1,
        '' AS REFKEY2,
        '' AS REF2,
        COALESCE(A.PRIMSEC, '') AS PRIMSEC,
        COALESCE(A.JOINTACC, '') AS JOINTACC,
        COALESCE(A.RLENCD, '') AS RLENCD,
        COALESCE(A.RELATIONDESC, '') AS RELATIONDESC,
        '001' AS BRANCHNO,
        'HOE' AS BRCABBRV,
        COALESCE(A.ACCTNAME, '') AS ACCTNAME,
        COALESCE(A.STATUS, '') AS STATUS,
        COALESCE(A.MONITOR, '') AS MONITOR,
        'MYR' AS ACCTCURRCODE,
        'MYR' AS ACCTCURRBASE,
        COALESCE(A.ACCTCODE, '') AS PRODTYPE,
        COALESCE(A.OVERDUEAMT, 0) AS OVERDUEAMT,
        COALESCE(A.TOTMIN, 0) AS TOTMIN,
        COALESCE(A.TOTCOMBLIMIT, 0) AS TOTCOMBLIMIT,
        COALESCE(A.DUEDAY, '') AS DUEDAY,
        COALESCE(A.DATEOPEN, '0001-01-01') AS DATEOPEN,
        COALESCE(A.DATECLOSE, '0001-01-01') AS DATECLOSE
    FROM (
        SELECT * FROM MATCHED_REC
        UNION ALL
        SELECT * FROM UN_MATCHED_REC
    ) A
""")

# ============================================================
# STEP 9 – WRITE OUTPUT (optional)
# ============================================================
con.execute(f"""
    COPY FINAL TO '{outfile_path}' (FORMAT PARQUET);
""")
