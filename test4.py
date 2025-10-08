con.execute("""
    CREATE TABLE final AS
    SELECT
        CUSTID,
        CUSTNO,
        'PBB' AS SOURCE,
        '001' AS BRANCHNO,
        'HOE' AS BRCABBRV,
        COALESCE(b.CUSTNAME, a.ACCTNAME) AS CUSTNAME,
        ACCTCODE,
        ACCTNOC,
        PRIMSEC,
        JOINTACC,
        RLENCD,
        RELATIONDESC,
        STATUS,
        MONITOR,
        DUEDAY,
        DATEOPEN,
        DATECLOSE,
        OVERDUEAMT,
        TOTMIN,
        TOTCOMBLIMIT
    FROM matched_rec a
    UNION ALL
    SELECT
        CUSTID,
        CUSTNO,
        'PBB' AS SOURCE,
        '001' AS BRANCHNO,
        'HOE' AS BRCABBRV,
        CUSTNAME,
        ACCTCODE,
        ACCTNOC,
        '' AS PRIMSEC,
        '' AS JOINTACC,
        '' AS RLENCD,
        '' AS RELATIONDESC,
        STATUS,
        MONITOR,
        DUEDAY,
        DATEOPEN,
        DATECLOSE,
        OVERDUEAMT,
        TOTMIN,
        TOTCOMBLIMIT
    FROM un_matched_rec
""")
