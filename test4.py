con.execute("""
    CREATE TABLE cis_newic AS
    SELECT
        ALIAS,
        TAXID,
        ALIASKEY,
        ACCTNOC,
        CUSTNO,
        CUSTNAME,
        PRIMSEC,
        RLENTYPE,
        RLENDESC,
        JOINTACC,
        ACCTCODE,
        RLENCODE,
        RLENCD,
        'NEWIC' AS TYPE,
        ALIASKEY || ALIAS AS CUSTID
    FROM cis
    WHERE ALIAS <> ''
""")

con.execute("""
    CREATE TABLE cis_oldic AS
    SELECT
        ALIAS,
        TAXID,
        ALIASKEY,
        ACCTNOC,
        CUSTNO,
        CUSTNAME,
        PRIMSEC,
        RLENTYPE,
        RLENDESC,
        JOINTACC,
        ACCTCODE,
        RLENCODE,
        RLENCD,
        'OLDIC' AS TYPE,
        'OC' || TAXID AS CUSTID
    FROM cis
    WHERE TAXID <> ''
""")

con.execute("""
    CREATE TABLE cis_custids AS
    SELECT * FROM cis_newic
    UNION ALL
    SELECT * FROM cis_oldic
""")
