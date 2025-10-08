con.execute("""
    CREATE OR REPLACE TABLE NEWIC AS
    SELECT 
        CUSTNO,
        ACCTNOC,
        TAXID,
        PRISEC,
        BORROWER_INDC,
        GTOR_INDC,
        PARTNER_INDC,
        BASICGRPCODE,
        ALIASKEY,
        ALIAS,
        (ALIASKEY || ALIAS) AS CUSTID
    FROM LNDETL
""")

con.execute("""
    CREATE OR REPLACE TABLE OLDIC AS
    SELECT 
        CUSTNO,
        ACCTNOC,
        TAXID,
        PRISEC,
        BORROWER_INDC,
        GTOR_INDC,
        PARTNER_INDC,
        BASICGRPCODE,
        '' AS ALIASKEY,
        '' AS ALIAS,
        ('OC ' || TAXID) AS CUSTID
    FROM LNDETL
    WHERE TAXID != '' AND TAXID != '000000000'
""")
