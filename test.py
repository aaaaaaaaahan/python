# ================================================================
# Merge with MSCO / MSIC
# ================================================================
con.execute("""
    CREATE OR REPLACE TABLE CUSTMSCA AS
    SELECT c.*, m.MASCO2008, m.MSCDESC
    FROM CUST c
    LEFT JOIN MSCO m
    ON c.MASCO2008 = m.MASCO2008
    WHERE m.MSICCODE IS NULL OR m.MSICCODE = ''
""")

con.execute("""
    CREATE OR REPLACE TABLE CUSTMSCB AS
    SELECT c.*, m.MSICCODE, m.MSCDESC
    FROM CUST c
    LEFT JOIN MSIC m
    ON c.MSICCODE = m.MSICCODE
""")

con.execute("""
    CREATE OR REPLACE TABLE CUSTMSC AS
    SELECT DISTINCT * FROM (
        SELECT * FROM CUSTMSCA
        UNION ALL
        SELECT * FROM CUSTMSCB
    )
""")

# Attach INDV (from earlier)
con.execute("""
    CREATE OR REPLACE TABLE CUSTA AS
    SELECT c.*, i.EMPLOYMENT_TYPE, i.LAST_UPDATE_DATE
    FROM CUSTMSC c
    LEFT JOIN INDV i
    ON c.CUSTNO = i.CUSTNO
""")

print("CUSTA (first 5 rows):")
print(con.execute("SELECT * FROM CUSTA LIMIT 5").fetchdf())

# ================================================================
# CISDP = Merge CUSTA with DEPOSIT2
# ================================================================
con.execute("""
    CREATE OR REPLACE TABLE CISDP AS
    SELECT c.*, d.*
    FROM CUSTA c
    INNER JOIN DEPOSIT2 d
    ON c.ACCTNO = d.ACCTNO
""")

print("CISDP (first 5 rows):")
print(con.execute("SELECT * FROM CISDP LIMIT 5").fetchdf())
