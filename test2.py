# -----------------------------
# Load tables
# -----------------------------
con.execute("CREATE TABLE RECCTZ AS SELECT * FROM 'dummy_parquet/RECCTZ.parquet'")
con.execute("CREATE TABLE RECRES AS SELECT * FROM 'dummy_parquet/RECRES.parquet'")
con.execute("CREATE TABLE RECSAL AS SELECT * FROM 'dummy_parquet/RECSAL.parquet'")
con.execute("CREATE TABLE CCODE AS SELECT * FROM 'dummy_parquet/CCODE.parquet'")
con.execute("CREATE TABLE BGCFL AS SELECT * FROM 'dummy_parquet/BGCFL.parquet'")
con.execute("CREATE TABLE MSICFL AS SELECT * FROM 'dummy_parquet/MSICFL.parquet'")
con.execute("CREATE TABLE MASCOFL AS SELECT * FROM 'dummy_parquet/MASCOFL.parquet'")
con.execute("CREATE TABLE EMPSEC AS SELECT * FROM 'dummy_parquet/EMPSEC.parquet'")
con.execute("CREATE TABLE EMPTYPE AS SELECT * FROM 'dummy_parquet/EMPTYPE.parquet'")
con.execute("CREATE TABLE RECPRC AS SELECT * FROM 'dummy_parquet/RECPRC.parquet'")
con.execute("CREATE TABLE RPTALL AS SELECT * FROM 'dummy_parquet/RPTALL.parquet'")

# -----------------------------
# OLDVALUE processing (no loop)
# -----------------------------
# CK_CTZN
con.execute("""
CREATE TABLE CK_CTZN AS
SELECT *, SUBSTR(OLDVALUE,1,2) AS CITIZEN
FROM RPTALL
WHERE FIELDS='NATIONALITY' AND OLDVALUE IS NOT NULL
""")
con.execute("""
CREATE TABLE MRG_CTZN AS
SELECT ck.*, r.DESC AS OLDVALUE_DESC
FROM CK_CTZN ck
LEFT JOIN RECCTZ r
ON ck.CITIZEN = r.CITIZEN
""")

# CK_RSDN
con.execute("""
CREATE TABLE CK_RSDN AS
SELECT *, SUBSTR(OLDVALUE,1,3) AS RESIDEN
FROM RPTALL
WHERE FIELDS='RESIDENCY' AND OLDVALUE IS NOT NULL
""")
con.execute("""
CREATE TABLE MRG_RSDN AS
SELECT ck.*, r.DESC AS OLDVALUE_DESC
FROM CK_RSDN ck
LEFT JOIN RECRES r
ON ck.RESIDEN = r.RESIDEN
""")

# Combine OLDVALUE
con.execute("""
CREATE TABLE COMOLD AS
SELECT * FROM MRG_CTZN
UNION ALL
SELECT * FROM MRG_RSDN
""")

# -----------------------------
# NEWVALUE processing (no loop)
# -----------------------------
# CN_CTZN
con.execute("""
CREATE TABLE CN_CTZN AS
SELECT *, SUBSTR(NEWVALUE,1,2) AS CITIZEN
FROM COMOLD
WHERE FIELDS='NATIONALITY' AND NEWVALUE IS NOT NULL
""")
con.execute("""
CREATE TABLE MRX_CTZN AS
SELECT cn.*, r.DESC AS NEWVALUE_DESC
FROM CN_CTZN cn
LEFT JOIN RECCTZ r
ON cn.CITIZEN = r.CITIZEN
""")

# CN_RSDN
con.execute("""
CREATE TABLE CN_RSDN AS
SELECT *, SUBSTR(NEWVALUE,1,3) AS RESIDEN
FROM COMOLD
WHERE FIELDS='RESIDENCY' AND NEWVALUE IS NOT NULL
""")
con.execute("""
CREATE TABLE MRX_RSDN AS
SELECT cn.*, r.DESC AS NEWVALUE_DESC
FROM CN_RSDN cn
LEFT JOIN RECRES r
ON cn.RESIDEN = r.RESIDEN
""")

# Combine NEWVALUE
con.execute("""
CREATE TABLE LASTREC AS
SELECT * FROM MRX_CTZN
UNION ALL
SELECT * FROM MRX_RSDN
""")

# Sort final table
con.execute("""
CREATE TABLE RECORDS AS
SELECT * FROM LASTREC
ORDER BY CUSTNO, FIELDS
""")

# -----------------------------
# Fetch final records
# -----------------------------
records = con.execute("SELECT * FROM RECORDS").fetchdf()
print(records)
