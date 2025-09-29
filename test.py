# ================================================================
# Merge DPOTH with FOREX
# ================================================================
con.execute("""
    CREATE OR REPLACE TABLE FOREXMRG AS
    SELECT a.*,
           b.FOREXRATE,
           ROUND(((a.FOREXAMT * b.FOREXRATE)/0.01)::INT * 0.01, 2) AS LEDGERBAL
    FROM DPOTH a
    LEFT JOIN FOREX b
    ON a.CURRCODE = b.CURRCODE
""")

print("FOREXMRG (first 5 rows):")
print(con.execute("SELECT * FROM FOREXMRG LIMIT 5").fetchdf())
