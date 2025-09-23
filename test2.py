#============================================#
#   PART 3 - SEARCH ONE SIDE ONLY (ICETOOL)  #
#============================================#

# Create base combined output
con.execute("""
    CREATE VIEW alloutput AS
    SELECT l.CUSTNO1, l.INDORG1, l.CODE1, l.DESC1,
           l.CUSTNO2, r.INDORG2, r.CODE2, r.DESC2,
           l.EXPDATE,
           l.CUSTNAME1, l.ALIAS1,
           r.CUSTNAME2, r.ALIAS2,
           l.OLDIC1, l.BASICGRPCODE1,
           r.OLDIC2, r.BASICGRPCODE2,
           l.EFFDATE
    FROM INPUT1 l
    LEFT JOIN INPUT2 r
      ON l.CUSTNO2 = r.CUSTNO2
    WHERE (l.EXPDATE IS NULL OR l.EXPDATE = ' ' OR l.EXPDATE >= '{batch_date}')
""")

# Step 4 - Unique records (FIRST by key)
con.execute(f"""
    CREATE VIEW UNQREC AS
    SELECT *
    FROM (
        SELECT a.*,
               ROW_NUMBER() OVER (
                   PARTITION BY 
                       a.CUSTNO1, a.INDORG1, a.CODE1, a.DESC1,
                       a.CUSTNO2, a.INDORG2, a.CODE2, a.DESC2,
                       a.EXPDATE,
                       a.CUSTNAME1, a.ALIAS1,
                       a.CUSTNAME2, a.ALIAS2,
                       a.OLDIC1, a.BASICGRPCODE1,
                       a.OLDIC2, a.BASICGRPCODE2,
                       a.EFFDATE
                   ORDER BY a.CUSTNO1, a.CODE1
               ) AS rn
        FROM alloutput a
    )
    WHERE rn = 1
""")

# Step 5 - Duplicate records (ALLDUPS by same key)
con.execute(f"""
    CREATE VIEW DUPREC AS
    SELECT a.*
    FROM (
        SELECT a.*,
               COUNT(*) OVER (
                   PARTITION BY 
                       a.CUSTNO1, a.INDORG1, a.CODE1, a.DESC1,
                       a.CUSTNO2, a.INDORG2, a.CODE2, a.DESC2,
                       a.EXPDATE,
                       a.CUSTNAME1, a.ALIAS1,
                       a.CUSTNAME2, a.ALIAS2,
                       a.OLDIC1, a.BASICGRPCODE1,
                       a.OLDIC2, a.BASICGRPCODE2,
                       a.EFFDATE
               ) AS cnt
        FROM alloutput a
    )
    WHERE cnt > 1
""")

# =====================#
# Export with PyArrow  #
# =====================#
queries = {
    "RLNSHIP_SRCH" : "SELECT * FROM UNQREC",   # Step 4 output
    "RLNSHIP_DUP"  : "SELECT * FROM DUPREC",   # Step 5 output
}

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    con.execute(f"""
    COPY ({query})
    TO '{parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);  
     """)
    
    con.execute(f"""
    COPY ({query})
    TO '{csv_path}'
    (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);  
     """)
