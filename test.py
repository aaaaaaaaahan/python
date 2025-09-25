import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

#================================#
#   CONNECT TO DUCKDB            #
#================================#
con = duckdb.connect()

#================================#
#   PART 1 - PROCESSING LEFT     #
#================================#
con.execute(f"""
            CREATE VIEW INFILE1   AS SELECT * FROM '{host_parquet_path("RLENCC_FB.parquet")}';
            CREATE VIEW CCCODE    AS SELECT * FROM '{host_parquet_path("BANKCTRL_RLENCODE_CC.parquet")}';
            CREATE VIEW NAMEFILE  AS SELECT * FROM '{host_parquet_path("PRIMNAME_OUT.parquet")}';
            CREATE VIEW ALIASFIL  AS SELECT * FROM '{host_parquet_path("ALLALIAS_OUT.parquet")}';
            CREATE VIEW CUSTFILE  AS SELECT * FROM '{host_parquet_path("ALLCUST_FB.parquet")}';
""")

# RELATION FILE
con.execute("""
    CREATE VIEW cccode_l AS
    SELECT DISTINCT RLENTYPE AS TYPE,
                    LPAD(CAST(CAST(RLENCODE AS BIGINT) AS VARCHAR), 3, '0') AS CODE1,
                    RLENDESC AS DESC1
    FROM CCCODE
""")

# CUSTOMER FILE
con.execute("""
    CREATE VIEW ciscust AS
    SELECT DISTINCT CUSTNO   AS CUSTNO1,
                    TAXID    AS OLDIC1,
                    BASICGRPCODE AS BASICGRPCODE1
    FROM CUSTFILE
""")

# NAME FILE
con.execute("""
    CREATE VIEW cisname AS
    SELECT DISTINCT CUSTNO   AS CUSTNO1,
                    INDORG   AS INDORG1,
                    CUSTNAME AS CUSTNAME1
    FROM NAMEFILE
""")

# ALIAS FILE
con.execute("""
    CREATE VIEW cisalias AS
    SELECT CUSTNO AS CUSTNO1,
           NAME_LINE AS ALIAS1
    FROM ALIASFIL
    ORDER BY CUSTNO1
""")

# RLENCC FILE (Left)
con.execute("""
    CREATE VIEW ccrlen1 AS
    SELECT *,
           CASE WHEN TRIM(EXPDATE1) = '' THEN NULL
                ELSE CAST(EXPDATE1 AS DATE)
           END AS EXPDATE
    FROM (
        SELECT CUSTNO     AS CUSTNO1,
               CAST(EFFDATE AS BIGINT) AS EFFDATE,
               CUSTNO2,
               LPAD(CAST(CAST(CODE1 AS BIGINT) AS VARCHAR), 3, '0') AS CODE1,
               LPAD(CAST(CAST(CODE2 AS BIGINT) AS VARCHAR), 3, '0') AS CODE2,
               TRIM(EXPIRE_DATE)       AS EXPDATE1
        FROM INFILE1
    )
    WHERE EXPDATE1 = ''
    ORDER BY CODE1
""")

# Merge (LEFTOUT)
con.execute("""
    CREATE VIEW LEFTOUT AS
    SELECT l.CUSTNO1, n.INDORG1, l.CODE1, c.DESC1, l.CUSTNO2, l.CODE2, l.EXPDATE,
           n.CUSTNAME1, a.ALIAS1, cu.OLDIC1, cu.BASICGRPCODE1, l.EFFDATE
    FROM ccrlen1 l
    LEFT JOIN cccode_l  c ON l.CODE1 = c.CODE1
    LEFT JOIN cisname n ON l.CUSTNO1 = n.CUSTNO1
    LEFT JOIN cisalias a ON l.CUSTNO1 = a.CUSTNO1
    LEFT JOIN ciscust cu ON l.CUSTNO1 = cu.CUSTNO1
""")

#================================#
#   PART 2 - PROCESSING RIGHT    #
#================================#

con.execute(f"""
            CREATE VIEW CCCODE_R   AS SELECT * FROM '{host_parquet_path("BANKCTRL_RLENCODE_CC.parquet")}';
            CREATE VIEW NAMEFILE_R AS SELECT * FROM '{host_parquet_path("PRIMNAME_OUT.parquet")}';
            CREATE VIEW ALIASFIL_R AS SELECT * FROM '{host_parquet_path("ALLALIAS_OUT.parquet")}';
            CREATE VIEW CUSTFILE_R AS SELECT * FROM '{host_parquet_path("ALLCUST_FB.parquet")}';
""")

con.execute("""
    CREATE VIEW cccode_r1 AS
    SELECT RLENTYPE AS TYPE,
           LPAD(CAST(CAST(RLENCODE AS BIGINT) AS VARCHAR), 3, '0') AS CODE2,
           RLENDESC AS DESC2
    FROM CCCODE_R
    ORDER BY CODE2
""")

con.execute("""
    CREATE VIEW ciscust_r AS
    SELECT DISTINCT CUSTNO   AS CUSTNO2,
                    TAXID    AS OLDIC2,
                    BASICGRPCODE AS BASICGRPCODE2
    FROM CUSTFILE_R
""")

con.execute("""
    CREATE VIEW cisname_r AS
    SELECT DISTINCT CUSTNO   AS CUSTNO2,
                    INDORG   AS INDORG2,
                    CUSTNAME AS CUSTNAME2
    FROM NAMEFILE_R
""")

con.execute("""
    CREATE VIEW cisalias_r AS
    SELECT CUSTNO   AS CUSTNO2,
           NAME_LINE AS ALIAS2
    FROM ALIASFIL_R
    ORDER BY CUSTNO2
""")

con.execute("""
    CREATE VIEW ccrlen2 AS
    SELECT CUSTNO1, INDORG1, CODE1, DESC1,
           CUSTNO2, CODE2, EXPDATE,
           CUSTNAME1, ALIAS1, OLDIC1, BASICGRPCODE1,
           EFFDATE,
           EXTRACT(YEAR FROM EXPDATE)  AS EXPYY,
           EXTRACT(MONTH FROM EXPDATE) AS EXPMM,
           EXTRACT(DAY FROM EXPDATE)   AS EXPDD
    FROM LEFTOUT
    ORDER BY CODE2
""")

con.execute("""
    CREATE VIEW RIGHTOUT AS
    SELECT r.CUSTNO2, n.INDORG2, r.CODE2, c.DESC2,
           r.CUSTNO1, r.CODE1, r.EXPDATE,
           n.CUSTNAME2, a.ALIAS2, cu.OLDIC2, cu.BASICGRPCODE2, r.EFFDATE
    FROM ccrlen2 r
    LEFT JOIN cccode_r1  c ON r.CODE2 = c.CODE2
    LEFT JOIN cisname_r n ON r.CUSTNO2 = n.CUSTNO2
    LEFT JOIN cisalias_r a ON r.CUSTNO2 = a.CUSTNO2
    LEFT JOIN ciscust_r cu ON r.CUSTNO2 = cu.CUSTNO2
""")

#============================================#
#   PART 3 - FILE TO SEARCH ONE SIDE ONLY    #
#============================================#
con.execute(f"""
            CREATE VIEW INPUT1   AS SELECT * FROM LEFTOUT;
            CREATE VIEW INPUT2   AS SELECT * FROM RIGHTOUT;
""")

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
SELECT sub.*,
       {year} AS year,
       {month} AS month,
       {day} AS day
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
) sub
WHERE sub.rn = 1
ORDER BY CUSTNO1, CODE1;
""")

# Step 5 - Duplicate records (ALLDUPS by same key)
con.execute(f"""
    CREATE VIEW DUPREC AS
SELECT sub.*,
       {year} AS year,
       {month} AS month,
       {day} AS day
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
) sub
WHERE sub.rn > 1
ORDER BY CUSTNO1, CODE1;
""")

# =====================#
# Export with PyArrow  #
# =====================#
queries = {
    "CCRIS_CC_RLNSHIP_SRCH" : "SELECT * FROM UNQREC",   # Step 4 output
    "CCRIS_CC_RLNSHIP_DUP"  : "SELECT * FROM DUPREC",   # Step 5 output
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
