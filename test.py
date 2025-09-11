import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from datetime import date, timedelta

#-------------------------------------------------------------------#
# Program: CCRCCRLN  (DuckDB + PyArrow Version)                     #
#-------------------------------------------------------------------#

# Set batchdate (SAS batchdate = one day before)
batchdate = date.today() - timedelta(days=1)
print("Batchdate:", batchdate.strftime("%Y-%m-%d"))

# Connect DuckDB
con = duckdb.connect()

#----------------------------#
# Register input parquet files
#----------------------------#
base_path = "/abc/test/"

con.execute(f"""
    CREATE OR REPLACE VIEW RLENCC_FB AS 
    SELECT * FROM read_parquet('{base_path}RLENCC_FB.parquet')
""")

con.execute(f"""
    CREATE OR REPLACE VIEW BANKCTRL_RLENCODE_CC AS 
    SELECT * FROM read_parquet('{base_path}BANKCTRL_RLENCODE_CC.parquet')
""")

con.execute(f"""
    CREATE OR REPLACE VIEW PRIMNAME_OUT AS 
    SELECT * FROM read_parquet('{base_path}PRIMNAME_OUT.parquet')
""")

con.execute(f"""
    CREATE OR REPLACE VIEW ALLALIAS_OUT AS 
    SELECT * FROM read_parquet('{base_path}ALLALIAS_OUT.parquet')
""")

con.execute(f"""
    CREATE OR REPLACE VIEW ALLCUST_FB AS 
    SELECT * FROM read_parquet('{base_path}ALLCUST_FB.parquet')
""")

#--------------------------------#
# Part 1 - PROCESSING LEFT SIDE  #
#--------------------------------#

# LEFTOUT
LEFTOUT = con.execute("""
    WITH cccode AS (
        SELECT RLENTYPE AS TYPE, RLENCODE AS CODE1, RLENDESC AS DESC1
        FROM BANKCTRL_RLENCODE_CC
        QUALIFY ROW_NUMBER() OVER (PARTITION BY RLENCODE ORDER BY RLENCODE) = 1
    ),
    ciscust AS (
        SELECT CUSTNO AS CUSTNO1, TAXID AS OLDIC1, BASICGRPCODE AS BASICGRPCODE1
        FROM ALLCUST_FB
        QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) = 1
    ),
    cisname AS (
        SELECT CUSTNO AS CUSTNO1, INDORG AS INDORG1, CUSTNAME AS CUSTNAME1
        FROM PRIMNAME_OUT
        QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) = 1
    ),
    cisalias AS (
        SELECT CUSTNO AS CUSTNO1, NAME_LINE AS ALIAS1
        FROM ALLALIAS_OUT
    ),
    ccrlen1 AS (
        SELECT 
            CUSTNO AS CUSTNO1,
            TRY_CAST(EFFDATE AS BIGINT) AS EFFDATE,
            CUSTNO2,
            TRY_CAST(CODE1 AS BIGINT) AS CODE1,
            TRY_CAST(CODE2 AS BIGINT) AS CODE2,
            EXPIRE_DATE AS EXPDATE1
        FROM RLENCC_FB
    ),
    ccrlen_clean AS (
        SELECT *,
               NULLIF(TRIM(EXPDATE1), '') AS EXPDATE1_CLEAN,
               TRY_CAST(EXPDATE1 AS DATE) AS EXPDATE
        FROM ccrlen1
        WHERE COALESCE(TRIM(EXPDATE1), '') = ''
    ),
    idx_l01 AS (
        SELECT l.*, c.DESC1
        FROM ccrlen_clean l
        LEFT JOIN cccode c ON l.CODE1 = c.CODE1
    ),
    idx_l02 AS (
        SELECT l.*, n.INDORG1, n.CUSTNAME1
        FROM idx_l01 l
        LEFT JOIN cisname n ON l.CUSTNO1 = n.CUSTNO1
    ),
    idx_l03 AS (
        SELECT l.*, a.ALIAS1
        FROM idx_l02 l
        LEFT JOIN cisalias a ON l.CUSTNO1 = a.CUSTNO1
    ),
    idx_l04 AS (
        SELECT l.*, c.OLDIC1, c.BASICGRPCODE1
        FROM idx_l03 l
        LEFT JOIN ciscust c ON l.CUSTNO1 = c.CUSTNO1
    )
    SELECT CUSTNO1, INDORG1, CODE1, DESC1, CUSTNO2, CODE2, EXPDATE, 
           CUSTNAME1, ALIAS1, OLDIC1, BASICGRPCODE1, EFFDATE
    FROM idx_l04
""").arrow()

print("LEFTOUT sample:")
print(LEFTOUT.to_pandas().head(5))

#--------------------------------#
# Part 2 - PROCESSING RIGHT SIDE #
#--------------------------------#

RIGHTOUT = con.execute("""
    WITH cccode AS (
        SELECT RLENTYPE AS TYPE, RLENCODE AS CODE2, RLENDESC AS DESC2
        FROM BANKCTRL_RLENCODE_CC
    ),
    ciscust AS (
        SELECT CUSTNO AS CUSTNO2, TAXID AS OLDIC2, BASICGRPCODE AS BASICGRPCODE2
        FROM ALLCUST_FB
        QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) = 1
    ),
    cisname AS (
        SELECT CUSTNO AS CUSTNO2, INDORG AS INDORG2, CUSTNAME AS CUSTNAME2
        FROM PRIMNAME_OUT
        QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) = 1
    ),
    cisalias AS (
        SELECT CUSTNO AS CUSTNO2, NAME_LINE AS ALIAS2
        FROM ALLALIAS_OUT
    ),
    ccrlen2 AS (
        SELECT l.*, 
               EXTRACT(YEAR FROM EXPDATE) AS EXPYY,
               EXTRACT(MONTH FROM EXPDATE) AS EXPMM,
               EXTRACT(DAY FROM EXPDATE) AS EXPDD
        FROM ({LEFT}) l
    ),
    idx_r01 AS (
        SELECT l.*, c.DESC2
        FROM ccrlen2 l
        LEFT JOIN cccode c ON l.CODE2 = c.CODE2
    ),
    idx_r02 AS (
        SELECT l.*, n.INDORG2, n.CUSTNAME2
        FROM idx_r01 l
        LEFT JOIN cisname n ON l.CUSTNO2 = n.CUSTNO2
    ),
    idx_r03 AS (
        SELECT l.*, a.ALIAS2
        FROM idx_r02 l
        LEFT JOIN cisalias a ON l.CUSTNO2 = a.CUSTNO2
    ),
    idx_r04 AS (
        SELECT l.*, c.OLDIC2, c.BASICGRPCODE2
        FROM idx_r03 l
        LEFT JOIN ciscust c ON l.CUSTNO2 = c.CUSTNO2
    )
    SELECT CUSTNO2, INDORG2, CODE2, DESC2, CUSTNO1, CODE1, EXPDATE,
           CUSTNAME2, ALIAS2, OLDIC2, BASICGRPCODE2, EFFDATE
    FROM idx_r04
""".format(LEFT="SELECT * FROM LEFTOUT")).arrow()

print("RIGHTOUT sample:")
print(RIGHTOUT.to_pandas().head(5))

#---------------------------------------#
# Part 3 - COMBINE + DEDUP              #
#---------------------------------------#

all_output = con.execute("""
    SELECT 
        l.CUSTNO1, l.INDORG1, l.CODE1, l.DESC1,
        l.CUSTNO2, r.INDORG2, r.CODE2, r.DESC2,
        l.EXPDATE,
        l.CUSTNAME1, l.ALIAS1, r.CUSTNAME2, r.ALIAS2,
        l.OLDIC1, l.BASICGRPCODE1, r.OLDIC2, r.BASICGRPCODE2,
        l.EFFDATE
    FROM LEFTOUT l
    LEFT JOIN RIGHTOUT r ON l.CUSTNO2 = r.CUSTNO2
""").arrow()

# Dedup
all_output_unique = con.execute("""
    SELECT DISTINCT CUSTNO1, CUSTNO2, CODE1, CODE2,
           INDORG1, DESC1, INDORG2, DESC2, EXPDATE,
           CUSTNAME1, ALIAS1, CUSTNAME2, ALIAS2,
           OLDIC1, BASICGRPCODE1, OLDIC2, BASICGRPCODE2,
           EFFDATE
    FROM ({}) t
    ORDER BY CUSTNO1
""".format("SELECT * FROM all_output")).arrow()

duplicates = con.execute("""
    SELECT * FROM all_output
    EXCEPT SELECT * FROM all_output_unique
""").arrow()

print("Unique sample:")
print(all_output_unique.to_pandas().head(5))
print("Duplicates sample:")
print(duplicates.to_pandas().head(5))

#---------------------------------------#
# SAVE OUTPUT                           #
#---------------------------------------#

pq.write_table(all_output_unique, "cis_internal/output/RLNSHIP.parquet")
csv.write_csv(all_output_unique, "cis_internal/output/RLNSHIP.csv")

pq.write_table(duplicates, "cis_internal/output/RLNSHIP_DUPLICATES.parquet")
csv.write_csv(duplicates, "cis_internal/output/RLNSHIP_DUPLICATES.csv")

