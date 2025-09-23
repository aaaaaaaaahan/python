import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

import pyarrow.csv as pv_csv
import pyarrow.parquet as pq
from datetime import date

#-------------------------------------------------------------------#
# Original Program: CCRCCRLN                                        #
#-------------------------------------------------------------------#
#-EJS A2014-00021883  (CRMS PROJECT)                                #
# INCLUDE ADDITIONAL COLUMNS TO BE PLACED INTO CIS INTERFACE FILES  #
# 2016-4519 INCLUDE EFFECTIVE DATE INTO FILE                        #
#-------------------------------------------------------------------#
# ESMR 2021-00002352                                                #
# TO EXCLUDE RECORD WITH EXPIRED DATE IN RLEN CC FILE               #
#-------------------------------------------------------------------#

#--------------------------------#
# Part 1 - PROCESSING LEFT  SIDE #
#--------------------------------#

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
cccode = con.execute("""
    SELECT DISTINCT RLENTYPE AS TYPE,
                    RLENCODE AS CODE1,
                    RLENDESC AS DESC1
    FROM CCCODE
""").arrow()
print("RELATION FILE (LEFT):")
print(cccode.to_pandas().head(5))

# CUSTOMER FILE
ciscust = con.execute("""
    SELECT DISTINCT CUSTNO   AS CUSTNO1,
                    TAXID    AS OLDIC1,
                    BASICGRPCODE AS BASICGRPCODE1
    FROM CUSTFILE
""").arrow()
print("CUSTOMER FILE (LEFT):")
print(ciscust.to_pandas().head(5))

# NAME FILE
cisname = con.execute("""
    SELECT DISTINCT CUSTNO   AS CUSTNO1,
                    INDORG   AS INDORG1,
                    CUSTNAME AS CUSTNAME1
    FROM NAMEFILE
""").arrow()
print("NAME FILE (LEFT):")
print(cisname.to_pandas().head(5))

# ALIAS FILE
cisalias = con.execute("""
    SELECT CUSTNO AS CUSTNO1,
           NAME_LINE AS ALIAS1
    FROM ALIASFIL
    ORDER BY CUSTNO1
""").arrow()
print("ALIAS FILE (LEFT):")
print(cisalias.to_pandas().head(5))

# RLENCC FILE (Left)
ccrlen1 = con.execute("""
    SELECT *,
           CASE WHEN TRIM(EXPDATE1) = '' THEN NULL
                ELSE CAST(EXPDATE1 AS DATE)
           END AS EXPDATE
    FROM (
        SELECT CUSTNO     AS CUSTNO1,
               CAST(EFFDATE AS BIGINT) AS EFFDATE,
               CUSTNO2,
               CAST(CODE1 AS BIGINT)   AS CODE1,
               CAST(CODE2 AS BIGINT)   AS CODE2,
               TRIM(EXPIRE_DATE)       AS EXPDATE1
        FROM INFILE1
    )
    WHERE EXPDATE1 = ''
    ORDER BY CODE1
""").arrow()
print("RLENCC FILE (LEFT):")
print(ccrlen1.to_pandas().head(5))

# Merge (LEFTOUT)
LEFTOUT = con.execute("""
    SELECT l.CUSTNO1, n.INDORG1, l.CODE1, c.DESC1, l.CUSTNO2, l.CODE2, l.EXPDATE,
           n.CUSTNAME1, a.ALIAS1, cu.OLDIC1, cu.BASICGRPCODE1, l.EFFDATE
    FROM ccrlen1 l
    LEFT JOIN cccode  c ON l.CODE1 = c.CODE1
    LEFT JOIN cisname n ON l.CUSTNO1 = n.CUSTNO1
    LEFT JOIN cisalias a ON l.CUSTNO1 = a.CUSTNO1
    LEFT JOIN ciscust cu ON l.CUSTNO1 = cu.CUSTNO1
""").arrow()
print("LEFTOUT:")
print(LEFTOUT.to_pandas().head(5))

#================================#
#   PART 2 - PROCESSING RIGHT    #
#================================#
con.register("LEFTOUT", LEFTOUT)

con.execute(f"""
            CREATE VIEW CCCODE_R   AS SELECT * FROM '{host_parquet_path("BANKCTRL_RLENCODE_CC.parquet")}';
            CREATE VIEW NAMEFILE_R AS SELECT * FROM '{host_parquet_path("PRIMNAME_OUT.parquet")}';
            CREATE VIEW ALIASFIL_R AS SELECT * FROM '{host_parquet_path("ALLALIAS_OUT.parquet")}';
            CREATE VIEW CUSTFILE_R AS SELECT * FROM '{host_parquet_path("ALLCUST_FB.parquet")}';
""")

cccode_r = con.execute("""
    SELECT RLENTYPE AS TYPE,
           RLENCODE AS CODE2,
           RLENDESC AS DESC2
    FROM CCCODE_R
    ORDER BY CODE2
""").arrow()

ciscust_r = con.execute("""
    SELECT DISTINCT CUSTNO   AS CUSTNO2,
                    TAXID    AS OLDIC2,
                    BASICGRPCODE AS BASICGRPCODE2
    FROM CUSTFILE_R
""").arrow()

cisname_r = con.execute("""
    SELECT DISTINCT CUSTNO   AS CUSTNO2,
                    INDORG   AS INDORG2,
                    CUSTNAME AS CUSTNAME2
    FROM NAMEFILE_R
""").arrow()

cisalias_r = con.execute("""
    SELECT CUSTNO   AS CUSTNO2,
           NAME_LINE AS ALIAS2
    FROM ALIASFIL_R
    ORDER BY CUSTNO2
""").arrow()

ccrlen2 = con.execute("""
    SELECT CUSTNO1, INDORG1, CODE1, DESC1,
           CUSTNO2, CODE2, EXPDATE,
           CUSTNAME1, ALIAS1, OLDIC1, BASICGRPCODE1,
           EFFDATE,
           EXTRACT(YEAR FROM EXPDATE)  AS EXPYY,
           EXTRACT(MONTH FROM EXPDATE) AS EXPMM,
           EXTRACT(DAY FROM EXPDATE)   AS EXPDD
    FROM LEFTOUT
    ORDER BY CODE2
""").arrow()

RIGHTOUT = con.execute("""
    SELECT r.CUSTNO2, n.INDORG2, r.CODE2, c.DESC2,
           r.CUSTNO1, r.CODE1, r.EXPDATE,
           n.CUSTNAME2, a.ALIAS2, cu.OLDIC2, cu.BASICGRPCODE2, r.EFFDATE
    FROM ccrlen2 r
    LEFT JOIN cccode_r  c ON r.CODE2 = c.CODE2
    LEFT JOIN cisname_r n ON r.CUSTNO2 = n.CUSTNO2
    LEFT JOIN cisalias_r a ON r.CUSTNO2 = a.CUSTNO2
    LEFT JOIN ciscust_r cu ON r.CUSTNO2 = cu.CUSTNO2
""").arrow()
print("RIGHTOUT:")
print(RIGHTOUT.to_pandas().head(5))

#================================#
#   PART 3 - COMBINE & OUTPUT    #
#================================#
con.register("INPUT1", LEFTOUT)
con.register("INPUT2", RIGHTOUT)

alloutput = con.execute("""
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
""").arrow()

# Deduplicate
all_output_unique = con.execute("""
    SELECT DISTINCT *
    FROM alloutput
""").arrow()

# Find duplicates
duplicates = con.execute("""
    SELECT f.*
    FROM alloutput f
    EXCEPT
    SELECT u.*
    FROM all_output_unique u
""").arrow()

print("Alloutput (unique):")
print(all_output_unique.to_pandas().head(5))

print("Duplicate rows removed:")
print(duplicates.to_pandas().head(5))

# Save outputs
#pq.write_table(all_output_unique, "cis_internal/output/RLNSHIP.parquet")
#pv_csv.write_csv(all_output_unique, "cis_internal/output/RLNSHIP.csv")

#pq.write_table(duplicates, "cis_internal/output/RLNSHIP_DUPLICATES.parquet")
#pv_csv.write_csv(duplicates, "cis_internal/output/RLNSHIP_DUPLICATES.csv")
