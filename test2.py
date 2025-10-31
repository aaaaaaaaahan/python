import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
import os

# ============================================================
# DATE SETUP (Equivalent to SAS &TODAYDATE)
# ============================================================
today = datetime.date.today()

# ============================================================
# PATH SETUP (Assumed Parquet input ready)
# ============================================================
host_parquet_path = "/path/to/input"      # Folder where your input parquet file is stored
parquet_output_path = "/path/to/output"   # Folder for parquet output
csv_output_path = "/path/to/output"       # Folder for CSV output

input_parquet = f"{host_parquet_path}/UNLOAD_CIREPTTT_FB.parquet"
output_parquet = f"{parquet_output_path}/CISREPT_UPDATE_REVIEW.parquet"
output_csv = f"{csv_output_path}/CISREPT_UPDATE_REVIEW.csv"

# ============================================================
# DUCKDB CONNECTION
# ============================================================
con = duckdb.connect()

# ============================================================
# STEP 1: READ INPUT DATA (REPTFILE)
# ============================================================
con.execute(f"""
CREATE OR REPLACE TABLE REPTDATA AS 
SELECT 
    BANKNO,
    RECTYPE,
    APPLCODE,
    APPLNO,
    NOTENO,
    REPORTDATE,
    SUBSTR(REPORTDATE,1,2) AS RPDATEDD,
    SUBSTR(REPORTDATE,4,2) AS RPDATEMM,
    SUBSTR(REPORTDATE,7,4) AS RPDATEYYYY,
    REPORTNO,
    BRANCHNO,
    NAME,
    CODE1,
    CODE2,
    CODE3,
    CODE4,
    CODE5,
    AMOUNT1,
    AMOUNT2,
    AMOUNT3,
    AMOUNT4,
    AMOUNT5,
    DATE1,
    DATE2,
    DATE3,
    DATE4,
    DATE5,
    REMARK1,
    REMARK2,
    REMARK3,
    REMARK4,
    REMARK5,
    VIEWED,
    CUSTASSESS,
    BRCHCOMMENTS,
    BRCHREVIEW,
    BRCHCHECK,
    HOCOMMENTS,
    HOREVIEW,
    HOCHECK,
    CUSTOCCUP,
    CUSTNATURE,
    CUSTEMPLOYER,
    INDORG,
    OCCUPDESC,
    NATUREDESC,
    VIEWOFF,
    REVIEW,
    TRY_CAST(SUBSTR(REPORTDATE,7,4) || '-' || SUBSTR(REPORTDATE,4,2) || '-' || SUBSTR(REPORTDATE,1,2) AS DATE) AS REPTSAS
FROM read_parquet('{input_parquet}');
""")

# ============================================================
# STEP 2: SPLIT INTO REPTDATA_CLEAN (REVIEW IS BLANK)
# ============================================================
con.execute("""
CREATE OR REPLACE TABLE REPTDATA_CLEAN AS
SELECT 
    BANKNO,
    RECTYPE,
    APPLCODE,
    APPLNO,
    NOTENO,
    REPORTDATE,
    RPDATEDD,
    RPDATEMM,
    RPDATEYYYY,
    REPORTNO,
    BRANCHNO,
    NAME,
    CODE1,
    CODE2,
    CODE3,
    CODE4,
    CODE5,
    AMOUNT1,
    AMOUNT2,
    AMOUNT3,
    AMOUNT4,
    AMOUNT5,
    DATE1,
    DATE2,
    DATE3,
    DATE4,
    DATE5,
    REMARK1,
    REMARK2,
    REMARK3,
    REMARK4,
    REMARK5,
    VIEWED,
    CUSTASSESS,
    BRCHCOMMENTS,
    BRCHREVIEW,
    BRCHCHECK,
    HOCOMMENTS,
    HOREVIEW,
    HOCHECK,
    CUSTOCCUP,
    CUSTNATURE,
    CUSTEMPLOYER,
    INDORG,
    OCCUPDESC,
    NATUREDESC,
    VIEWOFF,
    REVIEW,
    REPTSAS
FROM REPTDATA
WHERE REVIEW IS NULL OR TRIM(REVIEW) = '';
""")

# ============================================================
# STEP 3: FILTER FOR REVDATA (< 365 days)
# ============================================================
con.execute("""
CREATE OR REPLACE TABLE REVDATA AS
SELECT 
    BANKNO,
    RECTYPE,
    APPLCODE,
    APPLNO,
    NOTENO,
    REPORTDATE,
    RPDATEDD,
    RPDATEMM,
    RPDATEYYYY,
    REPORTNO,
    BRANCHNO,
    NAME,
    CODE1,
    CODE2,
    CODE3,
    CODE4,
    CODE5,
    AMOUNT1,
    AMOUNT2,
    AMOUNT3,
    AMOUNT4,
    AMOUNT5,
    DATE1,
    DATE2,
    DATE3,
    DATE4,
    DATE5,
    REMARK1,
    REMARK2,
    REMARK3,
    REMARK4,
    REMARK5,
    VIEWED,
    CUSTASSESS,
    BRCHCOMMENTS,
    BRCHREVIEW,
    BRCHCHECK,
    HOCOMMENTS,
    HOREVIEW,
    HOCHECK,
    CUSTOCCUP,
    CUSTNATURE,
    CUSTEMPLOYER,
    INDORG,
    OCCUPDESC,
    NATUREDESC,
    VIEWOFF,
    REVIEW,
    REPTSAS
FROM REPTDATA
WHERE BRCHCHECK IS NOT NULL
  AND (julianday(CURRENT_DATE) - julianday(REPTSAS)) < 365;
""")

# ============================================================
# STEP 4: REMOVE DUPLICATES (EQUIV. TO NODUPKEY)
# ============================================================
con.execute("""
CREATE OR REPLACE TABLE REVDATA_NODUP AS
SELECT DISTINCT ON (APPLCODE, APPLNO, NOTENO)
    BANKNO,
    RECTYPE,
    APPLCODE,
    APPLNO,
    NOTENO,
    REPORTDATE,
    RPDATEDD,
    RPDATEMM,
    RPDATEYYYY,
    REPORTNO,
    BRANCHNO,
    NAME,
    CODE1,
    CODE2,
    CODE3,
    CODE4,
    CODE5,
    AMOUNT1,
    AMOUNT2,
    AMOUNT3,
    AMOUNT4,
    AMOUNT5,
    DATE1,
    DATE2,
    DATE3,
    DATE4,
    DATE5,
    REMARK1,
    REMARK2,
    REMARK3,
    REMARK4,
    REMARK5,
    VIEWED,
    CUSTASSESS,
    BRCHCOMMENTS,
    BRCHREVIEW,
    BRCHCHECK,
    HOCOMMENTS,
    HOREVIEW,
    HOCHECK,
    CUSTOCCUP,
    CUSTNATURE,
    CUSTEMPLOYER,
    INDORG,
    OCCUPDESC,
    NATUREDESC,
    VIEWOFF,
    REVIEW,
    REPTSAS
FROM REVDATA
ORDER BY APPLCODE, APPLNO, NOTENO, RPDATEYYYY, RPDATEMM, RPDATEDD;
""")

# ============================================================
# STEP 5: KEEP ONLY REQUIRED COLUMNS
# ============================================================
con.execute("""
CREATE OR REPLACE TABLE ALLREV AS
SELECT REPORTDATE, APPLCODE, APPLNO, NOTENO
FROM REVDATA_NODUP;
""")

# ============================================================
# STEP 6: MERGE REPTDATA_CLEAN & ALLREV TO GET UPDREPT
# ============================================================
con.execute("""
CREATE OR REPLACE TABLE UPDREPT AS
SELECT 
    A.BANKNO,
    A.RECTYPE,
    A.APPLCODE,
    A.APPLNO,
    A.NOTENO,
    A.REPORTDATE,
    A.RPDATEDD,
    A.RPDATEMM,
    A.RPDATEYYYY,
    A.REPORTNO,
    A.BRANCHNO,
    A.NAME,
    A.CODE1,
    A.CODE2,
    A.CODE3,
    A.CODE4,
    A.CODE5,
    A.AMOUNT1,
    A.AMOUNT2,
    A.AMOUNT3,
    A.AMOUNT4,
    A.AMOUNT5,
    A.DATE1,
    A.DATE2,
    A.DATE3,
    A.DATE4,
    A.DATE5,
    A.REMARK1,
    A.REMARK2,
    A.REMARK3,
    A.REMARK4,
    A.REMARK5,
    A.VIEWED,
    A.CUSTASSESS,
    A.BRCHCOMMENTS,
    A.BRCHREVIEW,
    A.BRCHCHECK,
    A.HOCOMMENTS,
    A.HOREVIEW,
    A.HOCHECK,
    A.CUSTOCCUP,
    A.CUSTNATURE,
    A.CUSTEMPLOYER,
    A.INDORG,
    A.OCCUPDESC,
    A.NATUREDESC,
    A.VIEWOFF,
    A.REVIEW
FROM REPTDATA_CLEAN A
JOIN ALLREV B
  ON A.APPLCODE = B.APPLCODE
 AND A.APPLNO = B.APPLNO
 AND A.NOTENO = B.NOTENO
WHERE (
    SUBSTR(A.REPORTDATE,7,4) || SUBSTR(A.REPORTDATE,4,2) || SUBSTR(A.REPORTDATE,1,2)
  ) > (
    SUBSTR(B.REPORTDATE,7,4) || SUBSTR(B.REPORTDATE,4,2) || SUBSTR(B.REPORTDATE,1,2)
  );
""")

# ============================================================
# STEP 7: OUTPUT FINAL DATA
# ============================================================
final_query = """
SELECT 
    BANKNO,
    RECTYPE,
    APPLCODE,
    APPLNO,
    NOTENO,
    REPORTDATE,
    REPORTNO,
    BRANCHNO,
    NAME,
    '  ' AS PAD,
    'Y' AS REVIEWFLAG
FROM UPDREPT;
"""

# Write to Parquet
pq.write_table(con.execute(final_query).arrow(), output_parquet)

# Write to CSV
con.execute(f"""
COPY ({final_query})
TO '{output_csv}' (HEADER, DELIMITER ',');
""")

# ============================================================
# DONE
# ============================================================
print("✅ Process complete.")
print(f"Parquet output: {output_parquet}")
print(f"CSV output: {output_csv}")
