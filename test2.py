import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import datetime

# ----------------------------------------------------------------#
#  Setup paths
# ----------------------------------------------------------------#
input_path = "/host/cis/parquet/input/"
output_path = "/host/cis/parquet/output/"

con = duckdb.connect()

# ----------------------------------------------------------------#
#  Load Input Parquet Files
# ----------------------------------------------------------------#
CIRHOBCT = f"{input_path}UNLOAD_CIRHOBCT.parquet"
CIRHODCT = f"{input_path}UNLOAD_CIRHODCT.parquet"
CIRHOLDT = f"{input_path}UNLOAD_CIRHOLDT.parquet"

# Register input tables
con.sql(f"CREATE OR REPLACE TABLE CIRHOBCT AS SELECT * FROM read_parquet('{CIRHOBCT}')")
con.sql(f"CREATE OR REPLACE TABLE CIRHODCT AS SELECT * FROM read_parquet('{CIRHODCT}')")
con.sql(f"CREATE OR REPLACE TABLE CIRHOLDT AS SELECT * FROM read_parquet('{CIRHOLDT}')")

# ----------------------------------------------------------------#
#  1. Split CIRHODCT into CLASS, DEPT, NATURE
# ----------------------------------------------------------------#
con.sql("""
CREATE OR REPLACE TABLE CLASS AS
SELECT 
    KEY_CODE AS CLASS_CODE,
    KEY_DESCRIBE AS CLASS_DESC
FROM CIRHODCT
WHERE KEY_ID = 'CLASS '
""")

con.sql("""
CREATE OR REPLACE TABLE NATURE AS
SELECT 
    KEY_CODE AS NATURE_CODE,
    KEY_DESCRIBE AS NATURE_DESC
FROM CIRHODCT
WHERE KEY_ID = 'NATURE'
""")

con.sql("""
CREATE OR REPLACE TABLE DEPT AS
SELECT
    KEY_CODE AS DEPT_CODE,
    KEY_DESCRIBE AS DEPT_DESC,
    CASE WHEN KEY_REMARK_ID1 = 'CONTACT1' AND KEY_REMARK_1 <> '' THEN KEY_REMARK_1 END AS CONTACT1,
    CASE WHEN KEY_REMARK_ID2 = 'CONTACT2' AND KEY_REMARK_2 <> '' THEN KEY_REMARK_2 END AS CONTACT2,
    CASE WHEN KEY_REMARK_ID3 = 'CONTACT3' AND KEY_REMARK_3 <> '' THEN KEY_REMARK_3 END AS CONTACT3
FROM CIRHODCT
WHERE KEY_ID = 'DEPT  '
""")

# ----------------------------------------------------------------#
#  2. CONTROL dataset
# ----------------------------------------------------------------#
con.sql("""
CREATE OR REPLACE TABLE CONTROL AS
SELECT 
    CLASS_CODE,
    NATURE_CODE,
    DEPT_CODE,
    GUIDE_CODE,
    CLASS_ID,
    CTRL_OPERATOR,
    CTRL_LASTMNT_DATE,
    CTRL_LASTMNT_TIME
FROM CIRHOBCT
ORDER BY CLASS_ID
""")

# ----------------------------------------------------------------#
#  3. DETAIL dataset
# ----------------------------------------------------------------#
con.sql("""
CREATE OR REPLACE TABLE DETAIL AS
SELECT 
    CLASS_ID,
    INDORG,
    NAME,
    ID1,
    ID2,
    DTL_REMARK1,
    DTL_REMARK2,
    DTL_REMARK3,
    DTL_REMARK4,
    DTL_REMARK5,
    DTL_CRT_DATE,
    DTL_CRT_TIME,
    DTL_LASTOPERATOR,
    DTL_LASTMNT_DATE,
    DTL_LASTMNT_TIME
FROM CIRHOLDT
ORDER BY CLASS_ID
""")

# ----------------------------------------------------------------#
#  4. Merge DETAIL + CONTROL + CLASS + NATURE + DEPT
# ----------------------------------------------------------------#
con.sql("""
CREATE OR REPLACE TABLE DEPT_DESC AS
SELECT 
    D.CLASS_ID,
    D.INDORG,
    D.NAME,
    D.ID1,
    D.ID2,
    D.DTL_REMARK1,
    D.DTL_REMARK2,
    D.DTL_REMARK3,
    D.DTL_REMARK4,
    D.DTL_REMARK5,
    D.DTL_CRT_DATE,
    D.DTL_CRT_TIME,
    D.DTL_LASTOPERATOR,
    D.DTL_LASTMNT_DATE,
    D.DTL_LASTMNT_TIME,
    C.CLASS_CODE,
    CL.CLASS_DESC,
    C.NATURE_CODE,
    N.NATURE_DESC,
    C.DEPT_CODE,
    DP.DEPT_DESC,
    DP.CONTACT1,
    DP.CONTACT2,
    DP.CONTACT3,
    C.GUIDE_CODE
FROM DETAIL D
LEFT JOIN CONTROL C ON D.CLASS_ID = C.CLASS_ID
LEFT JOIN CLASS CL ON C.CLASS_CODE = CL.CLASS_CODE
LEFT JOIN NATURE N ON C.NATURE_CODE = N.NATURE_CODE
LEFT JOIN DEPT DP ON C.DEPT_CODE = DP.DEPT_CODE
ORDER BY D.CLASS_ID, D.INDORG, D.NAME
""")

# ----------------------------------------------------------------#
#  5. Output FULL FILE
# ----------------------------------------------------------------#
full_df = con.sql("SELECT * FROM DEPT_DESC").arrow()
pq.write_table(full_df, f"{output_path}RHOLD_FULL_LIST.parquet")

# ----------------------------------------------------------------#
#  6. PIVB output (exclude class_code=CLS0000004 & nature_code=NAT0000028)
# ----------------------------------------------------------------#
pivb_df = con.sql("""
SELECT INDORG, NAME, ID1, ID2, CONTACT1, CONTACT2, CONTACT3
FROM DEPT_DESC
WHERE NOT (CLASS_CODE='CLS0000004' AND NATURE_CODE='NAT0000028')
""").arrow()
pq.write_table(pivb_df, f"{output_path}RHOLD_FULL_LIST_PIVB.parquet")

# ----------------------------------------------------------------#
#  7. LABUAN output (same exclusion + replace 0x05 char)
# ----------------------------------------------------------------#
labuan_df = con.sql("""
SELECT 
    CLASS_CODE,
    INDORG,
    regexp_replace(NAME, '\x05', ' ') AS NAME,
    ID1,
    ID2,
    CONTACT1,
    CONTACT2,
    CONTACT3
FROM DEPT_DESC
WHERE NOT (CLASS_CODE='CLS0000004' AND NATURE_CODE='NAT0000028')
""").arrow()
pq.write_table(labuan_df, f"{output_path}RHOLD_FULL_LIST_LABUAN.parquet")

# ----------------------------------------------------------------#
#  8. PMB output (same exclusion)
# ----------------------------------------------------------------#
pmb_df = con.sql("""
SELECT INDORG, NAME, ID1, ID2, CONTACT1, CONTACT2, CONTACT3
FROM DEPT_DESC
WHERE NOT (CLASS_CODE='CLS0000004' AND NATURE_CODE='NAT0000028')
""").arrow()
pq.write_table(pmb_df, f"{output_path}RHOLD_FULL_LIST_PMB.parquet")

# ----------------------------------------------------------------#
#  9. CTOS output (filtered class_id and date range)
# ----------------------------------------------------------------#
ctos_df = con.sql("""
SELECT 
    CLASS_CODE,
    CLASS_DESC,
    NATURE_CODE,
    NATURE_DESC,
    DEPT_CODE,
    DEPT_DESC,
    CLASS_ID,
    INDORG,
    NAME,
    ID1,
    ID2,
    DTL_CRT_DATE,
    DTL_LASTMNT_DATE,
    DTL_LASTMNT_TIME
FROM DEPT_DESC
WHERE CLASS_ID IN (
  '0000000005','0000000006','0000000009','0000000010',
  '0000000014','0000000015','0000000019','0000000021',
  '0000000025','0000000026','0000000031','0000000032'
)
AND DTL_CRT_DATE > '2015-10-01' AND DTL_CRT_DATE < '2018-08-31'
""").arrow()
pq.write_table(ctos_df, f"{output_path}RHOLD_FULL_LIST_CTOS.parquet")

# ----------------------------------------------------------------#
#  Done
# ----------------------------------------------------------------#
print("All output files successfully generated.")
