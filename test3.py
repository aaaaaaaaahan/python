import duckdb
import datetime
import pyarrow as pa
import pyarrow.parquet as pq

#=======================================================================#
#  CONFIGURATION
#=======================================================================#
input_parquet = "RHOLD_FULL_LIST.parquet"
output_parquet = "RHOLD_PBCS_DAILY_ADD.parquet"

# Date setup (equivalent to SAS &SDATE)
batch_date = datetime.date.today() - datetime.timedelta(days=1)
SDATE = batch_date.strftime("%Y-%m-%d")  # format: YYYY-MM-DD

#=======================================================================#
#  INIT DUCKDB CONNECTION
#=======================================================================#
con = duckdb.connect()

#=======================================================================#
#  1. READ INPUT FILE
#=======================================================================#
con.execute(f"""
    CREATE TABLE data_add AS
    SELECT *
    FROM read_parquet('{input_parquet}')
""")

#=======================================================================#
#  2. FILTER AND CLEAN DATA
#=======================================================================#
# Equivalent to SAS logic:
# - Keep only records where LASTMNT_SAS = &SDATE
# - Exclude DEPT_CODE = 'PBCSS' or blank
# - Exclude CLASS_CODE='CLS0000004' + NATURE_CODE in ('NAT0000028','NAT0000044')

con.execute(f"""
    CREATE TABLE filtered AS
    SELECT
        CLASS_CODE,
        CLASS_DESC,
        NATURE_CODE,
        NATURE_DESC,
        DEPT_CODE,
        DEPT_DESC,
        GUIDE_CODE,
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
        SUBSTR(DTL_LASTMNT_DATE,1,4) AS MNT_DATE_YY,
        SUBSTR(DTL_LASTMNT_DATE,6,2) AS MNT_DATE_MM,
        SUBSTR(DTL_LASTMNT_DATE,9,2) AS MNT_DATE_DD,
        DTL_LASTMNT_TIME,
        CONTACT_1,
        CONTACT_2,
        CONTACT_3
    FROM data_add
    WHERE DATE(DTL_LASTMNT_DATE) = DATE '{SDATE}'
      AND TRIM(DEPT_CODE) <> 'PBCSS'
      AND TRIM(DEPT_CODE) <> ''
      AND NOT (
          CLASS_CODE = 'CLS0000004' AND 
          NATURE_CODE IN ('NAT0000028', 'NAT0000044')
      )
""")

#=======================================================================#
#  3. SORT BY DEPT_CODE (PROC SORT)
#=======================================================================#
con.execute("""
    CREATE TABLE sorted AS
    SELECT * FROM filtered
    ORDER BY DEPT_CODE
""")

#=======================================================================#
#  4. BUILD FINAL OUTPUT STRUCTURE (DATA OUT)
#=======================================================================#
final_df = con.sql("""
    SELECT
        REPLACE(NAME, CHAR(0x41), '') AS NAME,
        '' AS DT_ALIAS,
        '' AS DT_BANKRUPT_NO,
        ID2,
        ID1,
        'SN' AS CONST_SN,
        'L1' AS CONST_L1,
        'ADD' AS CONST_ADD,
        DEPT_CODE
    FROM sorted
""").df()

#=======================================================================#
#  5. WRITE OUTPUT USING PYARROW
#=======================================================================#
table = pa.Table.from_pandas(final_df)
pq.write_table(table, output_parquet)

print(f"✅ CIRHUNIA processing complete.")
print(f"Input : {input_parquet}")
print(f"Output: {output_parquet}")
print(f"Records written: {len(final_df)}")
