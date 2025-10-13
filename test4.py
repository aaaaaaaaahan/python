# ----------------------------------------------------------------#
# 5. Output FULL FILE with all fields (including one empty field)
# ----------------------------------------------------------------#
full_df = con.sql("""
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
    DTL_LASTMNT_TIME,
    CONTACT1,
    CONTACT2,
    CONTACT3,
    '' AS BLANK1
FROM DEPT_DESC
ORDER BY CLASS_ID, INDORG, NAME
""").arrow()

pq.write_table(full_df, f"{output_path}RHOLD_FULL_LIST.parquet")
