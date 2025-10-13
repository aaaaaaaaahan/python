#=======================================================================#
#  4. BUILD FINAL OUTPUT STRUCTURE (DATA OUT) - FIXED
#=======================================================================#
final_df = con.sql("""
    SELECT
        regexp_replace(NAME, '[\\x00-\\x1F]', '', 'g') AS NAME,
        regexp_replace(ID1, '[\\x00-\\x1F]', '', 'g') AS ID1,
        regexp_replace(ID2, '[\\x00-\\x1F]', '', 'g') AS ID2,
        '' AS DT_ALIAS,
        '' AS DT_BANKRUPT_NO,
        'SN' AS CONST_SN,
        'L1' AS CONST_L1,
        'ADD' AS CONST_ADD,
        DEPT_CODE
    FROM sorted
""").df()
