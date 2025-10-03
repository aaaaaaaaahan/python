# ==================================
# Step 5 - Get duplicate remarks (ICETOOL ALLDUPS equivalent)
# Using ROW_NUMBER() OVER (PARTITION BY ...) to find duplicates
# ==================================
con.execute("""
    CREATE OR REPLACE TABLE DUP AS
    SELECT *
    FROM (
        SELECT 
            e.*,
            ROW_NUMBER() OVER (
                PARTITION BY ACCTCODE, ACCTNOC, RMK_KEYWORD, RMK_LINE_1, RMK_LINE_2, RMK_LINE_3, RMK_LINE_4, RMK_LINE_5
                ORDER BY LAST_MNT_DATE
            ) AS rn,
            COUNT(*) OVER (
                PARTITION BY ACCTCODE, ACCTNOC, RMK_KEYWORD, RMK_LINE_1, RMK_LINE_2, RMK_LINE_3, RMK_LINE_4, RMK_LINE_5
            ) AS cnt
        FROM ENH_RMRK e
    ) t
    WHERE cnt > 1
    ORDER BY ACCTCODE, ACCTNOC, rn
""")

dup_arrow = con.execute("SELECT * FROM DUP").arrow()
pq.write_table(dup_arrow, dup_path)

print("Duplicate remark file written:", dup_path)
