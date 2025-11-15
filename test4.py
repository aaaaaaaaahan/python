# -----------------------------
# Align columns for DATA_DEL
# -----------------------------
con.execute("""
CREATE TABLE data_del_aligned AS
SELECT
    NAME,
    '' AS DT_ALIAS,
    ID2,
    ID1,
    '' AS DT_BANKRUPT_NO,
    'SN' AS SN,
    'L1' AS L1,
    'DEL' AS DEL,
    ' ' AS SPACE,
    DEPT_CODE
FROM data_del
""")

# -----------------------------
# Align columns for DATA_PURGED
# -----------------------------
con.execute("""
CREATE TABLE data_purged_aligned AS
SELECT
    NAME,
    '' AS DT_ALIAS,
    ID2,
    ID1,
    '' AS DT_BANKRUPT_NO,
    'SN' AS SN,
    'L1' AS L1,
    'DEL' AS DEL,
    ' ' AS SPACE,
    DEPT_CODE
FROM data_purged
""")

# -----------------------------
# Now UNION ALL works
# -----------------------------
con.execute("""
CREATE TABLE data_deleted AS
SELECT * FROM data_purged_aligned
UNION ALL
SELECT * FROM data_del_aligned
ORDER BY DEPT_CODE
""")
