# ---------------------------
# Step 1: Read Parquet
# ---------------------------
con.execute("""
CREATE OR REPLACE TABLE temp1 AS
SELECT *, code AS F1
FROM mergefound_expanded
""")

# ---------------------------
# Step 2: Deduplicate and sort
# ---------------------------
con.execute("""
CREATE OR REPLACE TABLE temp_sorted AS
SELECT DISTINCT CUSTNO, F1, RECTYPE, BRANCH, FILECODE, STAFFNO, STAFFNAME
FROM temp1
ORDER BY CUSTNO, F1
""")

# ---------------------------
# Step 3: Pivot F1 into W1-W10, W11-W20=0
# ---------------------------
con.execute("""
CREATE OR REPLACE TABLE temp2 AS
WITH numbered AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY F1) AS rn
    FROM temp_sorted
)
SELECT
    CUSTNO,
    RECTYPE,
    BRANCH,
    FILECODE,
    STAFFNO,
    STAFFNAME,
    COALESCE(MAX(CASE WHEN rn=1 THEN F1 END),0) AS W1,
    COALESCE(MAX(CASE WHEN rn=2 THEN F1 END),0) AS W2,
    COALESCE(MAX(CASE WHEN rn=3 THEN F1 END),0) AS W3,
    COALESCE(MAX(CASE WHEN rn=4 THEN F1 END),0) AS W4,
    COALESCE(MAX(CASE WHEN rn=5 THEN F1 END),0) AS W5,
    COALESCE(MAX(CASE WHEN rn=6 THEN F1 END),0) AS W6,
    COALESCE(MAX(CASE WHEN rn=7 THEN F1 END),0) AS W7,
    COALESCE(MAX(CASE WHEN rn=8 THEN F1 END),0) AS W8,
    COALESCE(MAX(CASE WHEN rn=9 THEN F1 END),0) AS W9,
    COALESCE(MAX(CASE WHEN rn=10 THEN F1 END),0) AS W10,
    0 AS W11,
    0 AS W12,
    0 AS W13,
    0 AS W14,
    0 AS W15,
    0 AS W16,
    0 AS W17,
    0 AS W18,
    0 AS W19,
    0 AS W20
FROM numbered
GROUP BY CUSTNO, RECTYPE, BRANCH, FILECODE, STAFFNO, STAFFNAME
ORDER BY CUSTNO
""")

# ---------------------------
# Step 4: Export fixed-width TXT
# ---------------------------
txt_path = output_txt
df_txt = con.execute("SELECT * FROM temp2 ORDER BY CUSTNO").fetchdf()

with open(txt_path, "w", encoding="utf-8") as f:
    for _, row in df_txt.iterrows():
        line = (
            f"{str(row['CUSTNO']).ljust(11)}"
            f"{str(row['RECTYPE']).ljust(1)}"
            f"{str(row['BRANCH']).ljust(7)}"
            + "".join([str(row[f"W{i}"]).zfill(3) for i in range(1, 21)])
            f"{str(row['FILECODE']).ljust(1)}"
            f"{str(row['STAFFNO']).ljust(9)}"
            f"{str(row['STAFFNAME']).ljust(40)}"
        )
        f.write(line + "\n")
