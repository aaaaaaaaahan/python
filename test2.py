import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

# =========================================================
# CONFIG
# =========================================================
INPUT1 = "/host/input/WINDOW.SIGNATOR.CA0801.parquet"        # contains ACCTNO, C1, C2, C3, C4
SORT_OUT = "/host/output/WINDOW.SIGNATOR.CA0801.SORT.parquet"

BRANCH_FILE = "/host/input/PBB.BRANCH.parquet"               # fields: BRANCH, BRHABV
MERGED_TXT = "/host/output/WINDOW.SIGNATOR.MERGED.txt"
MERGED_PARQUET = "/host/output/WINDOW.SIGNATOR.MERGED.parquet"

con = duckdb.connect()


# =========================================================
# STEP 1 — READ INPUT (HORIZONTAL)
# =========================================================
con.execute(f"""
    CREATE TABLE ACCT AS
    SELECT * FROM read_parquet('{INPUT1}');
""")


# =========================================================
# STEP 2 — TRANSPOSE (HORIZONTAL → VERTICAL)
# =========================================================
con.execute("""
    CREATE TABLE SORTED AS
    SELECT 
        ACCTNO,
        UNNEST([C1, C2, C3, C4]) AS OUTPUT
    FROM ACCT;
""")

# Remove empty nominee slots
con.execute("""
    DELETE FROM SORTED WHERE TRIM(OUTPUT) = '';
""")

# Write SORT as parquet only
pq.write_table(con.execute("SELECT * FROM SORTED").arrow(), SORT_OUT)


# =========================================================
# STEP 3 — READ STATUS + BRANCH INFO (FROM CA0801 AGAIN)
# =========================================================
# Your CA0801 input does NOT include STATUS/BRANCH fields.
# You MUST confirm field names. I assume your parquet contains:

# ACCTNO, STATUS, BRANCH

con.execute(f"""
    CREATE TABLE NOMIIN AS
    SELECT 
        ACCTNO,
        STATUS,
        BRANCH
    FROM read_parquet('{INPUT1}');
""")


# =========================================================
# STEP 4 — READ BRANCH FILE
# =========================================================
# Branch parquet contains: BRANCH, BRHABV

con.execute(f"""
    CREATE TABLE BRHTABLE AS
    SELECT * FROM read_parquet('{BRANCH_FILE}');
""")


# =========================================================
# STEP 5 — MERGE NOMIIN + BRANCH FILE
# =========================================================
con.execute("""
    CREATE TABLE NOM_BRANCH AS
    SELECT A.ACCTNO, A.STATUS, A.BRANCH, B.BRHABV
    FROM NOMIIN A 
    LEFT JOIN BRHTABLE B USING (BRANCH);
""")


# =========================================================
# STEP 6 — READ SORTED (VERTICAL) AND SPLIT OUTPUT
# =========================================================
# OUTPUT contains: "NAME + space + IC"
# Example: "ALI BIN AHMAD 900101015555A"

con.execute("""
    CREATE TABLE NOMIIN2 AS
    SELECT
        ACCTNO,
        LEFT(OUTPUT, 40) AS NAME,
        SUBSTR(OUTPUT, 41, 30) AS IC_NUMBER
    FROM SORTED
    WHERE TRIM(OUTPUT) <> '';
""")

# Remove missing data
con.execute("""
    DELETE FROM NOMIIN2
    WHERE TRIM(NAME)='' OR TRIM(IC_NUMBER)='';
""")


# =========================================================
# STEP 7 — MATCH TO PRODUCE FINAL MERGED TABLE
# =========================================================
con.execute("""
    CREATE TABLE NOM_FOUND AS
    SELECT 
        A.ACCTNO,
        A.IC_NUMBER,
        A.NAME,
        B.STATUS,
        B.BRHABV,
        B.BRANCH
    FROM NOMIIN2 A
    LEFT JOIN NOM_BRANCH B USING (ACCTNO)
    WHERE B.ACCTNO IS NOT NULL;
""")

# (Optional) unmatched but not required for output
con.execute("""
    CREATE TABLE NOM_XFOUND AS
    SELECT *
    FROM NOMIIN2 A
    WHERE NOT EXISTS (SELECT 1 FROM NOM_BRANCH B WHERE A.ACCTNO=B.ACCTNO);
""")


# =========================================================
# STEP 8 — WRITE MERGED OUTPUT (TXT + PARQUET)
# =========================================================
rows = con.execute("""
    SELECT ACCTNO, IC_NUMBER, NAME, STATUS, BRHABV, BRANCH
    FROM NOM_FOUND
""").fetchall()

# Write TXT output for DB2
with open(MERGED_TXT, "w", encoding="utf-8") as fp:
    for acctno, ic, name, status, brhabv, branch in rows:

        line = (
            f"{acctno:<11}"              # ACCTNO @21 (left-aligned)
            f"{ic:<20}"                  # IC @54
            f"{name:<40}"                # NAME @76
            f"Y"                          # Constant 'Y'
            f"{status}"                   # STATUS @117
            f"{brhabv:<3}"                # BRHABV @118
            f"{branch:03d}"               # BRANCH Z03. @121
        )

        fp.write(line + "\n")

# Write parquet version
pq.write_table(con.execute("SELECT * FROM NOM_FOUND").arrow(), MERGED_PARQUET)


print("CINOMEX1 COMPLETED — SORT to parquet only, MERGED to parquet & txt.")
