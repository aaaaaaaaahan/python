import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pc

# -----------------------------
# Part 0: Read Parquet files using DuckDB
# -----------------------------
con = duckdb.connect()

oldic = con.execute("""
    SELECT 
        LPAD(CAST(CUSTNO AS BIGINT)::VARCHAR, 11, '0') AS CUSTNO,
        CAST(CODE_OLD AS VARCHAR) AS CODE_OLD,
        CAST(INDORG AS VARCHAR) AS INDORG,
        CAST(OLDIC AS VARCHAR) AS OLDIC,
        LPAD(CAST(CUSTBRCH AS BIGINT)::VARCHAR, 5, '0') AS CUSTBRCH
    FROM 'CCRIS_OLDIC_GDG.parquet'
    ORDER BY CUSTNO
""").arrow()
print("OLDIC:\n", oldic.to_pandas().head(5))

newic = con.execute("""
    SELECT 
        LPAD(CAST(CUSTNO AS BIGINT)::VARCHAR, 11, '0') AS CUSTNO,
        CAST(CODE_NEW AS VARCHAR) AS CODE_NEW,
        CAST(NEWIC AS VARCHAR) AS NEWIC,
        CAST(KEYFIELD1 AS VARCHAR) AS KEYFIELD1,
        CAST(KEYFIELD2 AS VARCHAR) AS KEYFIELD2,
        SUBSTRING(NEWIC, 4) AS NEWIC1
    FROM 'CCRIS_ALIAS_GDG.parquet'
    ORDER BY CUSTNO
""").arrow()
print("NEWIC:\n", newic.to_pandas().head(5))

rhold_all = con.execute("""
    SELECT DISTINCT ALIAS 
    FROM (
        SELECT ID1 AS ALIAS FROM 'RHOLD_FULL_LIST.parquet' WHERE ID1 <> ''
        UNION
        SELECT ID2 AS ALIAS FROM 'RHOLD_FULL_LIST.parquet' WHERE ID2 <> ''
    )
    ORDER BY ALIAS
""").arrow()
print("RHOLD:\n", rhold_all.to_pandas().head(5))

# -----------------------------
# Part 1: TAXID merge OLDIC + NEWIC
# -----------------------------
taxid = con.execute("""
    SELECT o.*, n.CODE_NEW, n.NEWIC, n.KEYFIELD1, n.KEYFIELD2, n.NEWIC1,
           CASE WHEN o.INDORG = 'O' THEN n.NEWIC ELSE NULL END AS BUSREG
    FROM oldic o
    LEFT JOIN newic n
    USING (CUSTNO)
    ORDER BY NEWIC1
""").arrow()
print("TAXID:\n", taxid.to_pandas().head(5))

# -----------------------------
# Part 2: TAXID_NEWIC merge with RHOLD
# -----------------------------
taxid_newic = con.execute("""
    SELECT t.*, 
           CASE WHEN r.ALIAS IS NOT NULL THEN 1 ELSE 0 END AS C
    FROM taxid t
    LEFT JOIN rhold_all r
    ON t.NEWIC1 = r.ALIAS
    ORDER BY OLDIC
""").arrow()

# -----------------------------
# Part 3: TAXID_OLDIC merge with RHOLD
# -----------------------------
taxid_oldic = con.execute("""
    SELECT t.*, 
           CASE WHEN r.ALIAS IS NOT NULL THEN 1 ELSE 0 END AS F
    FROM taxid_newic t
    LEFT JOIN rhold_all r
    ON t.OLDIC = r.ALIAS
    ORDER BY CUSTNO
""").arrow()

# -----------------------------
# Part 4: Final OUT dataset
# -----------------------------
final = con.execute("""
    SELECT 
        CUSTNO, OLDIC, NEWIC, BUSREG, CUSTBRCH,
        CASE 
            WHEN C = 1 AND F = 1 THEN 'B'
            WHEN C = 1 AND F = 0 THEN 'N'
            WHEN C = 0 AND F = 1 THEN 'O'
            ELSE 'X'
        END AS MATCHID,
        CASE 
            WHEN C = 1 OR F = 1 THEN 'Y'
            ELSE 'N'
        END AS RHOLD_IND
    FROM taxid_oldic
""").arrow()

print("Final Output:\n", final.to_pandas().head(5))

# -----------------------------
# Part 5: Write output using PyArrow
# -----------------------------
pq.write_table(final, "cis_internal/output/CCRIS_TAXID_GDG.parquet")
pc.write_csv(final, "cis_internal/output/CCRIS_TAXID_GDG.csv")

