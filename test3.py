import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# =========================
#   FILE PATHS (parquet inputs)
# =========================
depfile1_parquet = "CCRIS_CISDEMO_SAFD.parquet"   # SAFD(0)
depfile2_parquet = "CCRIS_CISDEMO_DP.parquet"     # DP.GDG(0)
namefile_parquet  = "CCRIS_CISNAME.parquet"       # NAMEFILE
taxfile_parquet   = "CCRIS_TAXID.parquet"         # TAXFILE

# Output
dpfile_output_parquet = "BNMCTR_CISWEEK_DEP.parquet"
dpfile_output_txt     = "BNMCTR_CISWEEK_DEP.txt"

# =========================
#   DUCKDB CONNECTION
# =========================
con = duckdb.connect()

# =========================
#   STEP 1: DEPFILE (concat both sources → DEMODP)
# =========================
con.execute(f"""
    CREATE OR REPLACE VIEW dep AS 
    SELECT * FROM read_parquet('{depfile1_parquet}')
    UNION ALL
    SELECT * FROM read_parquet('{depfile2_parquet}')
""")

demodp = con.execute("""
    SELECT 
        ACCTNO,
        INDORG,
        CUSTNO,
        CACCCODE,
        PRISEC,
        CASE 
            WHEN NULLIF(CITIZENSHIP, '') IS NULL THEN '99'
            ELSE CITIZENSHIP
        END AS CITIZENSHIP,
        'DP' AS ACCTTYPE
    FROM dep
    ORDER BY CUSTNO
""").arrow()

print("=== DEMODP (first 10 rows) ===")
print(demodp.slice(0, 10).to_pandas())

# =========================
#   STEP 2: NAMEFILE
# =========================
name_tbl = con.execute(f"""
    SELECT CUSTNO, NAME
    FROM read_parquet('{namefile_parquet}')
    ORDER BY CUSTNO
""").arrow()

# =========================
#   STEP 3: MERGE1 (BY CUSTNO)
# =========================
con.register("demodp", demodp)
con.register("name_tbl", name_tbl)

merge1 = con.execute("""
    SELECT d.*, n.NAME
    FROM demodp d
    LEFT JOIN name_tbl n USING (CUSTNO)
    ORDER BY d.CUSTNO
""").arrow()

# =========================
#   STEP 4: TAXFILE
# =========================
tax_tbl = con.execute(f"""
    SELECT 
        CUSTNO,
        OLDIC,
        NEWIC,
        NEWIC2,
        BRANCH
    FROM read_parquet('{taxfile_parquet}')
    ORDER BY CUSTNO
""").arrow()

print("=== TAXID (first 10 rows) ===")
print(tax_tbl.slice(0, 10).to_pandas())

# =========================
#   STEP 5: MERGE2 (BY CUSTNO)
# =========================
con.register("merge1", merge1)
con.register("tax_tbl", tax_tbl)

merge2 = con.execute("""
    SELECT m.*, 
           t.OLDIC, t.NEWIC, t.NEWIC2, t.BRANCH
    FROM merge1 m
    LEFT JOIN tax_tbl t USING (CUSTNO)
    ORDER BY ACCTNO, CUSTNO, PRISEC
""").arrow()

# =========================
#   STEP 6: OUT1 FINAL TRANSFORMATION
# =========================
con.register("merge2", merge2)

out1 = con.execute("""
    SELECT 
        '033' AS CONST033,
        ACCTTYPE,
        ACCTNO,
        '         ' AS BLANKS9,
        CASE 
            WHEN PRISEC = '901' THEN 'P'
            WHEN PRISEC = '902' THEN 'S'
            ELSE PRISEC
        END AS PRISEC,
        CACCCODE,
        CUSTNO,
        INDORG,
        BRANCH,
        NEWIC,
        NULL AS CODE,   -- CODE not defined in SAS input
        NAME
    FROM merge2
    ORDER BY ACCTNO, CUSTNO, PRISEC
""").arrow()

print("=== OUT1 (first 10 rows) ===")
print(out1.slice(0, 10).to_pandas())

# =========================
#   STEP 7: SAVE TO PARQUET
# =========================
pq.write_table(out1, dpfile_output_parquet)

# =========================
#   STEP 8: SAVE FIXED-WIDTH TEXT (like SAS PUT)
# =========================
with open(dpfile_output_txt, "w") as f:
    for row in out1.to_pylist():
        line = (
            f"{row['CONST033']:<3}"
            f"{row['ACCTTYPE']:<5}"
            f"{row['ACCTNO']:<11}"
            f"{row['BLANKS9']:<9}"
            f"{row['PRISEC'] or ' ':<1}"
            f"{row['CACCCODE'] or '':<3}"
            f"{row['CUSTNO']:<11}"
            f"{row['INDORG'] or ' ':<1}"
            f"{row['BRANCH'] or '':<5}"
            f"{row['NEWIC'] or '':<23}"
            f"{row['CODE'] or '':<3}"
            f"{row['NAME'] or '':<40}"
        )
        f.write(line + "\n")

print("✅ Processing complete: output written to parquet and fixed-width text.")
