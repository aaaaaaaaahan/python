import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pacsv
import os
from datetime import date

# ------------------------------------------------------------------
# Converted script: DuckDB processing + PyArrow outputs
# - Follows the original program's renaming and logic (CUSTNO1/CUSTNO2, OLDIC1/OLDIC2, etc.)
# - Reads input parquet files directly (no load_input)
# - Keeps rows where EXPDATE1 == '' (same as original Polars logic)
# - Uses batchdate variable for run date
# - Writes outputs (parquet + csv) using PyArrow; filenames kept as requested
# ------------------------------------------------------------------

# Set batchdate
today = date.today()
batchdate = today.strftime("%m%d%Y")
print(f"Batchdate: {batchdate}")

# ensure output dir
os.makedirs('cis_internal/output', exist_ok=True)

# connect duckdb
con = duckdb.connect(database=':memory:')

# register parquet files directly
con.execute("CREATE TABLE rlencc_fb AS SELECT * FROM read_parquet('cis_internal/rawdata_converted/RLENCC_FB.parquet')")
con.execute("CREATE TABLE bankctrl_rlencode_cc AS SELECT * FROM read_parquet('cis_internal/rawdata_converted/BANKCTRL_RLENCODE_CC.parquet')")
con.execute("CREATE TABLE primname_out AS SELECT * FROM read_parquet('cis_internal/rawdata_converted/PRIMNAME_OUT.parquet')")
con.execute("CREATE TABLE allalias_out AS SELECT * FROM read_parquet('cis_internal/rawdata_converted/ALLALIAS_OUT.parquet')")
con.execute("CREATE TABLE allcust_fb AS SELECT * FROM read_parquet('cis_internal/rawdata_converted/ALLCUST_FB.parquet')")

# -----------------------------
# Part 1 - PROCESSING LEFT SIDE (mirror original renames and logic)
# -----------------------------
# cccode: rename RLENTYPE->TYPE, RLENCODE->CODE1, RLENDESC->DESC1 and dedupe by CODE1
con.execute('''
CREATE VIEW v_cccode AS
SELECT RLENTYPE AS TYPE, RLENCODE AS CODE1, RLENDESC AS DESC1
FROM bankctrl_rlencode_cc
''')

con.execute('''
CREATE VIEW v_cccode_unique AS
SELECT DISTINCT ON (CODE1) *
FROM v_cccode
''')

# ciscust: rename CUSTNO->CUSTNO1, TAXID->OLDIC1, BASICGRPCODE->BASICGRPCODE1 and unique
con.execute('''
CREATE VIEW v_ciscust AS
SELECT DISTINCT CUSTNO AS CUSTNO1, TAXID AS OLDIC1, BASICGRPCODE AS BASICGRPCODE1
FROM allcust_fb
''')

# cisname: rename CUSTNO->CUSTNO1, INDORG->INDORG1, CUSTNAME->CUSTNAME1 and unique
con.execute('''
CREATE VIEW v_cisname AS
SELECT DISTINCT CUSTNO AS CUSTNO1, INDORG AS INDORG1, CUSTNAME AS CUSTNAME1
FROM primname_out
''')

# cisalias: rename CUSTNO->CUSTNO1, NAME_LINE->ALIAS1 (keep order)
con.execute('''
CREATE VIEW v_cisalias AS
SELECT CUSTNO AS CUSTNO1, NAME_LINE AS ALIAS1
FROM allalias_out
ORDER BY CUSTNO
''')

# ccrlen1: select/rename and parse EXPDATE1 -> EXPDATE; keep rows where EXPDATE1 == ''
con.execute('''
CREATE VIEW v_ccrlen1 AS
SELECT
  CUSTNO AS CUSTNO1,
  EFFDATE,
  CUSTNO2,
  CODE1,
  CODE2,
  TRIM(EXPIRE_DATE) AS EXPDATE1,
  TRY_CAST(TRIM(EXPIRE_DATE) AS DATE) AS EXPDATE
FROM rlencc_fb
''')

con.execute('''
CREATE VIEW v_ccrlen1_f AS
SELECT
  CUSTNO1,
  COALESCE(TRY_CAST(EFFDATE AS BIGINT), NULL) AS EFFDATE,
  CUSTNO2,
  TRY_CAST(CODE1 AS BIGINT) AS CODE1,
  TRY_CAST(CODE2 AS BIGINT) AS CODE2,
  EXPDATE
FROM v_ccrlen1
WHERE EXPDATE1 = ''
''')

# Merge CCRLEN1 and CCCODES to get RELATIONSHIP CODES (left side) - original used left join on CODE1
con.execute('''
CREATE VIEW v_idx_l01 AS
SELECT a.*, b.TYPE AS TYPE1, b.CODE1 AS CODE1_DESC, b.DESC1 AS DESC1
FROM v_ccrlen1_f a
LEFT JOIN v_cccode_unique b
  ON a.CODE1 = b.CODE1
''')

# Merge with namefile to get INDORG + CUSTNAME
con.execute('''
CREATE VIEW v_idx_l02 AS
SELECT a.*, n.INDORG1 AS INDORG1, n.CUSTNAME1 AS CUSTNAME1
FROM v_idx_l01 a
LEFT JOIN v_cisname n
  ON a.CUSTNO1 = n.CUSTNO1
''')

# Merge with aliasfil to get ALIAS1
con.execute('''
CREATE VIEW v_idx_l03 AS
SELECT a.*, al.ALIAS1 AS ALIAS1
FROM v_idx_l02 a
LEFT JOIN v_cisalias al
  ON a.CUSTNO1 = al.CUSTNO1
''')

# Merge with allcust to get OLDIC1 and BASICGRPCODE1
con.execute('''
CREATE VIEW v_idx_l04 AS
SELECT a.*, c.OLDIC1 AS OLDIC1, c.BASICGRPCODE1 AS BASICGRPCODE1
FROM v_idx_l03 a
LEFT JOIN v_ciscust c
  ON a.CUSTNO1 = c.CUSTNO1
''')

# LEFTOUT: select fields in the original order and with original renamed names
con.execute('''
CREATE VIEW v_leftout AS
SELECT
  CUSTNO1,
  INDORG1,
  CODE1,
  DESC1,
  CUSTNO2,
  CODE2,
  EXPDATE,
  CUSTNAME1,
  ALIAS1,
  OLDIC1,
  BASICGRPCODE1,
  EFFDATE
FROM v_idx_l04
''')

print('LEFTOUT preview:')
print(con.execute('SELECT * FROM v_leftout LIMIT 5').df())

# -----------------------------
# Part 2 - PROCESSING RIGHT SIDE (follow original renames and logic)
# -----------------------------
# Register infile2 from LEFTOUT
con.execute("CREATE TABLE infile2 AS SELECT * FROM v_leftout")

# cccode for right side: rename RLENTYPE->TYPE, RLENCODE->CODE2, RLENDESC->DESC2
con.execute('''
CREATE VIEW v_cccode_r AS
SELECT RLENTYPE AS TYPE, RLENCODE AS CODE2, RLENDESC AS DESC2
FROM bankctrl_rlencode_cc
''')

# ciscust right side: rename CUSTNO->CUSTNO2, TAXID->OLDIC2, BASICGRPCODE->BASICGRPCODE2
con.execute('''
CREATE VIEW v_ciscust_r AS
SELECT DISTINCT CUSTNO AS CUSTNO2, TAXID AS OLDIC2, BASICGRPCODE AS BASICGRPCODE2
FROM allcust_fb
''')

# cisname right side: rename fields
con.execute('''
CREATE VIEW v_cisname_r AS
SELECT DISTINCT CUSTNO AS CUSTNO2, INDORG AS INDORG2, CUSTNAME AS CUSTNAME2
FROM primname_out
''')

# cisalias right side
con.execute('''
CREATE VIEW v_cisalias_r AS
SELECT CUSTNO AS CUSTNO2, NAME_LINE AS ALIAS2
FROM allalias_out
ORDER BY CUSTNO
''')

# ccrlen2: extract EXP date parts
con.execute('''
CREATE VIEW v_ccrlen2 AS
SELECT *,
  EXTRACT(YEAR FROM EXPDATE) AS EXPYY,
  EXTRACT(MONTH FROM EXPDATE) AS EXPMM,
  EXTRACT(DAY FROM EXPDATE) AS EXPDD
FROM infile2
''')

# Merge with cccode_r on CODE2
con.execute('''
CREATE VIEW v_idx_r01 AS
SELECT a.*, b.DESC2 AS DESC2
FROM v_ccrlen2 a
LEFT JOIN v_cccode_r b
  ON a.CODE2 = b.CODE2
''')

# Merge with namefile right
con.execute('''
CREATE VIEW v_idx_r02 AS
SELECT a.*, n.INDORG2 AS INDORG2, n.CUSTNAME2 AS CUSTNAME2
FROM v_idx_r01 a
LEFT JOIN v_cisname_r n
  ON a.CUSTNO2 = n.CUSTNO2
''')

# Merge with alias
con.execute('''
CREATE VIEW v_idx_r03 AS
SELECT a.*, al.ALIAS2 AS ALIAS2
FROM v_idx_r02 a
LEFT JOIN v_cisalias_r al
  ON a.CUSTNO2 = al.CUSTNO2
''')

# Merge with allcust right
con.execute('''
CREATE VIEW v_idx_r04 AS
SELECT a.*, c.OLDIC2 AS OLDIC2, c.BASICGRPCODE2 AS BASICGRPCODE2
FROM v_idx_r03 a
LEFT JOIN v_ciscust_r c
  ON a.CUSTNO2 = c.CUSTNO2
''')

# RIGHTOUT: select fields in original order
con.execute('''
CREATE VIEW v_rightout AS
SELECT
  CUSTNO2,
  INDORG2,
  CODE2,
  DESC2,
  CUSTNO1,
  CODE1,
  EXPDATE,
  CUSTNAME2,
  ALIAS2,
  OLDIC2,
  BASICGRPCODE2,
  EFFDATE
FROM v_idx_r04
''')

print('RIGHTOUT preview:')
print(con.execute('SELECT * FROM v_rightout LIMIT 5').df())

# -----------------------------
# Part 3 - FINAL MERGE, DEDUP and DUPLICATES
# -----------------------------
# Create INPUT1 and INPUT2
con.execute('CREATE VIEW v_input1 AS SELECT CUSTNO1, INDORG1, CODE1, DESC1, CUSTNO2, CODE2, EXPDATE, CUSTNAME1, ALIAS1, OLDIC1, BASICGRPCODE1, EFFDATE FROM v_leftout')
con.execute('CREATE VIEW v_input2 AS SELECT CUSTNO2, INDORG2, CODE2, DESC2, CUSTNO1, CODE1, EXPDATE, CUSTNAME2, ALIAS2, OLDIC2, BASICGRPCODE2, EFFDATE FROM v_rightout')

# Join INPUT1 and INPUT2 on CUSTNO2 (left join)
con.execute('''
CREATE VIEW v_alloutput AS
SELECT
  i1.CUSTNO1,
  i1.INDORG1,
  i1.CODE1,
  i1.DESC1,
  i1.CUSTNO2,
  i2.INDORG2,
  i2.CODE2,
  i2.DESC2,
  COALESCE(i1.EXPDATE, i2.EXPDATE) AS EXPDATE,
  i1.CUSTNAME1,
  i1.ALIAS1,
  i2.CUSTNAME2,
  i2.ALIAS2,
  i1.OLDIC1,
  i1.BASICGRPCODE1,
  i2.OLDIC2,
  i2.BASICGRPCODE2,
  COALESCE(i1.EFFDATE, i2.EFFDATE) AS EFFDATE
FROM v_input1 i1
LEFT JOIN v_input2 i2
  ON i1.CUSTNO2 = i2.CUSTNO2
''')

# Keep full copy
con.execute('CREATE VIEW v_alloutput_full AS SELECT * FROM v_alloutput')

# Deduplicate: keep first row per (CUSTNO1, CUSTNO2, CODE1, CODE2) ordering by CUSTNO1
con.execute('''
CREATE VIEW v_alloutput_with_rn AS
SELECT *, ROW_NUMBER() OVER (PARTITION BY CUSTNO1, CUSTNO2, CODE1, CODE2 ORDER BY CUSTNO1) AS rn
FROM v_alloutput
''')

con.execute('CREATE VIEW v_alloutput_unique AS SELECT * EXCLUDE(rn) FROM v_alloutput_with_rn WHERE rn = 1')
con.execute('CREATE VIEW v_duplicates AS SELECT * EXCLUDE(rn) FROM v_alloutput_with_rn WHERE rn > 1')

print('Alloutput (unique) preview:')
print(con.execute('SELECT * FROM v_alloutput_unique LIMIT 5').df())
print('Duplicates preview:')
print(con.execute('SELECT * FROM v_duplicates LIMIT 5').df())

# -----------------------------
# Write outputs using PyArrow
# -----------------------------
OUTPUT_PARQUET = 'cis_internal/output/RLNSHIP.parquet'
OUTPUT_CSV = 'cis_internal/output/RLNSHIP.csv'
DUP_PARQUET = 'cis_internal/output/RLNSHIP_DUPLICATES.parquet'
DUP_CSV = 'cis_internal/output/RLNSHIP_DUPLICATES.csv'

all_unique_tbl = con.execute('SELECT * FROM v_alloutput_unique').fetch_arrow_table()
dups_tbl = con.execute('SELECT * FROM v_duplicates').fetch_arrow_table()

pq.write_table(all_unique_tbl, OUTPUT_PARQUET)
pq.write_table(dups_tbl, DUP_PARQUET)

pacsv.write_csv(all_unique_tbl, OUTPUT_CSV)
pacsv.write_csv(dups_tbl, DUP_CSV)

print('Wrote outputs:')
print(OUTPUT_PARQUET, OUTPUT_CSV)
print(DUP_PARQUET, DUP_CSV)
 
