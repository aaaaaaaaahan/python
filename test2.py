import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime
import os

# ================================
# BATCH DATE
# ================================
batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ================================
# CONNECT TO DUCKDB
# ================================
con = duckdb.connect()

# ================================
# STEP 1 - Load SIGNATOR file
# ================================
input_file = host_parquet_path("UNLOAD_CISIGNAT_FB.parquet")

con.execute(f"""
    CREATE OR REPLACE TABLE SIGNATORY AS
    SELECT 
        BANKNO,
        ACCTNO,
        SEQNO,
        REPLACE(NAME, '\\\\', '\\\\\\\\') AS NAME,  -- escape backslash
        ID,
        SIGNATORY,
        MANDATEE,
        NOMINEE,
        STATUS,
        BRANCHNO,
        BRANCHX
    FROM read_parquet('{input_file}')
""")

print("✅ Loaded SIGNATORY, row count:",
      con.execute("SELECT COUNT(*) FROM SIGNATORY").fetchone()[0])

# ================================
# STEP 2 - Sort by ACCTNO + SEQNO
# ================================
con.execute("""
    CREATE OR REPLACE TABLE SIGNATORY_SORTED AS
    SELECT * 
    FROM SIGNATORY
    ORDER BY ACCTNO, SEQNO
""")

print("✅ SIGNATORY_SORTED row count:",
      con.execute("SELECT COUNT(*) FROM SIGNATORY_SORTED").fetchone()[0])

# Preview
print(con.execute("SELECT * FROM SIGNATORY_SORTED LIMIT 5").fetchdf())

# ================================
# STEP 3 - Format Output
# ================================
con.execute(f"""
    CREATE OR REPLACE TABLE TEMPOUT AS
    SELECT 
        '"' || BANKNO      || '","'
            || ACCTNO      || '","'
            || SEQNO       || '","'
            || NAME        || '","'
            || ID          || '","'
            || SIGNATORY   || '","'
            || MANDATEE    || '","'
            || NOMINEE     || '","'
            || STATUS      || '","'
            || LPAD(CAST(BRANCHNO AS VARCHAR), 5, '0') || '","'
            || BRANCHX     || '"' AS RECORD_LINE,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM SIGNATORY_SORTED
""")

print("✅ TEMPOUT row count:",
      con.execute("SELECT COUNT(*) FROM TEMPOUT").fetchone()[0])
print(con.execute("SELECT * FROM TEMPOUT LIMIT 3").fetchdf())

# ================================
# STEP 4 - Save Output
# ================================
parquet_path = parquet_output_path("SNGLVIEW_SIGN")
csv_dir = csv_output_path("SNGLVIEW_SIGN")

# Ensure CSV dir exists
os.makedirs(csv_dir, exist_ok=True)
csv_file = os.path.join(csv_dir, "SNGLVIEW_SIGN.csv")

# ---- Parquet (partitioned) ----
con.execute(f"""
COPY (SELECT * FROM TEMPOUT)
TO '{parquet_path}'
(FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
""")

# ---- CSV (single file, flat) ----
con.execute(f"""
COPY (SELECT * FROM TEMPOUT)
TO '{csv_file}'
(FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);
""")

print("✅ Output written:")
print("   Parquet ->", parquet_path)
print("   CSV     ->", csv_file)
