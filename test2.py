import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ================================
# CONNECT TO DUCKDB
# ================================
con = duckdb.connect()

# ================================
# STEP 1 - Load SIGNATOR file
# ================================
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
    FROM read_parquet('{host_parquet_path("UNLOAD_CISIGNAT_FB.parquet")}')
""")

# ================================
# STEP 2 - Sort by ACCTNO + SEQNO
# ================================
con.execute("""
    CREATE OR REPLACE TABLE SIGNATORY_SORTED AS
    SELECT * 
    FROM SIGNATORY
    ORDER BY ACCTNO, SEQNO
""")

# Preview
preview = con.execute("SELECT * FROM SIGNATORY_SORTED LIMIT 10").fetchdf()
print("Preview of SIGNATORY:")
print(preview)

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

# ================================
# STEP 4 - Save Output
# ================================
query = "SELECT * FROM TEMPOUT"

parquet_path = parquet_output_path("SNGLVIEW_SIGN")
csv_path = csv_output_path("SNGLVIEW_SIGN")

con.execute(f"""
COPY ({query})
TO '{parquet_path}'
(FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);  
""")

con.execute(f"""
COPY ({query})
TO '{csv_path}'
(FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);  
""")

print("✅ Output written:", parquet_path, "and", csv_path)
