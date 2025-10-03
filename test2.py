import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

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
    FROM '{host_parquet_path("UNLOAD_CISIGNAT_FB.parquet")}'
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
# STEP 3 - Build Output Table
# ================================
con.execute(f"""
    CREATE OR REPLACE TABLE TEMPOUT AS
    SELECT 
        BANKNO,
        ACCTNO,
        SEQNO,
        NAME,
        ID,
        SIGNATORY,
        MANDATEE,
        NOMINEE,
        STATUS,
        LPAD(CAST(BRANCHNO AS VARCHAR), 5, '0') AS BRANCHNO,
        BRANCHX,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM SIGNATORY_SORTED
""")

# ================================
# STEP 4 - Save Output
# ================================
out1 = "SELECT * FROM TEMPOUT"

queries = {
    "SNGLVIEW_SIGN": out1
}

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    # Save parquet (partitioned by year, month, day)
    con.execute(f"""
    COPY ({query})
    TO '{parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);  
    """)

    # Save CSV (all columns as separate CSV fields)
    con.execute(f"""
    COPY ({query})
    TO '{csv_path}'
    (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);  
    """)

print("✅ Export completed")
