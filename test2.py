import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq

from pathlib import Path

# ================================
# CONFIG PATHS
# ================================
input_parquet = "UNLOAD_CISIGNAT_FB.parquet"   # converted CISIGNAT file
output_file = "SNGLVIEW_SIGN.csv"              # output like SNGLVIEW.SIGN

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
    FROM read_parquet('{input_parquet}')
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

# Optional: Preview like PROC PRINT OBS=10
preview = con.execute("SELECT * FROM SIGNATORY_SORTED LIMIT 10").fetchdf()
print("Preview of SIGNATORY:")
print(preview)

# ================================
# STEP 3 - Format Output
# ================================
# Rebuild SAS PUT formatting into a CSV-style export
con.execute("""
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
            || BRANCHX     || '", \N' AS RECORD_LINE
    FROM SIGNATORY_SORTED
""")

# Fetch as Arrow table
arrow_tbl = con.execute("SELECT RECORD_LINE FROM TEMPOUT").fetch_arrow_table()

# ================================
# STEP 4 - Save Output
# ================================
# Convert to string column
lines = arrow_tbl.to_pandas()["RECORD_LINE"].tolist()

with open(output_file, "w", encoding="utf-8") as f:
    for line in lines:
        f.write(line + "\n")

print(f"✅ Output written to {output_file}")
