import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pacsv

# -----------------------------
# Step 1: Connect DuckDB
# -----------------------------
con = duckdb.connect()

# -----------------------------
# Step 2: Register input Parquet files
# -----------------------------
# You can change the file paths as needed
con.execute("CREATE VIEW namefile AS SELECT * FROM 'CCRIS_CISNAME_TEMP.parquet'")
con.execute("CREATE VIEW rmrkfile AS SELECT * FROM 'CCRIS_CISRMRK_LONGNAME.parquet'")

# -----------------------------
# Step 3: Clean & prepare NAMEFILE
# -----------------------------
con.execute("""
    CREATE VIEW name_clean AS
    SELECT DISTINCT
        LPAD(CAST(CUSTNO AS VARCHAR), 11, '0') AS CUSTNO,
        CAST(CUSTNAME AS VARCHAR) AS CUSTNAME,
        LPAD(CAST(ADREFNO AS VARCHAR), 11, '0') AS ADREFNO,
        LPAD(CAST(PRIPHONE AS VARCHAR), 11, '0') AS PRIPHONE,
        LPAD(CAST(SECPHONE AS VARCHAR), 11, '0') AS SECPHONE,
        CAST(CUSTTYPE AS VARCHAR) AS CUSTTYPE,
        CAST(CUSTNAME1 AS VARCHAR) AS CUSTNAME1,
        CAST(MOBILEPHONE AS VARCHAR) AS MOBILEPHONE
    FROM namefile
""")

# -----------------------------
# Step 4: Clean & prepare RMRKFILE
# -----------------------------
con.execute("""
    CREATE VIEW rmrk_clean AS
    SELECT DISTINCT
        CAST(BANKNO AS VARCHAR) AS BANKNO,
        CAST(APPLCODE AS VARCHAR) AS APPLCODE,
        CAST(CUSTNO AS VARCHAR) AS CUSTNO,
        CAST(EFFDATE AS VARCHAR) AS EFFDATE,
        CAST(RMKKEYWORD AS VARCHAR) AS RMKKEYWORD,
        CAST(LONGNAME AS VARCHAR) AS LONGNAME,
        CAST(RMKOPERATOR AS VARCHAR) AS RMKOPERATOR,
        CAST(EXPIREDATE AS VARCHAR) AS EXPIREDATE,
        CAST(LASTMNTDATE AS VARCHAR) AS LASTMNTDATE
    FROM rmrkfile
""")

# -----------------------------
# Step 5: Merge (LEFT JOIN like SAS "IF A;")
# -----------------------------
query = """
    SELECT 
        n.CUSTNO,
        n.CUSTNAME,
        n.ADREFNO,
        n.PRIPHONE,
        n.SECPHONE,
        n.CUSTTYPE,
        n.CUSTNAME1,
        n.MOBILEPHONE,
        r.LONGNAME
    FROM name_clean n
    LEFT JOIN rmrk_clean r
    ON n.CUSTNO = r.CUSTNO
    ORDER BY n.CUSTNO
"""

arrow_result = con.execute(query).arrow()  # PyArrow Table

print("Preview:")
print(arrow_result.to_pandas().head())  # just to preview

# -----------------------------
# Step 6: Write Output with PyArrow
# -----------------------------
# Write Parquet
pq.write_table(arrow_result, "cis_internal/output/CCRIS_CISNAME_OUT.parquet")

# Write CSV
pacsv.write_csv(
    arrow_result,
    "cis_internal/output/CCRIS_CISNAME_OUT.csv"
)

