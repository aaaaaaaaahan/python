import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# File paths (assumes input Parquet files already exist)
branch_parquet = "EBANK.BRANCH.OFFICER.COMBINE.parquet"
helpdesk_parquet = "PBB.BRANCH.HELPDESK.parquet"
output_parquet = "EBANK.BRANCH.PREFER.parquet"
output_txt = "EBANK.BRANCH.PREFER.txt"

# Connect to an in-memory DuckDB database
con = duckdb.connect(database=':memory:')

# -----------------------------
# Step 1: Load Parquet files
# -----------------------------
# Load branch and helpdesk data into DuckDB tables
con.execute(f"""
CREATE TABLE branch AS
SELECT *
FROM read_parquet('{branch_parquet}')
""")

con.execute(f"""
CREATE TABLE helpdesk AS
SELECT *
FROM read_parquet('{helpdesk_parquet}')
""")

# -----------------------------
# Step 2: Deduplicate data
# -----------------------------
# Remove duplicate BRANCHABRV entries, similar to SAS PROC SORT NODUPKEY
con.execute("""
CREATE TABLE branch_sorted AS
SELECT DISTINCT ON (BRANCHABRV) *
FROM branch
ORDER BY BRANCHABRV
""")

con.execute("""
CREATE TABLE helpdesk_sorted AS
SELECT DISTINCT ON (BRANCHABRV) *
FROM helpdesk
ORDER BY BRANCHABRV
""")

# -----------------------------
# Step 3: Merge datasets
# -----------------------------
# Replicate SAS DATA MERGE with IF B (keep only records present in helpdesk)
con.execute("""
CREATE TABLE active AS
SELECT h.*
FROM branch_sorted b
RIGHT JOIN helpdesk_sorted h
ON b.BRANCHABRV = h.BRANCHABRV
""")

# -----------------------------
# Step 4: Sort final data
# -----------------------------
# Sort by BRANCHNO to match SAS PROC SORT
con.execute("""
CREATE TABLE out_table AS
SELECT *
FROM active
ORDER BY BRANCHNO
""")

# -----------------------------
# Step 5: Export to Parquet
# -----------------------------
con.execute(f"COPY out_table TO '{output_parquet}' (FORMAT PARQUET)")

# -----------------------------
# Step 6: Export to fixed-width TXT
# -----------------------------
# Define fixed-width columns matching original SAS layout
col_specs = [
    ('BANKINDC', 1),
    ('BRANCHNO', 7),
    ('BRANCHABRV', 3),
    ('PB_BRNAME', 20),
    ('ADDRLINE1', 35),
    ('ADDRLINE2', 35),
    ('ADDRLINE3', 35),
    ('PHONENO', 11),
    ('STATENO', 3),
    ('BRANCHABRV2', 4),
]

# Fetch all rows from the final table
out_rows = con.execute("SELECT * FROM out_table").fetchall()

# Write rows to fixed-width text file
with open(output_txt, 'w', encoding='utf-8') as f:
    for row in out_rows:
        line = ""
        for i, (col, width) in enumerate(col_specs):
            val = row[i] if row[i] is not None else ""  # Replace NULLs with empty string
            val_str = str(val)[:width]  # Truncate if longer than column width
            line += val_str.ljust(width)
        f.write(line + "\n")

print("Processing complete. Parquet and fixed-width TXT files generated successfully.")
