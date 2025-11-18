import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path

# Paths for input and output Parquet files
branch_file = "EBANK.BRANCH.OUT.parquet"
officer_file = "EBANK.OFFICER.OUT.parquet"
branch_officer_file = "EBANK.BRANCH.OFFICER.FILE.parquet"
branch_officer_combine = "EBANK.BRANCH.OFFICER.COMBINE.parquet"
helpdesk_file = "PBB.BRANCH.HELPDESK.parquet"
branch_prefer_file = "EBANK.BRANCH.PREFER.parquet"

# Connect to DuckDB in memory
con = duckdb.connect(database=':memory:')

# Step 1: Concatenate branch and officer files (like SORT IN JCL)
con.execute(f"""
    CREATE TABLE branch_officer AS
    SELECT * FROM read_parquet('{branch_file}')
    UNION ALL
    SELECT * FROM read_parquet('{officer_file}')
""")

# Optional: sort by first 10 chars + next 3 chars (equivalent to SORT FIELDS=(1,10,CH,A,11,3,CH,A))
# Assuming columns have names 'col1', 'col2', ... if you want exact, map to real columns
# con.execute("CREATE TABLE branch_officer_sorted AS SELECT * FROM branch_officer ORDER BY col1, col2")

# Save concatenated result to Parquet
con.execute(f"COPY (SELECT * FROM branch_officer) TO '{branch_officer_file}' (FORMAT PARQUET)")

# Step 2: Read branch_officer and helpdesk files
con.execute(f"""
    CREATE TABLE branch AS
    SELECT 
        SUBSTRING(BANKINDC, 1, 1) AS BANKINDC,
        SUBSTRING(BRANCHNO, 1, 7) AS BRANCHNO,
        SUBSTRING(BRANCHABRV, 1, 3) AS BRANCHABRV,
        SUBSTRING(PB_BRNAME, 1, 20) AS PB_BRNAME,
        SUBSTRING(ADDRLINE1, 1, 35) AS ADDRLINE1,
        SUBSTRING(ADDRLINE2, 1, 35) AS ADDRLINE2,
        SUBSTRING(ADDRLINE3, 1, 35) AS ADDRLINE3,
        SUBSTRING(PHONENO, 1, 11) AS PHONENO,
        SUBSTRING(STATENO, 1, 3) AS STATENO,
        SUBSTRING(BRANCHABRV2, 1, 4) AS BRANCHABRV2
    FROM read_parquet('{branch_officer_file}')
""")

con.execute(f"CREATE TABLE branch_sorted AS SELECT DISTINCT * FROM branch ORDER BY BRANCHABRV")

con.execute(f"""
    CREATE TABLE helpdesk AS
    SELECT DISTINCT BRANCHABRV, HD_BRNAME
    FROM read_parquet('{helpdesk_file}')
""")

# Step 3: Merge branch and helpdesk (equivalent to DATA ACTIVE; MERGE)
con.execute("""
    CREATE TABLE active AS
    SELECT b.*, h.HD_BRNAME
    FROM branch_sorted b
    JOIN helpdesk h
    ON b.BRANCHABRV = h.BRANCHABRV
""")

# Sort final output by BRANCHNO
con.execute("CREATE TABLE out_final AS SELECT * FROM active ORDER BY BRANCHNO")

# Step 4: Save final output to Parquet and TXT
con.execute(f"COPY (SELECT * FROM out_final) TO '{branch_prefer_file}' (FORMAT PARQUET)")

# Save as fixed-width TXT similar to SAS PUT
out_txt_path = Path(branch_prefer_file).with_suffix(".txt")
with open(out_txt_path, "w") as f:
    result = con.execute("SELECT * FROM out_final").fetchall()
    for row in result:
        # Map to original field widths
        f.write(
            f"{row['BANKINDC']:<1}"
            f"{row['BRANCHNO']:<7}"
            f"{row['BRANCHABRV']:<3}"
            f"{row['PB_BRNAME']:<20}"
            f"{row['ADDRLINE1']:<35}"
            f"{row['ADDRLINE2']:<35}"
            f"{row['ADDRLINE3']:<35}"
            f"{row['PHONENO']:<11}"
            f"{row['STATENO']:<3}"
            f"{row['BRANCHABRV2']:<4}"
            f"\n"
        )

print("Process completed: Parquet and TXT output ready.")
