import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date

# -----------------------------
# File paths (adjust as needed)
# -----------------------------
rlenc_file = "RLEN_CA.parquet"
cust_file = "ALLCUST_FB.parquet"
id_file = "ALLALIAS_FB.parquet"

out_all_dp_parquet = "CUSTS_DP.parquet"
out_no_id_parquet = "CUSNOID_WTHACCT.parquet"

out_all_dp_txt = "CUSTS_DP.txt"
out_no_id_txt = "CUSNOID_WTHACCT.txt"

# Connect to DuckDB
con = duckdb.connect()

# -----------------------------
# 1. Load RLEN and filter DP accounts
# -----------------------------
con.execute(f"""
CREATE OR REPLACE TABLE RLEN AS
SELECT *
FROM read_parquet('{rlenc_file}')
WHERE ACCTCODE = 'DP   '
""")

# -----------------------------
# 2. Load CUST and filter by Citizenship MY and gender
# -----------------------------
con.execute(f"""
CREATE OR REPLACE TABLE CUST AS
SELECT *,
       CASE WHEN GENDER='O' THEN 'O' ELSE 'I' END AS INDORG
FROM read_parquet('{cust_file}')
WHERE CITIZENSHIP='MY'
""")

# -----------------------------
# 3. Load ICID and remove duplicates
# -----------------------------
con.execute(f"""
CREATE OR REPLACE TABLE ICID AS
SELECT DISTINCT ON (CUSTNO) *
FROM read_parquet('{id_file}')
""")

# -----------------------------
# 4. Merge RLEN and ICID for customers with aliases not 'IC'
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE MERGE1 AS
SELECT r.BANKNO, r.ACCTNOC, r.ACCTNO, r.ACCTCODE, r.RLENCODE, r.PRISEC,
       i.CUSTNO, i.ALIASKEY, i.ALIAS
FROM RLEN r
JOIN ICID i
  ON r.CUSTNO = i.CUSTNO
WHERE i.ALIASKEY <> 'IC'
""")

# -----------------------------
# 5. Merge CUST and MERGE1 to get ALLDP
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE ALLDP AS
SELECT c.*, m.BANKNO, m.ACCTNOC, m.ACCTNO, m.ACCTCODE AS ACCTCODE_MERGE,
       m.RLENCODE, m.PRISEC, m.ALIASKEY, m.ALIAS
FROM CUST c
JOIN MERGE1 m
  ON c.CUSTNO = m.CUSTNO
""")

# -----------------------------
# 6. Customers with NO ID
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE NOID AS
SELECT c.*
FROM CUST c
LEFT JOIN ICID i ON c.CUSTNO = i.CUSTNO
WHERE i.CUSTNO IS NULL
""")

# -----------------------------
# 7. NOID with RLEN info
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE NOIDREL AS
SELECT n.*, r.BANKNO, r.ACCTNOC, r.ACCTNO, r.ACCTCODE, r.RLENCODE, r.PRISEC
FROM NOID n
JOIN RLEN r ON n.CUSTNO = r.CUSTNO
""")

# -----------------------------
# 8. Write Parquet outputs
# -----------------------------
con.execute(f"COPY ALLDP TO '{out_all_dp_parquet}' (FORMAT PARQUET)")
con.execute(f"COPY NOIDREL TO '{out_no_id_parquet}' (FORMAT PARQUET)")

# -----------------------------
# 9. Write fixed-width TXT outputs
# -----------------------------
def write_fixed_width(table_name, file_path):
    df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
    with open(file_path, "w") as f:
        for _, row in df.iterrows():
            line = (
                f"{int(row.get('CUSTBRCH',0)):07d}"
                f"{row.get('CUSTNO',''):<11}"
                f"{row.get('ACCTCODE',''):<5}"
                f"{str(row.get('ACCTNO','')):>20}"
                f"{row.get('RACE',''):<1}"
                f"{row.get('ALIASKEY',''):<3}"
                f"{row.get('ALIAS',''):<20}"
                f"{row.get('CITIZENSHIP',''):<2}"
            )
            f.write(line + "\n")

write_fixed_width("ALLDP", out_all_dp_txt)
write_fixed_width("NOIDREL", out_no_id_txt)

print("Processing completed. Parquet and TXT files are generated.")
