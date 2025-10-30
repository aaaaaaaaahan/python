import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

# ============================================================
# 1. READ INPUT FILES
# ============================================================

custfile_path = f"{host_parquet_path}/ALLCUST_FB.parquet"
cus2file_path = f"{host_parquet_path}/CICUS2T_ALL.parquet"

con = duckdb.connect()

# CUSTOMER FILE - Individual record
con.execute(f"""
    CREATE OR REPLACE TABLE CICUSTT AS
    SELECT 
        CAST(BANKNO AS INTEGER) AS BANKNO,
        CIS_NO
    FROM read_parquet('{custfile_path}')
""")

# CICUS2T FILE
con.execute(f"""
    CREATE OR REPLACE TABLE CICUS2T AS
    SELECT 
        CIS_NO,
        CIS_TIN,
        CIS_SST,
        CIS_EINV_ACCEPT_DATE,
        CIS_EINV_ACCEPT_TIME
    FROM read_parquet('{cus2file_path}')
""")

# ============================================================
# 2. MERGE LOGIC (A AND NOT B)
# ============================================================

insert_new = con.execute("""
    SELECT A.CIS_NO
    FROM CICUSTT A
    LEFT JOIN CICUS2T B
    ON A.CIS_NO = B.CIS_NO
    WHERE B.CIS_NO IS NULL
    ORDER BY A.CIS_NO
""").arrow()

print(">>> Number of new customers:", len(insert_new))

# ============================================================
# 3. OUTPUT FILES
# ============================================================

output_table = pa.table(insert_new)

# Save as Parquet
parquet_out = f"{parquet_output_path}/CIS_CICUS2T_INSERT.parquet"
pq.write_table(output_table, parquet_out)

# Save as CSV
csv_out = f"{csv_output_path}/CIS_CICUS2T_INSERT.csv"
duckdb.query(f"""
    COPY (SELECT * FROM read_parquet('{parquet_out}'))
    TO '{csv_out}' (HEADER, DELIMITER ',')
""")

print(f">>> Output saved to:\n - {parquet_out}\n - {csv_out}")

con.close()
