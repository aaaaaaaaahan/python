import duckdb
import datetime
import pyarrow as pa
import pyarrow.parquet as pq

# === CONFIGURATION ===
input_file = "UNLOAD_ALLCUST_FB.parquet"     # assumed input parquet file
output_parquet_indv = "CIS_BLANK_MSIC.parquet"
output_parquet_org = "CIS_BLANK_MASCO.parquet"
output_txt_indv = "CIS_BLANK_MSIC.txt"
output_txt_org = "CIS_BLANK_MASCO.txt"

# === CREATE DUCKDB CONNECTION ===
con = duckdb.connect()

# === LOAD INPUT FILE ===
con.execute(f"""
    CREATE TABLE cust AS 
    SELECT * FROM read_parquet('{input_file}')
""")

# === PROCESS LOGIC ===
# In SAS:
#  - Drop GENDER
#  - Compute INDORG based on GENDER
#  - Split to DATA_INDV and DATA_ORG
#  - Output depending on MSICCODE and MASCO2008

con.execute("""
    CREATE TABLE data_indv AS
    SELECT 
        CUSTNO,
        '' AS MSICCODE
    FROM cust
    WHERE 
        (CASE WHEN GENDER = 'O' THEN 'O' ELSE 'I' END) = 'I'
        AND MSICCODE IS NOT NULL AND LENGTH(TRIM(MSICCODE)) > 0
""")

con.execute("""
    CREATE TABLE data_org AS
    SELECT 
        CUSTNO,
        '' AS MASCO2008
    FROM cust
    WHERE 
        (CASE WHEN GENDER = 'O' THEN 'O' ELSE 'I' END) = 'O'
        AND MASCO2008 IS NOT NULL AND LENGTH(TRIM(MASCO2008)) > 0
""")

# === SORT RESULTS ===
con.execute("CREATE TABLE out_indv AS SELECT * FROM data_indv ORDER BY CUSTNO")
con.execute("CREATE TABLE out_org AS SELECT * FROM data_org ORDER BY CUSTNO")

# === EXPORT TO PARQUET ===
con.execute(f"COPY out_indv TO '{output_parquet_indv}' (FORMAT PARQUET)")
con.execute(f"COPY out_org TO '{output_parquet_org}' (FORMAT PARQUET)")

# === EXPORT TO FIXED-WIDTH TXT FILES ===
def write_fixed_width_txt(table_name, output_path, columns, widths):
    """Write fixed-width text file like SAS PUT statement."""
    data = con.execute(f"SELECT * FROM {table_name}").fetchall()
    with open(output_path, "w", encoding="utf-8") as f:
        for row in data:
            line = ""
            for i, col in enumerate(columns):
                val = str(row[i]) if row[i] is not None else ""
                line += val.ljust(widths[i])  # pad right to fixed width
            f.write(line + "\n")

# For INDIVIDUAL (MSIC)
write_fixed_width_txt(
    "out_indv",
    output_txt_indv,
    columns=["CUSTNO", "MSICCODE"],
    widths=[44, 5]  # CUSTNO @001–@045 (11 chars), MSICCODE @045–@050
)

# For ORGANISATION (MASCO)
write_fixed_width_txt(
    "out_org",
    output_txt_org,
    columns=["CUSTNO", "MASCO2008"],
    widths=[44, 5]
)

print("✅ Process completed successfully.")
print(f"Generated: {output_parquet_indv}, {output_parquet_org}")
print(f"Generated: {output_txt_indv}, {output_txt_org}")
