import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")


# === CONFIGURATION ===
input_file = "UNLOAD_ALLCUST_FB.parquet"     # assumed input parquet file
output_parquet_indv = "CIS_BLANK_MSIC.parquet"
output_parquet_org = "CIS_BLANK_MASCO.parquet"
output_txt_indv = "CIS_BLANK_MSIC.txt"
output_txt_org = "CIS_BLANK_MASCO.txt"

# -----------------------------
# Connect to DuckDB (in-memory)
# -----------------------------
con = duckdb.connect()

# -----------------------------
# Load input Parquet file
# -----------------------------
con.execute(f"""
    CREATE TABLE cust AS 
    SELECT 
        CUSTNO,
        GENDER,
        MSICCODE,
        MASCO2008,
        CASE
            WHEN GENDER = 'O' THEN 'O'
            ELSE 'I'
        END AS INDORG
    FROM read_parquet('{host_parquet_path("ALLCUST_FB.parquet")}')
""")

# -----------------------------
# In SAS:
#  - Drop GENDER
#  - Compute INDORG based on GENDER
#  - Split to DATA_INDV and DATA_ORG
# -----------------------------

con.execute("""
    CREATE TABLE data_indv AS
    SELECT 
        CUSTNO,
        MSICCODE
    FROM cust
    WHERE 
        INDORG = 'I'
        AND MSICCODE IS NOT NULL AND LENGTH(TRIM(MSICCODE)) > 0
    ORDER BY CUSTNO
""")

con.execute("""
    CREATE TABLE data_org AS
    SELECT 
        CUSTNO,
        MASCO2008
    FROM cust
    WHERE 
        INDORG = 'O'
        AND MASCO2008 IS NOT NULL AND LENGTH(TRIM(MASCO2008)) > 0
    ORDER BY CUSTNO
""")

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

init_txt = csv_output_path(f"CIHRCRVP_INIT_{report_date}").replace(".csv", ".txt")

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

# -----------------------------
# Export Parquet outputs (INIT + UPDATE)
# -----------------------------
out1 = f"""
    SELECT
        *,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM data_indv
"""

out2 = f"""
    SELECT
        *,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM data_org
"""

queries = {
    "CIS_BLANK_MASCO": out1,
    "CIS_BLANK_MSIC ": out2
}

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    con.execute(f"""
        COPY ({query})
        TO '{parquet_path}'
        (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
    """)

print("✅ Process completed successfully.")
