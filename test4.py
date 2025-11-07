import duckdb
from CIS_PY_READER import host_parquet_path, csv_output_path, parquet_output_path
import datetime

#----------------------------------------------------------------------#
#  Original Program: CIHCMRPT                                          #
#----------------------------------------------------------------------#
# ESMR2019-1394 - REPORT DAILY                                         #
# CUSTOMER DELTA FILE - TO GET CHANGES OF THE DAY                      #
#----------------------------------------------------------------------#

# === Configuration ===
today = datetime.date.today()
today_str = today.strftime("%d-%m-%Y")

base_name = "CIS_IDIC_MONTHLY_RPT"
base_txt_path = csv_output_path(base_name).replace(".csv", "")
output_txt = f"{base_txt_path}_{today_str}.txt"
output_parquet = parquet_output_path(base_name)

# === Step 1: Connect DuckDB and read input ===
con = duckdb.connect()

query = f"""
    SELECT 
        UPDOPER,
        CUSTNO,
        ACCTNOC,
        CUSTNAME,
        FIELDS,
        OLDVALUE,
        NEWVALUE,
        UPDDATX,
        {today.year} AS year,
        {today.month} AS month,
        {today.day} AS day
    FROM '{host_parquet_path("CIS_IDIC_DAILY_RALL.parquet")}'
    ORDER BY CUSTNO
"""

df = con.execute(query).fetch_df()

# === Step 2: Export to Parquet (Hive-style partition) ===
con.execute(f"""
    COPY ({query})
    TO '{output_parquet}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
""")

# === Step 3: Prepare headers for TXT ===
header_1 = (
    f"{'USER ID':<20}"
    f"{'CIS NO':<20}"
    f"{'ACCOUNT NO':<20}"
    f"{'CUSTOMER NAME':<40}"
    f"{'FIELD':<20}"
    f"{'OLD VALUE':<150}"
    f"{'NEW VALUE':<150}"
    f"{'UPDATE DATE':<10}"
)

header_2 = (
    f"{'-'*7:<20}"
    f"{'-'*6:<20}"
    f"{'-'*10:<20}"
    f"{'-'*13:<40}"
    f"{'-'*5:<20}"
    f"{'-'*9:<150}"
    f"{'-'*9:<150}"
    f"{'-'*11:<10}"
)

# === Step 4: Write fixed-length TXT output ===
with open(output_txt, "w", encoding="utf-8") as f:
    f.write("PROGRAM : CIHCMRPT\n")
    f.write(header_1 + "\n")
    f.write(header_2 + "\n")

    for _, row in df.iterrows():
        line = (
            f"{(row['UPDOPER'] or ''):<20}"
            f"{(row['CUSTNO'] or ''):<20}"
            f"{(row['ACCTNOC'] or ''):<20}"
            f"{(row['CUSTNAME'] or ''):<40}"
            f"{(row['FIELDS'] or ''):<20}"
            f"{(row['OLDVALUE'] or ''):<150}"
            f"{(row['NEWVALUE'] or ''):<150}"
            f"{(row['UPDDATX'] or ''):<10}"
        )
        f.write(line + "\n")

print(f"TXT file written: {output_txt}")
print(f"Parquet file written: {output_parquet}")
