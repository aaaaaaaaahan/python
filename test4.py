import duckdb
from CIS_PY_READER_JKH import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet
import datetime
import os

# ==================================
# DEFINE DATE VARIABLES
# ==================================
batch_date = (datetime.date.today() - datetime.timedelta(days=1))
batch_year, batch_month, batch_day = batch_date.year, batch_date.month, batch_date.day

# ==================================
# CONNECT TO DUCKDB
# ==================================
con = duckdb.connect()

# ==================================
# GET INPUT PARQUET
# ==================================
CIS, year, month, day = get_hive_parquet('AMLHRC_EXTRACT_MASSCLS')

# ====================================
# STEP 1 - Load and filter CUST File
# ====================================
con.execute(f"""
    CREATE OR REPLACE TABLE CIS AS
    SELECT *
    FROM read_parquet('{CIS}')
""")

# ==================================
# STEP 4 - PRINT SAMPLE (OBS=5)
# ==================================
out1 = """
    SELECT *
    FROM CIS
"""

queries = {
    "test_jkh": out1
}

# ==================================
# STEP 5 - EXPORT TO HIVE-STYLE FOLDER (without date columns)
# ==================================
for name, query in queries.items():
    # build hive-style output path manually
    hive_folder = f"/host/cis/parquet/{name}/year={batch_year}/month={batch_month:02d}/day={batch_day:02d}"
    os.makedirs(hive_folder, exist_ok=True)

    parquet_path = os.path.join(hive_folder, "data_0.parquet")
    csv_path = csv_output_path(name)

    # write parquet (no date columns inside)
    con.execute(f"""
        COPY (
            {query}
        )
        TO '{parquet_path}'
        (FORMAT PARQUET, OVERWRITE_OR_IGNORE true);
    """)

    # also export CSV if needed
    con.execute(f"""
        COPY (
            {query}
        )
        TO '{csv_path}'
        (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);
    """)

print(f"✅ Output parquet saved to: {hive_folder}")
