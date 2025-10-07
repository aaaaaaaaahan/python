
import os
import re

host_input = '/host/cis/parquet/sas_parquet'
python_hive = '/host/cis/parquet'
csv_output = '/host/cis/output'

#for input
##data from host
def host_parquet_path(filename: str) -> str:
    """
    If filename has no date, resolve to the latest dated file.
    Example:
      host_parquet_path("data_test.parquet") -> data_test_YYYYMMDD.parquet (latest)
      host_parquet_path("data_test1.parquet") -> data_test1_YYYYMMDD.parquet (latest)
      host_parquet_path("data_test_20250917.parquet") -> exact match
    """
    # exact match first
    full_path = os.path.join(host_input, filename)
    if os.path.exists(full_path):
        return full_path

    # try to resolve latest date
    base, ext = os.path.splitext(filename)
    pattern = re.compile(rf"^{base}_(\d{{8}}){ext}$")

    candidates = []
    for f in os.listdir(host_input):
        match = pattern.match(f)
        if match:
            candidates.append((match.group(1), f))

    if not candidates:
        raise FileNotFoundError(f"No file found for base {base} in {host_input}")

    # pick latest by date
    latest_file = max(candidates, key=lambda x: x[0])[1]
    return os.path.join(host_input, latest_file)

#old
#def host_parquet_path(filename: str) -> str:
#    return f"{host_input}/{filename}"

##data from python hive-partition
def python_input_path(filename: str) -> str:
    return f"{python_hive}/{filename}"


#for output
##save as parquet hive-partition
def parquet_output_path(name: str) -> str:
    return f"{python_hive}/{name}"

##save as csv
def csv_output_path(name: str) -> str:
    return f"{csv_output}/{name}.csv"


def hive_latest_path(table: str) -> str:
    """
    Find the latest partition folder for a hive-partitioned table.
    Partition format: year=YYYY/month=MM/day=DD/data_*.parquet

    Returns full path to the folder containing parquet files.
    Example:
      hive_latest_path("accounts")
        -> /host/cis/parquet/accounts/year=2025/month=09/day=26
    """
    table_path = os.path.join(python_hive, table)
    if not os.path.exists(table_path):
        raise FileNotFoundError(f"Table folder not found: {table_path}")

    # --- find latest year ---
    years = []
    for f in os.listdir(table_path):
        if f.startswith("year="):
            try:
                years.append(int(f.split("=")[1]))
            except ValueError:
                continue
    if not years:
        raise FileNotFoundError(f"No year=YYYY partitions in {table_path}")
    latest_year = max(years)
    year_path = os.path.join(table_path, f"year={latest_year}")

    # --- find latest month ---
    months = []
    for f in os.listdir(year_path):
        if f.startswith("month="):
            try:
                months.append(int(f.split("=")[1]))
            except ValueError:
                continue
    if not months:
        raise FileNotFoundError(f"No month=MM partitions in {year_path}")
    latest_month = max(months)
    month_path = os.path.join(year_path, f"month={latest_month:02d}")

    # --- find latest day ---
    days = []
    for f in os.listdir(month_path):
        if f.startswith("day="):
            try:
                days.append(int(f.split("=")[1]))
            except ValueError:
                continue
    if not days:
        raise FileNotFoundError(f"No day=DD partitions in {month_path}")
    latest_day = max(days)
    day_path = os.path.join(month_path, f"day={latest_day:02d}")

    return day_path

# ============================================================
# FUNCTION: get_latest_hive_parquet_path
# ============================================================
def get_hive_parquet(base_folder: str):
    """
    Detect the latest Hive-style partition folder (year=YYYY/month=MM/day=DD)
    and return the latest .parquet file path along with year, month, day.

    Example:
        get_hive_parquet('hive')
        -> ('/host/cis/parquet/hive/year=2025/month=10/day=05/data_2.parquet', 2025, 10, 5)
    """

    python_hive = '/host/cis/parquet'  # ✅ Make sure this is correct and exists
    base_path = os.path.join(python_hive, base_folder)

    if not os.path.exists(base_path):
        raise FileNotFoundError(f"❌ Base folder not found: {base_path}")

    # --- Find latest year ---
    years = [f for f in os.listdir(base_path) if f.startswith("year=")]
    if not years:
        raise FileNotFoundError(f"❌ No year=YYYY folders under {base_path}")
    latest_year = max(int(f.split("=")[1]) for f in years)
    year_path = os.path.join(base_path, f"year={latest_year}")
    print(f"✅ Latest year path: {year_path}")

    # --- Find latest month ---
    months = [f for f in os.listdir(year_path) if f.startswith("month=")]
    if not months:
        raise FileNotFoundError(f"❌ No month=MM folders under {year_path}")
    latest_month = max(int(f.split("=")[1]) for f in months)
    month_path = os.path.join(year_path, f"month={latest_month:02d}")
    print(f"✅ Latest month path: {month_path}")

    # --- Find latest day ---
    days = [f for f in os.listdir(month_path) if f.startswith("day=")]
    if not days:
        raise FileNotFoundError(f"❌ No day=DD folders under {month_path}")
    latest_day = max(int(f.split("=")[1]) for f in days)
    day_path = os.path.join(month_path, f"day={latest_day:02d}")
    print(f"✅ Latest day path: {day_path}")

    # --- Collect parquet files (look recursively if needed) ---
    parquet_files = []
    for root, _, files in os.walk(day_path):
        for f in files:
            if f.endswith(".parquet"):
                parquet_files.append(os.path.join(root, f))

    if not parquet_files:
        raise FileNotFoundError(f"❌ No parquet files found in {day_path} or its subfolders")

    # --- Pick latest parquet file (sorted by name) ---
    parquet_files.sort()
    latest_parquet = parquet_files[-1]

    print(f"✅ Latest parquet file found: {latest_parquet}")
    return latest_parquet, latest_year, latest_month, latest_day
