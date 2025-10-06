
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
def get_hive_parquet(base_dir: str) -> str:
    """
    Find the latest parquet file under:
    base_dir/year=YYYY/month=MM/day=DD/
    Example:
      /host/cis/parquet/hive/year=2025/month=10/day=05/data_2.parquet
    """
    if not os.path.exists(base_dir):
        raise FileNotFoundError(f"Base directory not found: {base_dir}")

    # Step 1: Find latest year
    years = [d for d in os.listdir(base_dir) if d.startswith("year=")]
    if not years:
        raise FileNotFoundError("No 'year=' folders found.")
    latest_year = max(years, key=lambda y: int(y.split('=')[1]))

    # Step 2: Find latest month
    year_path = os.path.join(base_dir, latest_year)
    months = [d for d in os.listdir(year_path) if d.startswith("month=")]
    if not months:
        raise FileNotFoundError("No 'month=' folders found.")
    latest_month = max(months, key=lambda m: int(m.split('=')[1]))

    # Step 3: Find latest day
    month_path = os.path.join(year_path, latest_month)
    days = [d for d in os.listdir(month_path) if d.startswith("day=")]
    if not days:
        raise FileNotFoundError("No 'day=' folders found.")
    latest_day = max(days, key=lambda d: int(d.split('=')[1]))

    # Step 4: Find parquet files inside the latest day folder
    day_path = os.path.join(month_path, latest_day)
    parquet_files = [f for f in os.listdir(day_path) if f.endswith(".parquet")]
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {day_path}")

    # Step 5: Get latest parquet file (by number suffix)
    def extract_num(filename):
        match = re.search(r"_(\d+)\.parquet$", filename)
        return int(match.group(1)) if match else -1

    latest_parquet = max(parquet_files, key=extract_num)
    parquet_path = os.path.join(day_path, latest_parquet)

    return parquet_path
