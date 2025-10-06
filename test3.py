
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
def get_hive_parquet(base_folder: str) -> tuple[str, int, int, int]:
    """
    Automatically detect the latest Hive-style partition folder
    (year=YYYY/month=MM/day=DD) and return its parquet path + year/month/day.
    """
    base_path = Path(host_parquet_path(base_folder))
    partitions = list(base_path.glob("year=*/month=*/day=*"))

    if not partitions:
        raise FileNotFoundError(f"No partition folders found under {base_path}")

    latest_partition = max(
        partitions,
        key=lambda p: (
            int(p.parts[-3].split("=")[1]),  # year
            int(p.parts[-2].split("=")[1]),  # month
            int(p.parts[-1].split("=")[1])   # day
        )
    )

    year = int(latest_partition.parts[-3].split("=")[1])
    month = int(latest_partition.parts[-2].split("=")[1])
    day = int(latest_partition.parts[-1].split("=")[1])
    parquet_path = str(latest_partition / "*.parquet")
