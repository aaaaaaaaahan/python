import os
import re

def get_latest_hive_parquet(base_dir: str) -> str:
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
