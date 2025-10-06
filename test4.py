import os

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

    # --- Collect parquet files ---
    parquet_files = [
        os.path.join(day_path, f)
        for f in os.listdir(day_path)
        if f.endswith(".parquet")
    ]

    if not parquet_files:
        raise FileNotFoundError(f"❌ No parquet files found in {day_path}")

    # --- Pick latest parquet file (sorted by name) ---
    parquet_files.sort()
    latest_parquet = parquet_files[-1]

    print(f"✅ Latest parquet file found: {latest_parquet}")
    return latest_parquet, latest_year, latest_month, latest_day
