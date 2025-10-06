def get_hive_parquet(base_folder: str):
    """
    Detect the latest Hive-style partition folder (year=YYYY/month=MM/day=DD)
    and return all .parquet files from that folder, along with year, month, day.
    Example:
        get_hive_parquet('hive') 
        -> (['/host/cis/parquet/hive/year=2025/month=10/day=05/data_0.parquet'], 2025, 10, 5)
    """
    # --- ensure base path exists ---
    base_path = os.path.join(python_hive, base_folder)
    if not os.path.exists(base_path):
        raise FileNotFoundError(f"❌ Base folder not found: {base_path}")

    # --- get latest year ---
    year_folders = [f for f in os.listdir(base_path) if f.startswith("year=")]
    if not year_folders:
        raise FileNotFoundError(f"❌ No year=YYYY folders under {base_path}")
    latest_year = max(int(f.split("=")[1]) for f in year_folders)
    year_path = os.path.join(base_path, f"year={latest_year}")
    print(f"✅ Latest year path: {year_path}")

    # --- get latest month ---
    month_folders = [f for f in os.listdir(year_path) if f.startswith("month=")]
    if not month_folders:
        raise FileNotFoundError(f"❌ No month=MM folders under {year_path}")
    latest_month = max(int(f.split("=")[1]) for f in month_folders)
    month_path = os.path.join(year_path, f"month={latest_month:02d}")
    print(f"✅ Latest month path: {month_path}")

    # --- get latest day ---
    day_folders = [f for f in os.listdir(month_path) if f.startswith("day=")]
    if not day_folders:
        raise FileNotFoundError(f"❌ No day=DD folders under {month_path}")
    latest_day = max(int(f.split("=")[1]) for f in day_folders)
    day_path = os.path.join(month_path, f"day={latest_day:02d}")
    print(f"✅ Latest day path: {day_path}")

    # --- collect parquet files ---
    parquet_files = [
        os.path.join(day_path, f)
        for f in os.listdir(day_path)
        if f.endswith(".parquet")
    ]

    if not parquet_files:
        raise FileNotFoundError(f"❌ No parquet files found in {day_path}")

    print(f"✅ Found {len(parquet_files)} parquet file(s):")
    for f in parquet_files:
        print("   -", f)

    return parquet_files, latest_year, latest_month, latest_day
