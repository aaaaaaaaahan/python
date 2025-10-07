    # --- Collect parquet files (look recursively if needed) ---
    print(f"🔍 Scanning for parquet files under: {day_path}")
    parquet_files = []
    for root, _, files in os.walk(day_path):
        print(f"📁 Checking {root} with {len(files)} files")
        for f in files:
            print(f"  └─ {f}")
            if f.lower().endswith(".parquet"):
                parquet_files.append(os.path.join(root, f))

    if not parquet_files:
        raise FileNotFoundError(f"❌ No parquet files found in {day_path} or its subfolders")

    # --- Pick latest parquet file (sorted by name) ---
    parquet_files.sort()
    latest_parquet = parquet_files[-1]
    print(f"✅ Latest parquet file found: {latest_parquet}")
    return latest_parquet, latest_year, latest_month, latest_day
