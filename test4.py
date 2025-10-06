# --- Collect parquet files (look recursively if needed) ---
parquet_files = []
for root, _, files in os.walk(day_path):
    for f in files:
        if f.endswith(".parquet"):
            parquet_files.append(os.path.join(root, f))

if not parquet_files:
    raise FileNotFoundError(f"❌ No parquet files found in {day_path} or its subfolders")
