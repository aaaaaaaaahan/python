# ============================================================
# FUNCTION: Get Current and Previous generation parquet 
# ============================================================
def get_prev_latest_parquet(dataset_name: str, debug: bool = False) -> Tuple[str, str]:
    """
    Simulate SAS GDG behavior for Hive-style parquet folders.
    Example: CIS.SDB.MATCH.FULL -> finds (-1) and (0)
    Returns (previous_path, latest_path)
    """
    base_path = os.path.join(python_hive, dataset_name)

    if not os.path.exists(base_path):
        raise FileNotFoundError(f"Base parquet path not found: {base_path}")

    dated_folders = []

    # Walk through Hive structure: year=YYYY/month=MM/day=DD/
    for root, dirs, files in os.walk(base_path):
        parquet_files = [os.path.join(root, f) for f in files if f.endswith(".parquet")]
        if not parquet_files:
            continue

        # Extract year, month, day from folder path
        match = re.search(r"year=(\d+).*month=(\d+).*day=(\d+)", root.replace("\\", "/"))
        if not match:
            continue

        try:
            y, m, d = map(int, match.groups())
            date_val = datetime(y, m, d)
            # Always pick the first parquet file found for that date
            dated_folders.append((date_val, parquet_files[0]))
        except ValueError:
            continue  # skip invalid folder names

    if not dated_folders:
        raise FileNotFoundError(f"No parquet files found under {base_path}")

    # Sort by date (latest first)
    dated_folders.sort(key=lambda x: x[0], reverse=True)

    if len(dated_folders) < 2:
        raise ValueError(f"Not enough generations found for {dataset_name} (need at least 2 dated folders).")

    # Select latest (0) and previous (-1)
    latest = dated_folders[0][1]
    previous = dated_folders[1][1]

    if debug:
        print(f"[DEBUG][GDG] Base Folder: {base_path}")
        print(f"[DEBUG][GDG] Latest (0): {latest}")
        print(f"[DEBUG][GDG] Previous (-1): {previous}")

    return previous, latest
