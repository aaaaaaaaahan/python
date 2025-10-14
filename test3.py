def get_hive_parquet_dp(base_folder: str, debug: bool = False):
    """
    Pattern:
      /host/dp/parquet/Year/Month = 10/Day = 10/<file>.parquet
    """
    base_path = dp_parquet.rsplit('/', 1)[0]  # ✅ go up one level (/host/dp/parquet)

    year_path = os.path.join(base_path, "Year")
    if not os.path.exists(year_path):
        raise FileNotFoundError(f"'Year' folder not found: {year_path}")

    # --- find latest Month ---
    months = []
    for m_folder in os.listdir(year_path):
        if m_folder.startswith("Month"):
            match = re.search(r"(\d+)", m_folder)
            if match:
                months.append(int(match.group(1)))
    if not months:
        raise FileNotFoundError(f"No Month folders under {year_path}")
    latest_month = max(months)
    month_path = os.path.join(year_path, f"Month = {latest_month}")

    # --- find latest Day ---
    days = []
    for d_folder in os.listdir(month_path):
        if d_folder.startswith("Day"):
            match = re.search(r"(\d+)", d_folder)
            if match:
                days.append(int(match.group(1)))
    if not days:
        raise FileNotFoundError(f"No Day folders under {month_path}")
    latest_day = max(days)
    day_path = os.path.join(month_path, f"Day = {latest_day}")

    # --- collect parquet files ---
    parquet_files = [
        os.path.join(day_path, f)
        for f in os.listdir(day_path)
        if f.endswith(".parquet")
    ]

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {day_path}")

    if debug:
        print(f"[DEBUG][DP] Latest Path: Month={latest_month}, Day={latest_day}")
        for p in parquet_files:
            print(f"  -> {p}")

    return parquet_files, latest_month, latest_day
