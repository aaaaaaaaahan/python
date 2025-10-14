def get_hive_parquet_dp(base_folder: str, debug: bool = False):
    base_path = dp_parquet

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

    # The final parquet file is directly here, not a folder
    final_file = os.path.join(day_path, base_folder)
    if not final_file.endswith(".parquet"):
        final_file += ".parquet"

    if not os.path.exists(final_file):
        raise FileNotFoundError(f"Parquet file not found: {final_file}")

    parquet_files = [final_file]

    if debug:
        print(f"[DEBUG][DP] Latest Path: Month={latest_month}, Day={latest_day}")
        for p in parquet_files:
            print(f"  -> {p}")

    return parquet_files, latest_month, latest_day
