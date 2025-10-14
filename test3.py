def get_hive_parquet_dp_latest(base_folder: str, debug: bool = False):
    base_path = dp_parquet
    year_path = os.path.join(base_path, "Year")
    if not os.path.exists(year_path):
        raise FileNotFoundError(f"'Year' folder not found: {year_path}")

    # Find latest month
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

    # Find latest day
    days = []
    for d_folder in os.listdir(month_path):
        if d_folder.startswith("Day"):
            match = re.search(r"(\d+)", d_folder)
            if match:
                days.append(int(match.group(1)))
    if not days:
        # If no day folder exists in this month, fallback to previous month
        prev_months = [m for m in months if m < latest_month]
        if not prev_months:
            raise FileNotFoundError(f"No Day folders found in any month under {year_path}")
        latest_month = max(prev_months)
        month_path = os.path.join(year_path, f"Month = {latest_month}")
        days = [int(re.search(r"(\d+)", d).group(1)) for d in os.listdir(month_path) if d.startswith("Day")]
        if not days:
            raise FileNotFoundError(f"No Day folders found in fallback month={latest_month}")
    latest_day = max(days)

    day_path = os.path.join(month_path, f"Day = {latest_day}")
    final_file = os.path.join(day_path, base_folder)
    if not final_file.endswith(".parquet"):
        final_file += ".parquet"

    if not os.path.exists(final_file):
        raise FileNotFoundError(f"Parquet file not found: {final_file}")

    if debug:
        print(f"[DEBUG][DP] Latest available: Month={latest_month}, Day={latest_day}")
        print(f"  -> {final_file}")

    return [final_file], latest_month, latest_day
