# ====================================================================
# FUNCTION: get_hive_parquet_dp
# Pattern: /host/dp/parquet/Year = 2025/Month = 10/Day = 10/<file>.parquet
# ====================================================================
def get_hive_dp_parquet(base_folder: str, debug: bool = False):
    base_path = dp_parquet
    year_base_path = os.path.join(base_path)
    if not os.path.exists(year_base_path):
        raise FileNotFoundError(f"Base folder not found: {year_base_path}")

    # --- Find all available years ---
    years = []
    for y_folder in os.listdir(year_base_path):
        if y_folder.startswith("Year"):
            match = re.search(r"(\d+)", y_folder)
            if match:
                years.append(int(match.group(1)))
    if not years:
        raise FileNotFoundError(f"No Year folders under {year_base_path}")
    years.sort(reverse=True)  # newest first
    latest_year = years[0]

    year_path = os.path.join(year_base_path, f"Year = {latest_year}")
    if not os.path.exists(year_path):
        raise FileNotFoundError(f"'Year = {latest_year}' folder not found: {year_path}")

    # --- Find all available months ---
    months = []
    for m_folder in os.listdir(year_path):
        if m_folder.startswith("Month"):
            match = re.search(r"(\d+)", m_folder)
            if match:
                months.append(int(match.group(1)))
    if not months:
        raise FileNotFoundError(f"No Month folders under {year_path}")
    months.sort(reverse=True)  # newest first

    # --- Loop through months and days (newest first) ---
    for m in months:
        month_path = os.path.join(year_path, f"Month = {m}")
        if not os.path.exists(month_path):
            continue

        # collect all day folders under the month
        days = []
        for d_folder in os.listdir(month_path):
            if d_folder.startswith("Day"):
                match = re.search(r"(\d+)", d_folder)
                if match:
                    days.append(int(match.group(1)))
        if not days:
            continue
        days.sort(reverse=True)

        # Try to find the latest existing parquet file
        for d in days:
            day_path = os.path.join(month_path, f"Day = {d}")
            final_file = os.path.join(day_path, base_folder)
            if not final_file.endswith(".parquet"):
                final_file += ".parquet"

            if os.path.exists(final_file):
                if debug:
                    print(f"[DEBUG][DP] Found parquet: Year={latest_year}, Month={m}, Day={d}")
                    print(f"  -> {final_file}")
                return [final_file], latest_year, m, d

    # If no file found at all
    raise FileNotFoundError(f"No available parquet file found for {base_folder} under Year folders")
