# ====================================================================
# FUNCTION: get_hive_parquet_dp
# Pattern: /host/dp/parquet/Year = 2025/Month = 10/Day = 10/<file>.parquet
# ====================================================================
def get_hive_dp_parquet(base_folder: str, debug: bool = False):
    base_path = dp_parquet  # must be defined globally
    year_base_path = os.path.join(base_path)
    if not os.path.exists(year_base_path):
        raise FileNotFoundError(f"Base folder not found: {year_base_path}")

    # --- Find all available years ---
    years = []
    for y_folder in os.listdir(year_base_path):
        if y_folder.strip().startswith("Year"):
            match = re.search(r"(\d+)", y_folder)
            if match:
                years.append(int(match.group(1)))
    if not years:
        raise FileNotFoundError(f"No 'Year =' folders found under {year_base_path}. Found: {os.listdir(year_base_path)}")

    years.sort(reverse=True)  # newest first
    latest_year = years[0]
    year_path = os.path.join(year_base_path, f"Year = {latest_year}")

    if not os.path.exists(year_path):
        # try without spaces in case folder named "Year=2025"
        alt_year_path = os.path.join(year_base_path, f"Year={latest_year}")
        if os.path.exists(alt_year_path):
            year_path = alt_year_path
        else:
            raise FileNotFoundError(f"'Year = {latest_year}' folder not found (checked both styles).")

    if debug:
        print(f"[DEBUG] Using Year folder: {year_path}")

    # --- Find all available months ---
    months = []
    for m_folder in os.listdir(year_path):
        if m_folder.strip().startswith("Month"):
            match = re.search(r"(\d+)", m_folder)
            if match:
                months.append(int(match.group(1)))
    if not months:
        raise FileNotFoundError(f"No 'Month =' folders found under {year_path}")
    months.sort(reverse=True)

    # --- Loop through months and days ---
    for m in months:
        month_path = os.path.join(year_path, f"Month = {m}")
        if not os.path.exists(month_path):
            alt_month_path = os.path.join(year_path, f"Month={m}")
            if os.path.exists(alt_month_path):
                month_path = alt_month_path
            else:
                continue

        days = []
        for d_folder in os.listdir(month_path):
            if d_folder.strip().startswith("Day"):
                match = re.search(r"(\d+)", d_folder)
                if match:
                    days.append(int(match.group(1)))
        if not days:
            continue
        days.sort(reverse=True)

        for d in days:
            day_path = os.path.join(month_path, f"Day = {d}")
            if not os.path.exists(day_path):
                alt_day_path = os.path.join(month_path, f"Day={d}")
                if os.path.exists(alt_day_path):
                    day_path = alt_day_path
                else:
                    continue

            final_file = os.path.join(day_path, base_folder)
            if not final_file.endswith(".parquet"):
                final_file += ".parquet"

            if os.path.exists(final_file):
                if debug:
                    print(f"[DEBUG][DP] Found parquet: Year={latest_year}, Month={m}, Day={d}")
                    print(f"  -> {final_file}")
                return [final_file], latest_year, m, d

    raise FileNotFoundError(f"No available parquet file found for {base_folder} under any Year folder.")
