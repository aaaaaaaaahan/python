# ====================================================================
# FUNCTION: get_hive_parquet_dp
# Pattern: /host/dp/parquet/year=2025/month=10/day=10/<file>.parquet
# ====================================================================
def get_hive_dp_parquet(base_folder: str, debug: bool = False):
    base_path = dp_parquet
    year_base_path = os.path.join(base_path)
    if not os.path.exists(year_base_path):
        raise FileNotFoundError(f"Base folder not found: {year_base_path}")

    # --- Find all available years (case-insensitive, supports = or space) ---
    years = []
    for y_folder in os.listdir(year_base_path):
        folder_lower = y_folder.lower().strip()
        if folder_lower.startswith("year"):
            match = re.search(r"(\d+)", folder_lower)
            if match:
                years.append(int(match.group(1)))
    if not years:
        raise FileNotFoundError(
            f"No 'year=' folders found under {year_base_path}. Found: {os.listdir(year_base_path)}"
        )

    years.sort(reverse=True)
    latest_year = years[0]
    year_path = os.path.join(year_base_path, f"year={latest_year}")
    if not os.path.exists(year_path):
        year_path = os.path.join(year_base_path, f"year = {latest_year}")
        if not os.path.exists(year_path):
            # try capitalized style
            year_path = os.path.join(year_base_path, f"Year={latest_year}")
            if not os.path.exists(year_path):
                year_path = os.path.join(year_base_path, f"Year = {latest_year}")
                if not os.path.exists(year_path):
                    raise FileNotFoundError(f"No valid year folder found for {latest_year}")

    if debug:
        print(f"[DEBUG] Using Year folder: {year_path}")

    # --- Find all available months ---
    months = []
    for m_folder in os.listdir(year_path):
        folder_lower = m_folder.lower().strip()
        if folder_lower.startswith("month"):
            match = re.search(r"(\d+)", folder_lower)
            if match:
                months.append(int(match.group(1)))
    if not months:
        raise FileNotFoundError(f"No 'month=' folders under {year_path}")
    months.sort(reverse=True)

    # --- Loop through months and days ---
    for m in months:
        month_path = os.path.join(year_path, f"month={m}")
        if not os.path.exists(month_path):
            alt_month_path = os.path.join(year_path, f"month = {m}")
            if not os.path.exists(alt_month_path):
                alt_month_path = os.path.join(year_path, f"Month={m}")
            month_path = alt_month_path if os.path.exists(alt_month_path) else month_path

        if not os.path.exists(month_path):
            continue

        days = []
        for d_folder in os.listdir(month_path):
            folder_lower = d_folder.lower().strip()
            if folder_lower.startswith("day"):
                match = re.search(r"(\d+)", folder_lower)
                if match:
                    days.append(int(match.group(1)))
        if not days:
            continue
        days.sort(reverse=True)

        for d in days:
            day_path = os.path.join(month_path, f"day={d}")
            if not os.path.exists(day_path):
                alt_day_path = os.path.join(month_path, f"day = {d}")
                if not os.path.exists(alt_day_path):
                    alt_day_path = os.path.join(month_path, f"Day={d}")
                day_path = alt_day_path if os.path.exists(alt_day_path) else day_path

            if not os.path.exists(day_path):
                continue

            final_file = os.path.join(day_path, base_folder)
            if not final_file.endswith(".parquet"):
                final_file += ".parquet"

            if os.path.exists(final_file):
                if debug:
                    print(f"[DEBUG][DP] Found parquet: Year={latest_year}, Month={m}, Day={d}")
                    print(f"  -> {final_file}")
                return [final_file], latest_year, m, d

    raise FileNotFoundError(
        f"No available parquet file found for {base_folder} under {year_base_path}"
    )
