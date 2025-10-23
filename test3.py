def get_hive_dp_parquet(base_folder: str, debug: bool = False):
    """
    Pattern:
    /host/dp/parquet/year=YYYY/month=MM/day=DD/<file>.parquet

    Returns:
        [file_path], year, month, day
    """
    base_path = dp_parquet  # e.g. /host/dp/parquet

    # --- Find all available years ---
    years = []
    for y_folder in os.listdir(base_path):
        if y_folder.startswith("year="):
            match = re.search(r"year=(\d+)", y_folder)
            if match:
                years.append(int(match.group(1)))
    if not years:
        raise FileNotFoundError(f"No 'year=' folders under {base_path}")
    years.sort(reverse=True)  # newest first

    # --- Loop through years, months, and days ---
    for y in years:
        year_path = os.path.join(base_path, f"year={y}")
        if not os.path.exists(year_path):
            continue

        # --- Find all available months ---
        months = []
        for m_folder in os.listdir(year_path):
            if m_folder.startswith("month="):
                match = re.search(r"month=(\d+)", m_folder)
                if match:
                    months.append(int(match.group(1)))
        if not months:
            continue
        months.sort(reverse=True)

        # --- Loop through months ---
        for m in months:
            month_path = os.path.join(year_path, f"month={m}")
            if not os.path.exists(month_path):
                continue

            # --- Find all available days ---
            days = []
            for d_folder in os.listdir(month_path):
                if d_folder.startswith("day="):
                    match = re.search(r"day=(\d+)", d_folder)
                    if match:
                        days.append(int(match.group(1)))
            if not days:
                continue
            days.sort(reverse=True)

            # --- Find the latest parquet ---
            for d in days:
                day_path = os.path.join(month_path, f"day={d}")
                final_file = os.path.join(day_path, base_folder)
                if not final_file.endswith(".parquet"):
                    final_file += ".parquet"

                if os.path.exists(final_file):
                    if debug:
                        print(f"[DEBUG][DP] Found parquet: year={y}, month={m}, day={d}")
                        print(f"  -> {final_file}")
                    return [final_file], y, m, d

    # --- No file found ---
    raise FileNotFoundError(f"No available parquet file found for {base_folder}")
