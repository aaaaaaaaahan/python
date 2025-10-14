def get_hive_parquet_loan(base_folder: str, debug: bool = False) -> Tuple[List[str], int, int, int]:
    base_path = loan_parquet

    # --- find latest year ---
    years = []
    for y_folder in os.listdir(base_path):
        match = re.search(r"year=(\d+)", y_folder)
        if match:
            years.append(int(match.group(1)))
    if not years:
        raise FileNotFoundError(f"No year folders found under {base_path}")
    latest_year = max(years)
    year_path = os.path.join(base_path, f"year={latest_year}")

    # --- find latest month ---
    months = []
    for m_folder in os.listdir(year_path):
        match = re.search(r"month=(\d+)", m_folder)
        if match:
            months.append(int(match.group(1)))
    if not months:
        raise FileNotFoundError(f"No month folders under {year_path}")
    latest_month = max(months)
    month_path = os.path.join(year_path, f"month={latest_month}")

    # --- find latest day ---
    days = []
    for d_folder in os.listdir(month_path):
        match = re.search(r"day=(\d+)", d_folder)
        if match:
            days.append(int(match.group(1)))
    if not days:
        raise FileNotFoundError(f"No day folders under {month_path}")
    latest_day = max(days)
    day_path = os.path.join(month_path, f"day={latest_day}")

    final_path = os.path.join(day_path, base_folder)

    # --- collect parquet parts ---
    parquet_files = []
    for folder in os.listdir(final_path):
        if folder.endswith(".parquet"):
            part_path = os.path.join(final_path, folder)
            for f in os.listdir(part_path):
                if f.endswith(".parquet"):
                    parquet_files.append(os.path.join(part_path, f))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {final_path}")

    if debug:
        print(f"[DEBUG][LOAN] Latest Path: year={latest_year}, month={latest_month}, day={latest_day}")
        for p in parquet_files:
            print(f"  -> {p}")

    return parquet_files, latest_year, latest_month, latest_day
