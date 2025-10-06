import os

def get_hive_parquet(base_folder: str) -> tuple[list[str], int, int, int]:
    """
    Automatically detect the latest Hive-style partition folder
    (year=YYYY/month=MM/day=DD) and return a list of parquet file paths
    along with year/month/day.

    Example return:
      (["/host/cis/parquet/accounts/year=2025/month=09/day=26/data_001.parquet"], 2025, 9, 26)
    """
    base_path = os.path.join('/host/cis/parquet', base_folder)

    if not os.path.exists(base_path):
        raise FileNotFoundError(f"Base folder not found: {base_path}")

    # Find all year= folders
    years = []
    for y_folder in os.listdir(base_path):
        if y_folder.startswith("year="):
            try:
                years.append(int(y_folder.split("=")[1]))
            except ValueError:
                continue
    if not years:
        raise FileNotFoundError(f"No year=YYYY partitions found under {base_path}")
    latest_year = max(years)
    year_path = os.path.join(base_path, f"year={latest_year}")

    # Find all month= folders
    months = []
    for m_folder in os.listdir(year_path):
        if m_folder.startswith("month="):
            try:
                months.append(int(m_folder.split("=")[1]))
            except ValueError:
                continue
    if not months:
        raise FileNotFoundError(f"No month=MM partitions found under {year_path}")
    latest_month = max(months)
    month_path = os.path.join(year_path, f"month={latest_month:02d}")

    # Find all day= folders
    days = []
    for d_folder in os.listdir(month_path):
        if d_folder.startswith("day="):
            try:
                days.append(int(d_folder.split("=")[1]))
            except ValueError:
                continue
    if not days:
        raise FileNotFoundError(f"No day=DD partitions found under {month_path}")
    latest_day = max(days)
    day_path = os.path.join(month_path, f"day={latest_day:02d}")

    # Collect parquet files
    parquet_files = []
    for f in os.listdir(day_path):
        if f.endswith(".parquet"):
            parquet_files.append(os.path.join(day_path, f))

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {day_path}")

    return parquet_files, latest_year, latest_month, latest_day
