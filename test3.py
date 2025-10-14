import os
import re
from typing import List, Tuple

# ====================================================================
# FUNCTION 1: get_hive_parquet_loan
# Pattern: /host/loan/parquet/year=2025/month=10/day=01/<file>.parquet/part.N.parquet
# ====================================================================
def get_hive_parquet_loan(base_folder: str, debug: bool = False) -> Tuple[List[str], int, int, int]:
    base_path = os.path.join("/host/loan/parquet", base_folder)

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

    # --- collect parquet parts ---
    parquet_files = []
    for folder in os.listdir(day_path):
        if folder.endswith(".parquet"):
            part_path = os.path.join(day_path, folder)
            for f in os.listdir(part_path):
                if f.endswith(".parquet"):
                    parquet_files.append(os.path.join(part_path, f))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {day_path}")

    if debug:
        print(f"[DEBUG][LOAN] Latest Path: year={latest_year}, month={latest_month}, day={latest_day}")
        for p in parquet_files:
            print(f"  -> {p}")

    return parquet_files, latest_year, latest_month, latest_day


# ====================================================================
# FUNCTION 2: get_hive_parquet_dp
# Pattern: /host/dp/parquet/Year/Month = 10/Day = 10/<file>.parquet
# ====================================================================
def get_hive_parquet_dp(base_folder: str, debug: bool = False) -> Tuple[List[str], str, int, int]:
    base_path = os.path.join("/host/dp/parquet", base_folder)
    year_folder = os.path.join(base_path, "Year")

    if not os.path.exists(year_folder):
        raise FileNotFoundError(f"'Year' folder not found under {base_path}")

    # --- find latest Month ---
    months = []
    for m_folder in os.listdir(year_folder):
        match = re.search(r"(\d+)", m_folder)
        if match:
            months.append(int(match.group(1)))
    if not months:
        raise FileNotFoundError(f"No Month folders under {year_folder}")
    latest_month = max(months)
    month_folder = f"Month = {latest_month}"
    month_path = os.path.join(year_folder, month_folder)

    # --- find latest Day ---
    days = []
    for d_folder in os.listdir(month_path):
        match = re.search(r"(\d+)", d_folder)
        if match:
            days.append(int(match.group(1)))
    if not days:
        raise FileNotFoundError(f"No Day folders under {month_path}")
    latest_day = max(days)
    day_folder = f"Day = {latest_day}"
    day_path = os.path.join(month_path, day_folder)

    # --- collect parquet files ---
    parquet_files = [
        os.path.join(day_path, f)
        for f in os.listdir(day_path)
        if f.endswith(".parquet")
    ]
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {day_path}")

    if debug:
        print(f"[DEBUG][DP] Latest Path: Year='Year', Month={latest_month}, Day={latest_day}")
        for p in parquet_files:
            print(f"  -> {p}")

    return parquet_files, "Year", latest_month, latest_day
