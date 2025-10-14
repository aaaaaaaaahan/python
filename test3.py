import os
import re
from typing import List, Tuple

def get_hive_parquet_loan(base_folder: str, debug: bool = False) -> Tuple[List[str], int, int, int]:
    base_path = loan_parquet

    # --- find all available years ---
    years = []
    for y_folder in os.listdir(base_path):
        match = re.search(r"year=(\d+)", y_folder)
        if match:
            years.append(int(match.group(1)))
    if not years:
        raise FileNotFoundError(f"No year folders found under {base_path}")
    years.sort(reverse=True)

    # --- search by newest -> oldest ---
    for year in years:
        year_path = os.path.join(base_path, f"year={year}")
        if not os.path.exists(year_path):
            continue

        # --- find months ---
        months = []
        for m_folder in os.listdir(year_path):
            match = re.search(r"month=(\d+)", m_folder)
            if match:
                months.append(int(match.group(1)))
        if not months:
            continue
        months.sort(reverse=True)

        for month in months:
            month_path = os.path.join(year_path, f"month={month}")
            if not os.path.exists(month_path):
                continue

            # --- find days ---
            days = []
            for d_folder in os.listdir(month_path):
                match = re.search(r"day=(\d+)", d_folder)
                if match:
                    days.append(int(match.group(1)))
            if not days:
                continue
            days.sort(reverse=True)

            # --- loop through days (latest first) ---
            for day in days:
                day_path = os.path.join(month_path, f"day={day}")
                final_path = os.path.join(day_path, base_folder)

                if not os.path.exists(final_path):
                    continue

                parquet_files = []
                for folder in os.listdir(final_path):
                    if folder.endswith(".parquet"):
                        part_path = os.path.join(final_path, folder)
                        if os.path.isdir(part_path):
                            for f in os.listdir(part_path):
                                if f.endswith(".parquet"):
                                    parquet_files.append(os.path.join(part_path, f))
                        else:
                            parquet_files.append(part_path)

                if parquet_files:
                    if debug:
                        print(f"[DEBUG][LOAN] Found parquet for year={year}, month={month}, day={day}")
                        for p in parquet_files:
                            print(f"  -> {p}")
                    return parquet_files, year, month, day

    raise FileNotFoundError(f"No available parquet files found under {base_path} for {base_folder}")
