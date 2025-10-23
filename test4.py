import os
import re
from datetime import datetime
from typing import List, Tuple, Union

# ============================================================
# PATH CONFIGURATION
# ============================================================
host_input = '/host/cis/parquet/sas_parquet'
python_hive = '/host/cis/parquet'
csv_output = '/host/cis/output'
dp_parquet = '/host/dp/parquet'
loan_parquet = '/host/loan/parquet'

# ============================================================
# FUNCTION: host_parquet_path
# ============================================================
def host_parquet_path(filename: str) -> str:
    """
    If filename has no date, resolve to the latest dated file.
    Example:
      host_parquet_path("data_test.parquet") -> data_test_YYYYMMDD.parquet (latest)
      host_parquet_path("data_test1.parquet") -> data_test1_YYYYMMDD.parquet (latest)
      host_parquet_path("data_test_20250917.parquet") -> exact match
    """
    # exact match first
    full_path = os.path.join(host_input, filename)
    if os.path.exists(full_path):
        return full_path

    # try to resolve latest date
    base, ext = os.path.splitext(filename)
    pattern = re.compile(rf"^{re.escape(base)}_(\d{{8}}){re.escape(ext)}$")

    candidates = []
    for f in os.listdir(host_input):
        match = pattern.match(f)
        if match:
            candidates.append((match.group(1), f))

    if not candidates:
        raise FileNotFoundError(f"No file found for base '{base}' in {host_input}")

    # pick latest by date
    latest_file = max(candidates, key=lambda x: x[0])[1]
    return os.path.join(host_input, latest_file)


# ============================================================
# FUNCTION: python_input_path
# ============================================================
def python_input_path(filename: str) -> str:
    return f"{python_hive}/{filename}"


# ============================================================
# FUNCTION: parquet_output_path
# ============================================================
def parquet_output_path(name: str) -> str:
    return f"{python_hive}/{name}"


# ============================================================
# FUNCTION: csv_output_path
# ============================================================
def csv_output_path(name: str) -> str:
    return f"{csv_output}/{name}.csv"


# ============================================================
# FUNCTION: hive_latest_path
# ============================================================
def hive_latest_path(table: str, debug: bool = False) -> str:
    """
    Find the latest partition folder for a Hive-partitioned table.
    Partition format: year=YYYY/month=MM/day=DD/data_*.parquet

    Returns full path to the folder containing parquet files.
    Example:
      hive_latest_path("accounts")
        -> /host/cis/parquet/accounts/year=2025/month=09/day=26
    """
    table_path = os.path.join(python_hive, table)
    if not os.path.exists(table_path):
        raise FileNotFoundError(f"Table folder not found: {table_path}")

    # --- find latest year ---
    years = []
    for f in os.listdir(table_path):
        if f.startswith("year="):
            try:
                years.append(int(f.split("=")[1]))
            except ValueError:
                continue
    if not years:
        raise FileNotFoundError(f"No year=YYYY partitions in {table_path}")
    latest_year = max(years)
    year_path = os.path.join(table_path, f"year={latest_year}")

    # --- find latest month ---
    months = []
    for f in os.listdir(year_path):
        if f.startswith("month="):
            try:
                months.append(int(f.split("=")[1]))
            except ValueError:
                continue
    if not months:
        raise FileNotFoundError(f"No month=MM partitions in {year_path}")
    latest_month = max(months)

    # Handle possible zero-padding mismatch
    month_folder = f"month={latest_month:02d}"
    if not os.path.exists(os.path.join(year_path, month_folder)):
        month_folder = f"month={latest_month}"
    month_path = os.path.join(year_path, month_folder)

    # --- find latest day ---
    days = []
    for f in os.listdir(month_path):
        if f.startswith("day="):
            try:
                days.append(int(f.split("=")[1]))
            except ValueError:
                continue
    if not days:
        raise FileNotFoundError(f"No day=DD partitions in {month_path}")
    latest_day = max(days)

    # Handle possible zero-padding mismatch
    day_folder = f"day={latest_day:02d}"
    if not os.path.exists(os.path.join(month_path, day_folder)):
        day_folder = f"day={latest_day}"
    day_path = os.path.join(month_path, day_folder)

    if debug:
        print(f"[DEBUG] Latest Hive Path: year={latest_year}, month={latest_month}, day={latest_day}")
        print(f"[DEBUG] Full Path: {day_path}")

    return day_path

# ====================================================================
# FUNCTION: get_hive_parquet_loan
# Pattern: /host/loan/parquet/year=2025/month=10/day=01/<file>.parquet/part.N.parquet
# ====================================================================
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


# ====================================================================
# FUNCTION: get_hive_parquet_dp
# Pattern: /host/dp/parquet/Year/Month = 10/Day = 10/<file>.parquet
# ====================================================================
def get_hive_dp_parquet(base_folder: str, debug: bool = False):
    base_path = dp_parquet
    year_path = os.path.join(base_path, "Year")
    if not os.path.exists(year_path):
        raise FileNotFoundError(f"'Year' folder not found: {year_path}")

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
                    print(f"[DEBUG][DP] Found parquet: Month={m}, Day={d}")
                    print(f"  -> {final_file}")
                return [final_file], m, d

    # If no file found at all
    raise FileNotFoundError(f"No available parquet file found for {base_folder}")

# ============================================================
# FUNCTION: Get Current and Previous generation parquet 
# ============================================================
def get_hive_parquet(
    dataset_name: str,
    generations: Union[int, List[int], str] = 9,
    debug: bool = False
) -> List[str]:
    """
    Enhanced GDG-like parquet retriever.
    Supports retrieving flexible generations (latest, previous, etc.)
    
    Parameters:
    - dataset_name: Base folder name (Hive-style)
    - generations:
        * int  -> e.g. 2 means latest + (2-1) previous generations
        * list[int] -> specific generation indices, e.g. [0, -1, -2]
        * 'all' -> return all generations
    - debug: show debug print
    
    Returns:
        List of parquet file paths (ordered from newest to oldest)
    """
    base_path = os.path.join(python_hive, dataset_name)

    if not os.path.exists(base_path):
        raise FileNotFoundError(f"Base parquet path not found: {base_path}")

    dated_folders = []

    # Walk through Hive-style folders
    for root, dirs, files in os.walk(base_path):
        parquet_files = [os.path.join(root, f) for f in files if f.endswith(".parquet")]
        if not parquet_files:
            continue

        match = re.search(r"year=(\d+).*month=(\d+).*day=(\d+)", root.replace("\\", "/"))
        if not match:
            continue

        try:
            y, m, d = map(int, match.groups())
            date_val = datetime(y, m, d)
            dated_folders.append((date_val, parquet_files[0]))
        except ValueError:
            continue

    if not dated_folders:
        raise FileNotFoundError(f"No parquet files found under {base_path}")

    # Sort by date (latest first)
    dated_folders.sort(key=lambda x: x[0], reverse=True)

    total = len(dated_folders)
    available = [i for i in range(total)]

    # Determine which generations to pick
    if isinstance(generations, str) and generations.lower() == "all":
        gen_indices = available
    elif isinstance(generations, int):
        gen_indices = available[:generations]  # latest + N-1 previous
    elif isinstance(generations, list):
        # Convert negative indices to positive offsets
        gen_indices = []
        for g in generations:
            if g == 0:
                gen_indices.append(0)
            elif g < 0:
                idx = abs(g)
                if idx < total:
                    gen_indices.append(idx)
        gen_indices = sorted(set(gen_indices))
    else:
        raise ValueError("Invalid 'generations' argument. Use int, list[int], or 'all'.")

    # Collect files for those generations
    selected_files = [dated_folders[i][1] for i in gen_indices if i < total]

    if debug:
        print(f"[DEBUG][GDG] Found total {total} generations.")
        print(f"[DEBUG][GDG] Selected indices: {gen_indices}")
        for i, f in zip(gen_indices, selected_files):
            print(f"  -> Gen({-i}): {f}")

    return selected_files
