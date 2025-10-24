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
# FUNCTION: host_latest_prev_parquet
# ============================================================
def host_latest_prev_parquet(
    filename: str,
    generations: Union[int, str] = 1,
    debug: bool = False
) -> List[str]:
    """
    Enhanced version of host_parquet_path:
    Retrieve the latest and previous available dated parquet files.

    You can now call with or without '.parquet' extension.

    Example:
      host_latest_prev_parquet("data_test", generations=2)
      host_latest_prev_parquet("data_test.parquet", generations=2)
    """
    # Ensure file has .parquet extension
    if not filename.endswith(".parquet"):
        filename += ".parquet"

    base, ext = os.path.splitext(filename)

    # exact match first
    full_path = os.path.join(host_input, filename)
    if os.path.exists(full_path):
        return [full_path]

    # --- find all files that match base_YYYYMMDD.parquet ---
    pattern = re.compile(rf"^{re.escape(base)}_(\d{{8}}){re.escape(ext)}$")
    candidates = []
    for f in os.listdir(host_input):
        match = pattern.match(f)
        if match:
            date_str = match.group(1)
            try:
                date_val = datetime.strptime(date_str, "%Y%m%d")
                candidates.append((date_val, f))
            except ValueError:
                continue

    if not candidates:
        raise FileNotFoundError(f"No file found for base '{base}' in {host_input}")

    # --- sort descending by date ---
    candidates.sort(key=lambda x: x[0], reverse=True)

    # --- determine how many generations to return ---
    if isinstance(generations, str) and generations.lower() == "all":
        selected = candidates
    elif isinstance(generations, int) and generations > 0:
        selected = candidates[:generations]
    else:
        raise ValueError("Invalid 'generations' argument. Use positive int or 'all'.")

    # --- build full paths ---
    selected_paths = [os.path.join(host_input, f[1]) for f in selected]

    if debug:
        print(f"[DEBUG] Found {len(candidates)} total generations for '{base}'")
        for i, (dt, f) in enumerate(selected):
            print(f"  -> Gen({i}): {f} ({dt.strftime('%Y-%m-%d')})")

    return selected_paths


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

# ============================================================
# FUNCTION: Get Current and Previous generation Hive parquet 
# ============================================================
def get_hive_parquet(dataset_name: str, generations: Union[int, List[int], str] = 1, debug: bool = False) -> List[str]:
    """
    Enhanced version of get_hive_parquet():
    - Scans available parquet folders (even if not daily)
    - Returns latest and previous generations safely
    - Supports abc[0], abc[1], ...
    
    Parameters:
        dataset_name : str
            Dataset folder name under /host/dp/parquet
        generations : int | List[int] | str
            - 1 → latest only
            - 3 → latest + 2 previous
            - 'latest' or [0] → same as 1
            - [0,1,3] → pick specific generations (latest, prev, 3rd prev)
        debug : bool
            If True, print debug info
    """

    base_path = f"{python_hive}/{dataset_name}"
    if not os.path.exists(base_path):
        raise FileNotFoundError(f"Dataset path not found: {base_path}")

    # Find all folders matching year=YYYY/month=MM/day=DD
    parquet_files = []
    date_pattern = re.compile(r"year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})")

    for root, _, files in os.walk(base_path):
        for f in files:
            if f.endswith(".parquet"):
                match = date_pattern.search(root)
                if match:
                    y, m, d = map(int, match.groups())
                    dt = datetime(y, m, d)
                    parquet_files.append((dt, os.path.join(root, f)))

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found under {base_path}")

    # Sort by date (latest first)
    parquet_files.sort(key=lambda x: x[0], reverse=True)

    # Extract only paths
    sorted_files = [p for _, p in parquet_files]

    if debug:
        print(f"[DEBUG] Found total {len(sorted_files)} parquet files for {dataset_name}.")
        for i, (dt, p) in enumerate(parquet_files):
            print(f"  [{i}] {dt.strftime('%Y-%m-%d')} → {p}")

    # Handle generations argument
    if isinstance(generations, str) and generations.lower() == "latest":
        selected_indices = [0]
    elif isinstance(generations, int):
        selected_indices = list(range(min(generations, len(sorted_files))))
    elif isinstance(generations, list):
        selected_indices = [i for i in generations if i < len(sorted_files)]
    else:
        raise ValueError("Invalid 'generations' value")

    selected_files = [sorted_files[i] for i in selected_indices]

    if debug:
        print(f"[DEBUG] Selected indices: {selected_indices}")
        for i, path in enumerate(selected_files):
            print(f"  → {path}")

    return selected_files

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
# Pattern: /host/dp/parquet/year=2025/month=10/day=10/<file>.parquet
# ====================================================================
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
