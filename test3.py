import os
import re
from typing import List, Tuple

# ============================================================
# PATH CONFIGURATION
# ============================================================
host_input = '/host/cis/parquet/sas_parquet'
python_hive = '/host/cis/parquet'
csv_output = '/host/cis/output'


# ============================================================
# HELPER: create folder if not exists
# ============================================================
def _ensure_folder(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


# ============================================================
# FUNCTION: host_parquet_path
# ============================================================
def host_parquet_path(filename: str) -> str:
    """
    Returns the exact file if exists, otherwise latest dated file in host_input.
    Example: data_test_YYYYMMDD.parquet
    """
    full_path = os.path.join(host_input, filename)
    if os.path.exists(full_path):
        return full_path

    base, ext = os.path.splitext(filename)
    pattern = re.compile(rf"^{re.escape(base)}_(\d{{8}}){re.escape(ext)}$", re.IGNORECASE)

    candidates = []
    for f in os.listdir(host_input):
        match = pattern.match(f)
        if match:
            candidates.append((match.group(1), f))

    if not candidates:
        raise FileNotFoundError(f"No file found for base '{base}' in {host_input}")

    latest_file = max(candidates, key=lambda x: x[0])[1]
    return os.path.join(host_input, latest_file)


# ============================================================
# FUNCTION: python_input_path
# ============================================================
def python_input_path(filename: str) -> str:
    return os.path.join(python_hive, filename)


# ============================================================
# FUNCTION: parquet_output_path
# ============================================================
def parquet_output_path(name: str) -> str:
    path = os.path.join(python_hive, name)
    _ensure_folder(path)
    return path


# ============================================================
# FUNCTION: csv_output_path
# ============================================================
def csv_output_path(name: str) -> str:
    path = os.path.join(csv_output, name)
    _ensure_folder(path)
    return path


# ============================================================
# HELPER: latest partition detection
# ============================================================
def _latest_partition(base_path: str, prefix: str) -> Tuple[int, str]:
    values = []
    for f in os.listdir(base_path):
        if f.startswith(prefix):
            try:
                val = int(f.split("=")[1])
                values.append((val, f))
            except ValueError:
                continue
    if not values:
        raise FileNotFoundError(f"No partitions with prefix '{prefix}' in {base_path}")
    latest = max(values, key=lambda x: x[0])
    return latest  # (value, folder_name)


# ============================================================
# FUNCTION: hive_latest_path
# ============================================================
def hive_latest_path(table: str, debug: bool = False) -> str:
    table_path = os.path.join(python_hive, table)
    if not os.path.exists(table_path):
        raise FileNotFoundError(f"Table folder not found: {table_path}")

    year_val, year_folder = _latest_partition(table_path, "year=")
    month_val, month_folder = _latest_partition(os.path.join(table_path, year_folder), "month=")
    day_val, day_folder = _latest_partition(os.path.join(table_path, year_folder, month_folder), "day=")

    day_path = os.path.join(table_path, year_folder, month_folder, day_folder)

    if debug:
        print(f"[DEBUG] Latest Hive Path: year={year_val}, month={month_val}, day={day_val}")
        print(f"[DEBUG] Full Path: {day_path}")

    return day_path


# ============================================================
# FUNCTION: get_hive_parquet
# ============================================================
def get_hive_parquet(base_folder: str, debug: bool = False) -> Tuple[List[str], int, int, int]:
    """
    Returns all parquet files in the latest Hive partition folder, 
    including files named 'data_*.parquet' or '.parquet'.
    """
    base_path = os.path.join(python_hive, base_folder)
    if not os.path.exists(base_path):
        raise FileNotFoundError(f"Base folder not found: {base_path}")

    year_val, year_folder = _latest_partition(base_path, "year=")
    month_val, month_folder = _latest_partition(os.path.join(base_path, year_folder), "month=")
    day_val, day_folder = _latest_partition(os.path.join(base_path, year_folder, month_folder), "day=")

    day_path = os.path.join(base_path, year_folder, month_folder, day_folder)

    parquet_files = [
        os.path.join(day_path, f)
        for f in os.listdir(day_path)
        if f.lower().endswith(".parquet") or re.match(r"data_\d+\.parquet", f, re.IGNORECASE)
    ]

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {day_path}")

    if debug:
        print(f"[DEBUG] Found {len(parquet_files)} parquet files in {day_path}")
        for f in parquet_files:
            print(f"  -> {f}")

    return parquet_files, year_val, month_val, day_val
