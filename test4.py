import os
import re
from typing import Optional

python_hive = '/host/cis/parquet'  # your global base folder

def get_latest_parquet(base_folder: str) -> tuple[str, Optional[int], Optional[int], Optional[int]]:
    """
    Find the latest .parquet file (by modification time) under a Hive-style folder structure.
    Example:
      get_latest_parquet("accounts")
        -> ("/host/cis/parquet/accounts/year=2025/month=09/day=26/data_0.parquet", 2025, 9, 26)
    
    If the folder doesn't follow Hive structure, it still returns the latest parquet file path,
    but (year, month, day) will be None.
    """
    base_path = os.path.join(python_hive, base_folder)
    if not os.path.exists(base_path):
        raise FileNotFoundError(f"Base folder not found: {base_path}")

    latest_file = None
    latest_mtime = 0

    # Walk through all subdirectories
    for root, _, files in os.walk(base_path):
        for f in files:
            if f.endswith(".parquet"):
                full_path = os.path.join(root, f)
                mtime = os.path.getmtime(full_path)
                if mtime > latest_mtime:
                    latest_file = full_path
                    latest_mtime = mtime

    if not latest_file:
        raise FileNotFoundError(f"No parquet files found under {base_path}")

    # Try to extract year/month/day from folder structure if available
    year = month = day = None
    match = re.search(r"year=(\d{4})/month=(\d{2})/day=(\d{2})", latest_file)
    if match:
        year, month, day = map(int, match.groups())

    return latest_file, year, month, day
