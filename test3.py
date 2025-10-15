import os
import re
from datetime import datetime
from typing import Tuple, List

# ============================================================
# BASE PARQUET ROOT FOLDER
# ============================================================
# You only need to define this once
hive_base_path = "/host/cis/parquet"

# ============================================================
# FUNCTION: get_hive_parquet
# ============================================================
def get_hive_parquet(dataset_name: str, debug: bool = False) -> Tuple[str, str]:
    """
    Simulate SAS GDG behavior for Hive-style parquet folders.
    Example: CIS.SDB.MATCH.FULL -> finds (-1) and (0)
    Returns (previous_path, latest_path)
    """
    base_path = os.path.join(hive_base_path, dataset_name)

    if not os.path.exists(base_path):
        raise FileNotFoundError(f"Base parquet path not found: {base_path}")

    dated_folders = []

    # Walk through all folders (Hive structure: year=YYYY/month=MM/day=DD)
    for root, dirs, files in os.walk(base_path):
        parquet_files = [os.path.join(root, f) for f in files if f.endswith(".parquet")]
        if parquet_files:
            match = re.search(r"year=(\d+).*month=(\d+).*day=(\d+)", root.replace("\\", "/"))
            if match:
                y, m, d = map(int, match.groups())
                date_val = datetime(y, m, d)
                dated_folders.append((date_val, parquet_files[0]))

    # Sort by date (latest first)
    dated_folders.sort(key=lambda x: x[0], reverse=True)

    if len(dated_folders) < 2:
        raise ValueError(f"Not enough generations found for {dataset_name} (need at least 2 days).")

    latest = dated_folders[0][1]   # (0)
    previous = dated_folders[1][1] # (-1)

    if debug:
        print(f"[DEBUG] Latest (0): {latest}")
        print(f"[DEBUG] Previous (-1): {previous}")

    return previous, latest
