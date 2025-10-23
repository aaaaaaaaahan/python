import os
import re
from datetime import datetime
from typing import List

def get_hive_parquet(dataset_name: str, generations: int | List[int] | str = 1, debug: bool = False) -> List[str]:
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

    base_path = f"/host/dp/parquet/{dataset_name}"
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
