from typing import List, Union
import os
import re
from datetime import datetime

def get_gdg_generations(
    dataset_name: str,
    generations: Union[int, List[int], str] = 2,
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
