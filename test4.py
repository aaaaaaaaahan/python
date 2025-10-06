import os

def get_latest_parquet_in_hive(hive_base: str) -> str:
    """
    Find the latest Hive directory (not based on date string),
    then find the latest parquet file (by modified time or highest sequence number).
    
    Example:
      get_latest_parquet_in_hive("/host/cis/parquet/hive")
      -> "/host/cis/parquet/hive/20251004/data_3.parquet"
    """
    if not os.path.exists(hive_base):
        raise FileNotFoundError(f"Hive base path not found: {hive_base}")
    
    # Get all subdirectories (ignore non-dirs)
    dirs = [os.path.join(hive_base, d) for d in os.listdir(hive_base)
            if os.path.isdir(os.path.join(hive_base, d))]
    
    if not dirs:
        raise FileNotFoundError(f"No subdirectories found in {hive_base}")
    
    # Find latest directory by modified time
    latest_dir = max(dirs, key=os.path.getmtime)
    
    # Find parquet files inside that directory
    parquet_files = [os.path.join(latest_dir, f) for f in os.listdir(latest_dir)
                     if f.endswith(".parquet")]
    
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {latest_dir}")
    
    # Find latest parquet file by modified time
    latest_parquet = max(parquet_files, key=os.path.getmtime)
    
    return latest_parquet
