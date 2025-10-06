from pathlib import Path

def get_hive_parquet(base_folder: str) -> tuple[list[str], int, int, int]:
    """
    Automatically detect the latest Hive-style partition folder
    (year=YYYY/month=MM/day=DD) and return a list of parquet file paths
    along with year/month/day.
    
    Example return:
      (["/host/cis/parquet/accounts/year=2025/month=09/day=26/data_001.parquet"], 2025, 9, 26)
    """
    base_path = Path(f"{python_hive}/{base_folder}")

    if not base_path.exists():
        raise FileNotFoundError(f"Base folder not found: {base_path}")

    # Find all partitions
    partitions = list(base_path.glob("year=*/month=*/day=*"))
    if not partitions:
        raise FileNotFoundError(f"No partition folders found under {base_path}")

    # Find the latest partition by date
    latest_partition = max(
        partitions,
        key=lambda p: (
            int(p.parts[-3].split("=")[1]),  # year
            int(p.parts[-2].split("=")[1]),  # month
            int(p.parts[-1].split("=")[1])   # day
        )
    )

    year = int(latest_partition.parts[-3].split("=")[1])
    month = int(latest_partition.parts[-2].split("=")[1])
    day = int(latest_partition.parts[-1].split("=")[1])

    # Collect all parquet files under that partition
    parquet_files = [str(f) for f in latest_partition.glob("*.parquet")]

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {latest_partition}")

    return parquet_files, year, month, day
