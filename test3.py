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
