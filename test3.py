def get_latest_dp_parquet(base_file: str, debug=False):
    base_path = dp_parquet
    year_path = os.path.join(base_path, "Year")
    
    # --- collect all (year, month, day, file_path) ---
    available = []
    for y_folder in os.listdir(year_path):
        if y_folder.startswith("Year"):
            year = int(re.search(r"(\d+)", y_folder).group(1))
            y_path = os.path.join(year_path, y_folder)
            for m_folder in os.listdir(y_path):
                if m_folder.startswith("Month"):
                    month = int(re.search(r"(\d+)", m_folder).group(1))
                    m_path = os.path.join(y_path, m_folder)
                    for d_folder in os.listdir(m_path):
                        if d_folder.startswith("Day"):
                            day = int(re.search(r"(\d+)", d_folder).group(1))
                            d_path = os.path.join(m_path, d_folder, base_file)
                            if not d_path.endswith(".parquet"):
                                d_path += ".parquet"
                            if os.path.exists(d_path):
                                available.append((year, month, day, d_path))
    
    if not available:
        raise FileNotFoundError("No parquet files found at all.")
    
    # --- pick the latest date ---
    latest = max(available, key=lambda x: (x[0], x[1], x[2]))
    
    if debug:
        print(f"[DEBUG][DP] Latest parquet: {latest[3]} ({latest[0]}-{latest[1]:02d}-{latest[2]:02d})")
    
    return [latest[3]], latest[1], latest[2]
