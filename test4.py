def safe_str(val):
    """Convert None, NaN, literal 'NULL', or NUL bytes to empty string"""
    import math
    if val is None:
        return ""
    if isinstance(val, float) and math.isnan(val):
        return ""
    if isinstance(val, str):
        # Replace literal 'NULL', 'NaN', and NUL bytes
        return val.replace("\x00", "").replace("NULL", "").replace("NaN", "")
    return str(val)
