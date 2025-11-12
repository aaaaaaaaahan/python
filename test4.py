import math

def safe_str(val):
    """Convert None or NaN to empty string, otherwise str(val)."""
    if val is None:
        return ""
    if isinstance(val, float) and math.isnan(val):
        return ""
    return str(val)
