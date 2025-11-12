import math
import numpy as np

def safe_str(val):
    """Convert None, NaN, 'NULL', 'NAN' (string) to empty string"""
    if val is None:
        return ""
    if isinstance(val, float) and math.isnan(val):
        return ""
    if isinstance(val, str) and val.upper() in ("NULL", "NAN"):
        return ""
    return str(val)
