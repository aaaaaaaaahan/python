def safe_str(val):
    """
    Convert any 'None', 'NaN', 'NULL', or np.nan to empty string.
    """
    import numpy as np
    import math

    if val is None:
        return ""
    if isinstance(val, float) and math.isnan(val):
        return ""
    if isinstance(val, str) and val.upper() in ("NULL", "NAN"):
        return ""
    return str(val)
