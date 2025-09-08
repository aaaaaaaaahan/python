def assign_state(zipcode: str) -> str | None:
    if not zipcode or not zipcode.isdigit():
        return None
    z = int(zipcode)
    if 79000 <= z <= 86999: return "JOH"
    if 5000  <= z <= 9999: return "KED"
    if 15000 <= z <= 18999: return "KEL"
    if 75000 <= z <= 78999: return "MEL"
    if 70000 <= z <= 73999: return "NEG"
    if 25000 <= z <= 28999 or z == 69000: return "PAH"
    if 10000 <= z <= 14999: return "PEN"
    if 30000 <= z <= 36999 or 39000 <= z <= 39999: return "PRK"
    if 1000  <= z <= 2999:  return "PER"
    if 88000 <= z <= 91999: return "SAB"
    if 93000 <= z <= 98999: return "SAR"
    if (40000 <= z <= 49999) or (63000 <= z <= 64999) or (68000 <= z <= 68199): return "SEL"
    if 20000 <= z <= 24999: return "TER"
    if 50000 <= z <= 60999: return "W P"
    if 87000 <= z <= 87999: return "LAB"
    if 62000 <= z <= 62999: return "PUT"
    return None

addraele1 = addraele1.with_columns(
    pl.col("NEW_ZIP").map_elements(assign_state, return_dtype=pl.String).alias("STATEX")
)
