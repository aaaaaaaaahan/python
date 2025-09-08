import polars as pl
import csv

# Assume addraele1 is your DataFrame

# Clean up fields like SAS
addraele1 = addraele1.with_columns([
    pl.col("NEW_ZIP").str.strip_chars().alias("NEW_ZIP"),
    pl.col("NEW_CITY").str.strip_chars().alias("NEW_CITY"),
    pl.col("NEW_COUNTRY").str.strip_chars().alias("NEW_COUNTRY"),
])

# Drop rows with blank NEW_ZIP
addraele1 = addraele1.filter(pl.col("NEW_ZIP") != "")


# --------- OUTFILE.CSV ----------
out_rows = []

# Header row (like SAS header)
header = [
    "CIS #", "ADDR REF",
    "ADDLINE1", "ADDLINE2", "ADDLINE3", "ADDLINE4", "ADDLINE5",
    "ZIP_OLD", "CITY_OLD", "COUNTRY_OLD",
    "ZIP_NEW", "CITY_NEW", "STATEX", "COUNTRY_NEW"
]
out_rows.append(header)

for row in addraele1.iter_rows(named=True):
    out_rows.append([
        row["CUSTNO"],
        f"{int(row['ADDREF']):011d}",   # Z11.
        row["LINE1ADR"],
        row["LINE2ADR"],
        row["LINE3ADR"],
        row["LINE4ADR"],
        row["LINE5ADR"],
        row["ZIP"],
        row["CITY"],
        row["COUNTRY"],
        row["NEW_ZIP"],
        row["NEW_CITY"],
        row["STATEX"],
        row["NEW_COUNTRY"],
    ])

with open("OUTFILE.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerows(out_rows)

# Also save as parquet
pl.DataFrame(out_rows[1:], schema={c: str for c in header}).write_parquet("OUTFILE.parquet")


# --------- UPDFILE.CSV ----------
upd_rows = []

header_upd = ["CUSTNO", "ADDR REF", "NEW_CITY", "STATEX", "NEW_ZIP", "NEW_COUNTRY"]
upd_rows.append(header_upd)

for row in addraele1.iter_rows(named=True):
    upd_rows.append([
        row["CUSTNO"],
        f"{int(row['ADDREF']):011d}",   # Z11.
        row["NEW_CITY"].upper(),       # UPCASE
        row["STATEX"],
        row["NEW_ZIP"],
        row["NEW_COUNTRY"],
    ])

with open("UPDFILE.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerows(upd_rows)

pl.DataFrame(upd_rows[1:], schema={c: str for c in header_upd}).write_parquet("UPDFILE.parquet")

