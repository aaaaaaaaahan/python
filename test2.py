import polars as pl

# Assume addraele1 is your DataFrame

# Clean like SAS LEFT()
addraele1 = addraele1.with_columns([
    pl.col("NEW_ZIP").str.strip_chars().alias("NEW_ZIP"),
    pl.col("NEW_CITY").str.strip_chars().alias("NEW_CITY"),
    pl.col("NEW_COUNTRY").str.strip_chars().alias("NEW_COUNTRY"),
])

# Drop blank NEW_ZIP
addraele1 = addraele1.filter(pl.col("NEW_ZIP") != "")

# ---- OUTFILE equivalent ----
outfile_df = addraele1.select([
    pl.col("CUSTNO").alias("CIS #"),
    pl.lit("-").alias("SEP1"),
    pl.col("ADDREF").cast(pl.Int64).alias("ADDR REF"),
    pl.col("LINE1ADR").alias("ADDLINE1"),
    pl.col("LINE2ADR").alias("ADDLINE2"),
    pl.col("LINE3ADR").alias("ADDLINE3"),
    pl.col("LINE4ADR").alias("ADDLINE4"),
    pl.col("LINE5ADR").alias("ADDLINE5"),
    pl.lit("*OLD*").alias("OLD_FLAG"),
    pl.col("ZIP").alias("ZIP_OLD"),
    pl.col("CITY").alias("CITY_OLD"),
    pl.col("COUNTRY").alias("COUNTRY_OLD"),
    pl.lit("*NEW*").alias("NEW_FLAG"),
    pl.col("NEW_ZIP"),
    pl.col("NEW_CITY"),
    pl.col("STATEX"),
    pl.col("NEW_COUNTRY"),
])

outfile_df.write_csv("OUTFILE.csv")
outfile_df.write_parquet("OUTFILE.parquet")


# ---- UPDFILE equivalent ----
updfile_df = addraele1.select([
    pl.col("CUSTNO").alias("CIS #"),
    pl.col("ADDREF").cast(pl.Int64).alias("ADDR REF"),
    pl.col("NEW_CITY").str.to_uppercase().alias("NEW_CITY"),
    pl.col("STATEX"),
    pl.col("NEW_ZIP"),
    pl.col("NEW_COUNTRY"),
])

updfile_df.write_csv("UPDFILE.csv")
updfile_df.write_parquet("UPDFILE.parquet")

