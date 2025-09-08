import polars as pl

def assign_line_values(df, line_col):
    # Convert to string, fill nulls
    col = df[line_col].fill_null("").cast(pl.Utf8)

    # Extract ZIP candidate (first 5 chars)
    zip_candidate = col.str.slice(0, 5)

    # Extract city candidate (chars 7-31)
    city_candidate = col.str.slice(6, 25)

    # Condition: ZIP numeric, in range, 6th char = space, first 5 chars not special
    mask = (
        (zip_candidate > "00001") & (zip_candidate < "99998") &
        (col.str.get(5) == " ") &
        ~col.str.slice(0,5).str.contains(r"[ ,&/-]")
    )

    return mask, zip_candidate, city_candidate

# Apply for each line in order
mask2, zip2, city2 = assign_line_values(addraele1, "LINE2ADR")
mask3, zip3, city3 = assign_line_values(addraele1, "LINE3ADR")
mask4, zip4, city4 = assign_line_values(addraele1, "LINE4ADR")
mask5, zip5, city5 = assign_line_values(addraele1, "LINE5ADR")

# Assign NEW_ZIP / NEW_CITY / NEW_COUNTRY
addraele1 = addraele1.with_columns([
    pl.when(mask2).then(zip2)
      .when(mask3).then(zip3)
      .when(mask4).then(zip4)
      .when(mask5).then(zip5)
      .alias("NEW_ZIP"),

    pl.when(mask2).then(city2)
      .when(mask3).then(city3)
      .when(mask4).then(city4)
      .when(mask5).then(city5)
      .alias("NEW_CITY"),

    pl.lit("MALAYSIA").alias("NEW_COUNTRY")
])
