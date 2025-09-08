# ------------------------
# Step 5: Line checks (zip extraction from LINE2ADR..LINE5ADR)
# ------------------------

def extract_zip_city_full(df: pl.DataFrame, line: str):
    col = pl.col(line)

    # First 5 chars not space, ',', '&', '/', '-'
    invalid_chars = [" ", ",", "&", "/", "-"]
    first5_mask = ~pl.any_horizontal([col.str.slice(0,5).str.contains(char) for char in invalid_chars])

    # 6th char must be space
    sixth_mask = col.str.slice(5,1) == " "

    # ZIP range check
    zip_mask = (col.str.slice(0,5) > "00001") & (col.str.slice(0,5) < "99998")

    # Combine all conditions
    mask = zip_mask & sixth_mask & first5_mask

    return df.with_columns([
        pl.when(mask)
          .then(col.str.slice(0,5))
          .otherwise(pl.col("NEW_ZIP"))
          .alias("NEW_ZIP"),
        pl.when(mask)
          .then(col.str.slice(6,25))
          .otherwise(pl.col("NEW_CITY"))
          .alias("NEW_CITY"),
        pl.when(mask)
          .then(pl.lit("MALAYSIA"))
          .otherwise(pl.col("NEW_COUNTRY"))
          .alias("NEW_COUNTRY"),
    ])


# Initialize columns first
addr_aele = addr_aele.with_columns([
    pl.lit("").alias("NEW_ZIP"),
    pl.lit("").alias("NEW_CITY"),
    pl.lit("").alias("NEW_COUNTRY")
])

# Apply for each address line in order
for line in ["LINE2ADR","LINE3ADR","LINE4ADR","LINE5ADR"]:
    addr_aele = extract_zip_city_full(addr_aele, line)

print("ADDR_AELE after full line check:")
print(addr_aele.head(5))

