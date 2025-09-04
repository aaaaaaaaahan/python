# Apply padding logic (like IF … THEN in SAS)
df = df.with_columns([
    pl.when(pl.col("OPENMM").cast(pl.Int32) < 10)
      .then(pl.lit("0") + pl.col("OPENMM").str.slice(-1, 1))
      .otherwise(pl.col("OPENMM"))
      .alias("OPENMM"),

    pl.when(pl.col("CLSEMM").cast(pl.Int32) < 10)
      .then(pl.lit("0") + pl.col("CLSEMM").str.slice(-1, 1))
      .otherwise(pl.col("CLSEMM"))
      .alias("CLSEMM"),
])
