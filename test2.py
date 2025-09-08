taxid_oldic = taxid_oldic.with_columns([
    pl.when((pl.col("C") == 1) & (pl.col("F") == 1))
      .then("B")
      .when((pl.col("C") == 1) & pl.col("F").is_null())
      .then("N")
      .when(pl.col("C").is_null() & (pl.col("F") == 1))
      .then("O")
      .otherwise("X")
      .alias("MATCHID"),

    pl.when((pl.col("C") == 1) | (pl.col("F") == 1))
      .then("Y")
      .otherwise("N")
      .alias("RHOLD_IND")
])
