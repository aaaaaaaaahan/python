import polars as pl

# ----------------------------------------------------------------
# SAFE DEPOSIT BOX - SAFEBOX2
# ----------------------------------------------------------------
df_safebox = pl.read_parquet("SAFEBOX.parquet")

# Select only required columns (parquet already structured)
df_safebox = df_safebox.select([
    pl.col("INDORG").cast(pl.Utf8),
    pl.col("CUSTNAME_SDB").cast(pl.Utf8),
    pl.col("ALIASKEY_SDB").cast(pl.Utf8),
    pl.col("ALIAS").cast(pl.Utf8),
    pl.col("BRANCH_ABBR").cast(pl.Utf8),
    pl.col("BRANCHNO").cast(pl.Utf8),
    pl.col("ACCTCODE").cast(pl.Utf8),
    pl.col("ACCT_NO").cast(pl.Utf8),
    pl.col("BANKINDC").cast(pl.Utf8),
    pl.col("ACCTSTATUS").cast(pl.Utf8),
    pl.col("BAL1INDC").cast(pl.Utf8),
    pl.col("BAL1").cast(pl.Float64),
    pl.col("AMT1INDC").cast(pl.Utf8),
    pl.col("AMT1").cast(pl.Float64),
])

# Add computed fields
df_safebox = df_safebox.with_columns([
    pl.lit(0).alias("LEDGERBAL"),
    pl.lit("3").alias("CATEGORY"),
    pl.lit("SDB  ").alias("APPL_CODE"),
])

# Sort by ALIAS
df_safebox = df_safebox.sort("ALIAS")

print("SAFEBOX2")
print(df_safebox.head(10))


# ----------------------------------------------------------------
# NEW SDB MATCHING - MERGEALL
# ----------------------------------------------------------------
df_mergeall1 = pl.read_parquet("MERGEALL1.parquet")

# Left join (to replicate SAS MERGE BY ALIAS)
df_merge = df_mergeall1.join(
    df_safebox,
    on="ALIAS",
    how="left",
    suffix="_sdb"
)

# Add SDBIND and SDBBRH flags
df_merge = df_merge.with_columns([
    pl.when(pl.col("BRANCH_ABBR").is_not_null())
      .then("YES").otherwise("NIL")
      .alias("SDBIND"),
    pl.when(pl.col("BRANCH_ABBR").is_not_null())
      .then(pl.col("BRANCH_ABBR"))
      .otherwise("NIL")
      .alias("SDBBRH")
])

# Drop duplicates by ACCTNOC (NODUPKEY)
df_merge = df_merge.unique(subset=["ACCTNOC"], keep="first")

print("SDB MATCHING")
print(df_merge.head(10))
