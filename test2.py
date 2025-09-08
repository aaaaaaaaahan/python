import polars as pl

# Read parquet
taxid = pl.read_parquet("TAXID.parquet")
rhold1 = pl.read_parquet("RHOLD.parquet").rename({"ALIAS": "NEWIC1"})
rhold2 = pl.read_parquet("RHOLD2.parquet").rename({"ALIAS": "NEWIC1"})

# ---------- Step 1: Merge with RHOLD ----------
inner1 = (
    taxid.join(rhold1, on="NEWIC1", how="inner")
    .with_columns(pl.lit(1).alias("C1"))
    .select(["NEWIC1", "C1"])
)

taxid_newic = (
    taxid.join(inner1, on="NEWIC1", how="left")
    .with_columns(pl.col("C1").fill_null(0))
)

# ---------- Step 2: Merge with RHOLD2 ----------
inner2 = (
    taxid_newic.join(rhold2, on="NEWIC1", how="inner")
    .with_columns(pl.lit(1).alias("C2"))
    .select(["NEWIC1", "C2"])
)

taxid_newic = (
    taxid_newic.join(inner2, on="NEWIC1", how="left")
    .with_columns(pl.col("C2").fill_null(0))
)

# ---------- Final Sort ----------
taxid_newic = taxid_newic.sort("OLDIC")

# Save result
taxid_newic.write_parquet("TAXID_NEWIC.parquet")
