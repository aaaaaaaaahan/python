import polars as pl

# 1. Read all input parquet files
mergedp = pl.read_parquet("MERGEDP.parquet")
mergeln = pl.read_parquet("MERGELN.parquet")
mergesdb = pl.read_parquet("MERGESDB.parquet")
mergeuni = pl.read_parquet("MERGEUNI.parquet")
mergecom = pl.read_parquet("MERGECOM.parquet")

# 2. Concatenate vertically (union of columns, missing → null)
output_df = pl.concat(
    [mergedp, mergeln, mergesdb, mergeuni, mergecom],
    how="diagonal_relaxed"
)

# 3. Ensure all required output columns exist
required_cols = [
    "CUSTNO","ACCTNOC","OCCUP","MASCO2008","ALIASKEY","ALIAS",
    "CUSTNAME","DATEOPEN","DATECLSE","LEDGERBAL","BANKINDC","CITIZENSHIP",
    "APPL_CODE","PRODTY","DEMODESC","MASCODESC","JOINTACC","MSICCODE",
    "ACCTBRCH","BRANCH_ABBR","ACCTSTATUS","SICCODE"
]

for col in required_cols:
    if col not in output_df.columns:
        output_df = output_df.with_columns(pl.lit(None).alias(col))

# 4. Reorder columns to match SAS order
output_df = output_df.select(required_cols)

# 5. Save to Parquet
output_df.write_parquet("OUTPUT.parquet")

