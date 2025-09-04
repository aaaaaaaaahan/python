output_df = pl.concat(
    [df for df in [mergedp, mergeln, mergesdb, mergeuni, mergecom] if 'ACCTNOC' in df.columns],
    how="diagonal_relaxed"
)
