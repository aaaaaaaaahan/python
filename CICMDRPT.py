import polars as pl

# ---------------------------
# 1. Read input parquet files
# ---------------------------
mergedp = pl.read_parquet("MERGEDP.parquet")
mergeln = pl.read_parquet("MERGELN.parquet")
mergesdb = pl.read_parquet("MERGESDB.parquet")
mergeuni = pl.read_parquet("MERGEUNI.parquet")
mergecom = pl.read_parquet("MERGECOM.parquet")

# ---------------------------
# 2. Concatenate datasets
# ---------------------------
output_df = pl.concat(
    [mergedp, mergeln, mergesdb, mergeuni, mergecom],
    how="diagonal_relaxed"
)

# ---------------------------
# 3. Define required columns
# ---------------------------
required_cols = [
    "ALIASKEY","ALIAS","CUSTNAME","CUSTNO","DEMODESC","MASCODESC","SICCODE","MSICDESC",
    "ACCTNOC","BRANCH_ABBR","ACCTSTATUS","DATEOPEN","DATECLSE","SDBIND","SDBBRH",
    "TEMP_CURBAL","TEMP_CURR_CYC_DR","TEMP_CURR_AMT_DR","TEMP_CURR_CYC_CR","TEMP_CURR_AMT_CR",
    "TEMP_PREV_CYC_DR","TEMP_PREV_AMT_DR","TEMP_PREV_CYC_CR","TEMP_PREV_AMT_CR",
    "ACCT_PST_IND","ACCT_PST_REASON","TOT_HOLD",
    "SEQID_1","TEMP_AMT_1","DESC_1","SOURCE_1",
    "SEQID_2","TEMP_AMT_2","DESC_2","SOURCE_2",
    "SEQID_3","TEMP_AMT_3","DESC_3","SOURCE_3"
]

# Ensure all required columns exist
for col in required_cols:
    if col not in output_df.columns:
        output_df = output_df.with_columns(pl.lit(None).alias(col))

# Reorder columns
output_df = output_df.select(required_cols)

# ---------------------------
# 4. Write HEADER version to CSV
# ---------------------------
header_titles = [
    "NO","ID TYPE","ID NUMBER","CUST NAME","CIS NUMBER","OCCUPATION","MASCO",
    "SIC CODE","MSIC BIS TYPE","ACCT NUMBER","ACCT BRANCH","ACCT STATUS",
    "DATE ACCT OPEN","DATE ACCT CLOSED","SDB(YES/NO)","BR SDB MAINTAN","CURRENT BALANCE",
    "CURR CYC DR","CURR AMT DR","CURR CYC CR","CURR AMT CR",
    "PREV CYC DR","PREV AMT DR","PREV CYC CR","PREV AMT CR",
    "POST INDICATOR","POST INDICATOR REASON","TOTAL OF HOLD",
    "SEQ OF HOLD(1)","AMT OF HOLD(1)","DESCRIP OF HOLD(1)","SOURCE(1)",
    "SEQ OF HOLD(2)","AMT OF HOLD(2)","DESCRIP OF HOLD(2)","SOURCE(2)",
    "SEQ OF HOLD(3)","AMT OF HOLD(3)","DESCRIP OF HOLD(3)","SOURCE(3)"
]

# Save header + data to CSV (for human-readable output like SAS listing)
output_df.write_csv("OUTPUT.csv", include_header=True)

# ---------------------------
# 5. Write to PARQUET
# ---------------------------
output_df.write_parquet("OUTPUT.parquet")



import polars as pl

# ---------------------------
# 1. Read input parquet files
# ---------------------------
mergedp = pl.read_parquet("MERGEDP.parquet")
mergeln = pl.read_parquet("MERGELN.parquet")
mergesdb = pl.read_parquet("MERGESDB.parquet")
mergeuni = pl.read_parquet("MERGEUNI.parquet")
mergecom = pl.read_parquet("MERGECOM.parquet")

# ---------------------------
# 2. Concatenate datasets
# ---------------------------
output_df = pl.concat(
    [mergedp, mergeln, mergesdb, mergeuni, mergecom],
    how="diagonal_relaxed"
)

# ---------------------------
# 3. Define required columns
# ---------------------------
required_cols = [
    "ALIASKEY","ALIAS","CUSTNAME","CUSTNO","DEMODESC","MASCODESC","SICCODE","MSICDESC",
    "ACCTNOC","BRANCH_ABBR","ACCTSTATUS","DATEOPEN","DATECLSE","SDBIND","SDBBRH",
    "TEMP_CURBAL","TEMP_CURR_CYC_DR","TEMP_CURR_AMT_DR","TEMP_CURR_CYC_CR","TEMP_CURR_AMT_CR",
    "TEMP_PREV_CYC_DR","TEMP_PREV_AMT_DR","TEMP_PREV_CYC_CR","TEMP_PREV_AMT_CR",
    "ACCT_PST_IND","ACCT_PST_REASON","TOT_HOLD",
    "SEQID_1","TEMP_AMT_1","DESC_1","SOURCE_1",
    "SEQID_2","TEMP_AMT_2","DESC_2","SOURCE_2",
    "SEQID_3","TEMP_AMT_3","DESC_3","SOURCE_3"
]

# Ensure all required columns exist
for col in required_cols:
    if col not in output_df.columns:
        output_df = output_df.with_columns(pl.lit(None).alias(col))

# Reorder columns
output_df = output_df.select(required_cols)

# ---------------------------
# 4. Write HEADER version to CSV
# ---------------------------
header_titles = [
    "NO","ID TYPE","ID NUMBER","CUST NAME","CIS NUMBER","OCCUPATION","MASCO",
    "SIC CODE","MSIC BIS TYPE","ACCT NUMBER","ACCT BRANCH","ACCT STATUS",
    "DATE ACCT OPEN","DATE ACCT CLOSED","SDB(YES/NO)","BR SDB MAINTAN","CURRENT BALANCE",
    "CURR CYC DR","CURR AMT DR","CURR CYC CR","CURR AMT CR",
    "PREV CYC DR","PREV AMT DR","PREV CYC CR","PREV AMT CR",
    "POST INDICATOR","POST INDICATOR REASON","TOTAL OF HOLD",
    "SEQ OF HOLD(1)","AMT OF HOLD(1)","DESCRIP OF HOLD(1)","SOURCE(1)",
    "SEQ OF HOLD(2)","AMT OF HOLD(2)","DESCRIP OF HOLD(2)","SOURCE(2)",
    "SEQ OF HOLD(3)","AMT OF HOLD(3)","DESCRIP OF HOLD(3)","SOURCE(3)"
]

# Save header + data to CSV (for human-readable output like SAS listing)
output_df.write_csv("OUTPUT.csv", include_header=True)

# ---------------------------
# 5. Write to PARQUET
# ---------------------------
output_df.write_parquet("OUTPUT.parquet")
