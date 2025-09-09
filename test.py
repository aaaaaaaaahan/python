import polars as pl

# -----------------------------
# READ SOURCE DATA (Equivalent to INFILE)
# -----------------------------
# Replace 'read_parquet' with your actual paths or CSV files if needed
namefile = pl.read_parquet("CCRIS_CISNAME_TEMP.parquet")
rmrkfile = pl.read_parquet("CCRIS_CISRMRK_LONGNAME.parquet")

# -----------------------------
# NAME DATASET PROCESSING
# -----------------------------
# Keep only relevant columns to mimic SAS INPUT statement
namefile = namefile.select([
    pl.col("CUSTNO").cast(pl.Int64).cast(pl.Utf8).str.zfill(11),
    pl.col("CUSTNAME").cast(pl.Utf8),
    pl.col("ADREFNO").cast(pl.Int64).cast(pl.Utf8).str.zfill(11),
    pl.col("PRIPHONE").cast(pl.Int64).cast(pl.Utf8).str.zfill(11),
    pl.col("SECPHONE").cast(pl.Int64).cast(pl.Utf8).str.zfill(11),
    pl.col("CUSTTYPE").cast(pl.Utf8),
    pl.col("CUSTNAME1").cast(pl.Utf8),
    pl.col("MOBILEPHONE").cast(pl.Utf8)
])

# Remove duplicates by CUSTNO
namefile = namefile.unique(subset=["CUSTNO"])

# Print first 5 rows (like PROC PRINT OBS=5)
print("NAME:")
print(namefile.head(5))

# -----------------------------
# REMARKS DATASET PROCESSING
# -----------------------------
rmrkfile = rmrkfile.select([
    pl.col("BANKNO").cast(pl.Utf8),
    pl.col("APPLCODE").cast(pl.Utf8),
    pl.col("CUSTNO").cast(pl.Utf8),
    pl.col("EFFDATE").cast(pl.Utf8),
    pl.col("RMKKEYWORD").cast(pl.Utf8),
    pl.col("LONGNAME").cast(pl.Utf8),
    pl.col("RMKOPERATOR").cast(pl.Utf8),
    pl.col("EXPIREDATE").cast(pl.Utf8),
    pl.col("LASTMNTDATE").cast(pl.Utf8)
])

# Remove duplicates by CUSTNO
rmrkfile = rmrkfile.unique(subset=["CUSTNO"])

# Print first 5 rows
print("REMARKS:")
print(rmrkfile.head(5))

# -----------------------------
# MERGE NAME AND REMARKS (Equivalent to DATA MERGE; MERGE NAME RMRK BY CUSTNO; IF A;)
# -----------------------------
merge = namefile.join(rmrkfile, on="CUSTNO", how="left")  # left join to mimic IF A;

# Sort by CUSTNO
merge = merge.sort("CUSTNO")

# Print first 5 rows
print("MERGE:")
print(merge.head(5))

# -----------------------------
# OUTPUT DETAIL REPORT (Equivalent to DATA OUT; PUT ...)
# -----------------------------
# Keep columns in the same order as SAS PUT statement
output = merge.select([
    "CUSTNO",
    "CUSTNAME",
    "ADREFNO",
    "PRIPHONE",
    "SECPHONE",
    "CUSTTYPE",
    "CUSTNAME1",
    "MOBILEPHONE",
    "LONGNAME"
])

# Write to Parquet (or CSV if needed)
output.write_parquet("cis_internal/output/CCRIS_CISNAME_OUT.parquet")
output.write_csv("cis_internal/output/CCRIS_CISNAME_OUT.csv")
