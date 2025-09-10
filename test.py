import polars as pl
from reader import load_input

#--------------------------------#
# READ PARQUET FILES (ALL AT TOP)#
#--------------------------------#
aown_raw   = load_input("ADDRAOWN_FB")
dpaddr_raw = load_input("DP_CUST_DAILY_ADDRACC")

#--------------------------------#
# Part 1 - PROCESS AOWNFILE      #
#--------------------------------#
aown_raw = aown_raw.with_columns(
    pl.when(pl.col("ACCTNO").str.starts_with("0"))
    .then(pl.col("ACCTNO").str.slice(1))
    .otherwise(pl.col("ACCTNO"))
    .alias("ACCTNO")
)

aown = (
    aown_raw
    .with_columns(pl.col("ACCTNO").cast(pl.Utf8))
    .filter(
        (pl.col("O_APPL_CODE") == "DP") & 
        (pl.col("ACCTNO") > "10000000000")
    )
    .unique(subset=["ACCTNO"])   # PROC SORT NODUPKEY
)

print("AOWN")
print(aown.head(5))

#--------------------------------#
# Part 2 - PROCESS DEPOSIT FILE  #
#--------------------------------#
dpaddr = (
    dpaddr_raw
    .with_columns(pl.col("ACCTNO").cast(pl.Utf8))
    .filter(pl.col("ACCTNO") > "10000000000")
    .unique(subset=["ACCTNO"])
)

print("DEPOSIT ADDRESS")
print(dpaddr.head(5))

#--------------------------------#
# Part 3 - MERGE ON ACCTNO       #
#--------------------------------#
merge = (
    dpaddr.join(aown, on="ACCTNO", how="inner")   # SAS MERGE with IN=A and IN=B
    .sort("ACCTNO")
)

print("MERGED")
print(merge.head(5))

#--------------------------------#
# Part 4 - CREATE OUTPUT FILE    #
#--------------------------------#
out = merge.select([
    pl.col("O_APPL_CODE").alias("O_APPL_CODE"),
    pl.col("ACCTNO").alias("ACCTNO"),
    pl.col("NA_LINE_TYP1"),
    pl.col("ADD_NAME_1"),
    pl.col("NA_LINE_TYP2"),
    pl.col("ADD_NAME_2"),
    pl.col("NA_LINE_TYP3"),
    pl.col("ADD_NAME_3"),
    pl.col("NA_LINE_TYP4"),
    pl.col("ADD_NAME_4"),
    pl.col("NA_LINE_TYP5"),
    pl.col("ADD_NAME_5"),
    pl.col("NA_LINE_TYP6"),
    pl.col("ADD_NAME_6"),
    pl.col("NA_LINE_TYP7"),
    pl.col("ADD_NAME_7"),
    pl.col("NA_LINE_TYP8"),
    pl.col("ADD_NAME_8"),
])

# Save to parquet (instead of fixed-length file)
out.write_parquet("output/cis_internal/DAILY_ADDRACC.parquet")
out.write_csv("output/cis_internal/DAILY_ADDRACC.csv")

print("OUT FILE SAMPLE")
print(out.head(5))
