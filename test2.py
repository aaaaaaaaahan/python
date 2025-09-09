import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv

# -----------------------------
# READ SOURCE DATA (Equivalent to INFILE)
# -----------------------------
namefile = pl.read_parquet("CCRIS_CISNAME_TEMP.parquet")
rmrkfile = pl.read_parquet("CCRIS_CISRMRK_LONGNAME.parquet")

# -----------------------------
# NAME DATASET PROCESSING
# -----------------------------
namefile = namefile.select([
    pl.col("CUSTNO").cast(pl.Int64).cast(pl.Utf8).str.zfill(11).alias("CUSTNO"),
    pl.col("CUSTNAME").cast(pl.Utf8),
    pl.col("ADREFNO").cast(pl.Int64).cast(pl.Utf8).str.zfill(11),
    pl.col("PRIPHONE").cast(pl.Int64).cast(pl.Utf8).str.zfill(11),
    pl.col("SECPHONE").cast(pl.Int64).cast(pl.Utf8).str.zfill(11),
    pl.col("CUSTTYPE").cast(pl.Utf8),
    pl.col("CUSTNAME1").cast(pl.Utf8),
    pl.col("MOBILEPHONE").cast(pl.Utf8),
])
namefile = namefile.unique(subset=["CUSTNO"])
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
    pl.col("LASTMNTDATE").cast(pl.Utf8),
])
rmrkfile = rmrkfile.unique(subset=["CUSTNO"])
print("REMARKS:")
print(rmrkfile.head(5))

# -----------------------------
# MERGE WITH DUCKDB (Equivalent to DATA MERGE ... IF A;)
# -----------------------------
con = duckdb.connect()

# Register Polars DataFrames as DuckDB tables (via Arrow)
con.register("namefile", namefile.to_arrow())
con.register("rmrkfile", rmrkfile.to_arrow())

merge_arrow = con.execute("""
    SELECT 
        n.CUSTNO,
        n.CUSTNAME,
        n.ADREFNO,
        n.PRIPHONE,
        n.SECPHONE,
        n.CUSTTYPE,
        n.CUSTNAME1,
        n.MOBILEPHONE,
        r.LONGNAME
    FROM namefile n
    LEFT JOIN rmrkfile r
    ON n.CUSTNO = r.CUSTNO
    ORDER BY n.CUSTNO
""").arrow()

print("MERGE:")
print(merge_arrow.slice(0, 5))  # show first 5 rows

# -----------------------------
# OUTPUT WITH PYARROW
# -----------------------------
# Write to Parquet
pq.write_table(merge_arrow, "cis_internal/output/CCRIS_CISNAME_OUT.parquet")

# Write to CSV
csv.write_csv(merge_arrow, "cis_internal/output/CCRIS_CISNAME_OUT.csv")
