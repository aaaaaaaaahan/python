import polars as pl
from reader import load_input

# -----------------------------
# Part 0: Read Parquet files
# -----------------------------
oldic = load_input("CCRIS_OLDIC_GDG")    
newic = load_input("CCRIS_ALIAS_GDG")      
rhold = load_input("RHOLD_FULL_LIST")  

# -----------------------------
# Part 1: OLDIC processing
# -----------------------------
oldic = oldic.select([
    pl.col("CUSTNO").cast(pl.Int64).cast(pl.Utf8).str.zfill(11),
    pl.col("CODE_OLD").cast(pl.Utf8),
    pl.col("INDORG").cast(pl.Utf8),
    pl.col("OLDIC").cast(pl.Utf8),
    pl.col("CUSTBRCH").cast(pl.Int64).cast(pl.Utf8).str.zfill(5)
])
oldic = oldic.sort("CUSTNO")
print("OLD IC:\n", oldic.head(5))

# -----------------------------
# Part 2: NEWIC processing
# -----------------------------
newic = newic.select([
    pl.col("CUSTNO").cast(pl.Int64).cast(pl.Utf8).str.zfill(11),
    pl.col("CODE_NEW").cast(pl.Utf8),
    pl.col("NEWIC").cast(pl.Utf8),
    pl.col("KEYFIELD1").cast(pl.Utf8),
    pl.col("KEYFIELD2").cast(pl.Utf8)
])
newic = newic.with_columns(
    pl.col("NEWIC").str.slice(3, 20).alias("NEWIC1")  # SUBSTR(NEWIC,4,20)
)
newic = newic.sort("CUSTNO")
print("NEW IC:\n", newic.head(5))

# -----------------------------
# Part 3: RHOLD processing
# -----------------------------
rhold_alias1 = rhold.select(pl.col("ID1").alias("ALIAS")).filter(pl.col("ALIAS") != "")
rhold_alias2 = rhold.select(pl.col("ID2").alias("ALIAS")).filter(pl.col("ALIAS") != "")
rhold_all = pl.concat([rhold_alias1, rhold_alias2]).unique(subset="ALIAS").sort("ALIAS")
print("RHOLD:\n", rhold_all.head(5))

# -----------------------------
# Part 4: TAXID merge OLDIC + NEWIC
# -----------------------------
taxid = oldic.join(newic, on="CUSTNO", how="left")
taxid = taxid.with_columns(
    pl.when(pl.col("INDORG") == "O")
      .then(pl.col("NEWIC"))
      .otherwise(None)
      .alias("BUSREG")
)
taxid = taxid.sort("NEWIC1")
print("TAXID FILE:\n", taxid.head(5))

# -----------------------------
# Part 5: TAXID_NEWIC merge with RHOLD (NEWIC1)
# -----------------------------
rhold_newic = rhold_all.rename({"ALIAS": "NEWIC1"})
taxid_newic = taxid.join(rhold_newic, on="NEWIC1", how="left")
taxid_newic = taxid_newic.with_columns(
    pl.when(pl.col("NEWIC1").is_not_null())
      .then(1)
      .otherwise(0)
      .alias("C")
)
taxid_newic = taxid_newic.sort("OLDIC")

print("taxid_newic:")
print(taxid_newic.head(5))

# -----------------------------
# Part 6: TAXID_OLDIC merge with RHOLD (OLDIC)
# -----------------------------
rhold_oldic = rhold_all.rename({"ALIAS": "OLDIC"})
taxid_oldic = taxid_newic.join(rhold_oldic, on="OLDIC", how="left")
taxid_oldic = taxid_oldic.with_columns(
    pl.when(pl.col("OLDIC").is_not_null())
      .then(1)
      .otherwise(0)
      .alias("F")
)
taxid_oldic = taxid_oldic.sort("CUSTNO")

print("taxid_oldic:")
print(taxid_oldic.head(5))

# -----------------------------
# Part 7: OUT dataset creation
# -----------------------------
def match_type(row):
    if row["C"] == 1 and row["F"] == 1:
        return "B", "Y"
    elif row["C"] == 1 and row["F"] is None:
        return "N", "Y"
    elif row["C"] is None and row["F"] == 1:
        return "O", "Y"
    else:
        return "X", "N"

taxid_oldic = taxid_oldic.with_columns([
    pl.when((pl.col("C") == 1) & (pl.col("F") == 1))
      .then(pl.lit("B"))
      .when((pl.col("C") == 1) & (pl.col("F") == 0))
      .then(pl.lit("N"))
      .when((pl.col("C") == 0) & (pl.col("F") == 1))
      .then(pl.lit("O"))
      .otherwise(pl.lit("X"))
      .alias("MATCHID"),

    pl.when((pl.col("C") == 1) | (pl.col("F") == 1))
      .then(pl.lit("Y"))
      .otherwise(pl.lit("N"))
      .alias("RHOLD_IND")
])

# -----------------------------
# Part 8: Write output
# -----------------------------
taxid_oldic.select([
    "CUSTNO", "OLDIC", "NEWIC", "BUSREG", "CUSTBRCH", "RHOLD_IND", "MATCHID"
]).write_parquet("cis_internal/output/CCRIS_TAXID_GDG.parquet")
taxid_oldic.select([
    "CUSTNO", "OLDIC", "NEWIC", "BUSREG", "CUSTBRCH", "RHOLD_IND", "MATCHID"
]).write_csv("cis_internal/output/CCRIS_TAXID_GDG.csv")

print("Final Output:")
print(taxid_oldic.head(5))
