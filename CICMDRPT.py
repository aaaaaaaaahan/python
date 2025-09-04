import polars as pl

# ----------------------------------------------------------------
# DEPOSIT TRIAL BALANCE - DPTRBALS
# ----------------------------------------------------------------
df_dptrbals = pl.read_parquet("DPTRBALS.parquet")

# Apply filter REPTNO=1001 and FMTCODE in (1,10,22,19,20,21)
df_dptrbals = df_dptrbals.filter(
    (pl.col("REPTNO") == 1001) &
    (pl.col("FMTCODE").is_in([1, 10, 22, 19, 20, 21]))
)

# Derive new columns
df_dptrbals = df_dptrbals.with_columns([
    # Convert account numbers & branch
    pl.col("ACCTNO").cast(pl.Utf8).str.zfill(11).alias("ACCTNOC"),
    pl.col("ACCTBRCH1").cast(pl.Utf8).str.zfill(3).alias("ACCTBRCH"),
    pl.col("PRODTYPE").cast(pl.Utf8).str.zfill(3).alias("PRODTY"),

    # Bank indicator
    pl.when((pl.col("COSTCTR") > 3000) & (pl.col("COSTCTR") < 3999))
      .then("I").otherwise("C").alias("BANKINDC"),

    # Ledger balance divided by 100
    (pl.col("LEDGERBAL1") / 100).alias("LEDGERBAL"),

    # ACCTNAME copy
    pl.col("ACCTNAME").alias("ACCTNAME40"),

    # Default values for derived fields
    pl.lit("").alias("APPL_CODE"),
    pl.lit("").alias("ACCT_TYPE"),
])

# Application code rules (APPL_CODE) - apply ranges
df_dptrbals = df_dptrbals.with_columns([
    pl.when((pl.col("ACCTNOC") > "03000000000") & (pl.col("ACCTNOC") < "03999999999"))
      .then("CA   ")
    .when((pl.col("ACCTNOC") > "06200000000") & (pl.col("ACCTNOC") < "06299999999"))
      .then("CA   ")
    .when((pl.col("ACCTNOC") > "06710000000") & (pl.col("ACCTNOC") < "06719999999"))
      .then("CA   ")
    .when((pl.col("ACCTNOC") > "01000000000") & (pl.col("ACCTNOC") < "01999999999"))
      .then("FD   ")
    .when((pl.col("ACCTNOC") > "07000000000") & (pl.col("ACCTNOC") < "07999999999"))
      .then("FD   ")
    .when((pl.col("ACCTNOC") > "04000000000") & (pl.col("ACCTNOC") < "04999999999"))
      .then("SA   ")
    .when((pl.col("ACCTNOC") > "05000000000") & (pl.col("ACCTNOC") < "05999999999"))
      .then("SA   ")
    .when((pl.col("ACCTNOC") > "06000000000") & (pl.col("ACCTNOC") < "06199999999"))
      .then("SA   ")
    .when((pl.col("ACCTNOC") > "06300000000") & (pl.col("ACCTNOC") < "06709999999"))
      .then("SA   ")
    .when((pl.col("ACCTNOC") > "06720000000") & (pl.col("ACCTNOC") < "06999999999"))
      .then("SA   ")
    .otherwise(pl.col("APPL_CODE"))
    .alias("APPL_CODE")
])

# Override APPL_CODE for special PRODTYPEs
df_dptrbals = df_dptrbals.with_columns([
    pl.when(pl.col("PRODTYPE").is_in(
        ['371','350','351','352','353','354','355','356','357','358',
         '359','360','361','362']
    )).then("FCYFD")
    .when(pl.col("PRODTYPE").is_in(
        ['400','401','402','403','404','405','406','407','408','409',
         '410','411','413','414','420','421','422','423','424','425',
         '426','427','428','429','430','431','432','433','434','440',
         '441','442','443','444','450','451','452','453','454','460',
         '461','473','474','475','476']
    )).then("FCYCA")
    .otherwise(pl.col("APPL_CODE"))
    .alias("APPL_CODE")
])

# Purpose check (delete if blank or hex 00)
df_dptrbals = df_dptrbals.filter(
    ~(pl.col("PURPOSECD").is_null() | (pl.col("PURPOSECD") == ""))
)

# Account type
df_dptrbals = df_dptrbals.with_columns([
    pl.col("PURPOSECD").alias("ACCT_TYPE")
])

# ACCTSTATUS from OPENIND
df_dptrbals = df_dptrbals.with_columns([
    pl.when(pl.col("OPENIND") == "")
      .then("ACTIVE")
    .when(pl.col("OPENIND").is_in(["B","C","P"]))
      .then("CLOSED")
    .when(pl.col("OPENIND") == "Z")
      .then("ZERO BALANCE")
    .otherwise("").alias("ACCTSTATUS")
])

# Date conversions (string manip)
df_dptrbals = df_dptrbals.with_columns([
    (pl.col("OPENDATE").cast(pl.Utf8).str.slice(4,2)).alias("OPENDD"),
    (pl.col("OPENDATE").cast(pl.Utf8).str.slice(2,2)).alias("OPENMM"),
    (pl.col("OPENDATE").cast(pl.Utf8).str.slice(6,4)).alias("OPENYY"),
    (pl.col("CLSEDATE").cast(pl.Utf8).str.slice(4,2)).alias("CLSEDD"),
    (pl.col("CLSEDATE").cast(pl.Utf8).str.slice(2,2)).alias("CLSEMM"),
    (pl.col("CLSEDATE").cast(pl.Utf8).str.slice(6,4)).alias("CLSEYY"),
])

df_dptrbals = df_dptrbals.with_columns([
    (pl.col("OPENYY") + pl.col("OPENMM") + pl.col("OPENDD")).alias("DATEOPEN"),
    (pl.col("CLSEYY") + pl.col("CLSEMM") + pl.col("CLSEDD")).alias("DATECLSE")
])

# Sort by ACCTBRCH
df_dptrbals = df_dptrbals.sort("ACCTBRCH")

print("DEPOSIT REC")
print(df_dptrbals.head(10))


# ----------------------------------------------------------------
# MERGEBRCH (DPTRBALS + PBBBRCH)
# ----------------------------------------------------------------
df_pbbranch = pl.read_parquet("PBBBRCH.parquet")

df_mergebrch = df_dptrbals.join(df_pbbranch, on="ACCTBRCH", how="left")
df_mergebrch = df_mergebrch.sort("ACCTNOC")

print("DEPOSIT MATCH REC")
print(df_mergebrch.head(10))


# ----------------------------------------------------------------
# MERGEDP (MERGEALL + MERGEBRCH)
# ----------------------------------------------------------------
df_mergeall = pl.read_parquet("MERGEALL.parquet")

df_mergedp = df_mergeall.join(df_mergebrch, on="ACCTNOC", how="inner")
df_mergedp = df_mergedp.filter(pl.col("ACCTNOC") != "")
df_mergedp = df_mergedp.unique(subset=["ACCTNOC"], keep="first").sort("ACCTNOC")

print("DEPOSIT MATCH REC")
print(df_mergedp.head(10))


# ----------------------------------------------------------------
# MERGEDP1: MERGEDP + DPSTMT
# ----------------------------------------------------------------
df_dpstmt = pl.read_parquet("DPSTMT.parquet")
df_mergedp1 = df_mergedp.join(df_dpstmt, on="ACCTNOC", how="left").sort("ACCTNOC")

print("MERGE STMT FILE")
print(df_mergedp1.head(10))


# ----------------------------------------------------------------
# MERGEDP2: MERGEDP1 + DPPOST
# ----------------------------------------------------------------
df_dppost = pl.read_parquet("DPPOST.parquet")
df_mergedp2 = df_mergedp1.join(df_dppost, on="ACCTNOC", how="left").sort("ACCTNOC")

print("MERGE POST IND FILE")
print(df_mergedp2.head(10))


# ----------------------------------------------------------------
# MERGEDP3: MERGEDP2 + DEPOSIT1
# ----------------------------------------------------------------
df_deposit1 = pl.read_parquet("DEPOSIT1.parquet")
df_mergedp3 = df_mergedp2.join(df_deposit1, on="ACCTNOC", how="left").sort("ACCTNOC")

print("ADDITIONAL DP LIST")
print(df_mergedp3.head(10))
 
