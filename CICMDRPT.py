import polars as pl
from datetime import datetime
from reader import load_input

#---------------------------------------------------------------------#
# Original Program: CICMDRPT                                          #
#---------------------------------------------------------------------#
# ESMR 2022-2450 EXTRACTION OF CUSTOMERS INFORMATION BY MAPPING ID    #
#                TYPE (FROM CMD USER)                                 #
#---------------------------------------------------------------------#
# ESMR 2023-5004 ADDITIONAL DEPOSIT INFO TO BE EXTRACTED INTO REPORT  #
# INCLUDE 3 NEW FILES AS BELOW:                                       #
# -CYCLEFL                                                            #
# -POSTFL                                                             #
# -DEPOFL                                                             #
#---------------------------------------------------------------------#
# ESMR2024-2598                                                       #
# - TO INITIALIZE AMOUNT FIELDS TO ELIMINATE SPECIAL CHARACTER        #
#   DISPLAYED IN THE REPORT                                           #
#---------------------------------------------------------------------#

# ================================================================
# Read parquet datasets (no description, just loading)
# ================================================================
pbbrch    = pl.read_parquet("BRANCH.parquet")
alias     = pl.read_parquet("ALIAS.parquet")
cisfile   = load_input("CIS_CUST_DAILY")
dptrbals  = pl.read_parquet("DPTRBALS.parquet")
unicard   = pl.read_parquet("UNICARD.parquet")
comcard   = pl.read_parquet("COMCARD.parquet")
##
acctfile  = pl.read_parquet("ACCTFILE.parquet")
safebox   = pl.read_parquet("SAFEBOX.parquet")
occupfl   = load_input("BANKCTRL_DEMOCODE")
mascofl   = pl.read_parquet("MASCOFL.parquet")
msicfl    = pl.read_parquet("MSICFL.parquet")
cyclefl   = pl.read_parquet("CYCLEFL.parquet")
postfl    = pl.read_parquet("POSTFL.parquet")
depofl    = pl.read_parquet("DEPOFL.parquet")

# ================================================================
# Part 1: DATE PROCESSING
# ================================================================
today = datetime.now()
LOADDATE = today.strftime("%Y-%m-%d")  # equivalent to SAS YYMMDD10.
print(f"Load date: {LOADDATE}")

# ================================================================
# Part 2: ALIAS FILE INPUT
# ================================================================
alias = (
    alias
    .select(
        pl.col("ALIASKEY").cast(pl.Utf8),
        pl.col("ALIAS").cast(pl.Utf8),
    )
    .sort(["ALIASKEY", "ALIAS"])
)
print("\n=== Part 2: ALIASFL ===")
print(alias.head(5))

# ================================================================
# Part 3: BRANCH FILE PBBRANCH
# ================================================================
pbbrch = (
    pbbrch
    .select(
        pl.col("ACCTBRCH").cast(pl.Utf8),
        pl.col("BRANCH_ABBR").cast(pl.Utf8),
    )
    .unique(subset=["ACCTBRCH"], keep="first")
    .sort("ACCTBRCH")
)
print("\n=== Part 3: BRANCH ===")
print(pbbrch.head(5))

# ================================================================
# Part 4: OCCUP FILE
# ================================================================
occupfl = (
    occupfl
    .select(
        pl.col("TYPE").cast(pl.Utf8),
        pl.col("DEMOCODE").cast(pl.Utf8),
        pl.col("DEMODESC").cast(pl.Utf8),
    )
    .filter(pl.col("TYPE") == "OCCUP")
    .sort("DEMOCODE")
)
print("\n=== Part 4: OCCUPAT ===")
print(occupfl.head(5))

# ================================================================
# Part 5: MASCO FILE
# ================================================================
mascofl = (
    mascofl
    .select(
        pl.col("MASCO2008").cast(pl.Utf8),
        pl.col("MASCODESC").cast(pl.Utf8),
    )
    .sort("MASCO2008")
)
print("\n=== Part 4: MASCO ===")
print(mascofl.head(5))

# ================================================================
# Part 6: MSIC FILE
# ================================================================
msicfl = (
    msicfl
    .select(
        pl.col("MSICCODE").cast(pl.Utf8),
        pl.col("MSICDESC").cast(pl.Utf8),
    )
    .sort("MSICCODE")
)
print("\n=== Part 6: MSIC ===")
print(msicfl.head(5))

# ================================================================
# Part 7: STATEMENT CYCLE FILE
# ================================================================
cyclefl = (
    cyclefl
    .with_columns(
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("ACCTNO").cast(pl.Utf8).str.zfill(11).alias("ACCTNOC")
    )
    .select(
        "ACCTNOC",
        pl.col("ACCTNAME").cast(pl.Utf8),
        pl.col("CURR_CYC_DR").cast(pl.Int64, strict=False),
        pl.col("CURR_AMT_DR").cast(pl.Float64, strict=False),
        pl.col("CURR_CYC_CR").cast(pl.Int64, strict=False),
        pl.col("CURR_AMT_CR").cast(pl.Float64, strict=False),
        pl.col("PREV_CYC_DR").cast(pl.Int64, strict=False),
        pl.col("PREV_AMT_DR").cast(pl.Float64, strict=False),
        pl.col("PREV_CYC_CR").cast(pl.Int64, strict=False),
        pl.col("PREV_AMT_CR").cast(pl.Float64, strict=False),
    )
    .sort("ACCTNOC")
)
print("\n=== Part 7: STATEMENT CYCLE FILE ===")
print(cyclefl.head(5))

# ================================================================
# Part 8: POST INDICATOR FILE
# ================================================================
postfl = (
    postfl
    .select(
        pl.col("ACCTNOC").cast(pl.Utf8),
        pl.col("ACCT_PST_IND").cast(pl.Utf8),
        pl.col("ACCT_PST_REASON").cast(pl.Utf8),
    )
    .sort("ACCTNOC")
)
print("\n=== Part 8: POST INDICATOR FILE ===")
print(postfl.head(5))

# ================================================================
# Part 9: DEPOFL
# ================================================================
depofl = (
    depofl
    .with_columns(
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("ACCTNO").cast(pl.Utf8).str.zfill(11).alias("ACCTNOC")
    )
    .select(
        "ACCTNOC",
        pl.col("SEQID_1").cast(pl.Utf8),
        pl.col("SEQID_2").cast(pl.Utf8),
        pl.col("SEQID_3").cast(pl.Utf8),
        pl.col("AMT_1").cast(pl.Float64, strict=False),
        pl.col("AMT_2").cast(pl.Float64, strict=False),
        pl.col("AMT_3").cast(pl.Float64, strict=False),
        pl.col("DESC_1").cast(pl.Utf8),
        pl.col("DESC_2").cast(pl.Utf8),
        pl.col("DESC_3").cast(pl.Utf8),
        pl.col("SOURCE_1").cast(pl.Utf8),
        pl.col("SOURCE_2").cast(pl.Utf8),
        pl.col("SOURCE_3").cast(pl.Utf8),
        pl.col("TOT_HOLD").cast(pl.Utf8),
    )
    .sort("ACCTNOC")
)
print("\n=== Part 9: ADDITIONAL DP INPUT ===")
print(depofl.head(5))

# ================================================================
# Part 10: CIS + merges → mergeall1
# ================================================================
cisfile = (
    cisfile
    .with_columns(
        pl.col("OCCUP").cast(pl.Utf8).alias("DEMOCODE")
    )
    .select([
        pl.all(),
        pl.col("DEMOCODE")
    ])
    .sort(["ALIASKEY","ALIAS"])
)

# Step 1: Merge aliasfl with CIS on ALIASKEY + ALIAS
mergeals = alias.join(cisfile, on=["ALIASKEY","ALIAS"], how="inner")
print("\n=== Part 10: MERGE ALIAS ===")
print(mergeals.head(5))

# Step 2: Merge OCCUP code with occupation lookup table
mergeocc = mergeals.join(occupfl, on="DEMOCODE", how="left")
print("\n=== Part 10: MERGE OCCUP FILE ===")
print(mergeocc.head(5))

# Step 3: Merge MASCO
mergemsc = mergeocc.join(mascofl, on="MASCO2008", how="left")
print("\n=== Part 10: MERGE MASCO FILE ===")
print(mergemsc.head(5))

# Step 4: Merge MSIC
mergeall1 = mergemsc.join(msicfl, on="MSICCODE", how="left").sort("ALIAS")
print("\n=== Part 10: MERGE MSIC FILE ===")
print(mergeall1.head(5))

# ================================================================
# Part 11: SAFE DEPOSIT BOX
# ================================================================
safebox2 = safebox.select([
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
safebox2 = safebox.with_columns([
    pl.lit(0).alias("LEDGERBAL"),
    pl.lit("3").alias("CATEGORY"),
    pl.lit("SDB  ").alias("APPL_CODE"),
])

# Sort by ALIAS
safebox2 = safebox.sort("ALIAS")

print("\n=== Part 11: SAFEBOX2 ===")
print(safebox.head(10))


# ----------------------------------------------------------------
# NEW SDB MATCHING - MERGEALL
# ----------------------------------------------------------------
mergeall = mergeall1.join(safebox2, on="ALIAS", how="left", suffix="_sdb"
)

# Add SDBIND and SDBBRH flags
mergeall = mergeall.with_columns([
    pl.when(pl.col("BRANCH_ABBR").is_not_null())
      .then("YES").otherwise("NIL")
      .alias("SDBIND"),
    pl.when(pl.col("BRANCH_ABBR").is_not_null())
      .then(pl.col("BRANCH_ABBR"))
      .otherwise("NIL")
      .alias("SDBBRH")
])

# Drop duplicates by ACCTNOC (NODUPKEY)
mergeall = mergeall.unique(subset=["ACCTNOC"], keep="first")

print("\n=== Part 11: SDB MATCHING ===")
print(mergeall.head(10))

# ================================================================
# Part 12: DEPOSIT TRIAL BALANCE
# ================================================================
dptrbals = dptrbals.filter(
    (pl.col("REPTNO") == 1001) &
    (pl.col("FMTCODE").is_in([1,10,22,019,020,021]))
)

# Derive new columns
dptrbals = dptrbals.with_columns([
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
dptrbals = dptrbals.with_columns([
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
dptrbals = dptrbals.with_columns([
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
dptrbals = dptrbals.filter(
    ~(pl.col("PURPOSECD").is_null() | (pl.col("PURPOSECD") == ""))
)

# Account type
dptrbals = dptrbals.with_columns([
    pl.col("PURPOSECD").alias("ACCT_TYPE")
])

# ACCTSTATUS from OPENIND
dptrbals = dptrbals.with_columns([
    pl.when(pl.col("OPENIND") == "")
      .then("ACTIVE")
    .when(pl.col("OPENIND").is_in(["B","C","P"]))
      .then("CLOSED")
    .when(pl.col("OPENIND") == "Z")
      .then("ZERO BALANCE")
    .otherwise("").alias("ACCTSTATUS")
])

# Date conversions (string manip)
dptrbals = dptrbals.with_columns([
    (pl.col("OPENDATE").cast(pl.Utf8).str.slice(4,2)).alias("OPENDD"),
    (pl.col("OPENDATE").cast(pl.Utf8).str.slice(2,2)).alias("OPENMM"),
    (pl.col("OPENDATE").cast(pl.Utf8).str.slice(6,4)).alias("OPENYY"),
    (pl.col("CLSEDATE").cast(pl.Utf8).str.slice(4,2)).alias("CLSEDD"),
    (pl.col("CLSEDATE").cast(pl.Utf8).str.slice(2,2)).alias("CLSEMM"),
    (pl.col("CLSEDATE").cast(pl.Utf8).str.slice(6,4)).alias("CLSEYY"),
])

dptrbals = dptrbals.with_columns([
    pl.col("OPENMM"),alias("OPENMM1"),
    pl.col("CLSEMM"),alias("CLSEMM1"),
])

# Apply padding logic (like IF … THEN in SAS)
dptrbals = dptrbals.with_columns([
    pl.when(pl.col("OPENMM").cast(pl.Int32) < 10)
      .then(pl.lit("0") + pl.col("OPENMM").str.slice(-1, 1))
      .otherwise(pl.col("OPENMM"))
      .alias("OPENMM"),

    pl.when(pl.col("CLSEMM").cast(pl.Int32) < 10)
      .then(pl.lit("0") + pl.col("CLSEMM").str.slice(-1, 1))
      .otherwise(pl.col("CLSEMM"))
      .alias("CLSEMM"),
])

dptrbals = dptrbals.with_columns([
    (pl.col("OPENYY") + pl.col("OPENMM") + pl.col("OPENDD")).alias("DATEOPEN"),
    (pl.col("CLSEYY") + pl.col("CLSEMM") + pl.col("CLSEDD")).alias("DATECLSE")
])

# Sort by ACCTBRCH
dptrbals = dptrbals.sort("ACCTBRCH")

print("DEPOSIT REC")
print(dptrbals.head(10))


# ----------------------------------------------------------------
# MERGEBRCH (DPTRBALS + PBBBRCH)
# ----------------------------------------------------------------
mergebrch = dptrbals.join(pbbrch, on="ACCTBRCH", how="left")
mergebrch = mergebrch.sort("ACCTNOC")

print("DEPOSIT MATCH REC")
print(mergebrch.head(10))

# ----------------------------------------------------------------
# MERGEDP (MERGEALL + MERGEBRCH)
# ----------------------------------------------------------------
mergedp = mergeall.join(mergebrch, on="ACCTNOC", how="inner")
mergedp = mergedp.filter(pl.col("ACCTNOC") != "")
mergedp = mergedp.unique(subset=["ACCTNOC"], keep="first").sort("ACCTNOC")

print("DEPOSIT MATCH REC")
print(mergedp.head(10))


# ----------------------------------------------------------------
# MERGEDP1: MERGEDP + DPSTMT
# ----------------------------------------------------------------
mergedp1 = mergedp.join(cyclefl, on="ACCTNOC", how="left").sort("ACCTNOC")

print("MERGE STMT FILE")
print(mergedp1.head(10))


# ----------------------------------------------------------------
# MERGEDP2: MERGEDP1 + DPPOST
# ----------------------------------------------------------------
mergedp2 = mergedp1.join(postfl, on="ACCTNOC", how="left").sort("ACCTNOC")

print("MERGE POST IND FILE")
print(mergedp2.head(10))


# ----------------------------------------------------------------
# MERGEDP3: MERGEDP2 + DEPOSIT1
# ----------------------------------------------------------------
mergedp3 = mergedp2.join(depofl, on="ACCTNOC", how="left").sort("ACCTNOC")

print("ADDITIONAL DP LIST")
print(mergedp3.head(10))

# ================================================================
# Part 13: LOANS ACCOUNT FILE
# ================================================================
acctfile = (
    acctfile
    .with_columns(
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("ACCTNO").cast(pl.Utf8).str.zfill(11).alias("ACCTNOC"),
        pl.col("NOTENO").cast(pl.Int64),
        pl.col("NOTENO").cast(pl.Utf8).str.zfill(5).alias("NOTENOC"),
    )
    .with_columns(
        (pl.col("ACCTNOC") + pl.lit("-") + pl.col("NOTENOC")).alias("ACCTNOTE"),
        pl.col("ACCTNAME").cast(pl.Utf8).alias("ACCTNAME40"),
        pl.col("ORGTYPE").cast(pl.Utf8).alias("ACCT_TYPE"),
        pl.when((pl.col("ACCTNOC") > "02000000000") & (pl.col("ACCTNOC") < "02999999999"))
          .then(pl.lit("LN"))
         .when((pl.col("ACCTNOC") > "08000000000") & (pl.col("ACCTNOC") < "08999999999"))
          .then(pl.lit("HP"))
         .otherwise(pl.lit(None))
         .alias("APPL_CODE"),
        pl.when((pl.col("COSTCENTER") >= 3000) & (pl.col("COSTCENTER") <= 3999))
          .then(pl.lit("I")).otherwise(pl.lit("C")).alias("BANKINDC")
    )
    .with_columns(
        pl.col("COSTCENTER_i").cast(pl.Utf8).str.zfill(7).str.slice(-3).alias("COSTCTR1"),
        pl.col("COSTCTR1").cast(pl.Utf8).alias("ACCTBRCH"),
        pl.col("ACCTOPENDATE").cast(pl.Utf8).alias("ACCTOPNDT"),
        (pl.col("ACCTOPENDATE").cast(pl.Utf8).str.slice(0,8)).alias("ACCTOPNDT8"),
        pl.col("LASTTRANDATE").cast(pl.Utf8).alias("LASTTRNDT"),
        (pl.col("LASTTRANDATE").cast(pl.Utf8).str.slice(0,8)).alias("LASTTRNDT8"),
    )
    .with_columns(
        # Pad COSTCENTER to 7 characters (like Z7.)
        pl.col("COSTCENTER").cast(pl.Utf8).str.zfill(7).alias("COSTCENTERX"),

        # Extract substring (chars 5-7, SAS is 1-based so adjust → slice(4,3))
        pl.col("COSTCENTER").cast(pl.Utf8).str.zfill(7).str.slice(4, 3).cast(pl.Int32).alias("COSTCTR"),

        # Pad COSTCTR to 3 characters (like Z3.)
        pl.col("COSTCENTER").cast(pl.Utf8).str.zfill(7).str.slice(4, 3).alias("COSTCTR1"),

        # Account Open Date: take first 8 chars from 11-char padded string
        pl.col("ACCTOPENDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).alias("ACCTOPNDT"),

        # Extract MM/DD/YYYY from ACCTOPNDT
        pl.col("ACCTOPENDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(0, 2).alias("OPENMM"),
        pl.col("ACCTOPENDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(2, 2).alias("OPENDD"),
        pl.col("ACCTOPENDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(4, 4).alias("OPENYY"),

        # Build DATEOPEN = YYYY||MM||DD
        (pl.col("ACCTOPENDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(4, 4) +
        pl.col("ACCTOPENDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(0, 2) +
        pl.col("ACCTOPENDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(2, 2)
        ).alias("DATEOPEN"),

        # Last Transaction Date: same logic as ACCTOPENDATE
        pl.col("LASTTRANDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).alias("LASTTRNDT"),
        pl.col("LASTTRANDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(0, 2).alias("LTRNMM"),
        pl.col("LASTTRANDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(2, 2).alias("LTRNDD"),
        pl.col("LASTTRANDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(4, 4).alias("LTRNYY"),

        # DATECLSE = YYYY||MM||DD
        (pl.col("LASTTRANDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(4, 4) +
        pl.col("LASTTRANDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(0, 2) +
        pl.col("LASTTRANDATE").cast(pl.Utf8).str.zfill(11).str.slice(0, 8).str.slice(2, 2)
        ).alias("DATECLSE"),

        # Ledger balance
        (pl.col("NOTECURBAL") / 100).alias("LEDGERBAL"),

        # ACCTBRCH = COSTCTR1
        pl.col("COSTCENTER").cast(pl.Utf8).str.zfill(7).str.slice(4, 3).alias("ACCTBRCH"),
    )
    .with_columns(
        pl.when((pl.col("NPLINDC") == "3") | (pl.col("ARREARDAY") > 92))
          .then(pl.lit("NPL"))
         .when((pl.col("ARREARDAY") > 1) & (pl.col("ARREARDAY") < 92))
          .then(pl.lit("ACCOUNT IN ARREARS"))
         .when(pl.col("NOTEPAID") == "P")
          .then(pl.lit("PAID-OFF"))
         .otherwise(pl.lit(""))
         .alias("ACCTSTATUS")
    )
    .with_columns(
        pl.when((pl.col("NOTECURBAL") > 0) & ((pl.col("ACCTSTATUS") == "") | pl.col("ACCTSTATUS").is_null()))
          .then(pl.lit("ACTIVE"))
         .otherwise(pl.col("ACCTSTATUS"))
         .alias("ACCTSTATUS")
    )
    #.select([
    #    "ACCTNOC","ACCTNAME40","ACCT_TYPE","APPL_CODE","BANKINDC",
    #    "COSTCTR1","ACCTBRCH","DATEOPEN","DATECLSE","LEDGERBAL","ACCTSTATUS"
    #])
    .sort("ACCTBRCH")
)

# Join BRCH → MERGELNBRCH
mergelnbrch = acctfile.join(pbbrch, on="ACCTBRCH", how="left").sort("ACCTNOC")
print("\n=== Part 13: LOANS BRCH ===")
print(mergelnbrch.head(5))

# MERGEALL + MERGELNBRCH on ACCTNOC (inner like SAS IF C AND D) → MERGELN
mergeln = mergeall.join(mergelnbrch, on="ACCTNOC", how="inner").unique(subset=["ACCTNOC"], keep="first")
print("\n=== Part 13: LOANS MATCH REC ===")
print(mergeln.head(5))

# ================================================================
# Part 14: SAFE DEPOSIT BOX
# ================================================================
safebox = (
    safebox
    .select(
        pl.col("CUSTNO").cast(pl.Utf8),
        pl.col("ACCTNAME40").cast(pl.Utf8),
        pl.col("BRANCH_ABBR").cast(pl.Utf8),
        pl.col("ACCTNOC").cast(pl.Utf8),
        pl.col("BANKINDC").cast(pl.Utf8),
        pl.col("ACCTSTATUS").cast(pl.Utf8),
    )
    .with_columns(
        pl.lit(0.0).alias("LEDGERBAL"),
        pl.lit("3").alias("CATEGORY"),
        pl.lit("SDB").alias("APPL_CODE"),
    )
    .sort("ACCTNOC")
)

# Inner join SAFEBOX with mergeall, keep unique ACCTNOC
mergesdb = mergeall.join(safebox, on="ACCTNOC", how="inner").unique(subset=["ACCTNOC"], keep="first")

print("\n=== Part 14: SDB MATCH REC ===")
print(mergesdb.head(5))

# ================================================================
# Part 14: UNICARD processing + merge with MERGEALL
# ================================================================
unicard = (
    unicard
    .select(
        pl.col("BRANCH_ABBR").cast(pl.Utf8),
        pl.col("ACCTNOC").cast(pl.Utf8),
        pl.col("ACCTSTATUS").cast(pl.Utf8),
        pl.col("DATEOPEN").cast(pl.Utf8),
        pl.col("DATECLSE").cast(pl.Utf8),
    )
    .sort("ACCTNOC")
)

# Inner join UNICARD with mergeall, keep unique ACCTNOC
mergeuni = mergeall.join(unicard, on="ACCTNOC", how="inner").unique(subset=["ACCTNOC"], keep="first")
print("\n=== Part 14: UNICARD MATCH REC ===")
print(mergeuni.head(5))

# ================================================================
# Part 15: COMCARD processing + merge with MERGEALL
# ================================================================
comcard = (
    comcard
    .select(
        pl.col("BRANCH_ABBR").cast(pl.Utf8),
        pl.col("ACCTNOC").cast(pl.Utf8),
        pl.col("ACCTSTATUS").cast(pl.Utf8),
        pl.col("DATEOPEN").cast(pl.Utf8),
        pl.col("DATECLSE").cast(pl.Utf8),
    )
    .sort("ACCTNOC")
)

# Inner join COMCARD with mergeall, keep unique ACCTNOC
mergecom = mergeall.join(comcard, on="ACCTNOC", how="inner").unique(subset=["ACCTNOC"], keep="first")
print("\n=== Part 15: COMCARD MATCH REC ===")
print(mergecom.head(5))

########################### 
#                         #
#  Havent Check !!!!!!!!  #
#  Start from below part  #
#                         #
###########################

# ================================================================
# Part 16: Combine all merged dataframes into final output
# ================================================================

# Union rows (like SAS SET MERGEDP MERGELN MERGESDB MERGEUNI MERGECOM)
# diagonal_relaxed allows different schemas
# 2. Concatenate vertically (union of columns, missing → null)
output = pl.concat(
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
    if col not in output.columns:
        output = output.with_columns(pl.lit(None).alias(col))

# 4. Reorder columns to match SAS order
output = output.select(required_cols)

print("\n=== Part 16: Combined output preview ===")
print(output.head(5))

# ================================================================
# Part 17.1: Generate semicolon-delimited customer report
# ================================================================
report_base = pl.concat(
    [mergedp3, mergeln, mergesdb, mergeuni, mergecom],
    how="diagonal_relaxed"
)

report_df = (
    report_base
    .with_columns([
        # default NILs
        pl.col("DEMODESC").fill_null("").replace("", "NIL").alias("DEMODESC"),
        pl.col("MASCODESC").fill_null("").replace("", "NIL").alias("MASCODESC"),
        pl.col("SICCODE").fill_null("").replace("", "NIL").alias("SICCODE"),
        pl.col("MSICDESC").fill_null("").replace("", "NIL").alias("MSICDESC"),
        # TEMP_* per SAS logic
        pl.when(pl.col("CURBAL").is_not_null() & (pl.col("CURBAL") != 0))
          .then(pl.col("CURBAL")).otherwise(pl.lit(0.0)).alias("TEMP_CURBAL"),
        pl.when(pl.col("CURR_AMT_DR") > 0).then(pl.col("CURR_AMT_DR")).otherwise(0.0).alias("TEMP_CURR_AMT_DR"),
        pl.when(pl.col("CURR_AMT_CR") > 0).then(pl.col("CURR_AMT_CR")).otherwise(0.0).alias("TEMP_CURR_AMT_CR"),
        pl.when(pl.col("PREV_AMT_DR") > 0).then(pl.col("PREV_AMT_DR")).otherwise(0.0).alias("TEMP_PREV_AMT_DR"),
        pl.when(pl.col("PREV_AMT_CR") > 0).then(pl.col("PREV_AMT_CR")).otherwise(0.0).alias("TEMP_PREV_AMT_CR"),
        pl.when(pl.col("CURR_CYC_DR") > 0).then(pl.col("CURR_CYC_DR")).otherwise(0).alias("TEMP_CURR_CYC_DR"),
        pl.when(pl.col("CURR_CYC_CR") > 0).then(pl.col("CURR_CYC_CR")).otherwise(0).alias("TEMP_CURR_CYC_CR"),
        pl.when(pl.col("PREV_CYC_DR") > 0).then(pl.col("PREV_CYC_DR")).otherwise(0).alias("TEMP_PREV_CYC_DR"),
        pl.when(pl.col("PREV_CYC_CR") > 0).then(pl.col("PREV_CYC_CR")).otherwise(0).alias("TEMP_PREV_CYC_CR"),
        # holds
        pl.when(pl.col("AMT_1") > 0).then(pl.col("AMT_1")).otherwise(0.0).alias("TEMP_AMT_1"),
        pl.when(pl.col("AMT_2") > 0).then(pl.col("AMT_2")).otherwise(0.0).alias("TEMP_AMT_2"),
        pl.when(pl.col("AMT_3") > 0).then(pl.col("AMT_3")).otherwise(0.0).alias("TEMP_AMT_3"),
    ])
    # zero out TEMP_* if APPL_CODE not in ('FD','CA','SA')
    .with_columns([
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_CURBAL")).otherwise(0.0).alias("TEMP_CURBAL"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_CURR_AMT_DR")).otherwise(0.0).alias("TEMP_CURR_AMT_DR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_CURR_AMT_CR")).otherwise(0.0).alias("TEMP_CURR_AMT_CR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_PREV_AMT_DR")).otherwise(0.0).alias("TEMP_PREV_AMT_DR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_PREV_AMT_CR")).otherwise(0.0).alias("TEMP_PREV_AMT_CR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_CURR_CYC_DR")).otherwise(0).alias("TEMP_CURR_CYC_DR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_CURR_CYC_CR")).otherwise(0).alias("TEMP_CURR_CYC_CR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_PREV_CYC_DR")).otherwise(0).alias("TEMP_PREV_CYC_DR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_PREV_CYC_CR")).otherwise(0).alias("TEMP_PREV_CYC_CR"),
    ])
    .with_columns(
        pl.arange(1,pl.count() + 1).alias("NO")
    )
    .select([
        pl.col("NO"),
        pl.col("ALIASKEY"), pl.col("ALIAS"), pl.col("CUSTNAME"),
        pl.col("CUSTNO"), pl.col("DEMODESC"), pl.col("MASCODESC"),
        pl.col("SICCODE"), pl.col("MSICDESC"), pl.col("ACCTNOC"),
        pl.col("BRANCH_ABBR"), pl.col("ACCTSTATUS"),
        pl.col("DATEOPEN"), pl.col("DATECLSE"),
        pl.col("SDBIND"), pl.col("SDBBRH"),
        pl.col("TEMP_CURBAL"), pl.col("TEMP_CURR_CYC_DR"), pl.col("TEMP_CURR_AMT_DR"),
        pl.col("TEMP_CURR_CYC_CR"), pl.col("TEMP_CURR_AMT_CR"),
        pl.col("TEMP_PREV_CYC_DR"), pl.col("TEMP_PREV_AMT_DR"),
        pl.col("TEMP_PREV_CYC_CR"), pl.col("TEMP_PREV_AMT_CR"),
        pl.col("ACCT_PST_IND"), pl.col("ACCT_PST_REASON"),
        pl.col("TOT_HOLD"), pl.col("SEQID_1"), pl.col("TEMP_AMT_1"),
        pl.col("DESC_1"), pl.col("SOURCE_1"),
        pl.col("SEQID_2"), pl.col("TEMP_AMT_2"),
        pl.col("DESC_2"), pl.col("SOURCE_2"),
        pl.col("SEQID_3"), pl.col("TEMP_AMT_3"),
        pl.col("DESC_3"), pl.col("SOURCE_3"),
    ])
)

# Write semicolon-delimited report file
report_header_lines = [
    "LIST OF CUSTOMERS INFORMATION",
    ";".join([
        "NO","ID TYPE","ID NUMBER","CUST NAME","CIS NUMBER","OCCUPATION",
        "MASCO","SIC CODE","MSIC BIS TYPE","ACCT NUMBER","ACCT BRANCH",
        "ACCT STATUS","DATE ACCT OPEN","DATE ACCT CLOSED","SDB(YES/NO)",
        "BR SDB MAINTAN","CURRENT BALANCE","CURR CYC DR","CURR AMT DR",
        "CURR CYC CR","CURR AMT CR","PREV CYC DR","PREV AMT DR",
        "PREV CYC CR","PREV AMT CR","POST INDICATOR","POST INDICATOR REASON",
        "TOTAL OF HOLD","SEQ OF HOLD(1)","AMT OF HOLD(1)","DESCRIP OF HOLD(1)","SOURCE(1)",
        "SEQ OF HOLD(2)","AMT OF HOLD(2)","DESCRIP OF HOLD(2)","SOURCE(2)",
        "SEQ OF HOLD(3)","AMT OF HOLD(3)","DESCRIP OF HOLD(3)","SOURCE(3)"
    ])
]

report_df.write_parquet("CMD_REPORT.parquet")

report_body = report_df.with_columns([pl.all().cast(pl.Utf8, strict=False).fill_null("")])

with open("CMDREPORT.csv", "w", encoding="utf-8") as f:
    for line in report_header_lines:
        f.write(line + "\n")
    for row in report_body.iter_rows():
        f.write(";".join("" if v is None else str(v) for v in row) + "\n")
