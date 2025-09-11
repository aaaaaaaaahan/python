import polars as pl
from datetime import datetime
from reader import load_input

# --------------------------#
# 1. Load parquet datasets  #
# --------------------------#
cisfile = load_input("CIS_CUST_DAILY")
indfile = load_input("INDVDLY")
demofile = load_input("BANKCTRL_DEMOCODE")

# ----------------------#
# 2. Get reporting date #
# ----------------------#
today = datetime.today()
YEAR = f"{today.year:04d}"
MONTH = f"{today.month:02d}"
DAY = f"{today.day:02d}"
TIMEX = today.strftime("%H%M%S")

# ------------------------#
# 3. Process CIS dataset  #
# ------------------------#
cis = (
    cisfile
    .with_columns([
        pl.col("CUSTNO").alias("CUSTNOX"),
        pl.col("PRIPHONE").cast(pl.Int64).fill_null(0).cast(pl.Utf8).str.zfill(11).alias("PRIPHONEX"),
        pl.col("SECPHONE").cast(pl.Int64).fill_null(0).cast(pl.Utf8).str.zfill(11).alias("SECPHONEX"),
        pl.col("MOBILEPH").cast(pl.Int64).fill_null(0).cast(pl.Utf8).str.zfill(11).alias("MOBILEPHX"),
        pl.col("FAX").cast(pl.Int64).fill_null(0).cast(pl.Utf8).str.zfill(11).alias("FAXX"),
        pl.col("ADDREF").cast(pl.Int64).fill_null(0).cast(pl.Utf8).str.zfill(11).alias("ADDREFX"),
    ])
)

# Derive OPEN DATE
cis = cis.with_columns([
    pl.col("CUSTOPENDATE").cast(pl.Utf8).alias("CUSTOPEN")
])
cis = cis.with_columns([
    pl.when(pl.col("CUSTOPEN") == "00002000000")
      .then("OPENDT" == "20000101")
      .otherwise(
          pl.col("CUSTOPEN").str.slice(4, 4) +  # year
          pl.col("CUSTOPEN").str.slice(0, 2) +  # month
          pl.col("CUSTOPEN").str.slice(2, 2)    # day
      )
      .alias("OPENDT")
])

# HRCALL concat (HRC01..HRC20)
hrc_cols = [f"HRC{i:02d}" for i in range(1, 21)]
cis = cis.with_columns([
    pl.concat_str([pl.col(c).cast(pl.Utf8).str.zfill(3) for c in hrc_cols]).alias("HRCALL")
])

# Drop duplicates by CUSTNOX
cis = cis.unique(subset=["CUSTNOX"])
print("CIS: ")
print(cis.head(5))

# ----------------------------------------#
# 4. Process DEMOFILE (split by category) #
# ----------------------------------------#
demofile = demofile.rename({
    "RLENDESC":"CODEDESC"
})

sales = (
    demofile.filter(pl.col("DEMOCATEGORY") == "SALES")
    .select([
        pl.col("DEMOCODE").alias("SALES"),
        pl.col("CODEDESC").alias("SALDESC")
    ])
    .unique(subset=["SALES"])
)

print("Sales: ")
print(sales.head(5))

restr = (
    demofile.filter(pl.col("DEMOCATEGORY") == "RESTR")
    .select([
        pl.col("DEMOCODE").alias("RESTR"),
        pl.col("CODEDESC").alias("RESDESC")
    ])
    .unique(subset=["RESTR"])
)

print("Restr: ")
print(restr.head(5))

citzn = (
    demofile.filter(pl.col("DEMOCATEGORY") == "CITZN")
    .select([
        pl.col("DEMOCODX").alias("CITZN"),
        pl.col("CODEDESC").alias("CTZDESC")
    ])
    .unique(subset=["CITZN"])
)

print("Citzn: ")
print(citzn.head(5))

# -----------------------------#
# 5. Process INDFILE (INDVDLY) #
# -----------------------------#
indv = (
    indfile
    .filter(pl.col("CISNO").is_not_null())
    .with_columns([
        pl.col("CISNO").alias("CUSTNOX")
    ])
)

indv = indv.unique(subset=["CUSTNOX"], keep="last")  # keep last update
indv = indv.rename({
    "BANKNO":"BANKNO_INDV"
})

# --------------------------------#
# 6. Merge datasets step by step  #
# --------------------------------#
mrgcis = (
    cis.join(indv, on="CUSTNOX", how="left")
    .with_columns([
        pl.lit(YEAR + MONTH + DAY + TIMEX).alias("RUNTIMESTAMP"),
        pl.col("RESIDENCY").alias("RESTR"),
        pl.col("CORPSTATUS").alias("SALES"),
        pl.col("CITIZENSHIP").alias("CITZN"),
    ])
)

print("MRGCIS: ")
print(mrgcis.head(5))

mrgres = mrgcis.join(restr, on="RESTR", how="left")
print("MRGRES: ")
print(mrgres.head(5))

mrgsal = mrgres.join(sales, on="SALES", how="left")
print("MRGSAL: ")
print(mrgsal.head(5))

mrgctz = mrgsal.join(citzn, on="CITZN", how="left")
print("MRGCTZ: ")
print(mrgctz.head(5))

# ------------------------#
# 7. Final Output (OUT2)  #
# ------------------------#
out2 = mrgctz.select([
    "RUNTIMESTAMP","CUSTNOX","ADDREFX","CUSTNAME","PRIPHONEX","SECPHONEX",
    "MOBILEPHX","FAXX","ALIASKEY","ALIAS","PROCESSTIME","CUSTSTAT","TAXCODE",
    "TAXID","CUSTBRCH","COSTCTR","CUSTMNTDATE","CUSTLASTOPER","PRIM_OFF",
    "SEC_OFF","PRIM_LN_OFF","SEC_LN_OFF","RACE","RESIDENCY","CITIZENSHIP",
    "OPENDT","HRCALL","EXPERIENCE","HOBBIES","RELIGION","LANGUAGE","INST_SEC",
    "CUST_CODE","CUSTCONSENT","BASICGRPCODE","MSICCODE","MASCO2008","INCOME",
    "EDUCATION","OCCUP","MARITALSTAT","OWNRENT","EMPNAME","DOBDOR","SICCODE",
    "CORPSTATUS","NETWORTH","LAST_UPDATE_DATE","LAST_UPDATE_TIME",
    "LAST_UPDATE_OPER","PRCOUNTRY","EMPLOYMENT_TYPE","EMPLOYMENT_SECTOR",
    "EMPLOYMENT_LAST_UPDATE","BNMID","LONGNAME","INDORG","RESDESC",
    "SALDESC","CTZDESC"
])
print("OUT2: ")
print(out2.head(5))

# Save final output
out2.write_parquet("cis_internal/output/COMBINECUSTALL.parquet")
out2.write_csv("cis_internal/output/COMBINECUSTALL.csv")
