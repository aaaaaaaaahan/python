import polars as pl
from reader_polars import load_input

# ------------------------
# Step 1: Read Parquet files
# ------------------------
addr = load_input("ADDRLINE_FB")
aele = load_input("ADDRAELE_FB_PROD")

# ------------------------
# Step 2: Prepare ADDR
# ------------------------
addr = (
    addr.with_columns(
        pl.col("ADDREF1").cast(pl.Int64).cast(pl.Utf8).str.zfill(11),
    )
)

addr = (
    addr.select([
        "ADDREF1", "LINE1IND", "LINE1ADR", 
        "LINE2IND", "LINE2ADR",
        "LINE3IND", "LINE3ADR",
        "LINE4IND", "LINE4ADR",
        "LINE5IND", "LINE5ADR"
        ])
)

addr = (
    addr
    .rename({"ADDREF1": "ADDREF"})
   # .sort("ADDREF")
)
print("ADDR sample:")
print(addr.head(5))

# ------------------------
# Step 3: Prepare AELE
# ------------------------
aele = (
    aele.with_columns(
        pl.col("ADDREF1").cast(pl.Int64).cast(pl.Utf8).str.zfill(11),
    )
)

aele = (
    aele.select([
        "ADDREF1", "STREET", "CITY", "ZIP", "ZIP2", "COUNTRY"
        ])
)

aele = (
    aele
    .rename({"ADDREF1": "ADDREF"})
  #  .sort("ADDREF")
)
print("AELE sample:")
print(aele.head(100))

# ------------------------
# Step 4: Merge ADDR + AELE
# ------------------------
addr_aele = addr.join(aele, on="ADDREF", how="inner")

# concat address lines
addr_aele = addr_aele.with_columns(
    (pl.col("LINE1ADR") + pl.col("LINE2ADR") +
     pl.col("LINE3ADR") + pl.col("LINE4ADR") +
     pl.col("LINE5ADR")).alias("ADDRLINE")
)

# drop rows where CITY or ZIP is missing
addr_aele = addr_aele.filter(
    (pl.col("CITY") == "") | (pl.col("ZIP") == "")
)

# remove invalid countries
bad_countries = [
    "SINGAPORE ","CANADA    ","SINGAPORE`","LONDON    ","AUS       ",
    "AUSTRIA   ","BAHRAIN   ","BANGLADESH","BRUNEI DAR","CAMBODIA  ",
    "CAN       ","CAYMAN ISL","CHINA     ","BRUNEI    ","INDONESIA ",
    "DARUSSALAM","DENMARK   ","EMIRATES  ","ENGLAND   ","EUROPEAN  ",
    "FRANCE    ","GERMANY   ","HONG KONG ","INDIA     ","IRAN (ISLA",
    "IRELAND   ","JAPAN     ","KOREA REPU","MACAU     ","MAURITIUS ",
    "MEXICO    ","MYANMAR   ","NEPAL     ","NETHERLAND","NEW ZEALAN",
    "NEWZEALAND","NIGERIA   ","NORWAY    ","OMAN      ","PAKISTAN  ",
    "PANAMA    ","PHILIPPINE","ROC       ","S ARABIA  ","SAMOA     ",
    "SAUDI ARAB","SIGAPORE  ","SIMGAPORE ","SINGAPOREW","SINGPAORE ",
    "SINGPORE  ","SINAGPORE ","SNGAPORE  ","SINGOPORE ","SPAIN     ",
    "SRI LANKA ","SWAZILAND ","SWEDEN    ","SWITZERLAN","TAIWAN    ",
    "TAIWAN,PRO","THAILAND  ","U KINGDOM ","U.K.      ","UNITED ARA",
    "UK        ","UNITED KIN","UNITED STA","VIRGIN ISL","USA       ",
    "PAPUA NEW ","AUSTRALIA "
]
addr_aele = addr_aele.filter(~pl.col("COUNTRY").is_in(bad_countries))

# ------------------------
# Step 5: Line checks (zip extraction from LINE2ADR..LINE5ADR)
# ------------------------
def extract_zip_city(df: pl.DataFrame, line: str):
    return df.with_columns([
        pl.when(
            (pl.col(line).str.slice(0,5) > "00001") & 
            (pl.col(line).str.slice(0,5) < "99998") &
            (pl.col(line).str.slice(5,1) == " ")
        )
        .then(pl.col(line).str.slice(0,5))
        .otherwise(pl.col("NEW_ZIP"))
        .alias("NEW_ZIP"),
        
        pl.when(
            (pl.col(line).str.slice(0,5) > "00001") & 
            (pl.col(line).str.slice(0,5) < "99998") &
            (pl.col(line).str.slice(5,1) == " ")
        )
        .then(pl.col(line).str.slice(6,25))
        .otherwise(pl.col("NEW_CITY"))
        .alias("NEW_CITY"),

        pl.when(
            (pl.col(line).str.slice(0,5) > "00001") & 
            (pl.col(line).str.slice(0,5) < "99998") &
            (pl.col(line).str.slice(5,1) == " ")
        )
        .then(pl.lit("MALAYSIA"))
        .otherwise(pl.col("NEW_COUNTRY"))
        .alias("NEW_COUNTRY"),
    ])

addr_aele = addr_aele.with_columns([
    pl.lit("").alias("NEW_ZIP"),
    pl.lit("").alias("NEW_CITY"),
    pl.lit("").alias("NEW_COUNTRY")
])

for line in ["LINE1ADR","LINE2ADR","LINE3ADR","LINE4ADR","LINE5ADR"]:
    addr_aele = extract_zip_city(addr_aele, line)

print("ADDR_AELE sample:")
print(addr_aele.head(5))

# ------------------------
# Step 6: Exclusion filters + assign STATEX
# ------------------------
exclude_strings = [
    "SINGAPORE","HONG HONG","QATAR","TAMIL NADU","STAFFORDSHIRE",
    "HANOI","VIETNAM","NEW ZEALAND","ENGLAND","AUCKLAND","SHANGHAI",
    "DOHA QATAR","THAILAND","HONG KONG","SEOUL","#","NSW","NETHERLANDS",
    "AUSTRALIA","S'PORE"
]

addraele1 = addr_aele.filter(
    ~pl.any_horizontal([pl.col("ADDRLINE").str.contains(x) for x in exclude_strings])
)

# Assign STATEX by postal code
def assign_state(zipcode: str) -> str:
    if not zipcode.isdigit(): return None
    z = int(zipcode)
    if 79000 <= z <= 86999: return "JOH"
    if 5000 <= z <= 9999: return "KED"
    if 15000 <= z <= 18999: return "KEL"
    if 75000 <= z <= 78999: return "MEL"
    if 70000 <= z <= 73999: return "NEG"
    if 25000 <= z <= 28999 or z == 69000: return "PAH"
    if 10000 <= z <= 14999: return "PEN"
    if 30000 <= z <= 36999 or 39000 <= z <= 39999: return "PRK"
    if 1000 <= z <= 2999: return "PER"
    if 88000 <= z <= 91999: return "SAB"
    if 93000 <= z <= 98999: return "SAR"
    if 40000 <= z <= 49999 or 63000 <= z <= 64999 or 68000 <= z <= 68199: return "SEL"
    if 20000 <= z <= 24999: return "TER"
    if 50000 <= z <= 60999: return "W P"
    if 87000 <= z <= 87999: return "LAB"
    if 62000 <= z <= 62999: return "PUT"
    return None

addraele1 = addraele1.with_columns(
    pl.col("NEW_ZIP").map_elements(assign_state, return_dtype=pl.String).alias("STATEX")
)

print("ADDRAELE1 sample:")
print(addraele1.head(5))

# ------------------------
# Step 7: Write outputs
# ------------------------
# Equivalent of OUTFILE
# Clean like SAS LEFT()
addraele1 = addraele1.with_columns([
    pl.col("NEW_ZIP").str.strip_chars().alias("NEW_ZIP"),
    pl.col("NEW_CITY").str.strip_chars().alias("NEW_CITY"),
    pl.col("NEW_COUNTRY").str.strip_chars().alias("NEW_COUNTRY"),
])

# Drop blank NEW_ZIP
addraele1 = addraele1.filter(pl.col("NEW_ZIP") != "")

if "CUSTNO" not in addraele1.columns:
    addraele1 = addraele1.with_columns(
        pl.lit("").alias("CUSTNO")
    )

# ---- OUTFILE equivalent ----
outfile_df = addraele1.select([
    pl.col("CUSTNO").alias("CIS #"),
    pl.lit("-").alias("-"),
    pl.col("ADDREF").alias("ADDR REF"),
    pl.col("LINE1ADR").alias("ADDLINE1"),
    pl.col("LINE2ADR").alias("ADDLINE2"),
    pl.col("LINE3ADR").alias("ADDLINE3"),
    pl.col("LINE4ADR").alias("ADDLINE4"),
    pl.col("LINE5ADR").alias("ADDLINE5"),
    pl.lit("*OLD*").alias("OLD_FLAG"),
    pl.col("ZIP").alias("ZIP_OLD"),
    pl.col("CITY").alias("CITY_OLD"),
    pl.col("COUNTRY").alias("COUNTRY_OLD"),
    pl.lit("*NEW*").alias("NEW_FLAG"),
    pl.col("NEW_ZIP"),
    pl.col("NEW_CITY"),
    pl.col("STATEX"),
    pl.col("NEW_COUNTRY"),
])

#outfile_df.write_csv("cis_internal/output/CCRSADR4_VERIFY.csv")
#outfile_df.write_parquet("cis_internal/output/CCRSADR4_VERIFY.parquet")
outfile_df.write_csv("/host/cis/output/polars/CCRSADR4_VERIFY.csv")
outfile_df.write_parquet("/host/cis/output/polars/CCRSADR4_VERIFY.parquet")

# Equivalent of UPDFILE
# ---- UPDFILE equivalent ----
updfile_df = addraele1.select([
    pl.col("CUSTNO").alias("CIS #"),
    pl.col("ADDREF").alias("ADDR REF"),
    pl.col("NEW_CITY").str.to_uppercase().alias("NEW_CITY"),
    pl.col("STATEX"),
    pl.col("NEW_ZIP"),
    pl.col("NEW_COUNTRY"),
])

#updfile_df.write_csv("cis_internal/output/CCRSADR4_UPDATE.csv")
#updfile_df.write_parquet("cis_internal/output/CCRSADR4_UPDATE.parquet")
updfile_df.write_csv("/host/cis/output/polars/CCRSADR4_UPDATE.csv")
updfile_df.write_parquet("/host/cis/output/polars/CCRSADR4_UPDATE.parquet")
