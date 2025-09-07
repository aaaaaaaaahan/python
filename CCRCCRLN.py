import polars as pl
from datetime import date

#-------------------------------------------------------------------#
# Original Program: CCRCCRLN                                        #
#-------------------------------------------------------------------#
#-EJS A2014-00021883  (CRMS PROJECT)                                #
# INCLUDE ADDITIONAL COLUMNS TO BE PLACED INTO CIS INTERFACE FILES  #
# 2016-4519 INCLUDE EFFECTIVE DATE INTO FILE                        #
#-------------------------------------------------------------------#
# ESMR 2021-00002352                                                #
# TO EXCLUDE RECORD WITH EXPIRED DATE IN RLEN CC FILE               #
#-------------------------------------------------------------------#

#--------------------------------#
# Part 1 - PROCESSING LEFT  SIDE #
#--------------------------------#

#READ PARQUET FILE
INFILE1 = pl.read_parquet("cis_internal/rawdata_converted/RLENCC.parquet")
CCCODE = pl.read_parquet("cis_internal/rawdata_converted/CIS_RLENCC_MAP.parquet") 
NAMEFILE = pl.read_parquet("cis_internal/rawdata_converted/PRIMNAME_OUT.parquet")
ALIASFIL = pl.read_parquet("cis_internal/rawdata_converted/ALLALIAS_OUT.parquet")
CUSTFILE = pl.read_parquet("cis_internal/rawdata_converted/ALLCUST.parquet")

#RBP2.B033.BANKCTRL.RLENCODE.CC
cccode = CCCODE.select(["RLENTYPE", "RLENCODE","RLENDESC"]).rename({"RLENTYPE": "TYPE","RLENCODE": "CODE1", "RLENDESC": "DESC1"})
cccode = cccode.unique(subset=["CODE1"])
print("RELATION FILE:")
print(cccode.head(5))

#RBP2.B033.UNLOAD.ALLCUST.FB
#Left Side Only
ciscust = CUSTFILE.select(["CUSTNO", "TAXID","BASICGRPCODE"]).rename({"CUSTNO": "CUSTNO1","TAXID": "OLDIC1", "BASICGRPCODE": "BASICGRPCODE1"})
ciscust = ciscust.unique(subset=["CUSTNO1"])
print("ALL CUSTOMER FILE:")
print(ciscust.head(5))

#RBP2.B033.UNLOAD.PRIMNAME.OUT
cisname = NAMEFILE.select(["CUSTNO", "INDORG","CUSTNAME"]).rename({"CUSTNO": "CUSTNO1","INDORG": "INDORG1", "CUSTNAME": "CUSTNAME1"})
cisname = cisname.unique(subset=["CUSTNO1"])
print("Customer Name FILE:")
print(cisname.head(5))

#RBP2.B033.UNLOAD.ALLALIAS.OUT
cisalias = ALIASFIL.select(["CUSTNO", "NAME_LINE"]).rename({"CUSTNO": "CUSTNO1","NAME_LINE": "ALIAS1"})
cisalias = cisalias.sort("CUSTNO1")
print("ALIAS FILE:")
print(cisalias.head(5))

#RBP2.B033.UNLOAD.RLEN#CC.FB
ccrlen1 = INFILE1.select(["CUSTNO", "EFFDATE","CUSTNO2","CODE1","CODE2","EXPIRE_DATE"]).rename({"CUSTNO": "CUSTNO1","EXPIRE_DATE": "EXPDATE1"})
ccrlen1 = ccrlen1.with_columns([
    pl.col("EXPDATE1").str.strip_chars().alias("EXPDATE1"),
    pl.col("EXPDATE1")
      .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
      .alias("EXPDATE")
])
ccrlen1 = ccrlen1.filter(pl.col("EXPDATE1") == "")
ccrlen1 = ccrlen1.with_columns([
    pl.col("EFFDATE").cast(pl.Int64).alias("EFFDATE"),
    pl.col("CODE1").cast(pl.Int64).alias("CODE1"),
    pl.col("CODE2").cast(pl.Int64).alias("CODE2")
])
ccrlen1 = ccrlen1.sort("CODE1")
print("CC FILE:")
print(ccrlen1.head(5))

#-------------------------------------------------#
#LOOKUP CONTROL FILES FOR DESCRIPTIONS (Left Side)#
#-------------------------------------------------#

#Merge CCRLEN1 and CCCODES to get RELATIONSHIP CODES
IDX_L01 = ccrlen1.join(
    cccode,
    on="CODE1",
    how="left"
)
IDX_L01 = IDX_L01.sort("CUSTNO1")
print("IDX_L01:")
print(IDX_L01.head(5))

#Merge IDX_L01 and namefile to get CIS INDORG + NAME
IDX_L02 = IDX_L01.join(
    cisname,
    on="CUSTNO1",
    how="left"
)
print("IDX_L02:")
print(IDX_L02.head(5))

#Merge IDX_L02 and aliasfil to get CIS ALIAS
IDX_L03 = IDX_L02.join(
    cisalias,
    on="CUSTNO1",
    how="left"
)
print("IDX_L03:")
print(IDX_L03.head(5))

#Merge IDX_L03 and allcust to get CIS ALIAS
IDX_L04 = IDX_L03.join(
    ciscust,
    on="CUSTNO1",
    how="left"
)
print("IDX_L04:")
print(IDX_L04.head(5))

#----------------------------------#
# OUTPUT DETAIL REPORT (LEFT SIDE) #
#----------------------------------#
LEFTOUT = IDX_L04.select(["CUSTNO1","INDORG1","CODE1","DESC1","CUSTNO2","CODE2","EXPDATE","CUSTNAME1","ALIAS1","OLDIC1","BASICGRPCODE1","EFFDATE"])
print("LEFTOUT:")
print(LEFTOUT.head(5))

#--------------------------------#
# Part 2 - PROCESSING RIGHT SIDE #
#--------------------------------#
#READ PARQUET FILE
INFILE2 = LEFTOUT
CCCODE = pl.read_parquet("cis_internal/rawdata_converted/CIS_RLENCC_MAP.parquet") 
NAMEFILE = pl.read_parquet("cis_internal/rawdata_converted/PRIMNAME_OUT.parquet")
ALIASFIL = pl.read_parquet("cis_internal/rawdata_converted/ALLALIAS_OUT.parquet")
CUSTFILE = pl.read_parquet("cis_internal/rawdata_converted/ALLCUST.parquet")


#RBP2.B033.BANKCTRL.RLENCODE.CC
cccode = CCCODE.select(["RLENTYPE", "RLENCODE","RLENDESC"]).rename({"RLENTYPE": "TYPE","RLENCODE": "CODE2", "RLENDESC": "DESC2"})
cccode = cccode.sort("CODE2")
print("RELATION FILE:")
print(cccode.head(5))

#RBP2.B033.UNLOAD.ALLCUST.FB
#Right Side Only
ciscust = CUSTFILE.select(["CUSTNO", "TAXID","BASICGRPCODE"]).rename({"CUSTNO": "CUSTNO2","TAXID": "OLDIC2", "BASICGRPCODE": "BASICGRPCODE2"})
ciscust = ciscust.unique(subset=["CUSTNO2"])
print("ALL CUSTOMER FILE:")
print(ciscust.head(5))

#RBP2.B033.UNLOAD.PRIMNAME.OUT
cisname = NAMEFILE.select(["CUSTNO", "INDORG","CUSTNAME"]).rename({"CUSTNO": "CUSTNO2","INDORG": "INDORG2", "CUSTNAME": "CUSTNAME2"})
cisname = cisname.unique(subset=["CUSTNO2"])
print("Customer Name FILE:")
print(cisname.head(5))

#RBP2.B033.UNLOAD.ALLALIAS.OUT
cisalias = ALIASFIL.select(["CUSTNO", "NAME_LINE"]).rename({"CUSTNO": "CUSTNO2","NAME_LINE": "ALIAS2"})
cisalias = cisalias.sort("CUSTNO2")
print("ALIAS FILE:")
print(cisalias.head(5))

#RBP2.B033.UNLOAD.RLEN#CC.FB
ccrlen2 = INFILE2.select(["CUSTNO1", "INDORG1","CODE1","DESC1","CUSTNO2","CODE2","EXPDATE","CUSTNAME1","ALIAS1","OLDIC1","BASICGRPCODE1","EFFDATE"])
ccrlen2 = ccrlen2.with_columns(
    ccrlen2["EXPDATE"].dt.year().alias("EXPYY"),
    ccrlen2["EXPDATE"].dt.month().alias("EXPMM"),
    ccrlen2["EXPDATE"].dt.day().alias("EXPDD")
)
ccrlen2 = ccrlen2.sort("CODE2")
print("CC FILE:")
print(ccrlen2.head(5))

#---------------------------------------#
# MERGE  CONTROL FILES FOR DESCRIPTIONS #
#---------------------------------------#

#Merge CCRLEN1 and CCCODES to get RELATIONSHIP CODES
IDX_R01 = ccrlen2.join(
    cccode,
    on="CODE2",
    how="left"
)
IDX_R01 = IDX_R01.sort("CUSTNO2")
print("IDX_R01:")
print(IDX_R01.head(5))

#Merge IDX_R01 and namefile to get CIS INDORG + NAME
IDX_R02 = IDX_R01.join(
    cisname,
    on="CUSTNO2",
    how="left"
)
print("IDX_R02:")
print(IDX_R02.head(5))

#Merge IDX_R02 and aliasfil to get CIS ALIAS
IDX_R03 = IDX_R02.join(
    cisalias,
    on="CUSTNO2",
    how="left"
)
print("IDX_R03:")
print(IDX_R03.head(5))

#Merge IDX_R03 and allcust to get CIS ALIAS
IDX_R04 = IDX_R03.join(
    ciscust,
    on="CUSTNO2",
    how="left"
)
print("IDX_R04:")
print(IDX_R04.head(5))

#-----------------------------------#
# OUTPUT DETAIL REPORT (Right SIDE) #
#-----------------------------------#
RIGHTOUT = IDX_R04.select(["CUSTNO2","INDORG2","CODE2","DESC2","CUSTNO1","CODE1","EXPDATE","CUSTNAME2","ALIAS2","OLDIC2","BASICGRPCODE2","EFFDATE"])
print("RIGHTOUT:")
print(RIGHTOUT.head(5))

#---------------------------------------#
# Part 3 - FILE TO SEARCH ONE SIDE ONLY #
#---------------------------------------#

# Set Date
today = date.today()
date1 = today.strftime("%m%d%Y")

INPUT1 = LEFTOUT.select(["CUSTNO1","INDORG1","CODE1","DESC1","CUSTNO2","CODE2","EXPDATE","CUSTNAME1","ALIAS1","OLDIC1","BASICGRPCODE1","EFFDATE"])
print("INPUT1:")
print(INPUT1.head(5))

INPUT2 = RIGHTOUT.select(["CUSTNO2","INDORG2","CODE2","DESC2","CUSTNO1","CODE1","EXPDATE","CUSTNAME2","ALIAS2","OLDIC2","BASICGRPCODE2","EFFDATE"])
print("INPUT2:")
print(INPUT2.head(5))

alloutput = INPUT1.join(
    INPUT2,
    left_on="CUSTNO2",
    right_on="CUSTNO2",
    how="left",
    suffix="_r"
)
#################################################
#new code 7sep2025
all_output = alloutput.select([
    "CUSTNO1","INDORG1","CODE1","DESC1","CUSTNO2","INDORG2","CODE2","DESC2","EXPDATE",
    "CUSTNAME1","ALIAS1","CUSTNAME2","ALIAS2","OLDIC1","BASICGRPCODE1","OLDIC2","BASICGRPCODE2","EFFDATE"
])

# Keep a copy before deduplication
all_output_full = all_output.clone()

# Deduplicate based on relationship keys
all_output_unique = all_output.unique(subset=["CUSTNO1","CUSTNO2","CODE1","CODE2"])

# Find duplicates by doing an anti-join
duplicates = all_output_full.join(
    all_output_unique,
    on=["CUSTNO1","CUSTNO2","CODE1","CODE2"],
    how="anti"
)

print("Alloutput (unique):")
print(all_output_unique.head(5))

print("Duplicate rows removed:")
print(duplicates.head(5))

# Save outputs
all_output_unique.write_csv("cis_internal/output/RLNSHIP.csv")
all_output_unique.write_parquet("cis_internal/output/RLNSHIP.parquet")

duplicates.write_csv("cis_internal/output/RLNSHIP_DUPLICATES.csv")
duplicates.write_parquet("cis_internal/output/RLNSHIP_DUPLICATES.parquet")
