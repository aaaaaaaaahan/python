import polars as pl
from reader import load_input

#------------------------#
# READ PARQUET DATASETS  #
#------------------------#
main_df = load_input("CIDICUST_FB")
indv_df = load_input("CIDINDVT_FB")
org_df  = load_input("CIDIORGT_FB")
cart_df = load_input("CIDICART_FB")

#-----------------------------------------------#
# Part 1 - Process Individual Part              # 
#-----------------------------------------------#
# PROCESS MAIN CUSTOMER FILE (INDIVIDUAL PART)  #
#   - Drop GENDER = 'O'                         #
#-----------------------------------------------#
main_indv = (
    main_df
    .filter(pl.col("GENDER") != "O")
    .select([
        "CISNO", "BANKNO", "MAIN_ENTITY_TYPE", "BRANCH",
        "CUSTNAME", "BIRTHDATE", "GENDER"
    ])
    .sort("CISNO")
)

print("MAIN (INDIVIDUAL)")
print(main_indv.head(5))


#----------------------------#
# 0PROCESS INDIVIDUAL FILE   #
#----------------------------#
indv_df = (
    indv_df
    .select([
        "CUSTNO","IDTYPE","ID","CUSTBRANCH","FIRST_CREATE_DATE",
        "FIRST_CREATE_TIME","FIRST_CREATE_OPER","LAST_UPDATE_DATE",
        "LAST_UPDATE_TIME","LAST_UPDATE_OPER","LONGNAME","ENTITYTYPE",
        "BNM_ASSIGNED_ID","OLDIC","CITIZENSHIP","PRCOUNTRY",
        "RESIDENCY_STATUS","CUSTOMER_CODE","ADDRLINE1","ADDRLINE2",
        "ADDRLINE3","ADDRLINE4","ADDRLINE5","POSTCODE","TOWN_CITY",
        "STATE_CODE","COUNTRY","ADDR_LAST_UPDATE","ADDR_LAST_UPTIME",
        "PHONE_HOME","PHONE_BUSINESS","PHONE_FAX","PHONE_MOBILE",
        "PHONE_PAC","EMPLOYER_NAME","MASCO2008","MASCO2012",
        "EMPLOYMENT_TYPE","EMPLOYMENT_SECTOR","EMPLOYMENT_LAST_UPDATE",
        "EMPLOYMENT_LAST_UPTIME","INCOME_AMT","ENABLE_TAB"
    ])
    .rename({"CUSTNO": "CISNO"})
)

indv = (
    indv_df
    .select([
        "CISNO","IDTYPE","ID","CUSTBRANCH","FIRST_CREATE_DATE",
        "FIRST_CREATE_TIME","FIRST_CREATE_OPER","LAST_UPDATE_DATE",
        "LAST_UPDATE_TIME","LAST_UPDATE_OPER","LONGNAME","ENTITYTYPE",
        "BNM_ASSIGNED_ID","OLDIC","CITIZENSHIP","PRCOUNTRY",
        "RESIDENCY_STATUS","CUSTOMER_CODE","ADDRLINE1","ADDRLINE2",
        "ADDRLINE3","ADDRLINE4","ADDRLINE5","POSTCODE","TOWN_CITY",
        "STATE_CODE","COUNTRY","ADDR_LAST_UPDATE","ADDR_LAST_UPTIME",
        "PHONE_HOME","PHONE_BUSINESS","PHONE_FAX","PHONE_MOBILE",
        "PHONE_PAC","EMPLOYER_NAME","MASCO2008","MASCO2012",
        "EMPLOYMENT_TYPE","EMPLOYMENT_SECTOR","EMPLOYMENT_LAST_UPDATE",
        "EMPLOYMENT_LAST_UPTIME","INCOME_AMT","ENABLE_TAB"
    ])
    .sort(["CISNO","IDTYPE","ID"])
)

print("INDIVIDUAL FILE")
print(indv.head(5))


#--------------------#
# PROCESS CART FILE  #
#--------------------#
cart_df = (
    cart_df
    .select([
        "APPL_CODE","APPL_NO","PRI_SEC","RELATIONSHIP",
        "CUSTNO","IDTYPE","ID","AA_REF_NO","EFF_DATE",
        "EFF_TIME","LAST_MNT_DATE","LAST_MNT_TIME"
    ])
    .rename({"CUSTNO": "CISNO"})
)

cart = (
    cart_df
    .select([
        "APPL_CODE","APPL_NO","PRI_SEC","RELATIONSHIP",
        pl.col("CISNO").cast(pl.Utf8).str.zfill(11),"IDTYPE","ID","AA_REF_NO","EFF_DATE",
        "EFF_TIME","LAST_MNT_DATE","LAST_MNT_TIME"
    ])
    .sort(["CISNO","IDTYPE","ID"])
)

print("CART FILE")
print(cart.head(5))


#--------------------------#
# MERGE INDIVIDUAL + CART  #
#--------------------------#
custinfo_indv = (
    indv.join(cart, on=["CISNO","IDTYPE","ID"], how="left")
)

print("CUSTINFO (INDIVIDUAL)")
print(custinfo_indv.head(5))


#------------------------------------#
# MERGE MAIN + CUSTINFO (INDIVIDUAL) #
#------------------------------------#
indvdly = (
    main_indv.join(custinfo_indv, on="CISNO", how="inner")
)

# Create index equivalent (set unique keys)
indvdly = indvdly.unique(subset=["CISNO","IDTYPE","ID"])
indvdly = indvdly.sort("CISNO")

print("FINAL INDIVIDUAL DATASET")
print(indvdly.head(5))


#-------------------------------------------------#
# Part 2 - Process Organisation Part              # 
#-------------------------------------------------#
# PROCESS MAIN CUSTOMER FILE (ORGANISATION PART)  #
#   - Keep only GENDER = 'O'                      #
#-------------------------------------------------#
main_org = (
    main_df
    .filter(pl.col("GENDER") == "O")
    .select([
        "CISNO", "BANKNO", "MAIN_ENTITY_TYPE", "BRANCH",
        "CUSTNAME", "BIRTHDATE", "GENDER"
    ])
    .sort("CISNO")
)

print("MAIN (ORGANISATION)")
print(main_org.head(5))


#-------------------#
# PROCESS ORG FILE  #
#-------------------#
org_df = (
    org_df
    .select([
        "CUSTNO","IDTYPE","ID","BRANCH","FIRST_CREATE_DATE",
        "FIRST_CREATE_TIME","FIRST_CREATE_OPER","LAST_UPDATE_DATE",
        "LAST_UPDATE_TIME","LAST_UPDATE_OPER","LONG_NAME","ENTITY_TYPE",
        "BNM_ASSIGNED_ID","REGISTRATION_DATE","MSIC2008","RESIDENCY_STATUS",
        "CORPORATE_STATUS","CUSTOMER_CODE","CITIZENSHIP","ADDR_LINE_1",
        "ADDR_LINE_2","ADDR_LINE_3","ADDR_LINE_4","ADDR_LINE_5","POSTCODE",
        "TOWN_CITY","STATE_CODE","COUNTRY","ADDR_LAST_UPDATE","ADDR_LAST_UPTIME",
        "PHONE_PRIMARY","PHONE_SECONDARY","PHONE_FAX","PHONE_MOBILE",
        "PHONE_PAC","ENABLE_TAB"
    ])
    .rename({"CUSTNO": "CISNO"})
)

org = (
    org_df
    .select([
        "CISNO","IDTYPE","ID","BRANCH","FIRST_CREATE_DATE",
        "FIRST_CREATE_TIME","FIRST_CREATE_OPER","LAST_UPDATE_DATE",
        "LAST_UPDATE_TIME","LAST_UPDATE_OPER","LONG_NAME","ENTITY_TYPE",
        "BNM_ASSIGNED_ID","REGISTRATION_DATE","MSIC2008","RESIDENCY_STATUS",
        "CORPORATE_STATUS","CUSTOMER_CODE","CITIZENSHIP","ADDR_LINE_1",
        "ADDR_LINE_2","ADDR_LINE_3","ADDR_LINE_4","ADDR_LINE_5","POSTCODE",
        "TOWN_CITY","STATE_CODE","COUNTRY","ADDR_LAST_UPDATE","ADDR_LAST_UPTIME",
        "PHONE_PRIMARY","PHONE_SECONDARY","PHONE_FAX","PHONE_MOBILE",
        "PHONE_PAC","ENABLE_TAB"
    ])
    .sort(["CISNO","IDTYPE","ID"])
)

print("ORGANISATION FILE")
print(org.head(5))


#-------------------#
# MERGE ORG + CART  #
#-------------------#
custinfo_org = (
    org.join(cart, on=["CISNO","IDTYPE","ID"], how="left")
)

print("CUSTINFO (ORG)")
print(custinfo_org.head(5))


#------------------------------------#
# MERGE MAIN (ORG) + CUSTINFO (ORG)  #
#------------------------------------#
orgdly = (
    main_org.join(custinfo_org, on="CISNO", how="inner")
)

orgdly = orgdly.unique(subset=["CISNO","IDTYPE","ID"])
orgdly = orgdly.sort("CISNO")

print("FINAL ORGANISATION DATASET")
print(orgdly.head(5))


#-------------------------------------------------#
# Part 3 - Output                                 #
#-------------------------------------------------#
# SAVE RESULTS TO PARQUET                         #
#-------------------------------------------------#
indvdly.write_parquet("cis_internal/output/INDVDLY.parquet")
orgdly.write_parquet("cis_internal/output/ORGDLY.parquet")
indvdly.write_csv("cis_internal/output/INDVDLY.csv")
orgdly.write_csv("cis_internal/output/ORGDLY.CSV")
