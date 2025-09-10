import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pacsv
from reader import load_input

#------------------------#
# READ PARQUET DATASETS  #
#------------------------#
main_df = load_input("CIDICUST_FB")
indv_df = load_input("CIDINDVT_FB")
org_df  = load_input("CIDIORGT_FB")
cart_df = load_input("CIDICART_FB")

# Register dataframes as DuckDB tables
con = duckdb.connect()
con.register("main_df", main_df)
con.register("indv_df", indv_df)
con.register("org_df", org_df)
con.register("cart_df", cart_df)


#-----------------------------------------------#
# Part 1 - Process Individual Part              #
#-----------------------------------------------#

# MAIN (INDIVIDUAL)
main_indv = con.execute("""
    SELECT CISNO, BANKNO, MAIN_ENTITY_TYPE, BRANCH,
           CUSTNAME, BIRTHDATE, GENDER
    FROM main_df
    WHERE GENDER <> 'O'
    ORDER BY CISNO
""").arrow()
print("MAIN (INDIVIDUAL)")
print(main_indv.to_pandas().head(5))


# INDIVIDUAL FILE
indv = con.execute("""
    SELECT CUSTNO AS CISNO, IDTYPE, ID, CUSTBRANCH,
           FIRST_CREATE_DATE, FIRST_CREATE_TIME, FIRST_CREATE_OPER,
           LAST_UPDATE_DATE, LAST_UPDATE_TIME, LAST_UPDATE_OPER,
           LONGNAME, ENTITYTYPE, BNM_ASSIGNED_ID, OLDIC,
           CITIZENSHIP, PRCOUNTRY, RESIDENCY_STATUS, CUSTOMER_CODE,
           ADDRLINE1, ADDRLINE2, ADDRLINE3, ADDRLINE4, ADDRLINE5,
           POSTCODE, TOWN_CITY, STATE_CODE, COUNTRY,
           ADDR_LAST_UPDATE, ADDR_LAST_UPTIME,
           PHONE_HOME, PHONE_BUSINESS, PHONE_FAX, PHONE_MOBILE, PHONE_PAC,
           EMPLOYER_NAME, MASCO2008, MASCO2012,
           EMPLOYMENT_TYPE, EMPLOYMENT_SECTOR,
           EMPLOYMENT_LAST_UPDATE, EMPLOYMENT_LAST_UPTIME,
           INCOME_AMT, ENABLE_TAB
    FROM indv_df
    ORDER BY CISNO, IDTYPE, ID
""").arrow()
print("INDIVIDUAL FILE")
print(indv.to_pandas().head(5))


# CART FILE
cart = con.execute("""
    SELECT APPL_CODE, APPL_NO, PRI_SEC, RELATIONSHIP,
           LPAD(CAST(CUSTNO AS VARCHAR), 11, '0') AS CISNO,
           IDTYPE, ID, AA_REF_NO, EFF_DATE,
           EFF_TIME, LAST_MNT_DATE, LAST_MNT_TIME
    FROM cart_df
    ORDER BY CISNO, IDTYPE, ID
""").arrow()
print("CART FILE")
print(cart.to_pandas().head(5))


# MERGE INDIVIDUAL + CART
custinfo_indv = con.execute("""
    SELECT i.*, c.APPL_CODE, c.APPL_NO, c.PRI_SEC, c.RELATIONSHIP,
           c.AA_REF_NO, c.EFF_DATE, c.EFF_TIME, c.LAST_MNT_DATE, c.LAST_MNT_TIME
    FROM indv i
    LEFT JOIN cart c
    ON i.CISNO = c.CISNO AND i.IDTYPE = c.IDTYPE AND i.ID = c.ID
""").arrow()
print("CUSTINFO (INDIVIDUAL)")
print(custinfo_indv.to_pandas().head(5))


# MERGE MAIN + CUSTINFO (INDIVIDUAL)
indvdly = con.execute("""
    SELECT DISTINCT m.CISNO, m.BANKNO, m.MAIN_ENTITY_TYPE, m.BRANCH,
           m.CUSTNAME, m.BIRTHDATE, m.GENDER,
           ci.*
    FROM main_indv m
    INNER JOIN custinfo_indv ci ON m.CISNO = ci.CISNO
    ORDER BY m.CISNO
""").arrow()
print("FINAL INDIVIDUAL DATASET")
print(indvdly.to_pandas().head(5))


#-------------------------------------------------#
# Part 2 - Process Organisation Part              #
#-------------------------------------------------#

# MAIN (ORGANISATION)
main_org = con.execute("""
    SELECT CISNO, BANKNO, MAIN_ENTITY_TYPE, BRANCH,
           CUSTNAME, BIRTHDATE, GENDER
    FROM main_df
    WHERE GENDER = 'O'
    ORDER BY CISNO
""").arrow()
print("MAIN (ORGANISATION)")
print(main_org.to_pandas().head(5))


# ORGANISATION FILE
org = con.execute("""
    SELECT CUSTNO AS CISNO, IDTYPE, ID, BRANCH,
           FIRST_CREATE_DATE, FIRST_CREATE_TIME, FIRST_CREATE_OPER,
           LAST_UPDATE_DATE, LAST_UPDATE_TIME, LAST_UPDATE_OPER,
           LONG_NAME, ENTITY_TYPE, BNM_ASSIGNED_ID,
           REGISTRATION_DATE, MSIC2008, RESIDENCY_STATUS,
           CORPORATE_STATUS, CUSTOMER_CODE, CITIZENSHIP,
           ADDR_LINE_1, ADDR_LINE_2, ADDR_LINE_3, ADDR_LINE_4, ADDR_LINE_5,
           POSTCODE, TOWN_CITY, STATE_CODE, COUNTRY,
           ADDR_LAST_UPDATE, ADDR_LAST_UPTIME,
           PHONE_PRIMARY, PHONE_SECONDARY, PHONE_FAX, PHONE_MOBILE, PHONE_PAC,
           ENABLE_TAB
    FROM org_df
    ORDER BY CISNO, IDTYPE, ID
""").arrow()
print("ORGANISATION FILE")
print(org.to_pandas().head(5))


# MERGE ORG + CART
custinfo_org = con.execute("""
    SELECT o.*, c.APPL_CODE, c.APPL_NO, c.PRI_SEC, c.RELATIONSHIP,
           c.AA_REF_NO, c.EFF_DATE, c.EFF_TIME, c.LAST_MNT_DATE, c.LAST_MNT_TIME
    FROM org o
    LEFT JOIN cart c
    ON o.CISNO = c.CISNO AND o.IDTYPE = c.IDTYPE AND o.ID = c.ID
""").arrow()
print("CUSTINFO (ORG)")
print(custinfo_org.to_pandas().head(5))


# MERGE MAIN (ORG) + CUSTINFO (ORG)
orgdly = con.execute("""
    SELECT DISTINCT m.CISNO, m.BANKNO, m.MAIN_ENTITY_TYPE, m.BRANCH,
           m.CUSTNAME, m.BIRTHDATE, m.GENDER,
           co.*
    FROM main_org m
    INNER JOIN custinfo_org co ON m.CISNO = co.CISNO
    ORDER BY m.CISNO
""").arrow()
print("FINAL ORGANISATION DATASET")
print(orgdly.to_pandas().head(5))


#-------------------------------------------------#
# Part 3 - Output with PyArrow                    #
#-------------------------------------------------#

# Write INDVDLY
pq.write_table(indvdly, "cis_internal/output/INDVDLY.parquet")
with open("cis_internal/output/INDVDLY.csv", "wb") as f:
    pacsv.write_csv(indvdly, f)

# Write ORGDLY
pq.write_table(orgdly, "cis_internal/output/ORGDLY.parquet")
with open("cis_internal/output/ORGDLY.csv", "wb") as f:
    pacsv.write_csv(orgdly, f)

