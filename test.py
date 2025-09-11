import duckdb
import pyarrow.parquet as pq
import pyarrow.csv as pc
from reader import parquet_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

#---------------------------------------------------------------------#
# Original Program: CCRNIDIC                                          #
#---------------------------------------------------------------------#
#-TO MERGE ACCOUNT, NAME AND ALIAS INTO ONE DAILY FILE. INCL:UNICARD  #
# ESMR 2019-1394 TO EXTRACT DATA FROM IDIC FOR REPORTING PURPOSES     #
# ESMR 2018-512                                                       #
# ESMR 2018-911                                                       #
# ESMR 2018-1851                                                      #
#---------------------------------------------------------------------#
# Migration Timeline                                                  #
# 8Sep2025  - Done convert to python                                  #
# 10Sep2025 - Add DuckDB and PyArrow                                  #
#---------------------------------------------------------------------#

#------------------------#
# READ PARQUET DATASETS  #
#------------------------#
con = duckdb.connect()

#-----------------------------------------------#
# Part 1 - Process Individual Part              #
#-----------------------------------------------#

# MAIN (INDIVIDUAL)
con.execute(f"""
    CREATE VIEW main_indv AS
    SELECT CISNO, MAIN_ENTITY_TYPE, BRANCH,
           CUSTNAME, BIRTHDATE, GENDER
    FROM '{parquet_path("CIDICUST_FB.parquet")}'
    WHERE GENDER <> 'O'
    ORDER BY CISNO
""")


# INDIVIDUAL FILE
con.execute(f"""
    CREATE VIEW indv AS               
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
    FROM '{parquet_path("CIDINDVT_FB.parquet")}'
    ORDER BY CISNO, IDTYPE, ID
""")


# CART FILE
cart = con.execute(f"""
    CREATE VIEW cart AS
    SELECT APPL_CODE, APPL_NO, PRI_SEC, RELATIONSHIP,
           LPAD(CAST(CUSTNO AS VARCHAR), 11, '0') AS CISNO,
           IDTYPE, ID, AA_REF_NO, EFF_DATE,
           EFF_TIME, LAST_MNT_DATE, LAST_MNT_TIME
    FROM '{parquet_path("CIDICART_FB.parquet")}'
    ORDER BY CISNO, IDTYPE, ID
""")


# MERGE INDIVIDUAL + CART
con.execute("""
    CREATE VIEW custinfo_indv AS
    SELECT i.*, c.APPL_CODE, c.APPL_NO, c.PRI_SEC, c.RELATIONSHIP,
           c.AA_REF_NO, c.EFF_DATE, c.EFF_TIME, c.LAST_MNT_DATE, c.LAST_MNT_TIME
    FROM indv i
    LEFT JOIN cart c
    ON i.CISNO = c.CISNO AND i.IDTYPE = c.IDTYPE AND i.ID = c.ID
""")


# MERGE MAIN + CUSTINFO (INDIVIDUAL)
indvdly = """
    SELECT DISTINCT m.CISNO, m.MAIN_ENTITY_TYPE, m.BRANCH,
           m.CUSTNAME, m.BIRTHDATE, m.GENDER,
           ci.*
           ,{year} AS year
           ,{month} AS month
           ,{day} AS day
    FROM main_indv m
    INNER JOIN custinfo_indv ci ON m.CISNO = ci.CISNO
    ORDER BY m.CISNO
""".format(year=year,month=month,day=day)

#final_indvdly = con.execute(indvdly).arrow()

#-------------------------------------------------#
# Part 2 - Process Organisation Part              #
#-------------------------------------------------#

# MAIN (ORGANISATION)
con.execute(f"""
    CREATE VIEW main_org AS
    SELECT CISNO, MAIN_ENTITY_TYPE, BRANCH,
           CUSTNAME, BIRTHDATE, GENDER
    FROM '{parquet_path("CIDICUST_FB.parquet")}'
    WHERE GENDER = 'O'
    ORDER BY CISNO
""")


# ORGANISATION FILE
con.execute(f"""
    CREATE VIEW org AS
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
    FROM '{parquet_path("CIDIORGT_FB.parquet")}'
    ORDER BY CISNO, IDTYPE, ID
""")


# MERGE ORG + CART
con.execute("""
    CREATE VIEW custinfo_org AS
    SELECT o.*, c.APPL_CODE, c.APPL_NO, c.PRI_SEC, c.RELATIONSHIP,
           c.AA_REF_NO, c.EFF_DATE, c.EFF_TIME, c.LAST_MNT_DATE, c.LAST_MNT_TIME
    FROM org o
    LEFT JOIN cart c
    ON o.CISNO = c.CISNO AND o.IDTYPE = c.IDTYPE AND o.ID = c.ID
""")


# MERGE MAIN (ORG) + CUSTINFO (ORG)
orgdly = """
    SELECT DISTINCT m.CISNO, m.MAIN_ENTITY_TYPE, m.BRANCH,
           m.CUSTNAME, m.BIRTHDATE, m.GENDER,
           co.*
           ,{year} AS year
           ,{month} AS month
           ,{day} AS day
    FROM main_org m
    INNER JOIN custinfo_org co ON m.CISNO = co.CISNO
    ORDER BY m.CISNO
""".format(year=year,month=month,day=day)

#final_orgdly = con.execute(orgdly).arrow()

#-------------------------------------------------#
# Part 3 - Output with PyArrow                    #
#-------------------------------------------------#

# Write INDVDLY
#pq.write_table(final_indvdly, "/host/cis/parquet/INDVDLY.parquet")
# Write Hive-Partition Parquet
con.execute(f"""
COPY ({indvdly})
TO 'output/cis_internal/CIS_IDIC_DAILY_INDVDLY'
(FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
""")

final_indvdly = con.execute(indvdly).arrow()

pc.write_csv(final_indvdly, "Programmer/jkh/output/CIS_IDIC_DAILY_INDVDLY.csv")

# Write ORGDLY
#pq.write_table(final_orgdly, "/host/cis/parquet/ORGDLY.parquet")
# Write Hive-Partition Parquet
con.execute(f"""
COPY ({orgdly})
TO 'output/cis_internal/CIS_IDIC_DAILY_ORGDLY'
(FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);
""")

final_orgdly = con.execute(orgdly).arrow()

pc.write_csv(final_orgdly, "Programmer/jkh/output/CIS_IDIC_DAILY_ORGDLY.csv")
