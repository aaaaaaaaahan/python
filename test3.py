import duckdb
from CIS_PY_READER_PROD import host_parquet_path,parquet_output_path,csv_output_path
import datetime

#---------------------------------------------------------------------#
# Original Program: CIAMLHRC                                          #
#---------------------------------------------------------------------#
# ESMR2024-1681                                                       #
# -MASS CLOSE ODD ALERTS GENERATED ON CIS WITH HRC 002 AND 011 ONLY   #
#                                                                     #
# NOTE !!!  OUTPUT  TO 'CISU' IF FOR UAT                              #
# NOTE !!!  OUTPUT  TO 'CIS' IF FOR PRODUCTION                        #
#---------------------------------------------------------------------#

# =========================
#   DATE HANDLING
# =========================
batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year1, month1, day1 = batch_date.year, batch_date.month, batch_date.day

# =========================
#   CONNECT TO DUCKDB
# =========================
con = duckdb.connect()

# =========================
#   TRANSFORMATION IN SQL
# =========================
custout = con.execute(f"""
WITH base AS (
    SELECT CUSTNO,
           LPAD(CAST(CAST(HRC01 AS INTEGER) AS VARCHAR), 3, '0') AS HRC01,
           LPAD(CAST(CAST(HRC02 AS INTEGER) AS VARCHAR), 3, '0') AS HRC02,
           LPAD(CAST(CAST(HRC03 AS INTEGER) AS VARCHAR), 3, '0') AS HRC03,
           LPAD(CAST(CAST(HRC04 AS INTEGER) AS VARCHAR), 3, '0') AS HRC04,
           LPAD(CAST(CAST(HRC05 AS INTEGER) AS VARCHAR), 3, '0') AS HRC05,
           LPAD(CAST(CAST(HRC06 AS INTEGER) AS VARCHAR), 3, '0') AS HRC06,
           LPAD(CAST(CAST(HRC07 AS INTEGER) AS VARCHAR), 3, '0') AS HRC07,
           LPAD(CAST(CAST(HRC08 AS INTEGER) AS VARCHAR), 3, '0') AS HRC08,
           LPAD(CAST(CAST(HRC09 AS INTEGER) AS VARCHAR), 3, '0') AS HRC09,
           LPAD(CAST(CAST(HRC10 AS INTEGER) AS VARCHAR), 3, '0') AS HRC10,
           LPAD(CAST(CAST(HRC11 AS INTEGER) AS VARCHAR), 3, '0') AS HRC11,
           LPAD(CAST(CAST(HRC12 AS INTEGER) AS VARCHAR), 3, '0') AS HRC12,
           LPAD(CAST(CAST(HRC13 AS INTEGER) AS VARCHAR), 3, '0') AS HRC13,
           LPAD(CAST(CAST(HRC14 AS INTEGER) AS VARCHAR), 3, '0') AS HRC14,
           LPAD(CAST(CAST(HRC15 AS INTEGER) AS VARCHAR), 3, '0') AS HRC15,
           LPAD(CAST(CAST(HRC16 AS INTEGER) AS VARCHAR), 3, '0') AS HRC16,
           LPAD(CAST(CAST(HRC17 AS INTEGER) AS VARCHAR), 3, '0') AS HRC17,
           LPAD(CAST(CAST(HRC18 AS INTEGER) AS VARCHAR), 3, '0') AS HRC18,
           LPAD(CAST(CAST(HRC19 AS INTEGER) AS VARCHAR), 3, '0') AS HRC19,
           LPAD(CAST(CAST(HRC20 AS INTEGER) AS VARCHAR), 3, '0') AS HRC20,
           '{batch_date.strftime("%Y%m%d")}' AS CUSTMNTDATE
    FROM '{host_parquet_path("ALLCUST_FB.parquet")}'
),
flags AS (
    SELECT *,
        -- HRC002 flag
        CASE WHEN (
            HRC01 = '002' OR HRC02 = '002' OR HRC03 = '002' OR HRC04 = '002' OR
            HRC05 = '002' OR HRC06 = '002' OR HRC07 = '002' OR HRC08 = '002' OR
            HRC09 = '002' OR HRC10 = '002' OR HRC11 = '002' OR HRC12 = '002' OR
            HRC13 = '002' OR HRC14 = '002' OR HRC15 = '002' OR HRC16 = '002' OR
            HRC17 = '002' OR HRC18 = '002' OR HRC19 = '002' OR HRC20 = '002'
        ) THEN 'Y' ELSE ' ' END AS HRC002,

        -- HRC011 flag
        CASE WHEN (
            HRC01 = '011' OR HRC02 = '011' OR HRC03 = '011' OR HRC04 = '011' OR
            HRC05 = '011' OR HRC06 = '011' OR HRC07 = '011' OR HRC08 = '011' OR
            HRC09 = '011' OR HRC10 = '011' OR HRC11 = '011' OR HRC12 = '011' OR
            HRC13 = '011' OR HRC14 = '011' OR HRC15 = '011' OR HRC16 = '011' OR
            HRC17 = '011' OR HRC18 = '011' OR HRC19 = '011' OR HRC20 = '011'
        ) THEN 'Y' ELSE ' ' END AS HRC011,

        -- HRC999 flag
        CASE WHEN (
            HRC01 NOT IN ('000','002','011') OR
            HRC02 NOT IN ('000','002','011') OR
            HRC03 NOT IN ('000','002','011') OR
            HRC04 NOT IN ('000','002','011') OR
            HRC05 NOT IN ('000','002','011') OR
            HRC06 NOT IN ('000','002','011') OR
            HRC07 NOT IN ('000','002','011') OR
            HRC08 NOT IN ('000','002','011') OR
            HRC09 NOT IN ('000','002','011') OR
            HRC10 NOT IN ('000','002','011') OR
            HRC11 NOT IN ('000','002','011') OR
            HRC12 NOT IN ('000','002','011') OR
            HRC13 NOT IN ('000','002','011') OR
            HRC14 NOT IN ('000','002','011') OR
            HRC15 NOT IN ('000','002','011') OR
            HRC16 NOT IN ('000','002','011') OR
            HRC17 NOT IN ('000','002','011') OR
            HRC18 NOT IN ('000','002','011') OR
            HRC19 NOT IN ('000','002','011') OR
            HRC20 NOT IN ('000','002','011')
        ) THEN 'Y' ELSE ' ' END AS HRC999,

        -- HRC002O flag
        CASE WHEN (
            HRC01 NOT IN ('000','002') OR
            HRC02 NOT IN ('000','002') OR
            HRC03 NOT IN ('000','002') OR
            HRC04 NOT IN ('000','002') OR
            HRC05 NOT IN ('000','002') OR
            HRC06 NOT IN ('000','002') OR
            HRC07 NOT IN ('000','002') OR
            HRC08 NOT IN ('000','002') OR
            HRC09 NOT IN ('000','002') OR
            HRC10 NOT IN ('000','002') OR
            HRC11 NOT IN ('000','002') OR
            HRC12 NOT IN ('000','002') OR
            HRC13 NOT IN ('000','002') OR
            HRC14 NOT IN ('000','002') OR
            HRC15 NOT IN ('000','002') OR
            HRC16 NOT IN ('000','002') OR
            HRC17 NOT IN ('000','002') OR
            HRC18 NOT IN ('000','002') OR
            HRC19 NOT IN ('000','002') OR
            HRC20 NOT IN ('000','002')
        ) THEN 'Y' ELSE ' ' END AS HRC0020
    FROM base
)
SELECT * FROM flags
ORDER BY CUSTNO
""").arrow()

# =========================
#   OUTPUTS
# =========================
#df = custout.to_pandas()

# MASSCLS
masscls = """
    SELECT 
        'CIS' AS PREFIX, 
        CUSTNO
        ,{year1} AS year
        ,{month1} AS month
        ,{day1} AS day
    FROM custout
    WHERE HRC002='Y' AND HRC011='Y' AND HRC999=' '
""".format(year1=year1,month1=month1,day1=day1)
#pq.write_table(masscls, "AMLHRC_EXTRACT_MASSCLS.parquet")

# MASSCLS Bank Staff
masscls_bnk = """
    SELECT 
        'CIS' AS PREFIX, 
        CUSTNO
        ,{year1} AS year
        ,{month1} AS month
        ,{day1} AS day
    FROM custout
    WHERE HRC002='Y' AND HRC0020=' '
""".format(year1=year1,month1=month1,day1=day1)
#pq.write_table(masscls_bnk, "AMLHRC_EXTRACT_MASSCLS_BNKSTFF.parquet")

# Verification
verify = """
    SELECT CUSTNO,
           HRC01,HRC02,HRC03,HRC04,HRC05,HRC06,HRC07,HRC08,HRC09,HRC10,
           HRC11,HRC12,HRC13,HRC14,HRC15,HRC16,HRC17,HRC18,HRC19,HRC20,
           HRC002,HRC011,HRC999,HRC0020
           ,{year1} AS year
           ,{month1} AS month
           ,{day1} AS day
    FROM custout
""".format(year1=year1,month1=month1,day1=day1)
#pq.write_table(verify, "AMLHRC_EXTRACT_VERIFY.parquet")

# =========================
#   Output
# =========================
queries = {
    "AMLHRC_EXTRACT_MASSCLS"            : masscls,
    "AMLHRC_EXTRACT_MASSCLS_BNKSTFF"    : masscls_bnk,
    "AMLHRC_EXTRACT_VERIFY"             : verify
}

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    con.execute(f"""
    COPY ({query})
    TO '{parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);  
     """)
    
    con.execute(f"""
    COPY ({query})
    TO '{csv_path}'
    (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);  
     """)
