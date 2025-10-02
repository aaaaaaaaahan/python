import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import datetime

# =========================
#   DATE HANDLING
# =========================
today = datetime.date.today()
batch_date = today - datetime.timedelta(days=1)

today_year, today_month, today_day = today.year, today.month, today.day
batch_year, batch_month, batch_day = batch_date.year, batch_date.month, batch_date.day

print("Today:", today)
print("Batch Date:", batch_date)

# =========================
#   CONNECT TO DUCKDB
# =========================
con = duckdb.connect()

custfile_path = "UNLOAD_ALLCUST_FB.parquet"

# =========================
#   TRANSFORMATION IN SQL
# =========================
custout = con.execute(f"""
WITH base AS (
    SELECT *,
           '{batch_date.strftime("%Y%m%d")}' AS CUSTMNTDATE
    FROM read_parquet('{custfile_path}')
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
        ) THEN 'Y' ELSE ' ' END AS HRC002O
    FROM base
)
SELECT * FROM flags
ORDER BY CUSTNO
""").arrow()

# =========================
#   OUTPUTS
# =========================
df = custout.to_pandas()

# MASSCLS
masscls = con.execute("""
    SELECT 'CIS' AS PREFIX, CUSTNO
    FROM custout
    WHERE HRC002='Y' AND HRC011='Y' AND HRC999=' '
""").arrow()
pq.write_table(masscls, "AMLHRC_EXTRACT_MASSCLS.parquet")

# MASSCLS Bank Staff
masscls_bnk = con.execute("""
    SELECT 'CIS' AS PREFIX, CUSTNO
    FROM custout
    WHERE HRC002='Y' AND HRC002O=' '
""").arrow()
pq.write_table(masscls_bnk, "AMLHRC_EXTRACT_MASSCLS_BNKSTFF.parquet")

# Verification
verify = con.execute("""
    SELECT CUSTNO,
           HRC01,HRC02,HRC03,HRC04,HRC05,HRC06,HRC07,HRC08,HRC09,HRC10,
           HRC11,HRC12,HRC13,HRC14,HRC15,HRC16,HRC17,HRC18,HRC19,HRC20,
           HRC002,HRC011,HRC999,HRC002O
    FROM custout
""").arrow()
pq.write_table(verify, "AMLHRC_EXTRACT_VERIFY.parquet")

# =========================
#   SAMPLE PRINT
# =========================
print(df.head(5))
