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

# =========================
#   LOAD INPUT FILES
# =========================
custfile_path = "UNLOAD_ALLCUST_FB.parquet"  # assumed parquet already

cust = con.execute(f"""
    SELECT *,
           CAST(HRC01 AS VARCHAR) as HRC01,
           CAST(HRC02 AS VARCHAR) as HRC02,
           CAST(HRC03 AS VARCHAR) as HRC03,
           CAST(HRC04 AS VARCHAR) as HRC04,
           CAST(HRC05 AS VARCHAR) as HRC05,
           CAST(HRC06 AS VARCHAR) as HRC06,
           CAST(HRC07 AS VARCHAR) as HRC07,
           CAST(HRC08 AS VARCHAR) as HRC08,
           CAST(HRC09 AS VARCHAR) as HRC09,
           CAST(HRC10 AS VARCHAR) as HRC10,
           CAST(HRC11 AS VARCHAR) as HRC11,
           CAST(HRC12 AS VARCHAR) as HRC12,
           CAST(HRC13 AS VARCHAR) as HRC13,
           CAST(HRC14 AS VARCHAR) as HRC14,
           CAST(HRC15 AS VARCHAR) as HRC15,
           CAST(HRC16 AS VARCHAR) as HRC16,
           CAST(HRC17 AS VARCHAR) as HRC17,
           CAST(HRC18 AS VARCHAR) as HRC18,
           CAST(HRC19 AS VARCHAR) as HRC19,
           CAST(HRC20 AS VARCHAR) as HRC20
    FROM read_parquet('{custfile_path}')
""").arrow()

# =========================
#   TRANSFORM LOGIC
# =========================
df = cust.to_pandas()

# Build CUSTMNTDATE using batch_date (yesterday)
df["CUSTMNTDATE"] = batch_date.strftime("%Y%m%d")

# Flags HRC002, HRC011, HRC999, HRC002O
def flag_002(row):
    return "Y" if any(str(row[f"HRC{i}"]).zfill(3) == "002" for i in range(1, 21)) else " "

def flag_011(row):
    return "Y" if any(str(row[f"HRC{i}"]).zfill(3) == "011" for i in range(1, 21)) else " "

def flag_999(row):
    return "Y" if any(str(row[f"HRC{i}"]).zfill(3) not in ("000","002","011") for i in range(1, 21)) else " "

def flag_002O(row):
    return "Y" if any(str(row[f"HRC{i}"]).zfill(3) not in ("000","002") for i in range(1, 21)) else " "

df["HRC002"]  = df.apply(flag_002, axis=1)
df["HRC011"]  = df.apply(flag_011, axis=1)
df["HRC999"]  = df.apply(flag_999, axis=1)
df["HRC002O"] = df.apply(flag_002O, axis=1)

# Sort by CUSTNO
df = df.sort_values("CUSTNO")

# =========================
#   OUTPUT FILES
# =========================

# 1. MASSCLS output (CUSTCLS)
masscls = df[(df["HRC002"]=="Y") & (df["HRC011"]=="Y") & (df["HRC999"]==" ")]
masscls_out = masscls[["CUSTNO"]].copy()
masscls_out.insert(0,"PREFIX","CIS")
pq.write_table(pa.Table.from_pandas(masscls_out), "AMLHRC_EXTRACT_MASSCLS.parquet")

# 2. MASSCLS Bank Staff output (CUSTBNK)
masscls_bnk = df[(df["HRC002"]=="Y") & (df["HRC002O"]==" ")]
masscls_bnk_out = masscls_bnk[["CUSTNO"]].copy()
masscls_bnk_out.insert(0,"PREFIX","CIS")
pq.write_table(pa.Table.from_pandas(masscls_bnk_out), "AMLHRC_EXTRACT_MASSCLS_BNKSTFF.parquet")

# 3. Verification output (CUSTVRY)
verify_out = df[["CUSTNO"] + [f"HRC{i}" for i in range(1,21)] + ["HRC002","HRC011","HRC999","HRC002O"]]
pq.write_table(pa.Table.from_pandas(verify_out), "AMLHRC_EXTRACT_VERIFY.parquet")

# =========================
#   PRINT SAMPLE
# =========================
print("=== SAMPLE CUSTOUT ===")
print(df.head(5))
