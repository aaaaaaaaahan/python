import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import datetime
import os
import sys

# =====================================
# CONFIGURATION
# =====================================
input_parquet = "/host/cis/parquet/KWSP.EMPLOYER.FILE.parquet"
output_csv = "/host/cis/output/KWSP.EMPLOYER.FILE.LOAD.csv"

# =====================================
# PROCESS WITH DUCKDB
# =====================================
con = duckdb.connect()

# Read input Parquet file
df = con.execute(f"SELECT * FROM read_parquet('{input_parquet}')").fetchdf()

# Ensure correct column names (match SAS logic)
# Assuming the parquet already has these fields, else adjust accordingly
df["REC_ID"] = df["REC_ID"].astype(str).str.strip()
df["EMPLYR_NO"] = df["EMPLYR_NO"].fillna(0).astype(int)
df["EMPLYR_NAME1"] = df["EMPLYR_NAME1"].fillna("").astype(str).str.strip()

# =====================================
# VALIDATION (simulate SAS ABORT)
# =====================================
if df["REC_ID"].isnull().any() or (df["REC_ID"] == "").any():
    sys.exit("ABORT 111: REC_ID missing")

if ((df["REC_ID"] == "01") & ((df["EMPLYR_NO"] == 0) | (df["EMPLYR_NAME1"] == ""))).any():
    sys.exit("ABORT 222: Missing Employer Info for REC_ID=01")

# Check last record
last_rec = df.iloc[-1]
if last_rec["REC_ID"] != "02":
    sys.exit("ABORT 333: Last record is not 02")

# Check total count consistency (simulate SAS X counter)
x_count = (df["REC_ID"] == "01").sum()
if last_rec.get("TOTAL_REC", x_count) != x_count:
    sys.exit("ABORT 444: TOTAL_REC mismatch")

# =====================================
# DERIVED & FIXED FIELDS
# =====================================
df["IND_ORG"] = "O"
df["ROB_ROC"] = " "
df["EMPLYR_NAME2"] = " "

# Clean up CR characters
df["EMPLYR_NAME1"] = df["EMPLYR_NAME1"].str.replace("\r", "", regex=False)
df["EMPLYR_NAME2"] = df["EMPLYR_NAME2"].str.replace("\r", "", regex=False)

# =====================================
# OUTPUT WITH PYARROW
# =====================================
# Select only output columns in correct order
out_cols = [
    "REC_ID",
    "IND_ORG",
    "EMPLYR_NO",
    "ROB_ROC",
    "EMPLYR_NAME1",
    "EMPLYR_NAME2",
]

# Convert to Arrow Table
table = pa.Table.from_pandas(df[out_cols])

# Write to CSV (similar to SAS FILE OUTFILE)
csv.write_csv(table, output_csv)

print(f"✅ KWSP processing complete: {output_csv}")
