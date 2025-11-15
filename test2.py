import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

# ---------------------------------------------------------
# 1. Load CIHRCRVT parquet (already converted AS400 FB file)
# ---------------------------------------------------------
con = duckdb.connect()

df = con.execute(f"""
    SELECT
        HRV_MONTH,
        HRV_BRCH_CODE,
        HRV_ACCT_TYPE,
        HRV_ACCT_NO,
        HRV_CUSTNO,
        HRV_CUSTID,
        HRV_CUST_NAME,
        HRV_NATIONALITY,
        HRV_ACCT_OPENDATE,
        HRV_OVERRIDING_INDC,
        HRV_OVERRIDING_OFFCR,
        HRV_OVERRIDING_REASON,
        HRV_DOWJONES_INDC,
        HRV_FUZZY_INDC,
        HRV_FUZZY_SCORE,
        HRV_NOTED_BY,
        HRV_RETURNED_BY,
        HRV_ASSIGNED_TO,
        HRV_NOTED_DATE,
        HRV_RETURNED_DATE,
        HRV_ASSIGNED_DATE,
        HRV_COMMENT_BY,
        HRV_COMMENT_DATE,
        HRV_SAMPLING_INDC,
        HRV_RETURN_STATUS,
        HRV_RECORD_STATUS,
        HRV_FUZZY_SCREEN_DATE
    FROM read_parquet({host_parquet_path("UNLOAD_CIHRCRVT_FB")})
    ORDER BY
        HRV_MONTH,
        HRV_BRCH_CODE,
        HRV_ACCT_TYPE,
        HRV_ACCT_NO,
        HRV_CUSTNO
""").arrow()

# ---------------------------------------------------------
# 2. Save sorted output to Parquet
# ---------------------------------------------------------
output_parquet = parquet_output_path("UNLOAD_CIHRCRVT_EXCEL.parquet")
pq.write_table(df, output_parquet)

# ---------------------------------------------------------
# 3. Write TXT output (with header + | delimiter)
# ---------------------------------------------------------
output_txt = csv_output_path("UNLOAD_CIHRCRVT_EXCEL.txt")

header = [
    "DETAIL LISTING FOR CIHRCRVT",
    "MONTH|BRCH_CODE|ACCT_TYPE|ACCT_NO|CUSTNO|CUSTID|CUST_NAME|"
    "NATIONALITY|ACCT_OPENDATE|OVERRIDING_INDC|OVERRIDING_OFFCR|"
    "OVERRIDING_REASON|DOWJONES_INDC|FUZZY_INDC|FUZZY_SCORE|"
    "NOTED_BY|RETURNED_BY|ASSIGNED_TO|NOTED_DATE|RETURNED_DATE|"
    "ASSIGNED_DATE|COMMENT_BY|COMMENT_DATE|SAMPLING_INDC|RETURN_STATUS|"
    "RECORD_STATUS|FUZZY_SCREEN_DATE"
]

with open(output_txt, "w", encoding="utf-8") as f:
    for line in header:
        f.write(line + "\n")

    for batch in df.to_batches():
        arr = batch.to_pylist()
        for row in arr:
            row_values = [str(row[col] or "") for col in df.schema.names]
            f.write("|".join(row_values) + "\n")

print("Completed: Parquet + TXT generated.")
