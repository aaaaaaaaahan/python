import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow.compute as pc
import datetime
import os

# ============================================================
#  BATCH DATE SETUP (Use yesterday’s date)
# ============================================================
batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year1, month1, day1 = batch_date.year, batch_date.month, batch_date.day

# ============================================================
#  PATH CONFIGURATION
# ============================================================
input_parquet = f"/host/cis/parquet/sas_parquet/CIDOWJ1T_{year1}{month1:02d}{day1:02d}.parquet"
output_csv_path = f"/host/cis/output/AMLA_DOWJONE_EXTRACT_{year1}{month1:02d}{day1:02d}.csv"

# Ensure output directory exists
os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)

# ============================================================
#  DUCKDB PROCESSING
# ============================================================
con = duckdb.connect()

# Load input parquet
df = con.execute(f"""
    SELECT 
        DJ_NAME,
        DJ_ID_NO,
        DJ_PERSON_ID,
        DJ_IND_ORG,
        DJ_DESC1,
        DJ_DOB_DOR,
        DJ_NAME_TYPE,
        DJ_ID_TYPE,
        DJ_DATE_TYPE,
        DJ_GENDER,
        DJ_SANCTION_INDC,
        DJ_OCCUP_INDC,
        DJ_RLENSHIP_INDC,
        DJ_OTHER_LIST_INDC,
        DJ_ACTIVE_STATUS,
        DJ_CITIZENSHIP
    FROM read_parquet('{input_parquet}')
""").arrow()

# ============================================================
#  SORT BY DJ_ID_NO
# ============================================================
df = df.sort_by([("DJ_ID_NO", "ascending")])

# ============================================================
#  CREATE OUTPUT STRING COLUMN
#  Combine all fields with '|' separator
# ============================================================
combined = pc.binary_join_element_wise(
    df["DJ_NAME"],
    pa.scalar("|"), df["DJ_ID_NO"],
    pa.scalar("|"), df["DJ_PERSON_ID"],
    pa.scalar("|"), df["DJ_IND_ORG"],
    pa.scalar("|"), df["DJ_DESC1"],
    pa.scalar("|"), df["DJ_DOB_DOR"],
    pa.scalar("|"), df["DJ_NAME_TYPE"],
    pa.scalar("|"), df["DJ_ID_TYPE"],
    pa.scalar("|"), df["DJ_DATE_TYPE"],
    pa.scalar("|"), df["DJ_GENDER"],
    pa.scalar("|"), df["DJ_SANCTION_INDC"],
    pa.scalar("|"), df["DJ_OCCUP_INDC"],
    pa.scalar("|"), df["DJ_RLENSHIP_INDC"],
    pa.scalar("|"), df["DJ_OTHER_LIST_INDC"],
    pa.scalar("|"), df["DJ_ACTIVE_STATUS"],
    pa.scalar("|"), df["DJ_CITIZENSHIP"]
)

# ============================================================
#  CONVERT TO ARROW TABLE FOR OUTPUT
# ============================================================
output_table = pa.Table.from_arrays([combined], names=["OUTPUT_LINE"])

# ============================================================
#  WRITE TO CSV USING PYARROW
# ============================================================
csv.write_csv(output_table, output_csv_path)

# ============================================================
#  DISPLAY FIRST 5 RECORDS
# ============================================================
print("OUTPUT (First 5 Records):")
for row in output_table.slice(0, 5).column(0):
    print(row.as_py())

print(f"\n✅ CSV output generated at: {output_csv_path}")
