import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# -----------------------------
# File paths
# -----------------------------
input_parquet = "RBP2.B033.UNLOAD.PRIMNAME.FB.parquet"
output_del_parquet = "CIS_MISSING_NAMEKEY_DEL.parquet"
output_ins_parquet = "CIS_MISSING_NAMEKEY_INS.parquet"
output_del_txt = "CIS_MISSING_NAMEKEY_DEL.txt"
output_ins_txt = "CIS_MISSING_NAMEKEY_INS.txt"

# -----------------------------
# Connect DuckDB
# -----------------------------
con = duckdb.connect()

# -----------------------------
# Read input parquet into DuckDB table
# -----------------------------
con.execute(f"""
CREATE OR REPLACE TABLE name AS
SELECT *
FROM parquet_scan('{input_parquet}')
""")

# -----------------------------
# Add KF1-KF4 by splitting NAME_LINE
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE name_insert AS
SELECT *,
       coalesce(NULLIF(split_part(NAME_LINE, ' ', 1), ''), '') AS KF1,
       coalesce(NULLIF(split_part(NAME_LINE, ' ', 2), ''), '') AS KF2,
       coalesce(NULLIF(split_part(NAME_LINE, ' ', 3), ''), '') AS KF3,
       coalesce(NULLIF(split_part(NAME_LINE, ' ', 4), ''), '') AS KF4,
       CASE WHEN PARSE_IND IS NULL OR PARSE_IND = '' THEN 'C' ELSE PARSE_IND END AS PARSE_IND_NEW
FROM name
""")

# -----------------------------
# Create deletion table (KEY_FIELD_1 is null or empty)
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE to_delete AS
SELECT *
FROM name
WHERE KEY_FIELD_1 IS NULL OR KEY_FIELD_1 = ''
ORDER BY CUSTNO
""")

# -----------------------------
# Create insertion table
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE to_insert AS
SELECT *
FROM name_insert
ORDER BY CUSTNO
""")

# -----------------------------
# Export to Parquet
# -----------------------------
con.execute(f"COPY to_delete TO '{output_del_parquet}' (FORMAT PARQUET)")
con.execute(f"COPY to_insert TO '{output_ins_parquet}' (FORMAT PARQUET)")

# -----------------------------
# Export to fixed-width TXT
# -----------------------------
def export_fixed_width(query, filepath):
    df = con.execute(query).df()
    with open(filepath, 'w', encoding='utf-8') as f:
        for _, row in df.iterrows():
            f.write(
                f"{row['HOLD_CO_NO']:02d}"
                f"{row['BANK_NO']:02d}"
                f"{row['CUSTNO']:<20}"
                f"{row['REC_TYPE']:02d}"
                f"{row['REC_SEQ']:02d}"
                f"{row['EFF_DATE']:05d}"
                f"{row['PROCESS_TIME']:<8}"
                f"{row['ADR_HOLD_CO_NO']:02d}"
                f"{row['ADR_BANK_NO']:02d}"
                f"{row['ADR_REF_NO']:06d}"
                f"{row['CUST_TYPE']:<1}"
                f"{row.get('KEY_FIELD_1',''):<15}"
                f"{row.get('KEY_FIELD_2',''):<10}"
                f"{row.get('KEY_FIELD_3',''):<5}"
                f"{row.get('KEY_FIELD_4',''):<5}"
                f"{row.get('LINE_CODE',''):<1}"
                f"{row.get('NAME_LINE',''):<40}"
                f"{row.get('LINE_CODE_1',''):<1}"
                f"{row.get('NAME_TITLE_1',''):<40}"
                f"{row.get('LINE_CODE_2',''):<1}"
                f"{row.get('NAME_TITLE_2',''):<40}"
                f"{row.get('SALUTATION',''):<40}"
                f"{row.get('TITLE_CODE',0):02d}"
                f"{row.get('FIRST_MID',''):<30}"
                f"{row.get('SURNAME',''):<20}"
                f"{row.get('SURNAME_KEY',''):<3}"
                f"{row.get('SUFFIX_CODE',0):02d}"
                f"{row.get('APPEND_CODE',0):02d}"
                f"{row.get('PRIM_PHONE',0):06d}"
                f"{row.get('P_PHONE_LTH',0):02d}"
                f"{row.get('SEC_PHONE',0):06d}"
                f"{row.get('S_PHONE_LTH',0):02d}"
                f"{row.get('TELEX_PHONE',0):06d}"
                f"{row.get('T_PHONE_LTH',0):02d}"
                f"{row.get('FAX_PHONE',0):06d}"
                f"{row.get('F_PHONE_LTH',0):02d}"
                f"{row.get('LAST_CHANGE',''):<10}"
                f"{row.get('PARSE_IND_NEW',''):<1}\n"
            )

# Export TXT files
export_fixed_width("SELECT * FROM to_delete", output_del_txt)
export_fixed_width("SELECT * FROM to_insert", output_ins_txt)

print("DuckDB processing completed successfully!")
