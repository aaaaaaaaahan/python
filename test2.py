import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path

# Paths
name_parquet_path = Path("UNLOAD.PRIMNAME.OUT.parquet")  # NAMEFILE
rmrk_parquet_path = Path("CISRMRK.LONGNAME.parquet")    # RMRKFILE
output_txt_path = Path("CIS_LONGNAME_NONE.txt")
output_parquet_path = Path("CIS_LONGNAME_NONE.parquet")

# Connect to DuckDB in-memory
con = duckdb.connect(database=':memory:')

# Read input parquet files into DuckDB tables
con.execute(f"""
    CREATE TABLE NAME AS 
    SELECT * FROM read_parquet('{name_parquet_path}')
""")

con.execute(f"""
    CREATE TABLE RMRK AS 
    SELECT * FROM read_parquet('{rmrk_parquet_path}')
    WHERE RMK_LINE_1 IS NOT NULL AND RMK_LINE_1 != ''
""")

# Remove duplicates by CUSTNO
con.execute("CREATE TABLE NAME_UQ AS SELECT DISTINCT * FROM NAME ORDER BY CUSTNO")
con.execute("CREATE TABLE RMRK_UQ AS SELECT DISTINCT * FROM RMRK ORDER BY CUSTNO")

# Merge NAME and RMRK by CUSTNO, keep NAME only if no matching RMRK
con.execute("""
    CREATE TABLE MERGE AS
    SELECT n.*
    FROM NAME_UQ n
    LEFT JOIN RMRK_UQ r
    ON n.CUSTNO = r.CUSTNO
    WHERE r.CUSTNO IS NULL
""")

# Export to Parquet
con.execute(f"COPY MERGE TO '{output_parquet_path}' (FORMAT PARQUET)")

# Export to fixed-width TXT
rows = con.execute("SELECT * FROM MERGE ORDER BY CUSTNO").fetchall()
columns = [desc[0] for desc in con.description]

def format_fixed_width(row):
    # Map SAS input/output positions to Python slicing
    fmt = (
        '{:0>2}'   # HOLD_CO_NO PD2
        '{:0>2}'   # BANK_NO PD2
        '{:<20}'   # CUSTNO $20
        '{:0>2}'   # REC_TYPE PD2
        '{:0>2}'   # REC_SEQ PD2
        '{:0>5}'   # EFF_DATE PD5
        '{:<8}'    # PROCESS_TIME $8
        '{:0>2}'   # ADR_HOLD_CO_NO PD2
        '{:0>2}'   # ADR_BANK_NO PD2
        '{:0>6}'   # ADR_REF_NO PD6
        '{:<1}'    # CUST_TYPE $1
        '{:<15}'   # KEY_FIELD_1 $15
        '{:<10}'   # KEY_FIELD_2 $10
        '{:<5}'    # KEY_FIELD_3 $5
        '{:<5}'    # KEY_FIELD_4 $5
        '{:<1}'    # LINE_CODE $1
        '{:<40}'   # NAME_LINE $40
        '{:<1}'    # LINE_CODE_1 $1
        '{:<40}'   # NAME_TITLE_1 $40
        '{:<1}'    # LINE_CODE_2 $1
        '{:<40}'   # NAME_TITLE_2 $40
        '{:<40}'   # SALUTATION $40
        '{:0>2}'   # TITLE_CODE PD2
        '{:<30}'   # FIRST_MID $30
        '{:<20}'   # SURNAME $20
        '{:<3}'    # SURNAME_KEY $3
        '{:<2}'    # SUFFIX_CODE $2
        '{:<2}'    # APPEND_CODE $2
        '{:0>6}'   # PRIM_PHONE PD6
        '{:0>2}'   # P_PHONE_LTH PD2
        '{:0>6}'   # SEC_PHONE PD6
        '{:0>2}'   # S_PHONE_LTH PD2
        '{:0>6}'   # TELEX_PHONE PD6
        '{:0>2}'   # T_PHONE_LTH PD2
        '{:0>6}'   # FAX_PHONE PD6
        '{:0>2}'   # F_PHONE_LTH PD2
        '{:<10}'   # LAST_CHANGE $10
        '{:<1}'    # PARSE_IND $1
    )
    return fmt.format(*row)

with open(output_txt_path, 'w') as f:
    for r in rows:
        f.write(format_fixed_width(r) + '\n')

print("Processing complete. Output TXT and Parquet generated.")
