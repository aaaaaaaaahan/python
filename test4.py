import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")

# ---------------------------------------------------------------------
# DUCKDB PROCESSING
# ---------------------------------------------------------------------
con = duckdb.connect()

# ---------------------------------------------------------------------
# Read input parquet files into DuckDB tables
# ---------------------------------------------------------------------
con.execute(f"""
    CREATE TABLE NAME AS 
    SELECT DISTINCT * 
    FROM read_parquet('{host_parquet_path("PRIMNAME_OUT.parquet")}')
    ORDER BY CUSTNO
""")

con.execute(f"""
    CREATE TABLE RMRK AS 
    SELECT DISTINCT * 
    FROM read_parquet('{host_parquet_path("CCRIS_CISRMRK_LONGNAME.parquet")}')
    WHERE INDORG IS NOT NULL AND INDORG != ''
    ORDER BY CUSTNO
""")

# ---------------------------------------------------------------------
# Merge NAME and RMRK by CUSTNO, keep NAME only if no matching RMRK
# ---------------------------------------------------------------------
con.execute("""
    CREATE TABLE MERGE AS
    SELECT n.*
    FROM NAME n
    LEFT JOIN RMRK r
    ON n.CUSTNO = r.CUSTNO
    WHERE r.CUSTNO IS NULL
    ORDER BY CUSTNO
""")

# ---------------------------------------------------------------------
# Output as Parquet and CSV
# ---------------------------------------------------------------------
out = f"""
    SELECT
        *
        ,{year} AS year
        ,{month} AS month 
        ,{day} AS day
    FROM MERGE
"""

queries = {
    "CIS_LONGNAME_NONE": out
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

# ---------------------------------------------------------------------
# Output as TXT (match SAS field layout exactly)
# ---------------------------------------------------------------------
txt_queries = {
    "CIS_LONGNAME_NONE": out
}

for txt_name, txt_query in txt_queries.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(row.get('HOLD_CO_NO','')).rjust(2)}"
                f"{str(row.get('BANK_NO','')).rjust(2)}"
                f"{str(row.get('CUSTNO','')).ljust(20)}"
                f"{str(row.get('REC_TYPE','')).rjust(2)}"
                f"{str(row.get('REC_SEQ','')).rjust(2)}"
                f"{str(row.get('EFF_DATE','')).rjust(5)}"
                f"{str(row.get('PROCESS_TIME','')).ljust(8)}"
                f"{str(row.get('ADR_HOLD_CO_NO','')).rjust(2)}"
                f"{str(row.get('ADR_BANK_NO','')).rjust(2)}"
                f"{str(row.get('ADR_REF_NO','')).rjust(6)}"
                f"{str(row.get('CUST_TYPE','')).ljust(1)}"
                f"{str(row.get('KEY_FIELD_1','')).ljust(15)}"
                f"{str(row.get('KEY_FIELD_2','')).ljust(10)}"
                f"{str(row.get('KEY_FIELD_3','')).ljust(5)}"
                f"{str(row.get('KEY_FIELD_4','')).ljust(5)}"
                f"{str(row.get('LINE_CODE','')).ljust(1)}"
                f"{str(row.get('NAME_LINE','')).ljust(40)}"
                f"{str(row.get('LINE_CODE_1','')).ljust(1)}"
                f"{str(row.get('NAME_TITLE_1','')).ljust(40)}"
                f"{str(row.get('LINE_CODE_2','')).ljust(1)}"
                f"{str(row.get('NAME_TITLE_2','')).ljust(40)}"
                f"{str(row.get('SALUTATION','')).ljust(40)}"
                f"{str(row.get('TITLE_CODE','')).rjust(2)}"
                f"{str(row.get('FIRST_MID','')).ljust(30)}"
                f"{str(row.get('SURNAME','')).ljust(20)}"
                f"{str(row.get('SURNAME_KEY','')).ljust(3)}"
                f"{str(row.get('SUFFIX_CODE','')).ljust(2)}"
                f"{str(row.get('APPEND_CODE','')).ljust(2)}"
                f"{str(row.get('PRIM_PHONE','')).rjust(6)}"
                f"{str(row.get('P_PHONE_LTH','')).rjust(2)}"
                f"{str(row.get('SEC_PHONE','')).rjust(6)}"
                f"{str(row.get('S_PHONE_LTH','')).rjust(2)}"
                f"{str(row.get('TELEX_PHONE','')).rjust(6)}"
                f"{str(row.get('T_PHONE_LTH','')).rjust(2)}"
                f"{str(row.get('FAX_PHONE','')).rjust(6)}"
                f"{str(row.get('F_PHONE_LTH','')).rjust(2)}"
                f"{str(row.get('LAST_CHANGE','')).ljust(10)}"
                f"{str(row.get('PARSE_IND','')).ljust(1)}"
            )
            f.write(line + "\n")

    print(f"✅ TXT file generated: {txt_path}")
