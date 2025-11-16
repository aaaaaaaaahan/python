import duckdb
import datetime

# ============================
# CONFIG
# ============================
input_parquet = host_parquet_path("CIS/CUST/DAILY/CUSTDLY.parquet")
output_parquet = parquet_output_path("CICUSCD4/STAF002_INIT.parquet")
output_text = csv_output_path("CICUSCD4/STAF002.INIT.txt")

con = duckdb.connect()

# ============================
# STEP 1: LOAD PARQUET
# ============================
con.execute(f"""
    CREATE TABLE CUST AS
    SELECT * FROM read_parquet('{input_parquet}')
""")

# ============================
# STEP 2: PROCESS HRC FIELDS
# ============================
hrc_list = [f"HRC{str(i).zfill(2)}" for i in range(1, 21)]
# LPAD to 3 chars (Z3.), replace '002' -> '   '
processed_hrc = ",\n".join([
    f"CASE WHEN LPAD({h},3,'0')='002' THEN '   ' ELSE LPAD({h},3,'0') END AS {h}C"
    for h in hrc_list
])

# ============================
# STEP 3: FILTER BANK EMPLOYEES (any HRCxxC = '002')
# ============================
filter_condition = " OR ".join([f"LPAD({h},3,'0')='002'" for h in hrc_list])

con.execute(f"""
    CREATE TABLE CIS AS
    SELECT
        CUSTNO,
        INDORG,
        CUSTBRCH,
        CUSTNAME,
        'A' AS FILECODE,
        {processed_hrc},
        '000' AS CODEFILLER
    FROM CUST
    WHERE {filter_condition}
""")

# ============================
# STEP 4: CREATE CUSTCODEALL
# ============================
concat_fields = "||".join([f"{h}C" for h in hrc_list] + ["CODEFILLER"])
con.execute(f"""
    CREATE TABLE CIS2 AS
    SELECT *,
           REPLACE({concat_fields}, ' ', '') AS CUSTCODEALL
    FROM CIS
""")

# ============================
# STEP 5: REMOVE DUPLICATES BY CUSTNO
# ============================
con.execute("""
    CREATE TABLE FINAL AS
    SELECT *
    FROM CIS2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) = 1
""")

# ============================
# STEP 6: WRITE PARQUET
# ============================
con.execute(f"""
    COPY FINAL TO '{output_parquet}' (FORMAT PARQUET)
""")

# ============================
# STEP 7: WRITE FIXED-WIDTH TEXT
# ============================
# DuckDB allows fixed-width via LPAD/RPAD
# Columns: @1 CUSTNO $20, @21 INDORG $1, @22 CUSTBRCH Z7
# @29 CUSTCODEALL $60, @89 FILECODE $1, @90 STAFFID $9 (blank), @99 CUSTNAME $40

con.execute(f"""
    COPY (
        SELECT
            RPAD(CUSTNO,20,' ') ||
            RPAD(INDORG,1,' ') ||
            LPAD(CAST(CUSTBRCH AS VARCHAR),7,'0') ||
            RPAD(CUSTCODEALL,60,' ') ||
            RPAD(FILECODE,1,' ') ||
            RPAD('',9,' ') ||  -- STAFFID blank
            RPAD(CUSTNAME,40,' ')
        AS line
        FROM FINAL
    ) TO '{output_text}' (FORMAT CSV, DELIMITER '', HEADER FALSE)
""")

print("DuckDB-only processing done: parquet + fixed-width text generated.")
