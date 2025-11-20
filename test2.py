import duckdb

# =========================
# 1) INPUT FILES (Parquet assumed)
# =========================
custcode_path = "CUSTCODE.parquet"      # contains CUSTNO + HRCCODES(60)
resigned_path = "RESIGNED.parquet"      # STAFFID, CUSTNO, HRNAME
updfile_path_parquet = "EMPLOYEE_RESIGN_RMV.parquet"
updfile_path_txt = "EMPLOYEE_RESIGN_RMV.txt"

con = duckdb.connect()

# =========================
# 2) Extract CODE01-CODE20 from HRCCODES
# =========================
code_extract = ",\n    ".join([
    f"SUBSTRING(HRCCODES, {(i-1)*3 + 1}, 3) AS CODE{i:02}" for i in range(1, 21)
])

# Filter rows where any CODE = '002'
con.execute(f"""
    CREATE OR REPLACE TABLE custcode AS
    SELECT CUSTNO,
           HRCCODES,
           {code_extract}
    FROM read_parquet('{custcode_path}')
    WHERE POSITION('002' IN HRCCODES) > 0
""")

# =========================
# 3) Read RESIGNED
# =========================
con.execute(f"""
    CREATE OR REPLACE TABLE resigned AS
    SELECT DISTINCT *
    FROM read_parquet('{resigned_path}')
""")

# =========================
# 4) Merge tables on CUSTNO
# =========================
con.execute("""
    CREATE OR REPLACE TABLE merge1 AS
    SELECT a.*, b.STAFFID, b.HRNAME
    FROM custcode a
    INNER JOIN resigned b USING(CUSTNO)
""")

# =========================
# 5) Shift CODE columns (exactly like SAS)
# =========================
shift_cases = []
for i in range(1, 20):
    next_codes = ", ".join([f"CODE{j:02}" for j in range(i+1, 21)] + ["'000'"])
    shift_cases.append(
        f"CASE WHEN CODE{i:02} = '002' THEN {next_codes} ELSE CODE{i:02} END AS CODE{i:02}"
    )
shift_sql = ",\n    ".join(shift_cases)
shift_sql += ",\n    CODE20"

con.execute(f"""
    CREATE OR REPLACE TABLE shift1 AS
    SELECT CUSTNO,
           {shift_sql},
           STAFFID,
           HRNAME
    FROM merge1
""")

# =========================
# 6) Rebuild HRCCODES from shifted CODE columns
# =========================
con.execute(f"""
    CREATE OR REPLACE TABLE shift1_final AS
    SELECT
        CUSTNO,
        CONCAT(
            CODE01, CODE02, CODE03, CODE04, CODE05,
            CODE06, CODE07, CODE08, CODE09, CODE10,
            CODE11, CODE12, CODE13, CODE14, CODE15,
            CODE16, CODE17, CODE18, CODE19, CODE20
        ) AS HRCCODES,
        STAFFID,
        HRNAME
    FROM shift1
""")

# =========================
# 7) Output Parquet
# =========================
con.execute(f"COPY shift1_final TO '{updfile_path_parquet}' (FORMAT PARQUET)")

# =========================
# 8) Output TXT (fixed-width exactly like SAS)
# Line 1: CUSTNO (11) + HRCCODES (60)
# Line 2: STAFFID (10) + HRNAME (40)
# =========================
con.execute(f"""
    CREATE OR REPLACE TABLE txt_output AS
    SELECT
        CUSTNO || HRCCODES AS LINE1,
        STAFFID || RPAD(HRNAME, 40, ' ') AS LINE2
    FROM shift1_final
    ORDER BY CUSTNO
""")

# Union lines vertically for TXT export
con.execute(f"""
    COPY (
        SELECT LINE1 AS line FROM txt_output
        UNION ALL
        SELECT LINE2 AS line FROM txt_output
    ) TO '{updfile_path_txt}' (FORMAT CSV, DELIMITER '', HEADER FALSE)
""")
