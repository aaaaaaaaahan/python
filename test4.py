import duckdb

# =========================
# 1) Input file paths (assume parquet)
# =========================
custcode_path = "CUSTCODE.parquet"      # contains CUSTNO + HRCCODES(60)
resigned_path = "RESIGNED.parquet"      # STAFFID, CUSTNO, HRNAME
updfile_path_txt = "EMPLOYEE_RESIGN_RMV.txt"

con = duckdb.connect()

# =========================
# 2) Extract CODE01–CODE20 from HRCCODES
# =========================
con.execute(f"""
CREATE OR REPLACE TABLE custcode AS
SELECT
    CUSTNO,
    SUBSTRING(HRCCODES,1,3)  AS CODE01,
    SUBSTRING(HRCCODES,4,3)  AS CODE02,
    SUBSTRING(HRCCODES,7,3)  AS CODE03,
    SUBSTRING(HRCCODES,10,3) AS CODE04,
    SUBSTRING(HRCCODES,13,3) AS CODE05,
    SUBSTRING(HRCCODES,16,3) AS CODE06,
    SUBSTRING(HRCCODES,19,3) AS CODE07,
    SUBSTRING(HRCCODES,22,3) AS CODE08,
    SUBSTRING(HRCCODES,25,3) AS CODE09,
    SUBSTRING(HRCCODES,28,3) AS CODE10,
    SUBSTRING(HRCCODES,31,3) AS CODE11,
    SUBSTRING(HRCCODES,34,3) AS CODE12,
    SUBSTRING(HRCCODES,37,3) AS CODE13,
    SUBSTRING(HRCCODES,40,3) AS CODE14,
    SUBSTRING(HRCCODES,43,3) AS CODE15,
    SUBSTRING(HRCCODES,46,3) AS CODE16,
    SUBSTRING(HRCCODES,49,3) AS CODE17,
    SUBSTRING(HRCCODES,52,3) AS CODE18,
    SUBSTRING(HRCCODES,55,3) AS CODE19,
    SUBSTRING(HRCCODES,58,3) AS CODE20
FROM read_parquet('{custcode_path}')
WHERE STRPOS(HRCCODES, '002') > 0
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
# 4) Merge with SAS-like flags (D & E)
# =========================
con.execute("""
CREATE OR REPLACE TABLE merge1 AS
SELECT
    a.*,
    b.STAFFID,
    b.HRNAME,
    (a.CUSTNO IS NOT NULL) AS D_flag,
    (b.CUSTNO IS NOT NULL) AS E_flag
FROM custcode a
LEFT JOIN resigned b
    ON a.CUSTNO = b.CUSTNO
WHERE (a.CUSTNO IS NOT NULL) AND (b.CUSTNO IS NOT NULL)
""")

# =========================
# 5) Shift CODE columns like SAS
# =========================
shift_sql_parts = []
for i in range(1, 11):  # Only CODE01–CODE10 shifts in SAS example
    shift_sql_parts.append(f"""
    CASE WHEN CODE{i:02}='002' THEN
        {', '.join([f'CODE{j:02}' for j in range(i+1, 21)])}, '000'
    ELSE CODE{i:02} END AS CODE{i:02}
    """)
# But easier: just do one step per column using a loop for all 20 columns
# Here we implement shift sequentially like SAS
# (Full program below avoids overly complex SQL; handled in Python procedural style)

# =========================
# 6) Procedural shift in DuckDB using UPDATE statements
# =========================
for pos in range(1, 11):  # CODE01–CODE10 as per SAS
    codes = [f"CODE{p:02}" for p in range(pos+1, 21)]
    shift_expr = ", ".join([f"{codes[i-1]}" for i in range(len(codes))])
    con.execute(f"""
    UPDATE merge1
    SET {', '.join([f'CODE{pos+i:02} = CODE{pos+i+1:02}' for i in range(20-pos)])}, CODE20='000'
    WHERE CODE{pos:02} = '002'
    """)

# =========================
# 7) Export fixed-width TXT (like SAS PUT)
# =========================
con.execute(f"""
COPY (
    SELECT
        RPAD(CUSTNO,11,' ') ||
        CODE01 || CODE02 || CODE03 || CODE04 || CODE05 ||
        CODE06 || CODE07 || CODE08 || CODE09 || CODE10 ||
        CODE11 || CODE12 || CODE13 || CODE14 || CODE15 ||
        CODE16 || CODE17 || CODE18 || CODE19 || CODE20 ||
        RPAD(STAFFID,10,' ') ||
        RPAD(HRNAME,40,' ')
    AS line
    FROM merge1
    ORDER BY CUSTNO
) TO '{updfile_path_txt}' (FORMAT CSV, DELIMITER '', HEADER FALSE)
""")

