import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
import datetime

# -----------------------------
# Batch / report date
# -----------------------------
batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")
UPDDATX = batch_date.strftime("%d/%m/%Y")

# -----------------------------
# Output paths
# -----------------------------
output_parquet = "/host/cis/output/alias_change.parquet"
output_txt = "/host/cis/output/alias_change.txt"

# -----------------------------
# Connect DuckDB
# -----------------------------
con = duckdb.connect()

# -----------------------------
# Process EODRPT
# -----------------------------
exclude_keys_delete = ['CH ','CV ','EN ','NM ','VE ']

con.execute(f"""
    CREATE VIEW eodrpt_proc AS
    SELECT 
        CUSTNOX::BIGINT AS CUSTNOX,
        LPAD(CAST(CUSTNOX AS VARCHAR),11,'0') AS CUSTNO,
        OPERID AS UPDOPER,
        ALIAS,
        SUBSTRING(ALIAS,1,3) AS ALIASKEY,
        INDFUNCT,
        CASE 
            WHEN SUBSTRING(ALIAS,1,3) IN ('AI ','AO ') THEN 'BNM ASSIGNED ID'
            ELSE 'ID NUMBER'
        END AS FIELDS,
        CASE WHEN INDFUNCT='D' THEN ALIAS ELSE ' ' END AS OLDVALUE,
        CASE WHEN INDFUNCT='A' THEN ALIAS ELSE ' ' END AS NEWVALUE
    FROM '{host_parquet_path("CIDARPGS.parquet")}'
    WHERE OPERID IS NOT NULL
      AND INDALS = 230
      AND SUBSTRING(ALIAS,1,3) NOT IN ({','.join([f"'{k}'" for k in exclude_keys_delete])})
""")

# Remove duplicates like PROC SORT NODUPKEY
con.execute("CREATE VIEW eodrpt_final AS SELECT DISTINCT * FROM eodrpt_proc ORDER BY CUSTNO, INDFUNCT, ALIAS")

# -----------------------------
# Process NAME
# -----------------------------
con.execute(f"""
    CREATE VIEW name_final AS
    SELECT CUSTNO, CUSTNAME
    FROM '{host_parquet_path("PRIMNAME_OUT.parquet")}'
""")

# -----------------------------
# Process ACTIVE accounts
# -----------------------------
con.execute(f"""
    CREATE VIEW active_proc AS
    SELECT CUSTNO, ACCTNOC
    FROM '{host_parquet_path("CIS_CUST_DAILY_ACTVOD.parquet")}'
    WHERE ACCTCODE IN ('DP   ','LN   ')
      AND DATECLSE IN ('       .','        ','00000000')
""")

# -----------------------------
# Merge all
# -----------------------------
con.execute("""
    CREATE VIEW merged AS
    SELECT e.UPDOPER, e.CUSTNO, a.ACCTNOC, n.CUSTNAME,
           e.FIELDS, e.OLDVALUE, e.NEWVALUE
    FROM eodrpt_final e
    JOIN name_final n USING (CUSTNO)
    JOIN active_proc a USING (CUSTNO)
""")

# -----------------------------
# Write Parquet output
# -----------------------------
con.execute(f"COPY merged TO '{output_parquet}' (FORMAT PARQUET)")

# -----------------------------
# Write TXT output (fixed-width)
# -----------------------------
con.execute(f"""
    COPY (
        SELECT 
            LPAD(UPDOPER,10,' ') AS UPDOPER,
            LPAD(CUSTNO,20,' ') AS CUSTNO,
            LPAD(ACCTNOC,20,' ') AS ACCTNOC,
            LPAD(CUSTNAME,40,' ') AS CUSTNAME,
            LPAD(FIELDS,20,' ') AS FIELDS,
            LPAD(OLDVALUE,150,' ') AS OLDVALUE,
            LPAD(NEWVALUE,150,' ') AS NEWVALUE,
            '{UPDDATX}' AS UPDDATX
        FROM merged
        ORDER BY CUSTNO, ALIASKEY
    ) TO '{output_txt}' (FORMAT CSV, DELIMITER '', HEADER FALSE)
""")
