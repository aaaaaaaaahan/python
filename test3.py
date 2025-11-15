import duckdb
from datetime import datetime

# -----------------------------
# File paths
# -----------------------------
parquet_eodrpt = "/host/cis/parquet/eodrpt.parquet"
parquet_name = "/host/cis/parquet/name.parquet"
parquet_active = "/host/cis/parquet/active.parquet"

output_parquet = "/host/cis/output/alias_change.parquet"
output_txt = "/host/cis/output/alias_change.txt"

# -----------------------------
# Connect DuckDB
# -----------------------------
con = duckdb.connect()

# -----------------------------
# Reporting Date (replace SRSCTRL)
# -----------------------------
today = datetime.today()
report_date = today.strftime("%d/%m/%Y")
year = today.year
month = today.month
day = today.day

# -----------------------------
# Process EODRPT
# -----------------------------
exclude_keys = ['CH','CV','EN','NM','VE','BR','CI','PC','SA','GB',
                'LP','RE','AI','AO','IC','SI','BI','PP','ML','PL','BC']

con.execute(f"""
    CREATE VIEW eodrpt_proc AS
    SELECT 
        CUSTNOX::BIGINT AS CUSTNO,
        OPERID AS UPDOPER,
        ALIAS,
        SUBSTRING(ALIAS,1,2) AS ALIASKEY,
        INDFUNCT,
        CASE WHEN SUBSTRING(ALIAS,1,2) IN ('AI','AO') THEN 'BNM ASSIGNED ID'
             ELSE 'ID NUMBER'
        END AS FIELDS,
        CASE WHEN INDFUNCT='D' THEN ALIAS ELSE '' END AS OLDVALUE,
        CASE WHEN INDFUNCT='A' THEN ALIAS ELSE '' END AS NEWVALUE
    FROM parquet_scan('{parquet_eodrpt}')
    WHERE OPERID IS NOT NULL
      AND INDALS = 230
      AND SUBSTRING(ALIAS,1,2) NOT IN ({','.join([f"'{k}'" for k in exclude_keys])})
""")

# Remove duplicates like PROC SORT NODUPKEY
con.execute("CREATE VIEW eodrpt_final AS SELECT DISTINCT * FROM eodrpt_proc")

# -----------------------------
# Process NAME
# -----------------------------
con.execute(f"""
    CREATE VIEW name_final AS
    SELECT CUSTNO, CUSTNAME
    FROM parquet_scan('{parquet_name}')
""")

# -----------------------------
# Process ACTIVE accounts
# -----------------------------
con.execute(f"""
    CREATE VIEW active_proc AS
    SELECT CUSTNO, ACCTNOC
    FROM parquet_scan('{parquet_active}')
    WHERE ACCTCODE IN ('DP   ','LN   ')
      AND DATECLSE NOT IN ('       .','        ','00000000')
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
            LPAD(CAST(CUSTNO AS VARCHAR),20,' ') AS CUSTNO,
            LPAD(ACCTNOC,20,' ') AS ACCTNOC,
            LPAD(CUSTNAME,40,' ') AS CUSTNAME,
            LPAD(FIELDS,20,' ') AS FIELDS,
            LPAD(OLDVALUE,150,' ') AS OLDVALUE,
            LPAD(NEWVALUE,150,' ') AS NEWVALUE,
            '{report_date}' AS UPDDATX
        FROM merged
    ) TO '{output_txt}' (FORMAT CSV, DELIMITER '', HEADER FALSE)
""")
