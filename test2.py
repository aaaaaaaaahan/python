import duckdb
from pathlib import Path

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
input_hrc = "/host/cis/input/CUSTCODE.parquet"          # HRCFILE
input_cis = "/host/cis/input/CIS_CUST_DAILY.parquet"    # CISFILE.CUSTDLY
output_dir = Path("/host/cis/output")
output_dir.mkdir(parents=True, exist_ok=True)

# Output paths
out_dp_parquet = output_dir / "HRCCUST_DPACCTS.parquet"
out_ln_parquet = output_dir / "HRCCUST_LNACCTS.parquet"
out_ot_parquet = output_dir / "HRCCUST_OTACCTS.parquet"

out_dp_csv = output_dir / "HRCCUST_DPACCTS.csv"
out_ln_csv = output_dir / "HRCCUST_LNACCTS.csv"
out_ot_csv = output_dir / "HRCCUST_OTACCTS.csv"

# ---------------------------------------------------------------------
# Connect to DuckDB
# ---------------------------------------------------------------------
con = duckdb.connect()

# ---------------------------------------------------------------------
# Create Tables
# ---------------------------------------------------------------------
con.execute(f"""
    CREATE TABLE HRCCUST AS 
    SELECT 
        CUSTNO,
        HRCCODES
    FROM read_parquet('{input_hrc}');
""")

con.execute(f"""
    CREATE TABLE CIS AS 
    SELECT 
        CUSTBRCH,
        CUSTNO,
        RACE,
        CITIZENSHIP,
        INDORG,
        PRISEC,
        ALIASKEY,
        ALIAS,
        ACCTCODE,
        ACCTNO,
        CUSTNAME,
        CUSTLASTDATECC,
        CUSTLASTDATEYY,
        CUSTLASTDATEMM,
        CUSTLASTDATEDD
    FROM read_parquet('{input_cis}');
""")

# ---------------------------------------------------------------------
# Merge HRCCUST and CIS
# ---------------------------------------------------------------------
con.execute("""
    CREATE TABLE MERGED AS
    SELECT
        CIS.CUSTBRCH,
        CIS.CUSTNO,
        CIS.CUSTNAME,
        CIS.RACE,
        CIS.CITIZENSHIP,
        CIS.INDORG,
        CIS.PRISEC,
        CASE 
            WHEN CIS.PRISEC = 901 THEN 'P'
            WHEN CIS.PRISEC = 902 THEN 'S'
            ELSE '' 
        END AS PRIMSEC,
        CIS.CUSTLASTDATECC,
        CIS.CUSTLASTDATEYY,
        CIS.CUSTLASTDATEMM,
        CIS.CUSTLASTDATEDD,
        CIS.ALIASKEY,
        CIS.ALIAS,
        HRCCUST.HRCCODES,
        CIS.ACCTCODE,
        CIS.ACCTNO
    FROM CIS
    JOIN HRCCUST
    ON CIS.CUSTNO = HRCCUST.CUSTNO;
""")

# ---------------------------------------------------------------------
# Split into 3 account categories
# ---------------------------------------------------------------------
con.execute("""
    CREATE TABLE MRGCISDP AS
    SELECT * FROM MERGED WHERE ACCTCODE = 'DP'
    ORDER BY ACCTNO;
""")

con.execute("""
    CREATE TABLE MRGCISLN AS
    SELECT * FROM MERGED WHERE ACCTCODE = 'LN'
    ORDER BY ACCTNO;
""")

con.execute("""
    CREATE TABLE MRGCISOT AS
    SELECT * FROM MERGED WHERE ACCTCODE NOT IN ('DP','LN')
    ORDER BY ACCTNO;
""")

# ---------------------------------------------------------------------
# Output as Parquet and CSV
# ---------------------------------------------------------------------
con.execute(f"COPY MRGCISDP TO '{out_dp_parquet}' (FORMAT PARQUET);")
con.execute(f"COPY MRGCISLN TO '{out_ln_parquet}' (FORMAT PARQUET);")
con.execute(f"COPY MRGCISOT TO '{out_ot_parquet}' (FORMAT PARQUET);")

con.execute(f"COPY MRGCISDP TO '{out_dp_csv}' (HEADER, DELIMITER ',');")
con.execute(f"COPY MRGCISLN TO '{out_ln_csv}' (HEADER, DELIMITER ',');")
con.execute(f"COPY MRGCISOT TO '{out_ot_csv}' (HEADER, DELIMITER ',');")

# ---------------------------------------------------------------------
# Display sample records
# ---------------------------------------------------------------------
print("=== HRC WITH DP ACCTS ONLY ===")
print(con.execute("SELECT * FROM MRGCISDP LIMIT 10;").fetchdf())

print("\n=== HRC WITH LN ACCTS ONLY ===")
print(con.execute("SELECT * FROM MRGCISLN LIMIT 10;").fetchdf())

print("\n=== HRC WITH OT ACCTS ONLY ===")
print(con.execute("SELECT * FROM MRGCISOT LIMIT 10;").fetchdf())
