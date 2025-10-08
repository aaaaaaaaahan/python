 File "/pythonITD/cis_dev/jobs/cis_internal/CIS_PY_CCRCCRL1 copy.py", line 40, in <module>
    con.execute("""
duckdb.duckdb.InvalidInputException: Invalid Input Error: Parameter argument/count mismatch, identifiers of the excess parameters: 1

import os
import re
from typing import List, Tuple

# ============================================================
# PATH CONFIGURATION
# ============================================================
host_input = '/host/cis/parquet/sas_parquet'
python_hive = '/host/cis/parquet'
csv_output = '/host/cis/output'


# ============================================================
# FUNCTION: host_parquet_path
# ============================================================
def host_parquet_path(filename: str) -> str:
    """
    If filename has no date, resolve to the latest dated file.
    Example:
      host_parquet_path("data_test.parquet") -> data_test_YYYYMMDD.parquet (latest)
      host_parquet_path("data_test1.parquet") -> data_test1_YYYYMMDD.parquet (latest)
      host_parquet_path("data_test_20250917.parquet") -> exact match
    """
    # exact match first
    full_path = os.path.join(host_input, filename)
    if os.path.exists(full_path):
        return full_path

    # try to resolve latest date
    base, ext = os.path.splitext(filename)
    pattern = re.compile(rf"^{re.escape(base)}_(\d{{8}}){re.escape(ext)}$")

    candidates = []
    for f in os.listdir(host_input):
        match = pattern.match(f)
        if match:
            candidates.append((match.group(1), f))

    if not candidates:
        raise FileNotFoundError(f"No file found for base '{base}' in {host_input}")

    # pick latest by date
    latest_file = max(candidates, key=lambda x: x[0])[1]
    return os.path.join(host_input, latest_file)


# ============================================================
# FUNCTION: python_input_path
# ============================================================
def python_input_path(filename: str) -> str:
    return f"{python_hive}/{filename}"


# ============================================================
# FUNCTION: parquet_output_path
# ============================================================
def parquet_output_path(name: str) -> str:
    return f"{python_hive}/{name}"


# ============================================================
# FUNCTION: csv_output_path
# ============================================================
def csv_output_path(name: str) -> str:
    return f"{csv_output}/{name}.csv"


# ============================================================
# FUNCTION: hive_latest_path
# ============================================================
def hive_latest_path(table: str, debug: bool = False) -> str:
    """
    Find the latest partition folder for a Hive-partitioned table.
    Partition format: year=YYYY/month=MM/day=DD/data_*.parquet

    Returns full path to the folder containing parquet files.
    Example:
      hive_latest_path("accounts")
        -> /host/cis/parquet/accounts/year=2025/month=09/day=26
    """
    table_path = os.path.join(python_hive, table)
    if not os.path.exists(table_path):
        raise FileNotFoundError(f"Table folder not found: {table_path}")

    # --- find latest year ---
    years = []
    for f in os.listdir(table_path):
        if f.startswith("year="):
            try:
                years.append(int(f.split("=")[1]))
            except ValueError:
                continue
    if not years:
        raise FileNotFoundError(f"No year=YYYY partitions in {table_path}")
    latest_year = max(years)
    year_path = os.path.join(table_path, f"year={latest_year}")

    # --- find latest month ---
    months = []
    for f in os.listdir(year_path):
        if f.startswith("month="):
            try:
                months.append(int(f.split("=")[1]))
            except ValueError:
                continue
    if not months:
        raise FileNotFoundError(f"No month=MM partitions in {year_path}")
    latest_month = max(months)

    # Handle possible zero-padding mismatch
    month_folder = f"month={latest_month:02d}"
    if not os.path.exists(os.path.join(year_path, month_folder)):
        month_folder = f"month={latest_month}"
    month_path = os.path.join(year_path, month_folder)

    # --- find latest day ---
    days = []
    for f in os.listdir(month_path):
        if f.startswith("day="):
            try:
                days.append(int(f.split("=")[1]))
            except ValueError:
                continue
    if not days:
        raise FileNotFoundError(f"No day=DD partitions in {month_path}")
    latest_day = max(days)

    # Handle possible zero-padding mismatch
    day_folder = f"day={latest_day:02d}"
    if not os.path.exists(os.path.join(month_path, day_folder)):
        day_folder = f"day={latest_day}"
    day_path = os.path.join(month_path, day_folder)

    if debug:
        print(f"[DEBUG] Latest Hive Path: year={latest_year}, month={latest_month}, day={latest_day}")
        print(f"[DEBUG] Full Path: {day_path}")

    return day_path


# ============================================================
# FUNCTION: get_hive_parquet
# ============================================================
def get_hive_parquet(base_folder: str, debug: bool = False) -> Tuple[List[str], int, int, int]:
    """
    Detect the latest Hive-style partition folder (year=YYYY/month=MM/day=DD)
    and return all .parquet files (e.g. data_0.parquet) from that folder,
    along with year, month, day.
    """
    base_path = os.path.join(python_hive, base_folder)

    if not os.path.exists(base_path):
        raise FileNotFoundError(f"Base folder not found: {base_path}")

    # --- Find latest year ---
    years = []
    for y_folder in os.listdir(base_path):
        if y_folder.startswith("year="):
            try:
                years.append(int(y_folder.split("=")[1]))
            except ValueError:
                continue
    if not years:
        raise FileNotFoundError(f"No year=YYYY partitions under {base_path}")
    latest_year = max(years)
    year_path = os.path.join(base_path, f"year={latest_year}")

    # --- Find latest month ---
    months = []
    for m_folder in os.listdir(year_path):
        if m_folder.startswith("month="):
            try:
                months.append(int(m_folder.split("=")[1]))
            except ValueError:
                continue
    if not months:
        raise FileNotFoundError(f"No month=MM partitions under {year_path}")
    latest_month = max(months)

    month_folder = f"month={latest_month:02d}"
    if not os.path.exists(os.path.join(year_path, month_folder)):
        month_folder = f"month={latest_month}"
    month_path = os.path.join(year_path, month_folder)

    # --- Find latest day ---
    days = []
    for d_folder in os.listdir(month_path):
        if d_folder.startswith("day="):
            try:
                days.append(int(d_folder.split("=")[1]))
            except ValueError:
                continue
    if not days:
        raise FileNotFoundError(f"No day=DD partitions under {month_path}")
    latest_day = max(days)

    day_folder = f"day={latest_day:02d}"
    if not os.path.exists(os.path.join(month_path, day_folder)):
        day_folder = f"day={latest_day}"
    day_path = os.path.join(month_path, day_folder)

    # --- Collect parquet files ---
    parquet_files = [
        os.path.join(day_path, f)
        for f in os.listdir(day_path)
        if f.endswith(".parquet") or re.match(r"data_\d+$", f)
    ]

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {day_path}")

    if debug:
        print(f"[DEBUG] Found {len(parquet_files)} parquet files in {day_path}")
        for p in parquet_files:
            print(f"  -> {p}")

    return parquet_files, latest_year, latest_month, latest_day


import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path, get_hive_parquet
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year1, month1, day1 = batch_date.year, batch_date.month, batch_date.day

#-------------------------------------------------------------------#
# Original Program: CCRCCRL1                                        #
#-------------------------------------------------------------------#
#-ESMR 2011-2834                                                    #
# DISPLAY OF CUST-TO-CUST RELATIONSHIP IN CAS-IMIS                  #
# APPEND DP/LN ACCOUNT    TO CC(INDV-ORG) AND CC(ORG-ORG) ONLY      #
# TO SHOW PERSONAL ACCOUNT ONLY. JOINT ACCOUNT DROP                 #
# APPEND ORIGINAL RECORD  TO CC(ORG-INDV) AND CC(INDV-INDV)         #
#-------------------------------------------------------------------#
#-ESMR 2014-764                                                     #
# CHANGE FILE FORMAT FROM FIXED LENGTH TO DELIMITED                 #
#-------------------------------------------------------------------#

#--------------------------------#
# Open DuckDB in-memory database #
#--------------------------------#
con = duckdb.connect()
CCRALLL, year, month, day = get_hive_parquet('CCRIS_CC_RLNSHIP_SRCH')

#-----------------------------------#
# Load parquet datasets into DuckDB #
#-----------------------------------#
con.execute(f"""
    CREATE VIEW primary1 AS 
    SELECT 
        CAST(ACCTNO AS VARCHAR) AS ACCTNO,
        CAST(ACCTCODE AS VARCHAR) AS ACCTCODE,
        CAST(CUSTNO AS VARCHAR) AS CUSTNO
    FROM '{host_parquet_path("RLENCA_NONJOINT.parquet")}'
""")

# Single source file (RLNSHIP), then split into IND / ORG
con.execute("""
    CREATE VIEW ccr_all AS
    SELECT 
        CUSTNO1, 
        INDORG1 AS CUSTTYPE1, 
        CODE1 AS RLENCODE1, 
        DESC1,
        CUSTNO2 AS CUSTNO, 
        INDORG2 AS CUSTTYPE,
        CODE2 AS RLENCODE, 
        DESC2 AS RLENDESC,
        CUSTNAME1, ALIAS1, 
        CUSTNAME2 AS CUSTNAME, 
        ALIAS2 AS ALIAS
    FROM read_parquet('')
""", [CCRALLL])

# Split into ORG (O) and IND (I)
con.execute("""
    CREATE VIEW ccrlen AS
    SELECT * FROM ccr_all WHERE CUSTTYPE = 'O';

    CREATE VIEW ccrlen1 AS
    SELECT * FROM ccr_all WHERE CUSTTYPE = 'I';
""")

#------------------------------------------------------#
# Merge organisation CCRLEN with PRIMARY accounts      #
#------------------------------------------------------#
con.execute("""
    CREATE VIEW cc_primary AS
    SELECT
        c.CUSTNO1, c.CUSTTYPE1, c.RLENCODE1, c.DESC1,
        c.CUSTNO,  c.CUSTTYPE,  c.RLENCODE,  c.RLENDESC,
        c.CUSTNAME1, c.ALIAS1, c.CUSTNAME, c.ALIAS,
        p.ACCTNO, p.ACCTCODE
    FROM ccrlen c
    INNER JOIN primary1 p
        ON c.CUSTNO = p.CUSTNO
""")

#------------------------------------------------------#
# Union ORG+PRIMARY with IND relationship (ccrlen1)    #
#------------------------------------------------------#
con.execute("""
    CREATE VIEW out1 AS
    SELECT
        CUSTNO1, CUSTTYPE1, RLENCODE1, DESC1,
        CUSTNO, CUSTTYPE, RLENCODE, RLENDESC,
        ACCTCODE, ACCTNO, CUSTNAME1, ALIAS1,
        CUSTNAME, ALIAS
    FROM cc_primary

    UNION ALL

    SELECT
        CUSTNO1, CUSTTYPE1, RLENCODE1, DESC1,
        CUSTNO, CUSTTYPE, RLENCODE, RLENDESC,
        NULL AS ACCTCODE, NULL AS ACCTNO,
        CUSTNAME1, ALIAS1, CUSTNAME, ALIAS
    FROM ccrlen1
""")

#-----------------------------------#
# Export using PyArrow              #
#-----------------------------------#
out_table = """
    SELECT * ,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM out1
    ORDER BY CUSTNO, ACCTCODE, ACCTNO
""".format(year=year,month=month,day=day)

out1_imis = """
    SELECT
      '"' || CUSTNO1   || '"' AS CUSTNO1,
      '"' || CUSTTYPE1 || '"' AS CUSTTYPE1,
      '"' || RLENCODE1 || '"' AS RLENCODE1,
      '"' || DESC1     || '"' AS DESC1,
      '"' || CUSTNO    || '"' AS CUSTNO,
      '"' || CUSTTYPE  || '"' AS CUSTTYPE,
      '"' || RLENCODE  || '"' AS RLENCODE,
      '"' || RLENDESC    || '"' AS RLENDESC,
      '"' || COALESCE(ACCTCODE, '') || '"' AS ACCTCODE,
      '"' || COALESCE(ACCTNO, '')   || '"' AS ACCTNO,
      '"' || CUSTNAME1 || '"' AS CUSTNAME1,
      '"' || ALIAS1    || '"' AS ALIAS1,
      '"' || CUSTNAME  || '"' AS CUSTNAME,
      '"' || ALIAS     || '"' AS ALIAS,
      '"' || {day}     || '"' AS day,
      '"' || {month}   || '"' AS month,
      '"' || {year}    || '"' AS year
    FROM out1
    ORDER BY CUSTNO, ACCTCODE, ACCTNO
""".format(year=year,month=month,day=day)

queries = {
    "CCRIS_CC_RLNSHIP_PARTIES"            : out_table,
    "CCRIS_CC_RLNSHIP_PARTIES_IMIS"       : out1_imis,
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
    (FORMAT CSV, HEADER, DELIMITER ',', QUOTE '', OVERWRITE_OR_IGNORE true);  
     """)
