import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet
import datetime

# ================================================================
# 1. SYSTEM DATE (replace SAS DATEFILE)
# ================================================================
batch_date = datetime.date.today() - datetime.timedelta(days=1)
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")

DATEYY1 = str(year)
DATEMM1 = str(month).zfill(2)
DATEDD1 = str(day).zfill(2)

# ================================================================
# 2. DUCKDB CONNECTION
# ================================================================
con = duckdb.connect()
cis = get_hive_parquet('CIS_CUST_DAILY')

# ================================================================
# 3. LOAD CIS (SET CISFILE; IF INDORG='I')
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE CIS AS
    SELECT *
    FROM read_parquet('{cis[0]}')
    WHERE INDORG = 'I'
""")

# PROC SORT NODUPKEY
con.execute("""
    CREATE OR REPLACE TABLE CIS AS
    SELECT *
    FROM CIS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) = 1
""")

# ================================================================
# 4. LOAD PHONE (INPUT @9 CUSTNO $11.)
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE PHONE AS
    SELECT
        SUBSTR(CUSTNO, 1, 11) AS CUSTNO,
        0 AS PHONE,
        0 AS PROMPT
    FROM read_parquet('{host_parquet_path("UNLOAD_CIPHONET_FB.parquet")}')
""")

# ================================================================
# 5. MERGE (same as SAS MERGE PHONE(IN=A) CIS(IN=B); IF NOT A THEN OUTPUT)
# ================================================================
con.execute("""
    CREATE OR REPLACE TABLE MERGE AS
    SELECT B.*
         , 0 AS PHONE
         , 0 AS PROMPT
    FROM CIS B
    LEFT JOIN PHONE A
           ON A.CUSTNO = B.CUSTNO
    WHERE A.CUSTNO IS NULL
""")

# ================================================================
# 6. FIXED-WIDTH OUTPUT (SAS EXACT FORMAT)
# ================================================================
txt_name = "CIPHONET_CUSTNEW"
txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")

df_txt = con.execute("SELECT * FROM MERGE").fetchdf()

def fmt_pd8(n):
    return f"{int(n):08d}"

def fmt_pd1(n):
    return f"{int(n):1d}"

with open(txt_path, "w", encoding="utf-8") as f:
    for _, row in df_txt.iterrows():
        line = (
            f"033"                                   # @01
            f"CUST"                                  # @04
            f"{str(row['CUSTNO']).ljust(11)}"        # @09
            f"{'PRIMARY'.ljust(15)}"                 # @29
            f"{fmt_pd8(row['PHONE'])}"               # @44
            f"{fmt_pd8(row['PHONE'])}"               # @52
            f"{str(row['INDORG'])}"                  # @60
            f"{DATEYY1}"                             # @61
            f"-{DATEMM1}"                            # @65
            f"-{DATEDD1}"                            # @68
            f"{fmt_pd1(row['PROMPT'])}"              # @71
            f"INIT"                                  # @72
            f"{DATEYY1}"                             # @77
            f"-{DATEMM1}"                            # @81
            f"-{DATEDD1}"                            # @84
            f"01.01.01"                              # @87
            f"INIT"                                  # @95
            f"{DATEYY1}"                             # @100
            f"-{DATEMM1}"                            # @104
            f"-{DATEDD1}"                            # @107
            f"01.01.01"                              # @110
            f"INIT"                                  # @118
            f"INIT"                                  # @126
            f"INIT"                                  # @131
            f"{fmt_pd8(row['PHONE'])}"               # @151
        )
        f.write(line + "\n")

print("TXT Output written:", txt_path)
