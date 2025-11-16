import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
import os

# ================================================================
# 1. SYSTEM DATE (replace DATEFILE logic)
# ================================================================
today = datetime.date.today()
DATEYY1 = today.strftime("%Y")
DATEMM1 = today.strftime("%m")
DATEDD1 = today.strftime("%d")

# ================================================================
# 2. DUCKDB CONNECTION
# ================================================================
con = duckdb.connect()

# ================================================================
# 3. LOAD PARQUET INPUT FILES (already converted)
# ================================================================
CISFILE = "CIS_CUST_DAILY.parquet"            # CIS.CUST.DAILY
CIPHONET = "UNLOAD_CIPHONET_FB.parquet"       # UNLOAD.CIPHONET.FB

# ================================================================
# 4. LOAD CIS (same as DATA CIS; SET CISFILE; IF INDORG='I')
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE CIS AS
    SELECT *
    FROM read_parquet('{CISFILE}')
    WHERE INDORG = 'I'
""")

# Remove duplicates same as PROC SORT NODUPKEY
con.execute("""
    CREATE OR REPLACE TABLE CIS AS
    SELECT *
    FROM CIS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) = 1
""")

# ================================================================
# 5. LOAD PHONE FILE (same as DATA PHONE and INPUT @9 CUSTNO $11.)
# Defaults: PHONE=0, PROMPT=0
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE PHONE AS
    SELECT
        SUBSTR(CUSTNO, 1, 11) AS CUSTNO,
        0 AS PHONE,
        0 AS PROMPT
    FROM read_parquet('{CIPHONET}')
""")

# ================================================================
# 6. MERGE (same as SAS MERGE PHONE(IN=A) CIS(IN=B); IF NOT A THEN OUTPUT)
# Keep CIS rows that do not exist in PHONE
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
# 7. SAVE MERGE TO PARQUET (optional)
# ================================================================
MERGE_PARQUET = "CIPHONET_CUSTNEW.parquet"
pq.write_table(con.execute("SELECT * FROM MERGE").fetch_arrow_table(), MERGE_PARQUET)

# ================================================================
# 8. WRITE FIXED-WIDTH TXT (same layout as SAS PUT @ positions)
# ================================================================
OUTPUT_TXT = "CIPHONET_CUSTNEW.txt"

def fmt_pd8(n):
    """PD8. equivalent: numeric without decimals, 8 width"""
    return f"{int(n):08d}"

def fmt_pd1(n):
    """PD1. equivalent: width 1"""
    return f"{int(n):1d}"

with open(OUTPUT_TXT, "w") as f:
    rows = con.execute("SELECT * FROM MERGE").fetchall()

    for r in rows:
        (
            CUSTNO, INDORG, PHONE, PROMPT, *rest
        ) = r

        line = (
            f"033"                                       # @01
            f"CUST"                                      # @04
            f"{CUSTNO:<11}"                              # @09
            f"{'PRIMARY':<15}"                           # @29
            f"{fmt_pd8(PHONE)}"                          # @44
            f"{fmt_pd8(PHONE)}"                          # @52
            f"{INDORG}"                                  # @60
            f"{DATEYY1}"                                 # @61
            f"-{DATEMM1}-{DATEDD1}"                      # @65
            f"{fmt_pd1(PROMPT)}"                         # @71
            f"INIT"                                      # @72
            f"{DATEYY1}-{DATEMM1}-{DATEDD1}"             # @77
            f"01.01.01"                                  # @87
            f"INIT"                                      # @95
            f"{DATEYY1}-{DATEMM1}-{DATEDD1}"             # @100
            f"01.01.01"                                  # @110
            f"INIT"                                      # @118
            f"INIT"                                      # @126
            f"INIT"                                      # @131
            f"{fmt_pd8(PHONE)}"                          # @151
        )

        f.write(line + "\n")

print("Completed: Output written to:")
print(" -", MERGE_PARQUET)
print(" -", OUTPUT_TXT)
