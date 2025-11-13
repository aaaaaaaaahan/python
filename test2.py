import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

# -------------------------------
# CONFIG: File paths
# -------------------------------
consent_file = 'consent.parquet'  # Assuming you already converted CONSENT file to Parquet
cust_file = 'cust.parquet'        # Assuming you already converted CUSTFILE to Parquet

all_output_txt = 'ALLFILE.txt'
daily_output_txt = 'DLYFILE.txt'
all_output_parquet = 'ALLFILE.parquet'
daily_output_parquet = 'DLYFILE.parquet'

# -------------------------------
# CREATE TODAY DATE VARIABLES
# -------------------------------
today = datetime.today()
DATE1 = today.strftime('%Y%m%d')
DATE2 = today.strftime('%Y%m%d')  # Same as SAS &DATE2

# -------------------------------
# CONNECT TO DUCKDB
# -------------------------------
con = duckdb.connect()

# -------------------------------
# LOAD CONSENT DATA
# -------------------------------
# Convert EFFTIMESTAMP to inverted value like in SAS: 100000000000000 - EFFTIMESTAMP
con.execute(f"""
    CREATE OR REPLACE VIEW consent AS
    SELECT
        CUSTNO,
        100000000000000 - EFFTIMESTAMP AS EFFDATETIME,
        CAST(100000000000000 - EFFTIMESTAMP AS VARCHAR) AS EFFDATETIMEX,
        SUBSTR(CAST(100000000000000 - EFFTIMESTAMP AS VARCHAR), 1, 8) AS EFFDATE,
        SUBSTR(CAST(100000000000000 - EFFTIMESTAMP AS VARCHAR), 9, 6) AS EFFTIME,
        KEYWORD,
        CHANNEL,
        CONSENT
    FROM read_parquet('{consent_file}')
""")

# -------------------------------
# LOAD CUSTOMER DATA
# -------------------------------
# Filter out specific ACCTCODE, invalid ACCTNOC, and empty ALIASKEY & TAXID
con.execute(f"""
    CREATE OR REPLACE VIEW cust AS
    SELECT DISTINCT *
    FROM read_parquet('{cust_file}')
    WHERE ACCTCODE NOT IN ('DP   ','LN   ','EQC  ','FSF  ')
      AND ACCTNOC > '1000000000000000'
      AND ACCTNOC < '9999999999999999'
      AND NOT (ALIASKEY = '' AND TAXID = '')
""")

# -------------------------------
# MERGE CONSENT AND CUSTOMER DATA
# -------------------------------
con.execute("""
    CREATE OR REPLACE VIEW merge_data AS
    SELECT c.*, co.CONSENT, co.CHANNEL, co.EFFDATETIME, co.EFFDATE, co.EFFTIME
    FROM cust c
    INNER JOIN consent co
    ON c.CUSTNO = co.CUSTNO
""")

# -------------------------------
# OUTPUT ALLFILE
# -------------------------------
all_df = con.execute("""
    SELECT ACCTNOC, ALIASKEY, ALIAS, TAXID, CONSENT, CHANNEL
    FROM merge_data
    ORDER BY ACCTNOC
""").fetchdf()

# TXT output like SAS PUT
with open(all_output_txt, 'w') as f:
    for row in all_df.itertuples(index=False):
        f.write(f"{row.ACCTNOC:<16}{row.ALIASKEY:<3}{row.ALIAS:<12}{row.TAXID:<12}{row.CONSENT:<1}{row.CHANNEL:<8}\n")

# Parquet output
all_table = pa.Table.from_pandas(all_df)
pq.write_table(all_table, all_output_parquet)

# -------------------------------
# OUTPUT DAILY FILE (DAY)
# -------------------------------
daily_df = con.execute(f"""
    SELECT ACCTNOC, ALIASKEY, ALIAS, TAXID, CONSENT, CHANNEL
    FROM merge_data
    WHERE EFFDATE = '{DATE2}' AND CHANNEL != 'UNIBATCH'
    ORDER BY ACCTNOC
""").fetchdf()

# TXT output
with open(daily_output_txt, 'w') as f:
    for row in daily_df.itertuples(index=False):
        f.write(f"{row.ACCTNOC:<16}{row.ALIASKEY:<3}{row.ALIAS:<12}{row.TAXID:<12}{row.CONSENT:<1}{row.CHANNEL:<8}\n")

# Parquet output
daily_table = pa.Table.from_pandas(daily_df)
pq.write_table(daily_table, daily_output_parquet)

print("Processing completed!")
