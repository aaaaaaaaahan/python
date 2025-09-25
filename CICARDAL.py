import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# ================================
# CONFIGURATION
# ================================
acct_files = ["ACCT33.parquet", "ACCT55.parquet"]
card_files = ["CARD33.parquet", "CARD55.parquet"]
output_file = Path("CARDALL.parquet")

# ================================
# DUCKDB CONNECTION
# ================================
con = duckdb.connect()

# Register parquet datasets
con.execute(f"""
    CREATE OR REPLACE VIEW ACCT AS
    SELECT 
        ACCTNO,
        OPENDATE,
        UNINAME1,
        UNINAME2,
        CUSTNBR,
        CUSTREF,
        CARDTYPE
    FROM read_parquet({acct_files})
""")

con.execute(f"""
    CREATE OR REPLACE VIEW CARD AS
    SELECT 
        ACCTNO,
        ISSUEDATE,
        CARDNO,
        CLOSEDATE
    FROM read_parquet({card_files})
""")

# ================================
# TRANSFORM ACCT (replicate SAS DATA step)
# ================================
con.execute("""
    CREATE OR REPLACE TABLE ACCT_T AS
    SELECT
        ACCTNO,
        -- Convert OPENDATE(6.) = DDMMYY6. into date
        TRY_CAST(STRPTIME(LPAD(OPENDATE,6,'0'), '%d%m%y') AS DATE) AS NEWODATE,
        STRFTIME(TRY_CAST(STRPTIME(LPAD(OPENDATE,6,'0'), '%d%m%y') AS DATE), '%Y-%m-%d') AS NEWDATE,
        UNINAME1,
        UNINAME2,
        CARDTYPE,
        CUSTNBR,
        CUSTREF,
        CASE
            WHEN TRIM(CUSTNBR) != '' AND SUBSTR(CUSTNBR,12,1) = ' ' THEN CUSTNBR
            WHEN TRIM(CUSTREF) != '' AND SUBSTR(CUSTREF,12,1) = ' ' THEN CUSTREF
        END AS OLDIC,
        CASE
            WHEN TRIM(CUSTNBR) != '' AND REGEXP_MATCHES(CUSTNBR, '^[0-9]+$') THEN CUSTNBR
            WHEN TRIM(CUSTREF) != '' AND SUBSTR(CUSTREF,12,1) != ' ' THEN CUSTREF
        END AS NEWIC,
        CASE
            WHEN (TRIM(CUSTNBR) != '' OR TRIM(CUSTREF) != '') THEN 'IC '
        END AS ALIAS
    FROM ACCT
""")

# ================================
# MERGE ACCT + CARD
# ================================
con.execute("""
    CREATE OR REPLACE TABLE MRGCARD AS
    SELECT DISTINCT
        a.ACCTNO,
        a.NEWDATE,
        a.CARDTYPE,
        c.CARDNO,
        a.UNINAME1,
        a.UNINAME2,
        a.ALIAS,
        a.NEWIC,
        a.OLDIC
    FROM ACCT_T a
    LEFT JOIN CARD c USING(ACCTNO)
""")

# ================================
# FETCH FINAL RESULT
# ================================
table = con.execute("SELECT * FROM MRGCARD").arrow()

# ================================
# WRITE OUTPUT WITH PYARROW
# ================================
pq.write_table(table, output_file, compression="snappy")

print(f"✅ Output written to {output_file}")
