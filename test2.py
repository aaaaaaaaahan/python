import duckdb
from CIS_PY_READER import host_parquet_path, get_hive_parquet, parquet_output_path, csv_output_path
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day

# ==========================================
# 1) CONFIG
# ==========================================
con = duckdb.connect()
hrcstdp = get_hive_parquet('CIS_HRCCUST_DPACCTS')

# ========================================================
# 2) CISDP — SAS fixed-col input rewritten using DuckDB
# ========================================================
# Assumption: parquet already contains correct columns
# Otherwise, use substr() to slice fields
con.execute(f"""
    CREATE TABLE CISDP AS 
    SELECT
        BANKNUM,
        CUSTBRCH,
        CUSTNO,
        CUSTNAME,
        RACE,
        CITIZENSHIP,
        INDORG,
        PRIMSEC,
        CUSTLASTDATECC,
        CUSTLASTDATEYY,
        CUSTLASTDATEMM,
        CUSTLASTDATEDD,
        ALIASKEY,
        ALIAS,
        HRCCODES,
        ACCTCODE,
        ACCTNO
    FROM read_parquet('{hrcstdp[0]}')
    ORDER BY ACCTNO
""")

# ========================================================
# 3) DPDATA — SAS INPUT with conditional fields
# ========================================================
con.execute(f"""
    CREATE TABLE DPDATA AS
    SELECT
        CAST(BANKNO AS INTEGER) AS BANKNO,
        CAST(REPTNO AS INTEGER) AS REPTNO,
        CAST(FMTCODE AS INTEGER) AS FMTCODE,
        LPAD(CAST(CAST(BRANCH AS INT) AS VARCHAR),3,'0') AS BRANCH,
        LPAD(CAST(CAST(ACCTNO AS BIGINT) AS VARCHAR),11,'0') AS ACCTNO,
        CAST(CLOSEDT AS BIGINT) AS CLSDATE,
        CAST(REOPENDT AS BIGINT) AS OPENDATE,
        CAST(LEDGBAL AS BIGINT) AS LEDBAL,
        OPENIND AS ACCSTAT,
        CAST(COSTCTR AS INTEGER) AS COSTCTR,
        -- SAS: TMPACCT = PUT(ACCTNO,Z10.)
        LPAD(CAST(CAST(ACCTNO AS BIGINT) AS VARCHAR),10,'0') AS TMPACCT
    FROM '{host_parquet_path("DPTRBLGS_CIS.parquet")}'
    WHERE REPTNO = 1001
      AND FMTCODE IN (1,5,10,11,19,20,21,22)
      AND BRANCH <> 0
      AND OPENDATE <> 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY ACCTNO) = 1
    ORDER BY ACCTNO
""")

# ========================================================
# 4) MERGE → GOODDP / BADDP  (SAS MERGE logic)
# ========================================================
con.execute("""
    CREATE TABLE MERGED AS
    SELECT
        A.*,
        B.BANKNUM, B.CUSTBRCH, B.CUSTNO, B.CUSTNAME, B.RACE,
        B.CITIZENSHIP, B.INDORG, B.PRIMSEC,
        B.CUSTLASTDATECC, B.CUSTLASTDATEYY, B.CUSTLASTDATEMM, B.CUSTLASTDATEDD,
        B.ALIASKEY, B.ALIAS, B.HRCCODES, B.ACCTCODE
    FROM DPDATA A
    JOIN CISDP B USING (ACCTNO)
""")

# GOOD / BAD based on SAS conditions
con.execute("""
    CREATE TABLE GOODDP AS
    SELECT *
    FROM MERGED
    WHERE
        (
            SUBSTR(TMPACCT, 1, 1) IN ('1', '3')
            AND ACCSTAT NOT IN ('C', 'B', 'P', 'Z')
        )
        OR
        (
            SUBSTR(TMPACCT, 1, 1) NOT IN ('1','3')
            AND (ACCSTAT NOT IN ('C','B','P','Z') OR LEDBAL <> 0)
        )
    ORDER BY CUSTNO, ACCTNO
""")

con.execute("""
    CREATE TABLE BADDP AS
    SELECT *
    FROM MERGED
    EXCEPT
    SELECT * FROM GOODDP
""")

# ========================================================
# 5) PBB / PIBB SPLIT (SAS SORT OUTFIL)
#    OUTFIL:
#    - IF FIELD 210 != '3' → PBB (conventional)
#    - IF FIELD 210 == '3' → PIBB (Islamic)
# ========================================================
con.execute("""
    CREATE TABLE GOOD_PBB AS
    SELECT * FROM GOODDP
    WHERE COSTCTR <> 3     -- matches SAS INCLUDE=(210,1,CH,NE,'3')
""")

con.execute("""
    CREATE TABLE GOOD_PIBB AS
    SELECT * FROM GOODDP
    WHERE SUBSTR(CAST(COSTCTR AS VARCHAR),1,1) = 3      -- matches SAS INCLUDE=(210,1,CH,EQ,'3')
""")

# ----------------------------
# OUTPUT TO PARQUET, CSV, TXT
# ----------------------------

# Dictionary of tables to output
output_tables = {
    "CIS_HRCCUST_DPACCTS_GOOD"      : "GOODDP",
    "CIS_HRCCUST_DPACCTS_CLOSED"    : "BADDP",
    "CIS_HRCCUST_DPACCTS_GOOD_PBB"  : "GOOD_PBB",
    "CIS_HRCCUST_DPACCTS_GOOD_PIBB" : "GOOD_PIBB"
}

for name, table in output_tables.items():
    # Query with date columns
    query = f"""
        SELECT
            BANKNUM,
            CUSTBRCH,
            CUSTNO,
            CUSTNAME,
            RACE,
            CITIZENSHIP,
            INDORG,
            PRIMSEC,
            CUSTLASTDATECC,
            CUSTLASTDATEYY,
            CUSTLASTDATEMM,
            CUSTLASTDATEDD,
            ALIASKEY,
            ALIAS,
            HRCCODES,
            BRANCH,
            ACCTCODE,
            ACCTNO,
            OPENDATE,
            LEDBAL,
            ACCSTAT,
            COSTCTR,
            {year} AS year,
            {month} AS month,
            {day} AS day
        FROM {table}
    """
    
    # Paths
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)
    txt_path = csv_output_path(f"{name}_{batch_date}").replace(".csv", ".txt")
    
    # ----------------------------
    # COPY to Parquet with partitioning
    # ----------------------------
    con.execute(f"""
        COPY ({query})
        TO '{parquet_path}'
        (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE TRUE)
    """)
    
    # ----------------------------
    # COPY to CSV with header
    # ----------------------------
    con.execute(f"""
        COPY ({query})
        TO '{csv_path}'
        (FORMAT CSV, HEADER, DELIMITER ';', OVERWRITE_OR_IGNORE TRUE)
    """)
    
    # ----------------------------
    # Fixed-width TXT following SAS PUT layout
    # ----------------------------
    df_txt = con.execute(query).fetchdf()
    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(row.get('BANKNUM','')).rjust(3,'0')}"                  # @01 BANKNUM Z3.
                f"{str(row.get('CUSTBRCH','')).rjust(5,'0')}"                # @04 CUSTBRCH Z5.
                f"{str(row.get('CUSTNO','')).ljust(11)}"                     # @09 CUSTNO $11.
                f"{str(row.get('CUSTNAME','')).ljust(40)}"                   # @20 CUSTNAME $40.
                f"{str(row.get('RACE','')).ljust(1)}"                        # @60 RACE $1.
                f"{str(row.get('CITIZENSHIP','')).ljust(2)}"                 # @61 CITIZENSHIP $2.
                f"{str(row.get('INDORG','')).ljust(1)}"                      # @63 INDORG $1.
                f"{str(row.get('PRIMSEC','')).ljust(1)}"                     # @64 PRIMSEC $1.
                f"{str(row.get('CUSTLASTDATECC','')).rjust(2,'0')}"          # @65 CUSTLASTDATECC Z2.
                f"{str(row.get('CUSTLASTDATEYY','')).rjust(2,'0')}"          # @67 CUSTLASTDATEYY Z2.
                f"{str(row.get('CUSTLASTDATEMM','')).rjust(2,'0')}"          # @69 CUSTLASTDATEMM Z2.
                f"{str(row.get('CUSTLASTDATEDD','')).rjust(2,'0')}"          # @71 CUSTLASTDATEDD Z2.
                f"{str(row.get('ALIASKEY','')).rjust(3,'0')}"                # @73 ALIASKEY Z3.
                f"{str(row.get('ALIAS','')).ljust(20)}"                      # @76 ALIAS $20.
                f"{str(row.get('HRCCODES','')).ljust(60)}"                   # @96 HRCCODES $60.
                f"{str(row.get('BRANCH','')).rjust(7,'0')}"                  # @156 BRANCH Z7.
                f"{str(row.get('ACCTCODE','')).ljust(5)}"                    # @163 ACCTCODE $5.
                f"{str(row.get('ACCTNO','')).ljust(20)}"                     # @168 ACCTNO 20.
                f"{str(row.get('OPENDATE','')).rjust(8,'0')}"                # @188 OPDATE 8.
                f"{str(row.get('LEDBAL','')).rjust(13,'0')}"                 # @196 LEDBAL Z13.
                f"{str(row.get('ACCSTAT','')).ljust(1)}"                     # @209 ACCSTAT $1.
                f"{str(row.get('COSTCTR','')).rjust(4,'0')}"                 # @210 COSTCTR Z4.
            )
            f.write(line + "\n")
