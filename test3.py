# cicbdext_duckdb.py
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from CIS_PY_READER import host_parquet_path, parquet_output_path, text_output_path  # your helpers
import datetime
import os

# ============================================================
# SETUP
# ============================================================
con = duckdb.connect()

# Input parquet files (assumed already converted)
newchg_pq = host_parquet_path("IDIC.DAILY.INEW.parquet")   # NEWCHG
oldchg_pq = host_parquet_path("IDIC.DAILY.IOLD.parquet")   # OLDCHG
nochg_pq = host_parquet_path("IDIC.DAILY.NOCHG.parquet")   # NOCHG
rlen_pq  = host_parquet_path("UNLOAD.RLEN#CA.parquet")     # RLEN#CA
ctrldate_pq = host_parquet_path("SRSCTRL1.parquet")        # CTRLDATE - assumed small single-row parquet with SRSYY,SRSMM,SRSDD

# Output text file destinations (mimic OUTFILE, OUTFIL1, OUTFIL2, OUTBPM1)
out_dpfile   = text_output_path("CDB.DPFILE.txt")    # corresponds to RECORDS / OUTFILE
out_lnfile   = text_output_path("CDB.LNFILE.txt")    # corresponds to BPMREC2 / OUTFIL1
out_btfile   = text_output_path("CDB.BTFILE.txt")    # corresponds to RECORD2 / OUTFIL2
out_bpmfile  = text_output_path("BPM.DPFILE.txt")    # corresponds to BPMREC1 / OUTBPM1

# Helper to read control date (SRSYY,SRSMM,SRSDD) - fallback to today if not available
def get_rdate():
    try:
        tbl = con.execute(f"SELECT * FROM read_parquet('{ctrldate_pq}') LIMIT 1").fetchdf()
        if {'SRSYY','SRSMM','SRSDD'}.issubset(set(tbl.columns)):
            yy = int(tbl.at[0,'SRSYY'])
            mm = int(tbl.at[0,'SRSMM'])
            dd = int(tbl.at[0,'SRSDD'])
            d = datetime.date(yy, mm, dd)
            return d.strftime("%Y%m%d")
    except Exception:
        pass
    # fallback to today
    return datetime.date.today().strftime("%Y%m%d")

RDATE = get_rdate()

# ============================================================
# STEP 1: Read NEWCHG, OLDCHG, NOCHG into DuckDB tables
# (assume parquet columns correspond to SAS positions; adapt names as necessary)
# ============================================================
con.execute(f"""
    CREATE TABLE newchg AS
    SELECT
        -- expected parquet columns (adapt if different names)
        CUSTNO,
        CUSTMNTDATE      AS CUSTMNTDATE,
        CUSTLASTOPER     AS CUSTLASTOPER,
        CUST_CODE        AS CUST_CODE,
        MSICCODE         AS MSICCODE
    FROM read_parquet('{newchg_pq}')
    WHERE CUSTNO IS NOT NULL
""")

con.execute(f"""
    CREATE TABLE oldchg AS
    SELECT
        CUSTNO,
        CUSTMNTDATEX     AS CUSTMNTDATEX,
        CUSTLASTOPERX    AS CUSTLASTOPERX,
        CUST_CODEX       AS CUST_CODEX,
        MSICCODEX        AS MSICCODEX
    FROM read_parquet('{oldchg_pq}')
    WHERE CUSTNO IS NOT NULL
""")

# NOCHG -> NEWCUST: include runtime timestamp, select only rows matching a given DATESTAMP
# We assume nochg has RUNTIMESTAMP, CUSTNO, CUSTMNTDATEX, CUSTLASTOPERX, CUST_CODE, MSICCODE
# The SAS used macro vars YEAR/MONTH/DAY to form DATESTAMP; here we'll use RDATE from control date.
con.execute(f"""
    CREATE TABLE nochg_raw AS
    SELECT
        RUNTIMESTAMP,
        CUSTNO,
        CUSTMNTDATEX,
        CUSTLASTOPERX,
        CUST_CODE,
        MSICCODE
    FROM read_parquet('{nochg_pq}')
    WHERE CUSTNO IS NOT NULL
""")

# Make NEWCUST by filtering rows where substr(RUNTIMESTAMP,1,8) == RDATE
con.execute(f"""
    CREATE TABLE newcust AS
    SELECT
        RUNTIMESTAMP,
        CUSTNO,
        CUSTMNTDATEX,
        CUSTLASTOPERX,
        CUST_CODE,
        MSICCODE,
        '{RDATE}' AS DATESTAMP,
        SUBSTR(RUNTIMESTAMP,1,8) AS DATEREC,
        CASE WHEN COALESCE(CUST_CODE,'') <> '' THEN 'Y' ELSE 'N' END AS UPDMSIC,
        CASE WHEN COALESCE(MSICCODE,'') <> '' THEN 'Y' ELSE 'N' END AS UPDCCDE
    FROM nochg_raw
    WHERE SUBSTR(RUNTIMESTAMP,1,8) = '{RDATE}'
""")

# ============================================================
# STEP 2: Read RLEN file and filter ACCTCODE in ('DP','LN')
# Note: SAS used positional + PD2. for RLENCODE; expect numeric RLENCODE column.
# ============================================================
con.execute(f"""
    CREATE TABLE rlen AS
    SELECT
        ACCTNOC,
        TRIM(ACCTCODE) AS ACCTCODE,
        CUSTNO,
        CAST(RLENCODE AS INTEGER) AS RLENCODE,
        CAST(PRISEC AS INTEGER) AS PRISEC,
        LPAD(CAST(CAST(RLENCODE AS INTEGER) AS VARCHAR),3,'0') AS RLENCD
    FROM read_parquet('{rlen_pq}')
    WHERE TRIM(ACCTCODE) IN ('DP','LN')
""")

# ============================================================
# STEP 3: Merge NEWCHG + OLDCHG (inner join on CUSTNO) => MERGE_A
# ============================================================
con.execute("""
    CREATE TABLE merge_a AS
    SELECT a.*, b.CUSTMNTDATEX, b.CUSTLASTOPERX, b.CUST_CODEX, b.MSICCODEX
    FROM newchg a
    JOIN oldchg b USING (CUSTNO)
""")

# ============================================================
# STEP 4: DTCHG -> find rows where MSIC or CUST_CODE changed
# ============================================================
con.execute("""
    CREATE TABLE dtchg AS
    SELECT
        CUSTNO,
        CUSTMNTDATE,
        CUSTLASTOPER,
        CUST_CODE,
        MSICCODE,
        CUSTMNTDATEX,
        CUSTLASTOPERX,
        CUST_CODEX,
        MSICCODEX,
        CASE WHEN COALESCE(MSICCODE,'') <> COALESCE(MSICCODEX,'') THEN 'Y' ELSE 'N' END AS UPDMSIC,
        CASE WHEN COALESCE(CUST_CODE,'') <> COALESCE(CUST_CODEX,'') THEN 'Y' ELSE 'N' END AS UPDCCDE
    FROM merge_a
    WHERE COALESCE(MSICCODE,'') <> COALESCE(MSICCODEX,'') OR COALESCE(CUST_CODE,'') <> COALESCE(CUST_CODEX,'')
""")

# ============================================================
# STEP 5: MIXALL = DTCHG UNION ALL NEWCUST
# ============================================================
con.execute("""
    CREATE TABLE mixall AS
    SELECT *, UPDMSIC, UPDCCDE FROM dtchg
    UNION ALL
    SELECT
      RUNTIMESTAMP, CUSTNO, CUSTMNTDATEX, CUSTLASTOPERX, CUST_CODE, MSICCODE,
      NULL, NULL, NULL, NULL, UPDMSIC, UPDCCDE
    FROM newcust
""")

# Normalize column names in mixall for subsequent join with rlen:
# We'll create a view-like table with expected columns:
con.execute("""
    CREATE TABLE mixall_norm AS
    SELECT
      CUSTNO,
      COALESCE(CUSTMNTDATE, CUSTMNTDATEX) AS CUSTMNTDATE,
      COALESCE(CUSTLASTOPER, CUSTLASTOPERX) AS CUSTLASTOPER,
      COALESCE(CUST_CODE, CUST_CODEX) AS CUST_CODE,
      COALESCE(MSICCODE, MSICCODEX) AS MSICCODE,
      UPDMSIC,
      UPDCCDE
    FROM mixall
""")

# ============================================================
# STEP 6: Merge MIXALL with RLEN by CUSTNO -> create DPLIST, BTLIST, LNALL, DPALL
# Logic implemented per SAS:
#   - if F and G (i.e. match in both)
#   - BTRADE = 'Y' if ACCTNOC startswith '025' or startswith '0285'
#   - DPALL: ACCTCODE == 'DP'
#   - BTLIST: ACCTCODE == 'LN' AND BTRADE == 'Y'
#   - LNALL: ACCTCODE == 'LN' AND BTRADE != 'Y'
#   - DPLIST: RLENCD == '020' and ACCTCODE == 'DP'
# ============================================================
con.execute("""
    CREATE TABLE merged_mix_rlen AS
    SELECT
        m.CUSTNO,
        r.ACCTNOC,
        TRIM(r.ACCTCODE) AS ACCTCODE,
        m.CUST_CODE,
        m.MSICCODE,
        m.UPDMSIC,
        m.UPDCCDE,
        r.RLENCD,
        CASE
            WHEN SUBSTR(coalesce(r.ACCTNOC,''),1,3) = '025' THEN 'Y'
            WHEN SUBSTR(coalesce(r.ACCTNOC,''),1,4) = '0285' THEN 'Y'
            ELSE 'N'
        END AS BTRADE
    FROM mixall_norm m
    JOIN rlen r USING (CUSTNO)
""")

# DPLIST: RLENCD == '020' AND ACCTCODE == 'DP'
con.execute("""
    CREATE TABLE dplist AS
    SELECT *
    FROM merged_mix_rlen
    WHERE RLENCD = '020' AND ACCTCODE = 'DP'
""")

# DPALL: ACCTCODE == 'DP' (all RLEN values)
con.execute("""
    CREATE TABLE dpall AS
    SELECT *
    FROM merged_mix_rlen
    WHERE ACCTCODE = 'DP'
""")

# BTLIST: ACCTCODE == 'LN' AND BTRADE == 'Y'
con.execute("""
    CREATE TABLE btlist AS
    SELECT *
    FROM merged_mix_rlen
    WHERE ACCTCODE = 'LN' AND BTRADE = 'Y'
""")

# LNALL: ACCTCODE == 'LN' AND BTRADE != 'Y'
con.execute("""
    CREATE TABLE lnall AS
    SELECT *
    FROM merged_mix_rlen
    WHERE ACCTCODE = 'LN' AND BTRADE = 'N'
""")

# ============================================================
# STEP 7: Create text outputs with header/footer (mimic SAS PUT)
# - OUTFILE (DPLIST) -> out_dpfile (fixed width)
# - OUTFIL1 (LNALL) -> out_lnfile (fixed width)
# - OUTFIL2 (BTLIST) -> out_btfile (semicolon separated fields)
# - OUTBPM1 (DPALL) -> out_bpmfile (fixed width, include RLENCD)
# ============================================================

def write_fixed_width(rows, filepath, columns, header_date, footer_wrap=True):
    """
    rows: iterable of dict-like rows
    columns: list of (colname, width, start_pos) or just (colname,width)
    header_date: RDATE string
    footer_wrap: add trailer with count (T + TTL)
    """
    with open(filepath, 'w', newline='') as f:
        # Header line - @001 'FH ' + RDATE
        f.write(f"FH {header_date}\n")
        ttl = 0
        for row in rows:
            line = ""
            pos = 1
            for col, width in columns:
                val = row.get(col, '') or ''
                s = str(val)
                # ensure exact width: left align, pad spaces
                if len(s) > width:
                    s = s[:width]
                else:
                    s = s.ljust(width)
                line += s
                pos += width
            f.write(line + "\n")
            ttl += 1
        if footer_wrap:
            f.write(f"T{str(ttl).rjust(3,' ')}\n")  # rjust to imitate formatting
    return ttl

def write_semicolon_file(rows, filepath, cols_order, header_date):
    with open(filepath, 'w', newline='') as f:
        f.write(f"FH {header_date}\n")
        ttl = 0
        for row in rows:
            parts = []
            for c in cols_order:
                val = row.get(c, '') or ''
                parts.append(str(val))
            line = ';'.join(parts)
            f.write(line + "\n")
            ttl += 1
        f.write(f"FH\n")  # SAS had IF EOF THEN PUT @001 'FH' for RECORD2 (odd), we mimic minimal
    return ttl

# Pull tables into python arrow tables for customized writes
def table_to_dicts(tbl_name):
    """Return list of dicts for each row from duckdb table."""
    df = con.execute(f"SELECT * FROM {tbl_name}").fetchdf()
    return df.to_dict(orient='records')

# DPLIST -> out_dpfile (CUSTNO(11),ACCTNOC(20),UPDCCDE(1),CUST_CODE(3),UPDMSIC(1),MSICCODE(5))
dplist_rows = table_to_dicts("dplist")
dplist_cols = [("CUSTNO",11), ("ACCTNOC",20), ("UPDCCDE",1), ("CUST_CODE",3), ("UPDMSIC",1), ("MSICCODE",5)]
ttl_dplist = write_fixed_width(dplist_rows, out_dpfile, dplist_cols, RDATE)

# LNALL -> out_lnfile (same format as BPMREC2)
lnall_rows = table_to_dicts("lnall")
lnall_cols = [("CUSTNO",11), ("ACCTNOC",20), ("UPDCCDE",1), ("CUST_CODE",3), ("UPDMSIC",1), ("MSICCODE",5)]
ttl_lnall = write_fixed_width(lnall_rows, out_lnfile, lnall_cols, RDATE)

# BTLIST -> out_btfile (semicolon separated with some different formatting in SAS)
btlist_rows = table_to_dicts("btlist")
# SAS RECORD2 used semicolons and slightly different column placements; we create fields then semicolon join:
btlist_cols_order = ["CUSTNO", "ACCTNOC", "UPDCCDE", "CUST_CODE", "UPDMSIC", "MSICCODE"]
ttl_btlist = write_semicolon_file(btlist_rows, out_btfile, btlist_cols_order, RDATE)

# DPALL -> out_bpmfile (include RLENCD at pos 48 in SAS, we append RLENCD field)
dpall_rows = table_to_dicts("dpall")
dpall_cols = [("CUSTNO",11), ("ACCTNOC",20), ("UPDCCDE",1), ("CUST_CODE",3), ("UPDMSIC",1), ("MSICCODE",5), ("RLENCD",3)]
ttl_dpall = write_fixed_width(dpall_rows, out_bpmfile, dpall_cols, RDATE)

# ============================================================
# STEP 8: Optionally write intermediate sets to parquet for auditing
# ============================================================
audit_out_dir = parquet_output_path("cicbdext_audit")
os.makedirs(os.path.dirname(audit_out_dir), exist_ok=True)
# write a few small audit files (optional)
for tbl in ["dplist","lnall","btlist","dpall","merged_mix_rlen","mixall_norm","rlen"]:
    try:
        table = con.execute(f"SELECT * FROM {tbl}").arrow()
        pq.write_table(table, parquet_output_path(f"audit_{tbl}.parquet"))
    except Exception:
        pass

# ============================================================
# FINISH
# ============================================================
print("✅ Wrote files:")
print(f"  DPLIST -> {out_dpfile} (count {ttl_dplist})")
print(f"  LNALL -> {out_lnfile} (count {ttl_lnall})")
print(f"  BTLIST -> {out_btfile} (count {ttl_btlist})")
print(f"  DPALL -> {out_bpmfile} (count {ttl_dpall})")
print("✅ Optional audit parquet files written to parquet output folder (if helper supports it).")
