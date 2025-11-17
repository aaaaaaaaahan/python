#!/usr/bin/env python3
"""
cicuscd5_convert.py
Convert SAS job CICUSCD5 to Python using DuckDB + PyArrow.
Produces parquet outputs and fixed-width text outputs similar to SAS PUT files.
"""

import os
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from textwrap import dedent

# -----------------------
# CONFIG - change these
# -----------------------
paths = {
    # Input parquet files (already converted from original sources)
    "custfile_parquet": "input/CIS.CUST.DAILY.parquet",    # original CUSTFILE.CUSTDLY
    "custcode_parquet": "input/CUSTCODE.parquet",         # original CUSTCODE (CUSTNO, RECTYPE, BRANCH, C01..C20)
    "hrms_parquet": "input/HCMS.STAFF.TAG.parquet",       # original HRMSFILE (semicolon CSV originally)
    # Optionally existing temporary DP created earlier (we will overwrite)
}

outdir = "outputs"
os.makedirs(outdir, exist_ok=True)

# Output paths (txt fixed width and parquet)
outputs = {
    "notfound_txt": os.path.join(outdir, "NOTFOUND.txt"),
    "notfound_parquet": os.path.join(outdir, "NOTFOUND.parquet"),
    "dpteam_txt": os.path.join(outdir, "DPTEAM.txt"),                  # DPFILE initial
    "dpteam_parquet": os.path.join(outdir, "DPTEAM.parquet"),
    "outfile_temp_parquet": os.path.join(outdir, "UPDATE_OUT_TEMP.parquet"),  # OUT (pre-aggregation)
    "outfile_sorted_txt": os.path.join(outdir, "UPDATE_SORTED.txt"),   # final OUTFILE after W1..W20
    "outfile_sorted_parquet": os.path.join(outdir, "UPDATE_SORTED.parquet"),
    "dpfile_final_txt": os.path.join(outdir, "DPFILE_FINAL.txt"),      # final staff list
    "dpfile_final_parquet": os.path.join(outdir, "DPFILE_FINAL.parquet"),
}

# Helper formatting functions to create zero-padded strings like SAS PUT Z11., Z7., Z3.
def zfill_str(value, width):
    """Return zero-padded string for numeric or numeric-like strings. If NA/None/NaN -> zeros"""
    if pd.isna(value):
        return "0" * width
    try:
        iv = int(value)
        return f"{iv:0{width}d}"
    except Exception:
        s = str(value).strip()
        # if string contains leading zeros or non-numeric, left-pad/truncate
        return s.zfill(width)[:width]

def zfill_str_allow_blank(value, width):
    """Return blank string if value is missing; else zero-padded numeric string (used for STAFFNO? not needed)"""
    if pd.isna(value) or str(value).strip() == "":
        return " " * width
    try:
        iv = int(value)
        return f"{iv:0{width}d}"
    except Exception:
        s = str(value).strip()
        return s.zfill(width)[:width]

# -----------------------
# Connect to DuckDB
# -----------------------
con = duckdb.connect(database=':memory:')

# Register parquet files as DuckDB tables
con.execute(f"CREATE TABLE custfile AS SELECT * FROM read_parquet('{paths['custfile_parquet']}');")
con.execute(f"CREATE TABLE custcode AS SELECT * FROM read_parquet('{paths['custcode_parquet']}');")
con.execute(f"CREATE TABLE hrms_src AS SELECT * FROM read_parquet('{paths['hrms_parquet']}');")

# -----------------------
# Step 1: Build CUST table (filter ACCTCODE = 'DP' and PRISEC = 901)
# Equivalent of:
#   SET CUSTFILE.CUSTDLY;
#   IF ACCTCODE EQ 'DP';
#   IF PRISEC = 901 ;
#   BRANCH = PUT(CUSTBRCH,Z7.);
#   ACCTNOC = PUT(ACCTNO,Z11.)
# Keep fields: CUSTNO ACCTNOC ACCTCODE CUSTNAME TAXID ALIASKEY ALIAS JOINTACC BRANCH
# -----------------------
con.execute(dedent("""
    CREATE TABLE CUST AS
    SELECT
        CUSTNO,
        -- ACCTNO may be numeric; create ACCTNOC as zero-padded 11 digits
        LPAD(CAST(ACCTNO AS VARCHAR), 11, '0') AS ACCTNOC,
        ACCTCODE,
        CUSTNAME,
        TAXID,
        ALIASKEY,
        ALIAS,
        JOINTACC,
        LPAD(CAST(CUSTBRCH AS VARCHAR), 7, '0') AS BRANCH,
        PRISEC
    FROM custfile
    WHERE ACCTCODE = 'DP' AND PRISEC = 901
    ;
"""))

# Remove duplicates by ACCTNOC as SAS PROC SORT NODUPKEY;BY ACCTNOC;
con.execute("CREATE TABLE CUST_NODUP AS SELECT * FROM CUST GROUP BY ACCTNOC, CUSTNO, ACCTCODE, CUSTNAME, TAXID, ALIASKEY, ALIAS, JOINTACC, BRANCH, PRISEC;")
con.execute("CREATE TABLE CUST_FINAL AS SELECT * FROM CUST_NODUP ORDER BY CUSTNO;")

# -----------------------
# Step 2: Read CUSTCODE (already parquet). HRCCODE is created by a fixed-width read originally; now it's parquet
# Assume custcode has columns: CUSTNO, RECTYPE, BRANCH, C01..C20
# -----------------------
# Ensure numeric codes exist and cast to integers (NULLs => NULL)
# -----------------------
# We'll create HRCCODE table same column names but ensure types
cols_c = con.execute("PRAGMA table_info('custcode');").fetchdf()
# no need to detect, we'll create HRCCODE by selecting columns if present
# Create HRCCODE table as custcode with numeric C01..C20
c_fields = ["C{:02d}".format(i) for i in range(1,21)]
select_fields = "CUSTNO, RECTYPE, BRANCH"
for c in c_fields:
    select_fields += f", CAST({c} AS INTEGER) AS {c}"

con.execute(f"CREATE TABLE HRCCODE AS SELECT {select_fields} FROM custcode;")
con.execute("CREATE TABLE HRCCODE_SORTED AS SELECT * FROM HRCCODE ORDER BY CUSTNO;")

# -----------------------
# Step 3: Merge HRCCODE & CUST -> CUSTACCT (IF Y; kept when CUST exists)
# SAS: MERGE HRCCODE (IN=X)  CUST (IN=Y); BY CUSTNO; IF Y;
# This is effectively LEFT JOIN CUST LEFT JOIN HRCCODE ON CUSTNO (keep CUST rows, attach HRCCODE fields)
# -----------------------
con.execute(dedent("""
    CREATE TABLE CUSTACCT AS
    SELECT c.*, h.RECTYPE AS H_RECTYPE, h.BRANCH AS H_BRANCH,
           {hc_cols}
    FROM CUST_FINAL c
    LEFT JOIN HRCCODE_SORTED h
      ON c.CUSTNO = h.CUSTNO
    ;
""".format(hc_cols=", ".join([f"h.{c} AS {c}" for c in c_fields]))))

# -----------------------
# Step 4: HRMS table
# Original SAS: read HRMSFILE (semicolon-delimited) and create ACCTNOC = PUT(ACCTNO,Z11.), FILECODE='B'
# Now hrms_src is parquet with fields ORGCODE, STAFFNO, STAFFNAME, ACCTNO, NEWIC, OLDIC, BRANCHCODE
# Dedup by ACCTNOC
# -----------------------
con.execute(dedent("""
    CREATE TABLE HRMS AS
    SELECT
       ORGCODE,
       STAFFNO,
       STAFFNAME,
       LPAD(CAST(ACCTNO AS VARCHAR), 11, '0') AS ACCTNOC,
       NEWIC,
       OLDIC,
       BRANCHCODE,
       'B' AS FILECODE
    FROM hrms_src
    ;
"""))
con.execute("CREATE TABLE HRMS_NODUP AS SELECT * FROM HRMS GROUP BY ACCTNOC, ORGCODE, STAFFNO, STAFFNAME, NEWIC, OLDIC, BRANCHCODE, FILECODE;")

# -----------------------
# Step 5: MERGEFOUND and MERGEXMTCH
# SAS: MERGE HRMS(IN=A) CUSTACCT(IN=B); BY ACCTNOC; IF A AND B -> MERGEFOUND; IF A AND NOT B -> MERGEXMTCH;
# We need to join HRMS_NODUP (left) to CUSTACCT on ACCTNOC (CUSTACCT has ACCTNOC)
# -----------------------
con.execute(dedent("""
    CREATE TABLE MERGEFOUND AS
    SELECT h.*, c.CUSTNO AS CUSTNO, c.ACCTCODE AS ACCTCODE, c.JOINTACC AS JOINTACC,
           c.H_RECTYPE, c.H_BRANCH, {hc_cols}
    FROM HRMS_NODUP h
    INNER JOIN CUSTACCT c
      ON h.ACCTNOC = c.ACCTNOC
    ;
""".format(hc_cols=", ".join([f"c.{c}" for c in c_fields]))))

con.execute(dedent("""
    CREATE TABLE MERGEXMTCH AS
    SELECT h.*
    FROM HRMS_NODUP h
    LEFT JOIN CUSTACCT c
      ON h.ACCTNOC = c.ACCTNOC
    WHERE c.CUSTNO IS NULL
    ;
"""))

# -----------------------
# Step 6: NOTFOUND (MERGEXMTCH) -> write fixed-width text file (fields and positions same as SAS)
# SAS PUT positions:
#  @01 ORGCODE        $03.
#  @05 STAFFNO        $09.
#  @15 STAFFNAME      $40.
#  @55 ACCTNOC        $11.
#  @75 NEWIC          $12.
#  @87 OLDIC          $10.
#  @97 BRANCHCODE     $03.
# -----------------------
df_notfound = con.execute("SELECT ORGCODE, STAFFNO, STAFFNAME, ACCTNOC, NEWIC, OLDIC, BRANCHCODE FROM MERGEXMTCH ORDER BY ORGCODE, STAFFNAME, ACCTNOC").df()

# Write NOTFOUND.txt fixed width
with open(outputs["notfound_txt"], "w", encoding="utf-8") as fh:
    for _, r in df_notfound.iterrows():
        line = (
            f"{str(r['ORGCODE'])[:3].ljust(3)}"            # @01 ORGCODE (3)
            f"{str(r['STAFFNO'])[:9].ljust(9)}"            # @05 STAFFNO (9)
            f"{str(r['STAFFNAME'])[:40].ljust(40)}"        # @15 STAFFNAME (40)
            f"{str(r['ACCTNOC'])[:11].rjust(11,'0')}"      # @55 ACCTNOC (11) - keep zero-padding
            f"{str(r['NEWIC'])[:12].ljust(12)}"            # @75 NEWIC (12)
            f"{str(r['OLDIC'])[:10].ljust(10)}"            # @87 OLDIC (10)
            f"{str(r['BRANCHCODE'])[:3].ljust(3)}"         # @97 BRANCHCODE (3)
        )
        fh.write(line + "\n")

# Write NOTFOUND.parquet
table_nf = pa.Table.from_pandas(df_notfound)
pq.write_table(table_nf, outputs["notfound_parquet"])

# -----------------------
# Step 7: DPTEAM (FOR DP TEAM - ACCOUNT LIST PER CUSTOMER)
# SAS kept: KEEP STAFFNO CUSTNO ACCTCODE ACCTNOC JOINTACC and wrote DPFILE with fixed positions
# We'll build DPTEAM from MERGEFOUND
# PUT fields in SAS:
# @01 STAFFNO $9.
# @10 CUSTNO $20.
# @30 ACCTCODE $5.
# @35 ACCTNOC $11.
# @55 JOINTACC $1.
# @56 STAFFNAME $40.
# @96 BRANCHCODE $03.
# -----------------------
df_dpteam = con.execute(dedent("""
    SELECT
       STAFFNO,
       CUSTNO,
       ACCTCODE,
       ACCTNOC,
       JOINTACC,
       STAFFNAME,
       BRANCHCODE
    FROM MERGEFOUND
    ORDER BY STAFFNO, CUSTNO, ACCTCODE, ACCTNOC, JOINTACC;
""")).df()

# Write DPTEAM.txt
with open(outputs["dpteam_txt"], "w", encoding="utf-8") as fh:
    for _, r in df_dpteam.iterrows():
        line = (
            f"{str(r['STAFFNO'])[:9].ljust(9)}"             # @01 STAFFNO 9
            f"{str(r['CUSTNO'])[:20].ljust(20)}"            # @10 CUSTNO 20
            f"{str(r['ACCTCODE'])[:5].ljust(5)}"            # @30 ACCTCODE 5
            f"{str(r['ACCTNOC'])[:11].rjust(11,'0')}"       # @35 ACCTNOC 11
            f"{str(r['JOINTACC'])[:1].ljust(1)}"            # @55 JOINTACC 1
            f"{str(r['STAFFNAME'])[:40].ljust(40)}"         # @56 STAFFNAME 40
            f"{str(r['BRANCHCODE'])[:3].ljust(3)}"          # @96 BRANCHCODE 3
        )
        fh.write(line + "\n")

# Write DPTEAM.parquet
pq.write_table(pa.Table.from_pandas(df_dpteam), outputs["dpteam_parquet"])

# -----------------------
# Step 8: SORT step -> create OUT (the file with F1 entries)
# SAS: For each MERGEFOUND record: first output Y=002 row (F1=002),
# then for each C01..C20 if value != 0 and not missing, PUT same formatted line with F1 = Cxx.
#
# We'll create a dataframe with columns: CUSTNO, F1, RECTYPE, BRANCH, FILECODE, STAFFNO, STAFFNAME
# where FILECODE is coming from MERGEFOUND? In earlier code FILECODE was from HRMS FILECODE='B' (we keep h.filecode)
# SAS uses FILECODE column at @89 that came from HRMS 'B' earlier. We'll set FILECODE = h.FILECODE if exists else blank.
# -----------------------
# Build rows
rows = []
for _, r in df_dpteam.iterrows():
    staffno = r['STAFFNO']
    custno = r['CUSTNO']
    staffname = r['STAFFNAME']
    branch = r['BRANCHCODE']
    acctcode = r['ACCTCODE'] if 'ACCTCODE' in r else ''
    acctnoc = r['ACCTNOC']
    filecode = 'B'  # from HRMS FILECODE by design
    # Y=002 row
    rows.append({
        "CUSTNO": custno,
        "F1": 2,                    # Y = 002
        "RECTYPE": "",              # SAS used RECTYPE variable in other runs; unknown here so blank
        "BRANCH": branch,
        "FILECODE": filecode,
        "STAFFNO": staffno,
        "STAFFNAME": staffname
    })
# Now append rows for C01..C20 where value != 0 for the CUSTNO by looking at CUSTACCT's hrccode values
# We need HRCCODE values attached to CUSTACCT: query from CUSTACCT table
df_custacct = con.execute("SELECT * FROM CUSTACCT;").df()

# Create map from CUSTNO to list of codes C01..C20
for _, rec in df_custacct.iterrows():
    custno = rec['CUSTNO']
    # locate rows in MERGEFOUND that correspond to this CUSTNO to get staffno/staffname/branch/filecode (can be multiple)
    mf_rows = df_dpteam[df_dpteam['CUSTNO'] == custno]
    if mf_rows.empty:
        # If no MERGEFOUND for this CUSTNO, skip adding code rows (SAS would not produce them because MERGEFOUND is used)
        continue
    # For each staff occurrence (may be multiple), we will add code rows for each staff occurrence (SAS uses SET MERGEFOUND; so codes emitted are per MERGEFOUND record)
    # So for each matching MERGEFOUND row, add C01..C20 rows
    codes = [rec.get(f"C{str(i).zfill(2)}") for i in range(1,21)]
    for _, mf in mf_rows.iterrows():
        for c in codes:
            # treat None or 0 or NaN as skip
            if pd.isna(c):
                continue
            try:
                ci = int(c)
            except Exception:
                continue
            if ci != 0:
                rows.append({
                    "CUSTNO": mf['CUSTNO'],
                    "F1": ci,
                    "RECTYPE": "",  # unknown mapping, left blank to mimic SAS's @23 RECTYPE (if available could use H_RECTYPE)
                    "BRANCH": mf['BRANCHCODE'],
                    "FILECODE": mf.get('ACCTCODE', 'B') if 'ACCTCODE' in mf else 'B',
                    "STAFFNO": mf['STAFFNO'],
                    "STAFFNAME": mf['STAFFNAME']
                })

# Create dataframe OUT (equivalent to the file written by SAS DATA SORT)
df_out = pd.DataFrame(rows)
# In SAS they used PUT @02 CUSTNO $11. @20 F1 Z3. @23 RECTYPE $1. @24 BRANCH $7. @31 FILECODE $1. @33 STAFFNO $9. @42 STAFFNAME $40.
# We'll persist df_out to parquet and also to a simple "raw" fixed-width file used as INFILE in next step
# First, ensure CUSTNO is 11-char (left) or from data might be 20; SAS used @02 CUSTNO $11 (11 chars). We'll take first 11.
def format_out_row(r):
    # CUSTNO at @02 width 11
    custno_f = str(r['CUSTNO'])[:11].ljust(11)
    f1_f = f"{int(r['F1']):03d}"  # Z3
    rectype = (r['RECTYPE'] or "")[:1].ljust(1)
    branch = str(r['BRANCH'])[:7].rjust(7,'0') if r['BRANCH'] not in (None, "") else "0"*7
    filecode = (r.get('FILECODE') or "")[:1].ljust(1)
    staffno = (r['STAFFNO'] or "")[:9].ljust(9)
    staffname = (r['STAFFNAME'] or "")[:40].ljust(40)
    # Put together aligned like SAS: note SAS used @ positions (we'll build a conservative width)
    line = (
        " "  # position 1 reserved in SAS they used @02 for CUSTNO -> we put a leading space to emulate that
        + custno_f       # 11 chars -> positions 2..12
        + f1_f           # positions 20? but we'll keep ordering similar to SAS for downstream parsing
        + rectype
        + branch
        + filecode
        + staffno
        + staffname
    )
    return line

# Write raw OUT (temporary) as text used by next step - we emulate the same field positions for reading in TEMP1
raw_out_path = os.path.join(outdir, "UPDATE_OUT_RAW.txt")
with open(raw_out_path, "w", encoding="utf-8") as fh:
    for _, r in df_out.iterrows():
        fh.write(format_out_row(r) + "\n")

# Save df_out parquet as temporary
pq.write_table(pa.Table.from_pandas(df_out), outputs["outfile_temp_parquet"])

# -----------------------
# Step 9: STEP#002 - REFORMAT TO FIT PROGRAM CIUPDCCD (TEMP1 -> TEMP2 -> OUT)
# This step reads INFILE (the raw OUT file created above). It collects up to 20 occurrences of F1 per CUSTNO
# and outputs one record per CUSTNO with W1..W20 fields Z3 each, plus STAFFNO, STAFFNAME.
#
# Implementation:
#   - Use df_out (we already have rows with CUSTNO and F1).
#   - Remove duplicates by CUSTNO,F1 (SAS did PROC SORT NODUPKEY; BY CUSTNO F1)
#   - For each CUSTNO, collect F1 values in order into W1..W20. Missing => 0
#   - Output fields per SAS OUT layout:
#     @01 CUSTNO $11.
#     @21 RECTYPE $1.
#     @22 BRANCH $7.
#     @29 W1 Z3.
#     ...
#     @86 W20 Z3.
#     @89 FILECODE $1.
#     @90 STAFFNO $9.
#     @99 STAFFNAME $40.
# -----------------------
# Remove duplicates by CUSTNO,F1 preserving order of first occurrence:
df_temp1 = df_out[['CUSTNO','F1','RECTYPE','BRANCH','FILECODE','STAFFNO','STAFFNAME']].drop_duplicates(subset=['CUSTNO','F1'], keep='first')
# Group by CUSTNO and collect F1 into list in order
grouped = df_temp1.groupby('CUSTNO', sort=True)

records_sorted = []
for custno, g in grouped:
    # maintain the order as in df_temp1; reset index to preserve original order
    f1_list = list(g['F1'].astype(int).tolist())
    # pad or trim to 20
    w = [int(x) if (x is not None and not pd.isna(x)) else 0 for x in f1_list][:20]
    if len(w) < 20:
        w.extend([0] * (20 - len(w)))
    # For RECTYPE/BRANCH/FILECODE/STAFFNO/STAFFNAME, pick first row's values
    first = g.iloc[0]
    rec = {
        "CUSTNO": str(custno)[:11],
        "RECTYPE": (first['RECTYPE'] or "")[:1],
        "BRANCH": (first['BRANCH'] or "")[:7],
        "FILECODE": (first['FILECODE'] or "")[:1],
        "STAFFNO": (first['STAFFNO'] or "")[:9],
        "STAFFNAME": (first['STAFFNAME'] or "")[:40],
    }
    # Attach W1..W20
    for i in range(20):
        rec[f"W{i+1}"] = int(w[i])
    records_sorted.append(rec)

df_sorted = pd.DataFrame(records_sorted)

# Ensure types and fill missing with 0 for W's
for i in range(1,21):
    if f"W{i}" not in df_sorted.columns:
        df_sorted[f"W{i}"] = 0
    df_sorted[f"W{i}"] = df_sorted[f"W{i}"].fillna(0).astype(int)

# Write final sorted fixed-width text as SAS OUTFILE format
# Construct per SAS positions:
# @01 CUSTNO $11.
# @21 RECTYPE $1.
# @22 BRANCH $7.
# @29 W1 Z3.
# ...
# @86 W20 Z3.
# @89 FILECODE $1.
# @90 STAFFNO $9.
# @99 STAFFNAME $40.
def make_sorted_line(row):
    custno = str(row['CUSTNO']).ljust(11)[:11]
    rectype = (row['RECTYPE'] or "")[:1].ljust(1)
    branch = str(row['BRANCH']).rjust(7,'0')[:7]  # keep branch zero-padded similar to SAS
    # W1..W20 each Z3 -> 3-digit zero-padded numeric
    w_parts = "".join(f"{int(row[f'W{i}']):03d}" for i in range(1,21))
    filecode = (row['FILECODE'] or "")[:1].ljust(1)
    staffno = (row['STAFFNO'] or "")[:9].ljust(9)
    staffname = (row['STAFFNAME'] or "")[:40].ljust(40)
    # Compose with correct spacing: positions used above. We'll put custno then rectype then branch then w_parts then filecode staffno staffname
    line = custno + rectype + branch + w_parts + filecode + staffno + staffname
    return line

with open(outputs["outfile_sorted_txt"], "w", encoding="utf-8") as fh:
    for _, r in df_sorted.iterrows():
        fh.write(make_sorted_line(r) + "\n")

# Save final sorted as parquet
pq.write_table(pa.Table.from_pandas(df_sorted), outputs["outfile_sorted_parquet"])

# -----------------------
# Step 10: GET LISTING OF ACCOUNT PER STAFF (final DPFILE listing)
# SAS steps used:
#   - Recreate CUST (ACCTCODE == 'DP')
#   - Read STAFFACC from STAFFACC file (we already have DPTEAM earlier)
#   - Merge CUST(IN=S) STAFFACC(IN=T) BY CUSTNO; IF T; => keeps staffacc rows joined with CUST
#   - Output DPFILE with the same format as earlier DPTEAM
#
# Our DF df_dpteam (initial) is equivalent to reading STAFFACC (DP temp), and df_custacct/CUST provide CUST; so
# we will perform join STAFFACC -> CUST to keep only those where STAFFACC matches CUST (IF T)
# -----------------------
# Build STAFFACC from dpteam_parquet (we have df_dpteam)
# ensure ACCTNOC field is present
df_staffacc = df_dpteam.copy()
# Keep only distinct by CUSTNO (SAS did NODUPKEY BY CUSTNO)
df_staffacc = df_staffacc.drop_duplicates(subset=['CUSTNO'], keep='first')

# Merge
df_cust_for_merge = con.execute("SELECT CUSTNO, ACCTNOC, ACCTCODE, JOINTACC FROM CUST_FINAL;").df()
# Join: keep only records where STAFFACC exists: merge CUST (S) with STAFFACC (T) and IF T (so we only output staffacc rows merged with CUST)
df_merge_final = pd.merge(df_staffacc, df_cust_for_merge, on='CUSTNO', how='left', suffixes=('','_CUST'))

# Only keep rows with STAFFACC (we started from staffacc so all rows present)
# write DPFILE final lines same as earlier:
with open(outputs["dpfile_final_txt"], "w", encoding="utf-8") as fh:
    for _, r in df_merge_final.iterrows():
        line = (
            f"{str(r['STAFFNO'])[:9].ljust(9)}"
            f"{str(r['CUSTNO'])[:20].ljust(20)}"
            f"{str(r.get('ACCTCODE',''))[:5].ljust(5)}"
            f"{str(r.get('ACCTNOC',''))[:11].rjust(11,'0')}"
            f"{str(r.get('JOINTACC',''))[:1].ljust(1)}"
            f"{str(r['STAFFNAME'])[:40].ljust(40)}"
            f"{str(r.get('BRANCHCODE',''))[:3].ljust(3)}"
        )
        fh.write(line + "\n")

pq.write_table(pa.Table.from_pandas(df_merge_final), outputs["dpfile_final_parquet"])

# -----------------------
# Done
# -----------------------
print("Conversion completed. Outputs written to:", outdir)
print("Files created:")
for k,v in outputs.items():
    print(f" - {v}")
print("\nTemporary raw out file:", raw_out_path)
