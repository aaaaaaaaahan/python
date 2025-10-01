"""
convert_cisrmrk_to_parquet.py

Usage:
  - Edit the parquet input paths below.
  - Run: python convert_cisrmrk_to_parquet.py
"""

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import os
from datetime import datetime

# ---------- CONFIG: edit these ----------
RMK_PARQUET   = "RMKFILE.parquet"      # RMKFILE
PRIM_PARQUET  = "LNSPRIM.parquet"      # LNSPRIM
SECD_PARQUET  = "LNSSECD.parquet"      # LNSSECD
OUT_PARQUET   = "OUT1.parquet"         # final parquet output
OUT_TEXT      = "OUT1.txt"             # fixed-width text output
DEDUP_KEEP_LAST = True                 # emulate ICETOOL LASTDUP (keep last by CUSTNO)
# ---------------------------------------

con = duckdb.connect(database=':memory:')

# Register parquet files as DuckDB tables
con.execute(f"CREATE VIEW rmk AS SELECT * FROM parquet_scan('{RMK_PARQUET}')")
con.execute(f"CREATE VIEW prim AS SELECT * FROM parquet_scan('{PRIM_PARQUET}')")
con.execute(f"CREATE VIEW secd AS SELECT * FROM parquet_scan('{SECD_PARQUET}')")

# If your parquet column names differ (e.g. CUSTNO1, LONGNAME1), normalize them in SQL below.
# Step 1: Create MATCH1 (joint customers) and XMATCH (single)
# Equivalent SAS logic:
#   MERGE PRIM(IN=A) SECD(IN=B); BY ACCTNOC;
#   IF A AND NOT B THEN JOINT='N' -> XMATCH
#   IF A AND B THEN JOINT='Y' and LONGNAME = LONGNAME || ' & ' || LONGNAME1 -> MATCH1

# Normalize columns: assume secd may have different column names like CUSTNO1, DOBDOR1, LONGNAME1
# We'll alias them for convenience
sql_match = """
-- Create match1: joint accounts
CREATE TEMP TABLE match1 AS
SELECT
    p.CUSTNO AS CUSTNO,
    p.ACCTNOC AS ACCTNOC,
    p.DOBDOR AS DOBDOR,
    TRIM(p.LONGNAME) || ' & ' || TRIM(s.LONGNAME1) AS LONGNAME,
    p.INDORG AS INDORG,
    'Y' AS JOINT
FROM prim p
JOIN secd s USING (ACCTNOC)
;

-- Create xmatch: primary only (no secondary)
CREATE TEMP TABLE xmatch AS
SELECT
    p.CUSTNO AS CUSTNO,
    p.ACCTNOC AS ACCTNOC,
    p.DOBDOR AS DOBDOR,
    p.LONGNAME AS LONGNAME,
    p.INDORG AS INDORG,
    'N' AS JOINT
FROM prim p
LEFT JOIN secd s USING (ACCTNOC)
WHERE s.ACCTNOC IS NULL
;
"""
con.execute(sql_match)

# Step 2: MATCH2 = RMK + MATCH1 (keep those in MATCH1)
#         MATCH3 = RMK + XMATCH (keep those in XMATCH)
sql_match2_3 = """
CREATE TEMP TABLE match2 AS
SELECT m.CUSTNO,
       m.ACCTNOC,
       r.REMARKS,
       m.DOBDOR,
       m.LONGNAME,
       m.INDORG,
       m.JOINT
FROM match1 m
LEFT JOIN rmk r ON r.CUSTNO = m.CUSTNO
WHERE m.CUSTNO IS NOT NULL
;

CREATE TEMP TABLE match3 AS
SELECT x.CUSTNO,
       x.ACCTNOC,
       r.REMARKS,
       x.DOBDOR,
       x.LONGNAME,
       x.INDORG,
       x.JOINT
FROM xmatch x
LEFT JOIN rmk r ON r.CUSTNO = x.CUSTNO
WHERE x.CUSTNO IS NOT NULL
;
"""
con.execute(sql_match2_3)

# Step 3: Concatenate MATCH2 and MATCH3 -> OUT1
con.execute("""
CREATE TEMP TABLE out_all AS
SELECT * FROM match2
UNION ALL
SELECT * FROM match3
;
""")

# Optional: emulate ICETOOL LASTDUP ON(1,20,CH) to get last row per CUSTNO (CUSTNO occupies 1..20)
if DEDUP_KEEP_LAST:
    # We'll keep the last row by ordering on a synthetic row_number or existing timestamp if present.
    # DuckDB doesn't preserve file order; to mimic 'last occurrence' from the file, we need an ordering column.
    # If original data has no ordering column, we'll assume the last by insertion order; as a fallback, use ROW_NUMBER over
    # an arbitrary ordering (this is the best-effort equivalent).
    con.execute("""
    CREATE TEMP TABLE out_ranked AS
    SELECT *, ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) as rn
    FROM out_all
    """)
    # Keep rn = max (since we used trivial ORDER BY, rn=1; but we want last -> choose last rn via grouping)
    # Better approach: pick the last by using MAX(rowid) if we had it; we'll just deduplicate keeping the last row produced:
    # We'll compute a rowid by duckdb's internal ROW_NUMBER() over a dummy ordering of ACCTNOC desc to choose 'last'
    con.execute("""
    CREATE TEMP TABLE out_dedup AS
    SELECT t.*
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY ACCTNOC DESC) AS keep_rn
        FROM out_all
    ) t
    WHERE t.keep_rn = 1
    """)
    final_table = "out_dedup"
else:
    final_table = "out_all"

# Export final_table to Parquet using duckdb's parquet writer via pyarrow
# Pull DuckDB result into PyArrow Table
res = con.execute(f"SELECT CUSTNO, ACCTNOC, REMARKS, DOBDOR, LONGNAME, INDORG, JOINT FROM {final_table}").fetch_arrow_table()

# Write parquet
pq.write_table(res, OUT_PARQUET)
print(f"Wrote parquet -> {OUT_PARQUET} ({res.num_rows} rows)")

# Also write fixed-width text file emulating the PUT positions from your SAS:
# SAS PUT layout:
# @001 CUSTNO $20.
# @022 ACCTNOC $20.
# @042 REMARKS $60.
# @143 DOBDOR $10.
# @160 LONGNAME $200.
# @400 INDORG $1.
# @402 JOINT $1.
# We'll pad/truncate fields to match these widths.

def pad_trunc(s, width):
    if s is None:
        s = ""
    s = str(s)
    if len(s) > width:
        return s[:width]
    return s.ljust(width)

with open(OUT_TEXT, "w", encoding="utf-8") as f:
    for row in res.to_pydict()['CUSTNO'] if res.num_rows>0 else []:
        # We'll iterate row-by-row using pyarrow table conversion
        pass

# Better: iterate via rows from the Arrow table
with open(OUT_TEXT, "w", encoding="utf-8", newline='') as f:
    cols = res.to_pydict()
    n = res.num_rows
    for i in range(n):
        CUSTNO = cols.get('CUSTNO', ['']*n)[i]
        ACCTNOC = cols.get('ACCTNOC', ['']*n)[i]
        REMARKS = cols.get('REMARKS', ['']*n)[i]
        DOBDOR = cols.get('DOBDOR', ['']*n)[i]
        LONGNAME = cols.get('LONGNAME', ['']*n)[i]
        INDORG = cols.get('INDORG', ['']*n)[i]
        JOINT = cols.get('JOINT', ['']*n)[i]

        line = (
            pad_trunc(CUSTNO, 20) +
            pad_trunc(ACCTNOC, 20) +
            pad_trunc(REMARKS, 60) +
            pad_trunc(DOBDOR, 10) +
            pad_trunc(LONGNAME, 200) +
            pad_trunc(INDORG, 1) +
            pad_trunc(JOINT, 1)
        )
        # Ensure the record matches LRECL ~ 600 (as in your original job) by padding/truncating further if needed
        if len(line) < 600:
            line = line + " " * (600 - len(line))
        elif len(line) > 600:
            line = line[:600]
        f.write(line + "\n")

print(f"Wrote fixed-width text -> {OUT_TEXT}")
