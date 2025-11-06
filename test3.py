# output "name" used by helper functions
OUT_NAME = "CISHRC_STATUS_DAILY"
# Decide txt file path:
csv_path = csv_output_path(OUT_NAME)
txt_path = csv_path
if txt_path.lower().endswith('.csv'):
    txt_path = txt_path[:-4] + '.txt'
else:
    txt_path = txt_path + '.txt'

# Fetch rows from SUMMARY (we used the same SELECT as out1 but without year/month/day)
rows = con.execute("""
    SELECT
        BRCHCODE,
        HOEREJ, HOEDEL, HOEPDREV, HOEPDAPPR, HOEAPPR,
        HOEPDNOTE, HOEACCT, HOEXACCT, BRHREJ, DELAP1,
        BRHCOM, BRHEDD, BRHAPPR, BRHACCT, BRHXACCT,
        HOENOTED, HOEPDNOTE1, HOENOTED1, TOTAL
    FROM SUMMARY
    ORDER BY BRCHCODE
""").fetchall()

# Build header (match SAS header labels and approximate positions)
header = (
    f"{'BRANCH':<7}" +
    f"{'':1}" +  # spacer to approximately match positions
    "HOE REJECT, HOE DELETE, PEND REVIEW, PEND APPROVAL, HOE APPROVED, HOE PEND NOTE, " +
    "HOE APPR ACCT OPEN, HOE APPR NO ACCT, BRANCH REJECT, BRANCH DELETE, BRANCH RECOM, " +
    "BRANCH EDD, BRANCH APPROVED, BRANCH APPR ACCT, BRANCH APPR NO ACCT, HOE NOTED, " +
    "PENDING NOTING (HOE), NOTED (HOE), TOTAL"
)

# Now write to txt_path with formatting similar to SAS PUT:
# SAS used Z8. (zero-padded width 8) for numeric fields and $7. for branch.
def z8(val):
    try:
        ival = int(val)
    except Exception:
        ival = 0
    return f"{ival:0>8d}"  # zero-padded to width 8

with open(txt_path, 'w', encoding='utf-8') as f:
    # write header line (SAS prints header only once at _N_=1)
    f.write(header + "\n")
    for r in rows:
        # r tuple as (BRCHCODE, HOEREJ, HOEDEL, HOEPDREV, HOEPDAPPR, HOEAPPR,
        #   HOEPDNOTE, HOEACCT, HOEXACCT, BRHREJ, DELAP1, BRHCOM, BRHEDD,
        #   BRHAPPR, BRHACCT, BRHXACCT, HOENOTED, HOEPDNOTE1, HOENOTED1, TOTAL)
        brchcode = (r[0] or "").ljust(7)[:7]
        # Compose the line following the SAS numeric ordering and comma separators
        parts = [
            brchcode,
            ", ",
            z8(r[1]), ", ",
            z8(r[2]), ", ",
            z8(r[3]), ", ",
            z8(r[4]), ", ",
            z8(r[5]), ", ",
            z8(r[6]), ", ",
            z8(r[7]), ", ",
            z8(r[8]), ", ",
            z8(r[9]), ", ",
            z8(r[10]), ", ",
            z8(r[11]), ", ",
            z8(r[12]), ", ",
            z8(r[13]), ", ",
            z8(r[14]), ", ",
            z8(r[15]), ", ",
            z8(r[16]), ", ",
            z8(r[17]), ", ",
            z8(r[18]), ", ",
            z8(r[19])
        ]
        line = "".join(parts)
        f.write(line + "\n")
