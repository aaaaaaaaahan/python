import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pyarrow import compute as pc
import pyarrow.dataset as ds
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

# ================================================
# SETUP
# ================================================
con = duckdb.connect()

# Input parquet paths (already converted from SAS datasets)
snglview_files = [
    "SNGLVIEW.DEPOSIT.DP01",
    "SNGLVIEW.DEPOSIT.DP03",
    "SNGLVIEW.DEPOSIT.DP04",
    "SNGLVIEW.DEPOSIT.DP05",
    "SNGLVIEW.DEPOSIT.DP06",
    "SNGLVIEW.DEPOSIT.DP07",
    "RBP2.B051.SNGLVIEW.DEPOSIT.DP01",
    "RBP2.B051.SNGLVIEW.DEPOSIT.DP03",
    "RBP2.B051.SNGLVIEW.DEPOSIT.DP04",
    "RBP2.B051.SNGLVIEW.DEPOSIT.DP05",
    "RBP2.B051.SNGLVIEW.DEPOSIT.DP06",
    "RBP2.B051.SNGLVIEW.DEPOSIT.DP07",
    "SNGLVIEW.LOANS.LN02",
    "SNGLVIEW.LOANS.LN08",
    "RBP2.B051.SNGLVIEW.LOANS.LN02",
    "RBP2.B051.SNGLVIEW.LOANS.LN08",
    "SNGLVIEW.PBCS",
    "SNGLVIEW.PMMD",
    "SNGLVIEW.COMCARD",
    "SNGLVIEW.SDBX",
]

cisignat_file = "UNLOAD.CISIGNAT.FB"
ccrlens_file = "CCRIS.CC.RLNSHIP.SRCH"
output_file = f"{parquet_output_path}/SNGLVIEW_IMIS_EXTRACT.parquet"
csv_output = f"{csv_output_path}/SNGLVIEW_IMIS_EXTRACT.csv"

# ================================================
# 1. READ ALL SNGLVIEW FILES & CONCATENATE
# ================================================
print("Loading SNGLVIEW files...")
con.execute(f"""
    CREATE OR REPLACE TABLE IMIS AS 
    SELECT * FROM read_parquet([{','.join([f"'{host_parquet_path}/{f}.parquet'" for f in snglview_files])}])
""")

# ================================================
# 2. LOAD SIGNATORY FILE
# ================================================
print("Loading SIGNATORY...")
con.execute(f"""
    CREATE OR REPLACE TABLE SIGNATORY AS
    SELECT 
        BANKNO,
        ACCTNOX,
        SEQNO,
        NAME,
        ALIAS,
        SIGNATORY,
        MANDATEE,
        NOMINEE,
        STATUS,
        BRANCHNOX,
        BRANCHNO,
        CAST(ACCTNOX AS VARCHAR) AS ACCTNO
    FROM read_parquet('{host_parquet_path}/{cisignat_file}.parquet')
""")

# Remove duplicates by (ACCTNO, ALIAS)
con.execute("""
    CREATE OR REPLACE TABLE SIGNATORY AS
    SELECT DISTINCT ON (ACCTNO, ALIAS) * FROM SIGNATORY
""")

# ================================================
# 3. LOAD CCRLEN FILE
# ================================================
print("Loading CCRLEN...")
con.execute(f"""
    CREATE OR REPLACE TABLE CCRLEN AS
    SELECT DISTINCT ON (CUSTNO)
        CUSTNO, NAME, ALIASKEY, ALIAS
    FROM read_parquet('{host_parquet_path}/{ccrlens_file}.parquet')
""")

# ================================================
# 4. CLEAN IMIS (REPLACE TABS WITH SPACES)
# ================================================
print("Cleaning IMIS data...")
con.execute("""
    CREATE OR REPLACE TABLE IMIS_CLEAN AS
    SELECT DISTINCT CUSTNO, ALIAS, ACCTNO, NOTENO, ALIASKEY, PRIMSEC,
           REPLACE(NAME1, '\t', ' ') AS NAME
    FROM IMIS
    WHERE COALESCE(NAME1, '') <> '' OR COALESCE(ALIAS, '') <> ''
""")

# ================================================
# 5. OUTPUT FINAL DATA
# ================================================
print("Exporting IMIS Extract...")
final_arrow = con.execute("""
    SELECT 
        CUSTNO,
        ACCTNO,
        NOTENO,
        ALIASKEY,
        ALIAS,
        PRIMSEC,
        NAME
    FROM IMIS_CLEAN
""").arrow()

# ================================================
# 6. WRITE OUTPUT USING PYARROW
# ================================================
print("Writing to Parquet and CSV...")
pq.write_table(final_arrow, output_file)
csv.write_csv(final_arrow, csv_output)

print("✅ Process completed successfully!")
print(f"Output Parquet: {output_file}")
print(f"Output CSV: {csv_output}")
