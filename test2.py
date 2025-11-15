import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path
from datetime import datetime, timedelta

# ---------------------------------------------------------
# Batch Date
# ---------------------------------------------------------
batch_date = datetime.today() - timedelta(days=1)
report_date = batch_date.strftime("%Y%m%d")

# ---------------------------------------------------------
# DuckDB Connection
# ---------------------------------------------------------
con = duckdb.connect()

# ---------------------------------------------------------
# 1. Load all parquet files into DuckDB
# ---------------------------------------------------------
con.execute(f"""
    CREATE VIEW RHOB AS
    SELECT
        CLASSIFY,
        NATURE,
        KEY_CODE,
        CLASSID
    FROM read_parquet({host_parquet_path("UNLOAD_CIRHOBCT_FB")})
""")

con.execute(f"""
    CREATE VIEW RHOD AS
    SELECT
        KEY_ID,
        KEY_CODE,
        KEY_DESCRIBE,
        KEY_REMARK_ID1,
        KEY_REMARK_1,
        KEY_REMARK_ID2,
        KEY_REMARK_2,
        KEY_REMARK_ID3,
        KEY_REMARK_3,
        DESC_LASTOPERATOR,
        DESC_LASTMNT_DATE,
        DESC_LASTMNT_TIME
    FROM read_parquet({host_parquet_path("UNLOAD_CIRHODCT_FB")})
    WHERE KEY_ID = 'DEPT'
""")

con.execute(f"""
    CREATE VIEW RHOLD AS
    SELECT
        CLASSID,
        INDORG,
        NAME,
        NEWIC,
        OTHID,
        CRTDTYYYY,
        CRTDTMM,
        CRTDTDD,
        DOBDTYYYY,
        DOBDTMM,
        DOBDTDD,
        (DOBDTYYYY || DOBDTMM || DOBDTDD) AS DOBDOR,
        (CRTDTYYYY || CRTDTMM || CRTDTDD) AS CRTDATE
    FROM read_parquet({host_parquet_path("UNLOAD_CIRHOLDT_FB")})
""")

# ---------------------------------------------------------
# 2. Process RHOD (build CONTACT1, CONTACT2, CONTACT3, REMARKS)
# ---------------------------------------------------------
con.execute("""
    CREATE TEMP TABLE RHOD_CLEAN AS
    SELECT
        KEY_CODE,
        COALESCE(NULLIF(KEY_REMARK_1, ''), '') AS CONTACT1,
        COALESCE(NULLIF(KEY_REMARK_2, ''), '') AS CONTACT2,
        COALESCE(NULLIF(KEY_REMARK_3, ''), '') AS CONTACT3,
        TRIM(KEY_DESCRIBE) ||
        TRIM(COALESCE(NULLIF(KEY_REMARK_1, ''), '')) ||
        TRIM(COALESCE(NULLIF(KEY_REMARK_2, ''), '')) AS REMARKS
    FROM RHOD
""")

# ---------------------------------------------------------
# 3. Merge RHOLD + RHOB (BY CLASSID)
# ---------------------------------------------------------
con.execute("""
    CREATE TEMP TABLE GETCLASSID AS
    SELECT *
    FROM RHOLD h
    INNER JOIN RHOB b USING (CLASSID)
""")

# ---------------------------------------------------------
# 4. Merge with RHOD_CLEAN (BY KEY_CODE)
# ---------------------------------------------------------
con.execute("""
    CREATE TEMP TABLE OBODCT AS
    SELECT *
    FROM GETCLASSID g
    INNER JOIN RHOD_CLEAN d USING (KEY_CODE)
""")

# ---------------------------------------------------------
# 5. Final OUTPUT dataset
# ---------------------------------------------------------
df = con.execute("SELECT * FROM OBODCT ORDER BY CLASSID").fetchdf()

# ---------------------------------------------------------
# 6. Write TXT file (fixed positions like SAS PUT)
# ---------------------------------------------------------
txt_path = csv_output_path(f"AMLA_RHOLD_EXTRACT_{report_date}").replace(".csv", ".txt")

with open(txt_path, "w", encoding="utf-8") as f:
    for _, row in df.iterrows():
        line = (
            f"{str(row['INDORG'])[0:1]:<1}" +
            ";" +
            f"{str(row['NAME'])[0:40]:<40}" +
            ";" +
            f"{str(row['NEWIC'])[0:20]:<20}" +
            ";" +
            f"{str(row['OTHID'])[0:20]:<20}" +
            ";" +
            f"{str(row['CONTACT1'])[0:50]:<50}" +
            ";" +
            f"{str(row['CONTACT2'])[0:50]:<50}" +
            ";" +
            f"{str(row['CONTACT3'])[0:50]:<50}"
        )
        f.write(line + "\n")

print("TXT Generated:", txt_path)
