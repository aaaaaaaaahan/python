import duckdb
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path, get_hive_parquet
import datetime

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
report_date = batch_date.strftime("%d-%m-%Y")
PURGEDATE = batch_date.isoformat()

con = duckdb.connect()

# -----------------------------------------------------------------
# DESCRIPTION → CLASS, NATURE, DEPT
# -----------------------------------------------------------------
con.execute(f"""
    CREATE VIEW descr_trim AS
    SELECT TRIM(KEY_ID) AS KEY_ID,
           KEY_CODE,
           KEY_DESCRIBE
    FROM '{host_parquet_path("UNLOAD_CIRHODCT_FB.parquet")}'
""")

con.execute("""
    CREATE VIEW class_map AS
    SELECT 
        KEY_CODE AS CLASS_CODE, 
        KEY_DESCRIBE AS CLASS_DESC
    FROM descr_trim
    WHERE KEY_ID = 'CLASS'
""")

con.execute("""
    CREATE VIEW nature_map AS
    SELECT 
        KEY_CODE AS NATURE_CODE, 
        KEY_DESCRIBE AS NATURE_DESC
    FROM descr_trim
    WHERE KEY_ID = 'NATURE'
""")

con.execute("""
    CREATE VIEW dept_map AS
    SELECT 
        KEY_CODE AS DEPT_CODE, 
        KEY_DESCRIBE AS DEPT_DESC
    FROM descr_trim
    WHERE KEY_ID = 'DEPT'
""")


# -----------------------------------------------------------------
# CONTROL: filter
# -----------------------------------------------------------------
con.execute(f"""
    CREATE VIEW control AS
    SELECT *
    FROM '{host_parquet_path("UNLOAD_CIRHOBCT_FB.parquet")}'
    WHERE TRIM(CLASS_CODE) = 'CLS0000004'
      AND TRIM(NATURE_CODE) = 'NAT0000044'
""")


# -----------------------------------------------------------------
# DETAIL FILTERING (match SAS logic)
# -----------------------------------------------------------------
con.execute(f"""
    CREATE VIEW det AS
    SELECT *,
           MAKE_DATE(CAST(LASTMNT_YYYY AS INTEGER),
                     CAST(LASTMNT_MM AS INTEGER),
                     CAST(LASTMNT_DD AS INTEGER)) AS lastsas,
           DATE_ADD('day', 732,
              MAKE_DATE(CAST(LASTMNT_YYYY AS INTEGER),
                        CAST(LASTMNT_MM AS INTEGER),
                        CAST(LASTMNT_DD AS INTEGER))
           ) AS twoyr
    FROM '{host_parquet_path("UNLOAD_CIRHOLDT_FB.parquet")}'
    WHERE COALESCE(TRIM(ACTV_IND),'') <> 'Y'
""")

con.execute(f"""
    CREATE VIEW detail AS
    SELECT *
    FROM det
    WHERE twoyr < DATE '{PURGEDATE}'
""")


# -----------------------------------------------------------------
# MERGE DETAIL + CONTROL (SAS MERGE A AND B)
# -----------------------------------------------------------------
con.execute("""
    CREATE VIEW first_merge AS
    SELECT d.*,
           c.CLASS_CODE AS CTRL_CLASS_CODE,
           c.NATURE_CODE AS CTRL_NATURE_CODE,
           c.DEPT_CODE AS CTRL_DEPT_CODE,
           c.GUIDE_CODE AS CTRL_GUIDE_CODE
    FROM detail d
    INNER JOIN control c
      ON TRIM(d.CLASS_ID) = TRIM(c.CLASS_ID)
""")


# -----------------------------------------------------------------
# ADD CLASS_DESC, NATURE_DESC, DEPT_DESC
# -----------------------------------------------------------------
con.execute("""
    CREATE VIEW final_enriched AS
    SELECT f.*,
           cm.CLASS_DESC,
           nm.NATURE_DESC,
           dm.DEPT_DESC
    FROM first_merge f
    LEFT JOIN class_map cm  ON TRIM(f.CTRL_CLASS_CODE)  = TRIM(cm.CLASS_CODE)
    LEFT JOIN nature_map nm ON TRIM(f.CTRL_NATURE_CODE) = TRIM(nm.NATURE_CODE)
    LEFT JOIN dept_map dm   ON TRIM(f.CTRL_DEPT_CODE)   = TRIM(dm.DEPT_CODE)
""")

df = con.execute("""
    SELECT *
    FROM final_enriched
    ORDER BY CLASS_ID, INDORG, NAME
""").fetchall()

cols = [c[0] for c in con.execute("DESCRIBE final_enriched").fetchall()]


# -----------------------------------------------------------------
# WRITE PARQUET (NO pandas)
# -----------------------------------------------------------------
pa_table = pa.Table.from_pylist([dict(zip(cols, r)) for r in df])
pq.write_table(pa_table, OUT_PARQUET)
print("Generated parquet:", OUT_PARQUET)


# -----------------------------------------------------------------
# GENERATE FIXED-WIDTH TXT
# -----------------------------------------------------------------
def fw(v, w):
    s = "" if v is None else str(v)
    return s[:w].ljust(w)

with open(OUT_TEXT, "w", encoding="utf-8") as f:
    for r in df:
        row = dict(zip(cols, r))

        line = (
            fw(row.get("INDORG",""), 1) +
            fw(row.get("NAME",""), 40) +
            fw(row.get("ID1",""), 20) +
            fw(row.get("ID2",""), 20) +
            fw(row.get("CLASS_ID",""), 10) +
            fw(PURGEDATE, 10) +
            fw(row.get("DTL_REMARK1",""), 40) +
            fw(row.get("DTL_REMARK2",""), 40) +
            fw(row.get("DTL_REMARK3",""), 40) +
            fw(row.get("DTL_REMARK4",""), 40) +
            fw(row.get("DTL_REMARK5",""), 40) +
            fw(row.get("DTL_CRT_DATE",""), 10) +
            fw(row.get("DTL_CRT_TIME",""), 8) +
            fw(row.get("DTL_LASTOPERATOR",""), 8) +
            fw(row.get("DTL_LASTMNT_DATE",""), 10) +
            fw(row.get("DTL_LASTMNT_TIME",""), 8) +
            fw(row.get("CTRL_CLASS_CODE",""), 10) +
            fw(row.get("CLASS_DESC",""), 150) +
            fw(row.get("CTRL_NATURE_CODE",""), 10) +
            fw(row.get("NATURE_DESC",""), 150) +
            fw(row.get("CTRL_DEPT_CODE",""), 10) +
            fw(row.get("DEPT_DESC",""), 150) +
            fw(row.get("CTRL_GUIDE_CODE",""), 10)
        )

        # Ensure exact 835 bytes like SAS
        line = line[:835].ljust(835)
        f.write(line + "\n")

print("Generated TXT:", OUT_TEXT)
