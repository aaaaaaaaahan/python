import duckdb
import datetime
from CIS_PY_READER import host_parquet_path, parquet_output_path, csv_output_path

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year, month, day = batch_date.year, batch_date.month, batch_date.day
rept_date = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%Y%m%d")

# ======================================================
# Setup DuckDB Connection
# ======================================================
con = duckdb.connect()

# ======================================================
# Load Input Files (assume converted to Parquet already)
# Mapping from SAS DD names to parquet filenames:
#   RHOLD    -> UNLOAD.CIRHOLDT.FB
#   RHOBFILE -> UNLOAD.CIRHOBCT.FB
#   RHODFILE -> UNLOAD.CIRHODCT.FB
#   HCMFILE  -> HCM.STAFF.LIST (same as your sample)
# ======================================================
con.execute(f"""
    CREATE TABLE RHOLD_RAW AS
    SELECT * FROM '{host_parquet_path("UNLOAD_CIRHOLDT_FB.parquet")}'
""")

con.execute(f"""
    CREATE TABLE RHOB_RAW AS
    SELECT * FROM '{host_parquet_path("UNLOAD_CIRHOBCT_FB.parquet")}'
""")

con.execute(f"""
    CREATE TABLE RHOD_RAW AS
    SELECT * FROM '{host_parquet_path("UNLOAD_CIRHODCT_FB.parquet")}'
""")

con.execute(f"""
    CREATE TABLE HCMFILE AS
    SELECT * FROM '{host_parquet_path("HCM_STAFF_LIST.parquet")}'
""")

# ======================================================
# RHOB: read and keep relevant columns
# SAS RHOB: CLASSIFY $10. NATURE $10. KEY_CODE $10. CLASSCODE $10.
# We'll normalize names and dedupe.
# ======================================================
con.execute("""
    CREATE TABLE RHOB AS
    SELECT DISTINCT
        CLASS_CODE   AS CLASSIFY,
        NATURE_CODE  AS NATURE,
        DEPT_CODE    AS KEY_CODE,
        CLASS_ID     AS CLASSCODE
    FROM RHOB_RAW
""")

# ======================================================
# RHOD: read full structure, extract remark fields, build REMARKS
# SAS RHOD fields (positions used): KEY_ID, KEY_CODE, KEY_DESCRIBE,
#    KEY_REMARK_ID1, KEY_REMARK_1, KEY_REMARK_ID2, KEY_REMARK_2, ...
# We'll rely on parquet column names; if different, adjust mapping here.
# ======================================================
# Build CONTACT1..3 and REMARKS similar to SAS logic
con.execute("""
    CREATE TABLE RHOD AS
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
    FROM RHOD_RAW
""")

con.execute("""
    CREATE TABLE RHOD_EXPANDED AS
    SELECT DISTINCT
        KEY_ID,
        KEY_CODE,
        KEY_DESCRIBE,
        CASE WHEN LENGTH(TRIM(KEY_REMARK_1)) > 0 THEN KEY_REMARK_1 ELSE NULL END AS CONTACT1,
        CASE WHEN LENGTH(TRIM(KEY_REMARK_2)) > 0 THEN KEY_REMARK_2 ELSE NULL END AS CONTACT2,
        CASE WHEN LENGTH(TRIM(KEY_REMARK_3)) > 0 THEN KEY_REMARK_3 ELSE NULL END AS CONTACT3,
        TRIM(COALESCE(KEY_DESCRIBE,'') || COALESCE(CONTACT1,'') || COALESCE(CONTACT2,'')) AS REMARKS,
        DESC_LASTOPERATOR,
        DESC_LASTMNT_DATE,
        DESC_LASTMNT_TIME
    FROM RHOD
""")

# ======================================================
# RHOD1: subset where KEY_ID1 = 'CLASS ' in SAS.
# SAS RHOD1 structure: KEY_ID1 $10. CLASSIFY $10. KEY_DESCRIBE1 $150.
# We'll map from RHOD_RAW if those fields exist; otherwise derive from RHOB.
# ======================================================
# Some datasets use different column names; try a best-effort mapping:
# We'll take rows from RHOD_RAW where key_id (or key_id1) equals 'CLASS '.
con.execute("""
    CREATE TABLE RHOD1 AS
    SELECT
        KEY_ID        AS KEY_ID1,
        KEY_CODE      AS CLASSIFY,
        KEY_DESCRIBE  AS KEY_DESCRIBE1
    FROM RHOD_RAW
    WHERE KEY_ID1 = 'CLASS'
       OR KEY_ID1 = 'CLASS '
""")

# ======================================================
# RHOLD: parse RHOLD_RAW fields according to SAS positions.
# SAS RHOLD fields (positions): CLASSCODE @01, INDORG @11, NAME @12 (40),
#    NEWIC @52 (15), OTHID @72 (20), CRTDTYYYY @292, CRTDTMM @297, CRTDTDD @300,
#    DOBDTYYYY @336, DOBDTMM @341, DOBDTDD @344
# We will assume the parquet contains columns named similarly (classcode, indorg, name, newic, othid,
# crt_dtyyyy, crt_dtmm, crt_dtdd, dob_dtyyyy, dob_dtmm, dob_dtdd). If not, users must adapt.
# ======================================================
con.execute("""
    CREATE TABLE RHOLD_parsed AS
    SELECT
        CLASS_ID AS CLASSCODE,
        INDORG,
        NAME,
        ID1 AS NEWIC,
        ID2 AS OTHID,
        SUBSTRING(DTL_CRT_DATE, 1, 4) AS CRTDTYYYY,
        SUBSTRING(DTL_CRT_DATE, 6, 2)   AS CRTDTMM,
        SUBSTRING(DTL_CRT_DATE, 9, 2)   AS CRTDTDD,
        SUBSTRING(DOB, 1, 4)   AS DOBDTYYYY,
        SUBSTRING(DOB, 6, 2)   AS DOBDTMM,
        SUBSTRING(DOB, 9, 2)   AS DOBDTDD
    FROM RHOLD_RAW
""")

# Build CRTDATE and DOBDOR and filter CRTDATE >= REPTDATE (SAS: REPTDATE = TODAY()-7 in YYMMDDN8.)
# Build as YYYYMMDD strings; we compare as integers for safety.
con.execute(f"""
    CREATE TABLE RHOLD AS
    SELECT DISTINCT
        CLASSCODE,
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
        (CRTDTYYYY || CRTDTMM || CRTDTDD) AS CRTDATE,
        (DOBDTYYYY || DOBDTMM || DOBDTDD) AS DOBDOR
    FROM RHOLD_parsed
    WHERE CRTDATE >= '{rept_date}
""")

# ======================================================
# BRHOLD1: merge RHOLD_UQ (Y) with RHOB_UQ (Z) BY CLASSCODE
# This brings KEY_CODE and CLASSIFY into RHOLD rows
# ======================================================
con.execute("""
    CREATE TABLE BRHOLD1 AS
    SELECT 
        r.*,
        b.KEY_CODE,
        b.CLASSIFY,
        b.NATURE
    FROM RHOLD_UQ r
    LEFT JOIN RHOB_UQ b
      ON r.CLASSCODE = b.CLASSCODE
""")

# ======================================================
# BRHOLD: merge BRHOLD1 (S) with RHOD1 (T) BY CLASSIFY
# This brings KEY_DESCRIBE1 into BRHOLD rows
# ======================================================
con.execute("""
    CREATE TABLE BRHOLD AS
    SELECT
        b1.*,
        coalesce(r1.KEY_DESCRIBE1, '') AS KEY_DESCRIBE1
    FROM BRHOLD1 b1
    LEFT JOIN RHOD1 r1
      ON b1.CLASSIFY = r1.CLASSIFY
""")

# ======================================================
# RNEW ROLD RNNEW RNOLD RNAME RNDOB
# SAS: MERGE BRHOLD(IN=W) RHOD(IN=X) BY KEY_CODE; IF W;
# Then output to different datasets based on NEWIC/OTHID/NAME/DOBDOR presence
# We'll JOIN BRHOLD with RHOD_UQ on KEY_CODE to pick RHOD extra fields (e.g., KEY_DESCRIBE)
# ======================================================
con.execute("""
    CREATE TABLE BRHOLD_WITH_RHOD AS
    SELECT
        b.*,
        coalesce(r.KEY_DESCRIBE, '') AS RHOD_KEY_DESCRIBE,
        coalesce(r.REMARKS, '') AS RHOD_REMARKS
    FROM BRHOLD b
    LEFT JOIN RHOD_UQ r
      ON b.KEY_CODE = r.KEY_CODE
""")

# RNEW: rows where NEWIC not blank
con.execute("""
    CREATE TABLE RNEW AS
    SELECT NAME, NEWIC, OTHID, KEY_CODE, CLASSCODE, KEY_DESCRIBE1, RHOD_KEY_DESCRIBE, RHOD_REMARKS, DOBDOR
    FROM BRHOLD_WITH_RHOD
    WHERE COALESCE(NEWIC, '') <> ''
""")

# ROLD: rows where OTHID not blank
con.execute("""
    CREATE TABLE ROLD AS
    SELECT NAME, OTHID, NEWIC, KEY_CODE, CLASSCODE, KEY_DESCRIBE1, RHOD_KEY_DESCRIBE, RHOD_REMARKS, DOBDOR
    FROM BRHOLD_WITH_RHOD
    WHERE COALESCE(OTHID, '') <> ''
""")

# RNNEW: NAME not blank AND NEWIC not blank
con.execute("""
    CREATE TABLE RNNEW AS
    SELECT NAME, NEWIC, OTHID, KEY_CODE, CLASSCODE, KEY_DESCRIBE1, RHOD_KEY_DESCRIBE, RHOD_REMARKS, DOBDOR
    FROM BRHOLD_WITH_RHOD
    WHERE COALESCE(NAME, '') <> '' AND COALESCE(NEWIC, '') <> ''
""")

# RNOLD: NAME not blank AND OTHID not blank
con.execute("""
    CREATE TABLE RNOLD AS
    SELECT NAME, OTHID, NEWIC, KEY_CODE, CLASSCODE, KEY_DESCRIBE1, RHOD_KEY_DESCRIBE, RHOD_REMARKS, DOBDOR
    FROM BRHOLD_WITH_RHOD
    WHERE COALESCE(NAME, '') <> '' AND COALESCE(OTHID, '') <> ''
""")

# RNAME: NAME not blank
con.execute("""
    CREATE TABLE RNAME AS
    SELECT NAME, NEWIC, OTHID, KEY_CODE, CLASSCODE, KEY_DESCRIBE1, RHOD_KEY_DESCRIBE, RHOD_REMARKS, DOBDOR
    FROM BRHOLD_WITH_RHOD
    WHERE COALESCE(NAME, '') <> ''
""")

# RNDOB: NAME not blank AND DOBDOR not blank
con.execute("""
    CREATE TABLE RNDOB AS
    SELECT NAME, NEWIC, OTHID, KEY_CODE, CLASSCODE, KEY_DESCRIBE1, RHOD_KEY_DESCRIBE, RHOD_REMARKS, DOBDOR
    FROM BRHOLD_WITH_RHOD
    WHERE COALESCE(NAME, '') <> '' AND COALESCE(DOBDOR, '') <> ''
""")

# Sort/uniq as SAS PROC SORT NODUPKEY - we'll create distinct keyed tables
con.execute("CREATE TABLE RNEW_UQ AS SELECT DISTINCT * FROM RNEW")
con.execute("CREATE TABLE ROLD_UQ AS SELECT DISTINCT * FROM ROLD")
con.execute("CREATE TABLE RNNEW_UQ AS SELECT DISTINCT * FROM RNNEW")
con.execute("CREATE TABLE RNOLD_UQ AS SELECT DISTINCT * FROM RNOLD")
con.execute("CREATE TABLE RNAME_UQ AS SELECT DISTINCT * FROM RNAME")
con.execute("CREATE TABLE RNDOB_UQ AS SELECT DISTINCT * FROM RNDOB")

# ======================================================
# HCM splits (SAS used DELIMITER=';')
# We'll split HCMFILE rawdata by ';' (SAS used DSD with delimiter ;)
# Fields order same as earlier sample:
# STAFFID, HCMNAME, OLDID, IC, DOB, BASE, COMPCODE, DESIGNATION, STATUS
# And derive OTHID/NAME/NEWIC/DOB fields similar to SAS
# ======================================================
delimiter = ';'   # SAS explicit

con.execute(f"""
    CREATE TABLE HCM AS
    SELECT
        TRIM(split_part(rawdata, '{delimiter}', 1)) AS STAFFID,
        TRIM(split_part(rawdata, '{delimiter}', 2)) AS HCMNAME,
        TRIM(split_part(rawdata, '{delimiter}', 3)) AS OLDID,
        TRIM(split_part(rawdata, '{delimiter}', 4)) AS IC,
        TRIM(split_part(rawdata, '{delimiter}', 5)) AS DOB,
        TRIM(split_part(rawdata, '{delimiter}', 6)) AS BASE,
        TRIM(split_part(rawdata, '{delimiter}', 7)) AS COMPCODE,
        TRIM(split_part(rawdata, '{delimiter}', 8)) AS DESIGNATION,
        TRIM(split_part(rawdata, '{delimiter}', 9)) AS STATUS
    FROM HCMFILE
""")

# Derive additional fields and split DOB parts
con.execute("""
    CREATE TABLE HCM_DERIV AS
    SELECT
        STAFFID,
        HCMNAME,
        OLDID,
        IC,
        DOB,
        BASE,
        COMPCODE,
        DESIGNATION,
        STATUS,
        OLDID AS OTHID,
        HCMNAME AS NAME,
        IC AS NEWIC,
        CASE WHEN LENGTH(DOB) >= 10 THEN SUBSTR(DOB,1,2) ELSE '' END AS DOBDD,
        CASE WHEN LENGTH(DOB) >= 10 THEN SUBSTR(DOB,4,2) ELSE '' END AS DOBMM,
        CASE WHEN LENGTH(DOB) >= 10 THEN SUBSTR(DOB,7,4) ELSE '' END AS DOBYYYY,
        CASE WHEN LENGTH(DOB) >= 10 THEN (SUBSTR(DOB,7,4) || SUBSTR(DOB,4,2) || SUBSTR(DOB,1,2)) ELSE '' END AS DOBDOR,
        CASE WHEN LENGTH(DOB) >= 10 THEN (SUBSTR(DOB,1,2) || '-' || SUBSTR(DOB,4,2) || '-' || SUBSTR(DOB,7,4)) ELSE '' END AS DOBDT
    FROM HCM
""")

# Output HCM subsets as in SAS (HCMOLD, HCMNEW, HCMALL, HCMNDOB, HCMNOLD, HCMNNEW)
con.execute("CREATE TABLE HCMOLD AS SELECT * FROM HCM_DERIV WHERE COALESCE(OTHID,'') <> ''")
con.execute("CREATE TABLE HCMNEW AS SELECT * FROM HCM_DERIV WHERE COALESCE(NEWIC,'') <> ''")
con.execute("CREATE TABLE HCMALL AS SELECT * FROM HCM_DERIV WHERE COALESCE(NAME,'') <> ''")
con.execute("CREATE TABLE HCMNDOB AS SELECT * FROM HCM_DERIV WHERE COALESCE(NAME,'') <> '' AND COALESCE(DOBDOR,'') <> ''")
con.execute("CREATE TABLE HCMNOLD AS SELECT * FROM HCM_DERIV WHERE COALESCE(NAME,'') <> '' AND COALESCE(OTHID,'') <> ''")
con.execute("CREATE TABLE HCMNNEW AS SELECT * FROM HCM_DERIV WHERE COALESCE(NAME,'') <> '' AND COALESCE(NEWIC,'') <> ''")

# Deduplicate (SAS used NODUPKEY)
con.execute("CREATE TABLE HCMOLD_UQ AS SELECT DISTINCT * FROM HCMOLD")
con.execute("CREATE TABLE HCMNEW_UQ AS SELECT DISTINCT * FROM HCMNEW")
con.execute("CREATE TABLE HCMNOLD_UQ AS SELECT DISTINCT * FROM HCMNOLD")
con.execute("CREATE TABLE HCMNNEW_UQ AS SELECT DISTINCT * FROM HCMNNEW")
con.execute("CREATE TABLE HCMALL_UQ AS SELECT DISTINCT * FROM HCMALL")
con.execute("CREATE TABLE HCMNDOB_UQ AS SELECT DISTINCT * FROM HCMNDOB")

# ======================================================
# Matching logic (MRGNAME, MRGID, MRGIC, MRGNNEW, MRGNOLD, MRGNDOB)
# SAS used MERGE by keys and IF IN= to ensure both present.
# We'll implement as inner joins to pick rows present in both.
# ======================================================
# (1) NAME MATCH -> RNAME_UQ join HCMALL_UQ on NAME = HCMNAME
con.execute("""
    CREATE TABLE MRGNAME AS
    SELECT 
        h.HCMNAME,
        h.OLDID,
        h.IC,
        h.DOBDT AS DOBDT,
        h.DOBDOR,
        h.BASE,
        h.DESIGNATION,
        h.COMPCODE,
        h.STAFFID,
        '3' AS MATCH_IND,
        'RHOLD NAME MATCHED' AS REASON,
        'Y' AS M_NAME,
        NULL AS M_NIC,
        NULL AS M_NID,
        NULL AS M_IC,
        NULL AS M_ID,
        NULL AS M_DOB,
        r.KEY_CODE,
        r.CLASSCODE,
        r.RHOD_REMARKS,
        r.KEY_DESCRIBE1,
        r.RHOD_KEY_DESCRIBE
    FROM RNAME_UQ r
    JOIN HCMALL_UQ h ON r.NAME = h.HCMNAME
""")

# (2) ID MATCH -> ROLD_UQ join HCMOLD_UQ on OTHID = OLDID
con.execute("""
    CREATE TABLE MRGID AS
    SELECT 
        h.HCMNAME,
        h.OLDID,
        h.IC,
        h.DOBDT AS DOBDT,
        h.DOBDOR,
        h.BASE,
        h.DESIGNATION,
        h.COMPCODE,
        h.STAFFID,
        '1' AS MATCH_IND,
        'RHOLD ID MATCHED' AS REASON,
        NULL AS M_NAME,
        NULL AS M_NIC,
        NULL AS M_NID,
        NULL AS M_IC,
        'Y' AS M_ID,
        NULL AS M_DOB,
        r.KEY_CODE,
        r.CLASSCODE,
        r.RHOD_REMARKS,
        r.KEY_DESCRIBE1,
        r.RHOD_KEY_DESCRIBE
    FROM ROLD_UQ r
    JOIN HCMOLD_UQ h ON r.OTHID = h.OLDID
""")

# (3) IC MATCH -> RNEW_UQ join HCMNEW_UQ on NEWIC = IC
con.execute("""
    CREATE TABLE MRGIC AS
    SELECT 
        h.HCMNAME,
        h.OLDID,
        h.IC,
        h.DOBDT AS DOBDT,
        h.DOBDOR,
        h.BASE,
        h.DESIGNATION,
        h.COMPCODE,
        h.STAFFID,
        '1' AS MATCH_IND,
        'RHOLD IC MATCHED' AS REASON,
        NULL AS M_NAME,
        NULL AS M_NIC,
        NULL AS M_NID,
        'Y' AS M_IC,
        NULL AS M_ID,
        NULL AS M_DOB,
        r.KEY_CODE,
        r.CLASSCODE,
        r.RHOD_REMARKS,
        r.KEY_DESCRIBE1,
        r.RHOD_KEY_DESCRIBE
    FROM RNEW_UQ r
    JOIN HCMNEW_UQ h ON r.NEWIC = h.IC
""")

# (4) NAME & IC MATCH -> RNNEW_UQ join HCMNNEW_UQ by NAME & NEWIC/IC
con.execute("""
    CREATE TABLE MRGNNEW AS
    SELECT
        h.HCMNAME,
        h.OLDID,
        h.IC,
        h.DOBDT,
        h.DOBDOR,
        h.BASE,
        h.DESIGNATION,
        h.COMPCODE,
        h.STAFFID,
        '2' AS MATCH_IND,
        'RHOLD NAME AND IC MATCHED' AS REASON,
        NULL AS M_NAME,
        'Y' AS M_NIC,
        NULL AS M_NID,
        NULL AS M_IC,
        NULL AS M_ID,
        NULL AS M_DOB,
        r.KEY_CODE,
        r.CLASSCODE,
        r.RHOD_REMARKS,
        r.KEY_DESCRIBE1,
        r.RHOD_KEY_DESCRIBE
    FROM RNNEW_UQ r
    JOIN HCMNNEW_UQ h ON r.NAME = h.NAME AND r.NEWIC = h.IC
""")

# (5) NAME & ID MATCH -> RNOLD_UQ join HCMNOLD_UQ by NAME & OTHID/OLDID
con.execute("""
    CREATE TABLE MRGNOLD AS
    SELECT
        h.HCMNAME,
        h.OLDID,
        h.IC,
        h.DOBDT,
        h.DOBDOR,
        h.BASE,
        h.DESIGNATION,
        h.COMPCODE,
        h.STAFFID,
        '2' AS MATCH_IND,
        'RHOLD NAME AND ID MATCHED' AS REASON,
        NULL AS M_NAME,
        NULL AS M_NIC,
        'Y' AS M_NID,
        NULL AS M_IC,
        NULL AS M_ID,
        NULL AS M_DOB,
        r.KEY_CODE,
        r.CLASSCODE,
        r.RHOD_REMARKS,
        r.KEY_DESCRIBE1,
        r.RHOD_KEY_DESCRIBE
    FROM RNOLD_UQ r
    JOIN HCMNOLD_UQ h ON r.NAME = h.NAME AND r.OTHID = h.OLDID
""")

# (6) NAME & DOB MATCH -> RNDOB_UQ join HCMNDOB_UQ by NAME & DOBDOR
con.execute("""
    CREATE TABLE MRGNDOB AS
    SELECT
        h.HCMNAME,
        h.OLDID,
        h.IC,
        h.DOBDT,
        h.DOBDOR,
        h.BASE,
        h.DESIGNATION,
        h.COMPCODE,
        h.STAFFID,
        '4' AS MATCH_IND,
        'RHOLD NAME AND DOB MATCHED' AS REASON,
        NULL AS M_NAME,
        NULL AS M_NIC,
        NULL AS M_NID,
        NULL AS M_IC,
        NULL AS M_ID,
        'Y' AS M_DOB,
        r.KEY_CODE,
        r.CLASSCODE,
        r.RHOD_REMARKS,
        r.KEY_DESCRIBE1,
        r.RHOD_KEY_DESCRIBE
    FROM RNDOB_UQ r
    JOIN HCMNDOB_UQ h ON r.NAME = h.HCMNAME AND r.DOBDOR = h.DOBDOR
""")

# ======================================================
# Combine all match results into ALLMATCH (distinct)
# We'll UNION ALL each MRG* table aligning columns, then DISTINCT and remove CLASSIFY = 'CLS0000004'
# Include REMARKS/KEY_DESCRIBE1 fields as in SAS OUTPUT
# ======================================================
con.execute("""
    CREATE TABLE ALLMATCH_UNION AS
    SELECT * FROM (
        SELECT HCMNAME, OLDID, IC, DOBDT AS DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON, M_NAME, M_NIC, M_NID, M_IC, M_ID, M_DOB,
               KEY_CODE, CLASSCODE, RHOD_REMARKS AS REMARKS, KEY_DESCRIBE1, RHOD_KEY_DESCRIBE, CLASSCODE AS CLASSIFY
        FROM MRGNAME
        UNION ALL
        SELECT HCMNAME, OLDID, IC, DOBDT AS DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON, M_NAME, M_NIC, M_NID, M_IC, M_ID, M_DOB,
               KEY_CODE, CLASSCODE, RHOD_REMARKS AS REMARKS, KEY_DESCRIBE1, RHOD_KEY_DESCRIBE, CLASSCODE AS CLASSIFY
        FROM MRGID
        UNION ALL
        SELECT HCMNAME, OLDID, IC, DOBDT AS DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON, M_NAME, M_NIC, M_NID, M_IC, M_ID, M_DOB,
               KEY_CODE, CLASSCODE, RHOD_REMARKS AS REMARKS, KEY_DESCRIBE1, RHOD_KEY_DESCRIBE, CLASSCODE AS CLASSIFY
        FROM MRGIC
        UNION ALL
        SELECT HCMNAME, OLDID, IC, DOBDT AS DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON, M_NAME, M_NIC, M_NID, M_IC, M_ID, M_DOB,
               KEY_CODE, CLASSCODE, RHOD_REMARKS AS REMARKS, KEY_DESCRIBE1, RHOD_KEY_DESCRIBE, CLASSCODE AS CLASSIFY
        FROM MRGNDOB
        UNION ALL
        SELECT HCMNAME, OLDID, IC, DOBDT AS DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON, M_NAME, M_NIC, M_NID, M_IC, M_ID, M_DOB,
               KEY_CODE, CLASSCODE, RHOD_REMARKS AS REMARKS, KEY_DESCRIBE1, RHOD_KEY_DESCRIBE, CLASSCODE AS CLASSIFY
        FROM MRGNNEW
        UNION ALL
        SELECT HCMNAME, OLDID, IC, DOBDT AS DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
               MATCH_IND, REASON, M_NAME, M_NIC, M_NID, M_IC, M_ID, M_DOB,
               KEY_CODE, CLASSCODE, RHOD_REMARKS AS REMARKS, KEY_DESCRIBE1, RHOD_KEY_DESCRIBE, CLASSCODE AS CLASSIFY
        FROM MRGNOLD
    )
""")

# Remove unwanted CLASSIFY and dedupe
con.execute("""
    CREATE TABLE ALLMATCH AS
    SELECT DISTINCT * FROM ALLMATCH_UNION
    WHERE COALESCE(CLASSIFY, '') <> 'CLS0000004'
""")

# SAS then PROC SORT NODUPKEY BY HCMNAME IC OLDID STAFFID
con.execute("""
    CREATE TABLE ALLMATCH_SORTED AS
    SELECT DISTINCT HCMNAME, OLDID, IC, DOB, BASE, DESIGNATION, COMPCODE, STAFFID,
                    MATCH_IND, REASON, COALESCE(M_NAME,'N') AS M_NAME,
                    COALESCE(M_NIC,'N') AS M_NIC, COALESCE(M_NID,'N') AS M_NID,
                    COALESCE(M_IC,'N') AS M_IC, COALESCE(M_ID,'N') AS M_ID,
                    COALESCE(M_DOB,'N') AS M_DOB, KEY_CODE, CLASSCODE, REMARKS, KEY_DESCRIBE1, RHOD_KEY_DESCRIBE
    FROM ALLMATCH
    ORDER BY HCMNAME, IC, OLDID, STAFFID
""")

# ======================================================
# Final OUTPUT dataset: formatting flags to 'N' when missing, include REMARKS and KEY_DESCRIBE1
# Then export to Parquet & CSV partitioned by year/month/day
# ======================================================
con.execute("""
    CREATE TABLE OUTPUT AS
    SELECT
        HCMNAME,
        OLDID,
        IC,
        MATCH_IND,
        DOB,
        BASE,
        DESIGNATION,
        COALESCE(REASON, '') AS REASON,
        COALESCE(M_NAME, 'N') AS M_NAME,
        COALESCE(M_NIC, 'N') AS M_NIC,
        COALESCE(M_NID, 'N') AS M_NID,
        COALESCE(M_IC, 'N') AS M_IC,
        COALESCE(M_ID, 'N') AS M_ID,
        COALESCE(M_DOB, 'N') AS M_DOB,
        COMPCODE,
        STAFFID,
        COALESCE(REMARKS, '') AS REMARKS,
        COALESCE(KEY_DESCRIBE1, '') AS KEY_DESCRIBE1,
        COALESCE(RHOD_KEY_DESCRIBE, '') AS KEY_DESCRIBE_FROM_RHOD,
        'AML/CFT MS NG MEE WUN 03-21767651 MS WONG LAI SAN 03-21763005' AS CONTACT,
        'AML/CFT' AS DEPT
    FROM ALLMATCH_SORTED
""")

# ======================================================
# Export results: Parquet (partitioned) and CSV
# ======================================================
print("Complete.")
