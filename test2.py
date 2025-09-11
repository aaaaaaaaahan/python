import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pc
from datetime import datetime

# ----------------------#
# 1. Get reporting date #
# ----------------------#
today = datetime.today()
YEAR = f"{today.year:04d}"
MONTH = f"{today.month:02d}"
DAY = f"{today.day:02d}"
TIMEX = today.strftime("%H%M%S")
RUNTIME = YEAR + MONTH + DAY + TIMEX

# --------------------------#
# 2. Connect DuckDB engine  #
# --------------------------#
con = duckdb.connect()

# --------------------------#
# 3. Load parquet datasets  #
# --------------------------#
con.execute("""
    CREATE VIEW cisfile AS SELECT * FROM 'CIS_CUST_DAILY.parquet';
    CREATE VIEW indfile AS SELECT * FROM 'INDVDLY.parquet';
    CREATE VIEW demofile AS SELECT * FROM 'BANKCTRL_DEMOCODE.parquet';
""")

# -----------------------------------#
# 4. Process CIS dataset              #
# -----------------------------------#
con.execute(f"""
    CREATE VIEW cis AS
    SELECT
        CUSTNO AS CUSTNOX,
        LPAD(CAST(COALESCE(CAST(PRIPHONE AS BIGINT),0) AS VARCHAR),11,'0') AS PRIPHONEX,
        LPAD(CAST(COALESCE(CAST(SECPHONE AS BIGINT),0) AS VARCHAR),11,'0') AS SECPHONEX,
        LPAD(CAST(COALESCE(CAST(MOBILEPH AS BIGINT),0) AS VARCHAR),11,'0') AS MOBILEPHX,
        LPAD(CAST(COALESCE(CAST(FAX AS BIGINT),0) AS VARCHAR),11,'0') AS FAXX,
        LPAD(CAST(COALESCE(CAST(ADDREF AS BIGINT),0) AS VARCHAR),11,'0') AS ADDREFX,
        CAST(CUSTOPENDATE AS VARCHAR) AS CUSTOPEN,
        CASE 
            WHEN CUSTOPENDATE = '00002000000' THEN '20000101'
            ELSE SUBSTRING(CUSTOPENDATE,5,4) || SUBSTRING(CUSTOPENDATE,1,2) || SUBSTRING(CUSTOPENDATE,3,2)
        END AS OPENDT,
        {" || ".join([f"LPAD(CAST(COALESCE(CAST(HRC{i:02d} AS INTEGER),0) AS VARCHAR),3,'0')" for i in range(1,21)])} AS HRCALL,
        *
    FROM cisfile
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTNO ORDER BY CUSTNO) = 1
""")

# -----------------------------------#
# 5. Process DEMOFILE (categories)   #
# -----------------------------------#
con.execute("""
    CREATE VIEW sales AS
    SELECT DEMOCODE AS SALES, RLENDESC AS SALDESC
    FROM demofile
    WHERE DEMOCATEGORY = 'SALES'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DEMOCODE ORDER BY DEMOCODE)=1;

    CREATE VIEW restr AS
    SELECT DEMOCODE AS RESTR, RLENDESC AS RESDESC
    FROM demofile
    WHERE DEMOCATEGORY = 'RESTR'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DEMOCODE ORDER BY DEMOCODE)=1;

    CREATE VIEW citzn AS
    SELECT DEMOCODX AS CITZN, RLENDESC AS CTZDESC
    FROM demofile
    WHERE DEMOCATEGORY = 'CITZN'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DEMOCODX ORDER BY DEMOCODX)=1;
""")

# -----------------------------------#
# 6. Process INDFILE                 #
# -----------------------------------#
con.execute("""
    CREATE VIEW indv AS
    SELECT CISNO AS CUSTNOX, BANKNO AS BANKNO_INDV, *
    FROM indfile
    WHERE CISNO IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CISNO ORDER BY CISNO DESC)=1;
""")

# -----------------------------------#
# 7. Merge datasets step by step     #
# -----------------------------------#
con.execute(f"""
    CREATE VIEW mrgcis AS
    SELECT
        cis.CUSTNOX,
        cis.ADDREFX,
        cis.CUSTNAME,
        cis.PRIPHONEX,
        cis.SECPHONEX,
        cis.MOBILEPHX,
        cis.FAXX,
        cis.ALIASKEY,
        cis.ALIAS,
        cis.PROCESSTIME,
        cis.CUSTSTAT,
        cis.TAXCODE,
        cis.TAXID,
        cis.CUSTBRCH,
        cis.COSTCTR,
        cis.CUSTMNTDATE,
        cis.CUSTLASTOPER,
        cis.PRIM_OFF,
        cis.SEC_OFF,
        cis.PRIM_LN_OFF,
        cis.SEC_LN_OFF,
        cis.RACE,
        cis.RESIDENCY,
        cis.CITIZENSHIP,
        cis.OPENDT,
        cis.HRCALL,
        cis.EXPERIENCE,
        cis.HOBBIES,
        cis.RELIGION,
        cis.LANGUAGE,
        cis.INST_SEC,
        cis.CUST_CODE,
        cis.CUSTCONSENT,
        cis.BASICGRPCODE,
        cis.MSICCODE,
        cis.MASCO2008,
        cis.INCOME,
        cis.EDUCATION,
        cis.OCCUP,
        cis.MARITALSTAT,
        cis.OWNRENT,
        cis.EMPNAME,
        cis.DOBDOR,
        cis.SICCODE,
        cis.CORPSTATUS,
        cis.NETWORTH,
        cis.LAST_UPDATE_DATE,
        cis.LAST_UPDATE_TIME,
        cis.LAST_UPDATE_OPER,
        cis.PRCOUNTRY,
        cis.EMPLOYMENT_TYPE,
        cis.EMPLOYMENT_SECTOR,
        cis.EMPLOYMENT_LAST_UPDATE,
        cis.BNMID,
        cis.LONGNAME,
        cis.INDORG,
        indv.BANKNO_INDV,
        '{RUNTIME}' AS RUNTIMESTAMP,
        cis.RESIDENCY AS RESTR,
        cis.CORPSTATUS AS SALES,
        cis.CITIZENSHIP AS CITZN
    FROM cis
    LEFT JOIN indv ON cis.CUSTNOX = indv.CUSTNOX;
""")

# Join with restr, sales, citzn
con.execute("""
    CREATE VIEW mrgres AS
    SELECT mrgcis.*, restr.RESDESC
    FROM mrgcis LEFT JOIN restr ON mrgcis.RESTR = restr.RESTR;

    CREATE VIEW mrgsal AS
    SELECT mrgres.*, sales.SALDESC
    FROM mrgres LEFT JOIN sales ON mrgres.SALES = sales.SALES;

    CREATE VIEW mrgctz AS
    SELECT mrgsal.*, citzn.CTZDESC
    FROM mrgsal LEFT JOIN citzn ON mrgsal.CITZN = citzn.CITZN;
""")

# -----------------------------------#
# 8. Final Output (OUT2)             #
# -----------------------------------#
out2 = con.execute("SELECT * FROM mrgctz").arrow()

# Save outputs
pq.write_table(out2, "cis_internal/output/COMBINECUSTALL.parquet")
pc.write_csv(out2, "cis_internal/output/COMBINECUSTALL.csv")

