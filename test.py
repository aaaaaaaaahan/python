# ================================================================
# Deposit Trial Balance (split MYR / non-MYR)
# ================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE DP_ALL AS
    SELECT
        BANKNO,
        REPTNO,
        FMTCODE,
        ACCTBRCH1 AS BRANCHNO,
        ACCTNO,
        ACCTX,
        CLSEDATE,
        OPENDATE,
        HOLDAMT1/100.0 AS HOLDAMT,
        LEDGERBAL1/100.0 AS LEDGERBAL,
        ODLIMIT,
        CURRCODE,
        OPENIND,
        DORMIND,
        COSTCTR,
        POSTIND,
        CASE 
            WHEN COSTCTR > 3000 AND COSTCTR < 3999 THEN 'I'
            ELSE 'C'
        END AS BANKINDC,
        CASE 
            WHEN CURRCODE <> 'MYR' THEN LEDGERBAL1/100.0
            ELSE 0
        END AS FOREXAMT,
        CASE 
            WHEN OPENIND = ''  THEN 'ACTIVE'
            WHEN OPENIND IN ('B','C','P') THEN 'CLOSED'
            WHEN OPENIND = 'Z' THEN 'ZERO BALANCE'
            ELSE ''
        END AS ACCTSTATUS,
        substr(OPENDATE,4,2) AS OPENDD,
        substr(OPENDATE,2,2) AS OPENMM,
        substr(OPENDATE,6,4) AS OPENYY,
        substr(CLSEDATE,4,2) AS CLSEDD,
        substr(CLSEDATE,2,2) AS CLSEMM,
        substr(CLSEDATE,6,4) AS CLSEYY
    FROM '{host_parquet_path("DPTRBLGS.parquet")}'
    WHERE REPTNO = 1001
      AND FMTCODE IN (1,10,22,19,20,21)
      AND ACCTNO > 1000000000 AND ACCTNO < 1999999999
""")
