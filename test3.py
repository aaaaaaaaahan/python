# ============================================================
# STEP 2: READ RLEN FILES & FILTER (using safe cast)
# ============================================================
con.execute(f"""
    CREATE OR REPLACE VIEW rlen_files AS 
    SELECT * FROM read_parquet('{host_parquet_path("RLENCA_LN02.parquet")}')
    UNION ALL
    SELECT * FROM read_parquet('{host_parquet_path("RLENCA_LN08.parquet")}')
""")

con.execute("""
    CREATE OR REPLACE TABLE rlen AS
    SELECT 
        U_IBS_APPL_NO   AS ACCTNO,
        C_IBS_APPL_CODE AS ACCTCODE,
        U_IBS_R_APPL_NO AS CUSTNO,
        try_cast(C_IBS_E1_TO_E2 AS INTEGER) AS RLENCODE,
        try_cast(C_IBS_E2_TO_E1 AS INTEGER) AS PRISEC
    FROM rlen_files
    WHERE try_cast(C_IBS_E2_TO_E1 AS INTEGER) = 901
      AND try_cast(C_IBS_E1_TO_E2 AS INTEGER) IN (3,11,12,13,14,16,17,18,19,21,22,23,27,28)
""")
