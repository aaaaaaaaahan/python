# Remove unused VALID_STATUS and align header
con.execute(f"""
    CREATE TABLE hrc_filtered AS
    SELECT
        *,
        substring(UPDATEDATE, 1, 4) AS UPDDATE,
        CASE 
            WHEN ACCTNO != ' ' AND POSITION('Noted by' IN HOVERIFYREMARKS) <= 0 THEN 1 
            ELSE 0 
        END AS HOEPDNOTE,
        CASE 
            WHEN ACCTNO != ' ' AND POSITION('Noted by' IN HOVERIFYREMARKS) > 0 THEN 1 
            ELSE 0 
        END AS HOENOTED
    FROM '{host_parquet_path("UNLOAD_CIHRCAPT_FB.parquet")}'
    WHERE substring(UPDATEDATE, 1, 4) = '{yyyy}'
      AND ACCTTYPE IN ('CA','SA','SDB','FD','FC','FCI','O','FDF')
      AND APPROVALSTATUS = '08'
""")

# Fix header to match SAS exactly
header = (
    f"{'BRANCH':<7}"
    "HOE PEND NOTE, HOE NOTED, TOTAL"
)
