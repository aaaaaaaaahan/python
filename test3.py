# -------------------------------
# Process data: increment only numeric PERIOD_OVERDUEX, else keep as string
# -------------------------------
con.execute(f"""
    CREATE TABLE processed AS
    SELECT *,
           CASE
               WHEN TRY_CAST(PERIOD_OVERDUEX AS INTEGER) IS NOT NULL
               THEN CAST(CAST(PERIOD_OVERDUEX AS INTEGER) + 1 AS VARCHAR)
               ELSE PERIOD_OVERDUEX
           END AS PERIOD_OVERDUE
    FROM pending
    WHERE LOAD_DATE <> '{today_dt}'
""")
