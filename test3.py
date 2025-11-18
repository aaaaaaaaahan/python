# -------------------------------
# Process data: increment PERIOD_OVERDUEX only if numeric
# -------------------------------
con.execute(f"""
    CREATE TABLE processed AS
    SELECT *,
           CASE
               WHEN TRY_CAST(PERIOD_OVERDUEX AS INTEGER) IS NOT NULL
               THEN CAST(PERIOD_OVERDUEX AS INTEGER) + 1
               ELSE PERIOD_OVERDUEX
           END AS PERIOD_OVERDUE
    FROM pending
    WHERE LOAD_DATE <> '{today_dt}'
""")
