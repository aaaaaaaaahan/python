con.execute(f"""
    CREATE TABLE processed AS
    SELECT *,
           CASE
               WHEN PERIOD_OVERDUEX ~ '^[0-9]+$'  -- only digits
               THEN CAST(CAST(PERIOD_OVERDUEX AS INTEGER) + 1 AS VARCHAR)
               ELSE PERIOD_OVERDUEX
           END AS PERIOD_OVERDUE
    FROM pending
    WHERE LOAD_DATE <> '{today_dt}'
""")
