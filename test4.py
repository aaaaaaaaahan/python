# 1. Check approval status values
con.execute("""
SELECT DISTINCT APPROVALSTATUS FROM 'your_parquet_path_here'
""").fetchall()

# 2. Check sample of remarks
con.execute("""
SELECT ACCTNO, HOVERIFYREMARKS FROM 'your_parquet_path_here' LIMIT 10
""").fetchdf()

# 3. See counts before filtering
con.execute("""
SELECT 
  COUNT(*) AS total,
  SUM(CASE WHEN POSITION('Noted by' IN HOVERIFYREMARKS)>0 THEN 1 ELSE 0 END) AS found_noted,
  SUM(CASE WHEN POSITION('Noted by' IN HOVERIFYREMARKS)<=0 THEN 1 ELSE 0 END) AS not_found
FROM 'your_parquet_path_here'
""").fetchdf()
