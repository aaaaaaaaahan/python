import duckdb
from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path, get_hive_parquet
import datetime
import sys

batch_date = (datetime.date.today() - datetime.timedelta(days=1))
year1, month1, day1 = batch_date.year, batch_date.month, batch_date.day

# =====================================================
# PROCESS FILE USING DUCKDB
# =====================================================
con = duckdb.connect()

# Read parquet into DuckDB
con.execute(f"""
    CREATE TABLE fclbfull AS 
    SELECT 
        RECORD_TYPE,  
        NEW_EMPL_CODE,
        TOTAL_REC,    
        EMPL_NAME,    
        NOTICEID,     
        AMOUNT        
    FROM '{host_parquet_path("PERKESO_FCLBFILE_FULL.parquet")}'
""")

# =====================================================
# FILTERING & VALIDATION (same as SAS logic)
# =====================================================
# Remove header and footer
con.execute("""
    DELETE FROM fclbfull WHERE record_type = 'H'
""")

# Count data records
x = con.execute("""
    SELECT COUNT(*) FROM fclbfull WHERE record_type = 'D'
""").fetchone()[0]

# Get footer total record
footer_total = con.execute("""
    SELECT CAST(total_rec AS INTEGER) 
    FROM fclbfull 
    WHERE record_type = 'F'
""").fetchone()

if footer_total:
    total_rec_num = footer_total[0]
    if total_rec_num != x:
        print(f"ERROR: Footer total ({total_rec_num}) does not match data record count ({x}).")
        sys.exit(88)
else:
    print("ERROR: No footer record found.")
    sys.exit(77)

# Delete footer records
con.execute("""
    DELETE FROM fclbfull WHERE record_type = 'F'
""")

# Remove invalid NOTICEID (missing last 3 chars)
con.execute("""
    DELETE FROM fclbfull 
    WHERE substr(noticeid, 15, 3) = '   '
""")

# =====================================================
# SORTING (two sorts like SAS)
# =====================================================
# POSITION FOR NAME AND NOTICEID IS SWITCHED AS  
# IS BEING USED AS PRIMARY KEY FOR CIFCLBTT TABLE
con.execute("""
    CREATE TABLE fclbfull_sorted AS
    SELECT DISTINCT  
        NEW_EMPL_CODE,
        NOTICEID, 
        EMPL_NAME,        
        AMOUNT 
    FROM fclbfull
    ORDER BY new_empl_code, noticeid, empl_name, amount
""")

# =====================================================
# Output
# =====================================================
final = """
    SELECT 
        *
        ,{year1} AS year
        ,{month1} AS month
        ,{day1} AS day
    FROM fclbfull_sorted
""".format(year1=year1,month1=month1,day1=day1)

queries = {
    "PERKESO_FCLBEISC_FULLLOAD"            : final
}

for name, query in queries.items():
    parquet_path = parquet_output_path(name)
    csv_path = csv_output_path(name)

    con.execute(f"""
    COPY ({query})
    TO '{parquet_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE true);  
     """)
    
    con.execute(f"""
    COPY ({query})
    TO '{csv_path}'
    (FORMAT CSV, HEADER, DELIMITER ',', OVERWRITE_OR_IGNORE true);  
     """)
