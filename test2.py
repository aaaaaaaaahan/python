import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq

# ------------------------
# Step 1: Read Parquet files via DuckDB
# ------------------------
con = duckdb.connect()

con.execute("""
    CREATE OR REPLACE VIEW addr AS 
    SELECT 
        LPAD(CAST(ADDREF1 AS VARCHAR), 11, '0') AS ADDREF,
        LINE1IND, LINE1ADR, 
        LINE2IND, LINE2ADR,
        LINE3IND, LINE3ADR,
        LINE4IND, LINE4ADR,
        LINE5IND, LINE5ADR
    FROM read_parquet('/host/cis/input/ADDRLINE_FB.parquet')
""")

con.execute("""
    CREATE OR REPLACE VIEW aele AS 
    SELECT 
        LPAD(CAST(ADDREF1 AS VARCHAR), 11, '0') AS ADDREF,
        STREET, CITY, ZIP, ZIP2, COUNTRY
    FROM read_parquet('/host/cis/input/ADDRAELE_FB_PROD.parquet')
""")

print("ADDR sample:")
print(con.execute("SELECT * FROM addr LIMIT 5").df())

print("AELE sample:")
print(con.execute("SELECT * FROM aele LIMIT 5").df())

# ------------------------
# Step 2: Merge ADDR + AELE
# ------------------------
con.execute("""
    CREATE OR REPLACE VIEW addr_aele AS
    SELECT 
        a.ADDREF, 
        LINE1ADR, LINE2ADR, LINE3ADR, LINE4ADR, LINE5ADR,
        CITY, ZIP, COUNTRY,
        (COALESCE(LINE1ADR,'') || COALESCE(LINE2ADR,'') ||
         COALESCE(LINE3ADR,'') || COALESCE(LINE4ADR,'') ||
         COALESCE(LINE5ADR,'')) AS ADDRLINE
    FROM addr a
    INNER JOIN aele e
    ON a.ADDREF = e.ADDREF
    WHERE CITY <> '' AND ZIP <> ''
""")

# ------------------------
# Step 3: Remove invalid countries
# ------------------------
bad_countries = [
    "SINGAPORE ","CANADA    ","SINGAPORE`","LONDON    ","AUS       ",
    "AUSTRIA   ","BAHRAIN   ","BANGLADESH","BRUNEI DAR","CAMBODIA  ",
    "CAN       ","CAYMAN ISL","CHINA     ","BRUNEI    ","INDONESIA ",
    "DARUSSALAM","DENMARK   ","EMIRATES  ","ENGLAND   ","EUROPEAN  ",
    "FRANCE    ","GERMANY   ","HONG KONG ","INDIA     ","IRAN (ISLA",
    "IRELAND   ","JAPAN     ","KOREA REPU","MACAU     ","MAURITIUS ",
    "MEXICO    ","MYANMAR   ","NEPAL     ","NETHERLAND","NEW ZEALAN",
    "NEWZEALAND","NIGERIA   ","NORWAY    ","OMAN      ","PAKISTAN  ",
    "PANAMA    ","PHILIPPINE","ROC       ","S ARABIA  ","SAMOA     ",
    "SAUDI ARAB","SIGAPORE  ","SIMGAPORE ","SINGAPOREW","SINGPAORE ",
    "SINGPORE  ","SINAGPORE ","SNGAPORE  ","SINGOPORE ","SPAIN     ",
    "SRI LANKA ","SWAZILAND ","SWEDEN    ","SWITZERLAN","TAIWAN    ",
    "TAIWAN,PRO","THAILAND  ","U KINGDOM ","U.K.      ","UNITED ARA",
    "UK        ","UNITED KIN","UNITED STA","VIRGIN ISL","USA       ",
    "PAPUA NEW ","AUSTRALIA "
]
con.execute(f"""
    CREATE OR REPLACE VIEW addr_aele_clean AS
    SELECT * FROM addr_aele
    WHERE COUNTRY NOT IN ({",".join(["'"+x+"'" for x in bad_countries])})
""")

# ------------------------
# Step 4: Extract NEW_ZIP / NEW_CITY from address lines
# ------------------------
def extract_zip_city(line):
    return f"""
        WHEN substr({line},1,5) BETWEEN '00001' AND '99998'
             AND substr({line},6,1)=' '
        THEN substr({line},1,5)
    """

zip_case = "CASE " + " ".join([extract_zip_city(l) for l in ["LINE1ADR","LINE2ADR","LINE3ADR","LINE4ADR","LINE5ADR"]]) + " ELSE '' END"
city_case = "CASE " + " ".join([
    f"""WHEN substr({l},1,5) BETWEEN '00001' AND '99998' AND substr({l},6,1)=' ' 
        THEN substr({l},7,25)""" for l in ["LINE1ADR","LINE2ADR","LINE3ADR","LINE4ADR","LINE5ADR"]
]) + " ELSE '' END"

con.execute(f"""
    CREATE OR REPLACE VIEW addr_aele_zip AS
    SELECT *,
        {zip_case} AS NEW_ZIP,
        {city_case} AS NEW_CITY,
        CASE WHEN {zip_case} <> '' THEN 'MALAYSIA' ELSE '' END AS NEW_COUNTRY
    FROM addr_aele_clean
""")

# ------------------------
# Step 5: Exclude bad substrings
# ------------------------
exclude_strings = [
    "SINGAPORE","HONG HONG","QATAR","TAMIL NADU","STAFFORDSHIRE",
    "HANOI","VIETNAM","NEW ZEALAND","ENGLAND","AUCKLAND","SHANGHAI",
    "DOHA QATAR","THAILAND","HONG KONG","SEOUL","#","NSW","NETHERLANDS",
    "AUSTRALIA","S'PORE"
]
where_clause = " AND ".join([f"ADDRLINE NOT LIKE '%{x}%'" for x in exclude_strings])

con.execute(f"""
    CREATE OR REPLACE VIEW addraele1 AS
    SELECT * FROM addr_aele_zip
    WHERE {where_clause}
""")

# ------------------------
# Step 6: Assign STATEX
# ------------------------
def assign_state(zipcode: str):
    if zipcode is None or not zipcode.isdigit():
        return None
    z = int(zipcode)
    if 79000 <= z <= 86999: return "JOH"
    if 5000 <= z <= 9999: return "KED"
    if 15000 <= z <= 18999: return "KEL"
    if 75000 <= z <= 78999: return "MEL"
    if 70000 <= z <= 73999: return "NEG"
    if 25000 <= z <= 28999 or z == 69000: return "PAH"
    if 10000 <= z <= 14999: return "PEN"
    if 30000 <= z <= 36999 or 39000 <= z <= 39999: return "PRK"
    if 1000 <= z <= 2999: return "PER"
    if 88000 <= z <= 91999: return "SAB"
    if 93000 <= z <= 98999: return "SAR"
    if 40000 <= z <= 49999 or 63000 <= z <= 64999 or 68000 <= z <= 68199: return "SEL"
    if 20000 <= z <= 24999: return "TER"
    if 50000 <= z <= 60999: return "W P"
    if 87000 <= z <= 87999: return "LAB"
    if 62000 <= z <= 62999: return "PUT"
    return None

duckdb.create_function("assign_state", assign_state, ["VARCHAR"], "VARCHAR")

con.execute("""
    CREATE OR REPLACE VIEW addraele2 AS
    SELECT *, assign_state(NEW_ZIP) AS STATEX
    FROM addraele1
    WHERE NEW_ZIP <> ''
""")

# ------------------------
# Step 7: Output with PyArrow
# ------------------------
outfile_df = con.execute("""
    SELECT 
        '' AS "CIS #",
        '-' AS "-",
        ADDREF AS "ADDR REF",
        LINE1ADR AS "ADDLINE1",
        LINE2ADR AS "ADDLINE2",
        LINE3ADR AS "ADDLINE3",
        LINE4ADR AS "ADDLINE4",
        LINE5ADR AS "ADDLINE5",
        '*OLD*' AS "OLD_FLAG",
        ZIP AS "ZIP_OLD",
        CITY AS "CITY_OLD",
        COUNTRY AS "COUNTRY_OLD",
        '*NEW*' AS "NEW_FLAG",
        TRIM(NEW_ZIP) AS NEW_ZIP,
        TRIM(NEW_CITY) AS NEW_CITY,
        STATEX,
        TRIM(NEW_COUNTRY) AS NEW_COUNTRY
    FROM addraele2
""").arrow()

updfile_df = con.execute("""
    SELECT
        '' AS "CIS #",
        ADDREF AS "ADDR REF",
        UPPER(TRIM(NEW_CITY)) AS NEW_CITY,
        STATEX,
        TRIM(NEW_ZIP) AS NEW_ZIP,
        TRIM(NEW_COUNTRY) AS NEW_COUNTRY
    FROM addraele2
""").arrow()

csv.write_csv(outfile_df, "/host/cis/output/duckdb/CCRSADR4_VERIFY.csv")
pq.write_table(outfile_df, "/host/cis/output/duckdb/CCRSADR4_VERIFY.parquet")

csv.write_csv(updfile_df, "/host/cis/output/duckdb/CCRSADR4_UPDATE.csv")
pq.write_table(updfile_df, "/host/cis/output/duckdb/CCRSADR4_UPDATE.parquet")
