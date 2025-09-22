import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pc

# ======================================================
# DuckDB connection
# ======================================================
con = duckdb.connect()

# ======================================================
# Read Input Parquet Files (all upfront)
# ======================================================
con.execute("""
    CREATE VIEW ccrfile1 AS SELECT * FROM 'CCRIS_CISDEMO_DP_GDG.parquet';
    CREATE VIEW ccrfile2 AS SELECT * FROM 'CCRIS_CISDEMO_SAFD.parquet';
    CREATE VIEW ccrfile3 AS SELECT * FROM 'CCRIS_CISDEMO_LN_GDG.parquet';
    CREATE VIEW rlencc_in AS SELECT * FROM 'CCRIS_CISRLCC_GDG.parquet';
""")

# ======================================================
# Part 1 - GET CA RELATIONSHIP
# ======================================================
con.execute("""
    CREATE VIEW ccrfile AS
    SELECT * FROM ccrfile1
    UNION ALL
    SELECT * FROM ccrfile2
    UNION ALL
    SELECT * FROM ccrfile3;
""")

con.execute("""
    CREATE VIEW cis AS
    SELECT
        *,
        CASE
            WHEN ACCTNOR IN ('01','03','04','05','06','07') THEN 'DP'
            WHEN ACCTNOR IN ('02','08') THEN 'LN'
            ELSE NULL
        END AS ACCTCODE
    FROM ccrfile
    WHERE PRISEC = '901'
    ORDER BY ACCTNOC;
""")

print("=== Part 1 Output (first 5 rows) ===")
print(con.execute("SELECT * FROM cis LIMIT 5").fetchdf())

# ======================================================
# Part 2 - FLIP CC RELATIONSHIP
# ======================================================
con.execute("""
    CREATE VIEW flipped AS
    SELECT
        CUST2 AS CUST1,
        CODE2 AS CODE1,
        CUST1 AS CUST2,
        CODE1 AS CODE2
    FROM rlencc_in;
""")

print("=== Part 2 Output (first 5 rows) ===")
print(con.execute("SELECT * FROM flipped LIMIT 5").fetchdf())

# ======================================================
# Part 3 - MATCH RECORD WITH CC RELATIONSHIP
# ======================================================
con.execute("""
    CREATE VIEW merge1 AS
    SELECT
        cis.ACCTCODE,
        cis.ACCTNOC,
        cis.CUSTNO,
        cis.RLENCODE,
        flipped.CODE1,
        flipped.CUST2,
        flipped.CODE2
    FROM cis
    INNER JOIN flipped
        ON cis.CUSTNO = flipped.CUST1
    ORDER BY cis.ACCTNOC, cis.CUSTNO, flipped.CUST2;
""")

print("=== Part 3 Output (first 5 rows) ===")
print(con.execute("SELECT * FROM merge1 LIMIT 5").fetchdf())

# ======================================================
# Part 4 - REMOVE DUPLICATES
# ======================================================
con.execute("""
    CREATE VIEW final AS
    SELECT DISTINCT * FROM merge1;
""")

print("=== Part 4 Output (first 5 rows) ===")
print(con.execute("SELECT * FROM final LIMIT 5").fetchdf())

# ======================================================
# Export with PyArrow
# ======================================================
final_arrow: pa.Table = con.execute("SELECT * FROM final").arrow()

pq.write_table(final_arrow, "cis_internal/output/CISOWNER.parquet")
pc.write_csv(final_arrow, "cis_internal/output/CISOWNER.csv")
