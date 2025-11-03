Traceback (most recent call last):
  File "/pythonITD/cis_dev/jobs/cis_internal/CIHCMRPT.py", line 95, in <module>
    write_fixed_width_txt(table, output_folder / filename, title=table)
  File "/pythonITD/cis_dev/jobs/cis_internal/CIHCMRPT.py", line 57, in write_fixed_width_txt
    df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
duckdb.duckdb.CatalogException: Catalog Error: Table with name MPBB does not exist!

LINE 1: SELECT * FROM MPBB
