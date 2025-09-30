Error in sitecustomize; set PYTHONVERBOSE for traceback:
TypeError: expected str, bytes or os.PathLike object, not NoneType
Traceback (most recent call last):
  File "/pythonITD/cis_dev/jobs/cis_internal/CIS_PY_CCRNMX3B.py", line 91, in <module>
    con.execute(f"""
duckdb.duckdb.ConversionException: Conversion Error: Could not convert string 'I' to INT64 when casting from source column SECPHONE

LINE 16:     FROM name_clean n
