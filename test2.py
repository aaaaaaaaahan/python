Traceback (most recent call last):
  File "/pythonITD/cis_dev/jobs/cis_internal/CCRSADR4.py", line 219, in <module>
    con.execute(f"""
duckdb.duckdb.InvalidInputException: Invalid Input Error: 
The returned result contained NULL values, but the 'null_handling' was set to DEFAULT.
If you want more control over NULL values then 'null_handling' should be set to SPECIAL.

With DEFAULT all rows containing NULL have been filtered from the UDFs input.
Those rows are automatically set to NULL in the final result.
The UDF is not expected to return NULL values.
