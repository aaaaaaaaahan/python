Traceback (most recent call last):
  File "/pythonITD/cis_dev/jobs/cis_internal/CICMDRPT.py", line 35, in <module>
    dp, year, month, day = get_hive_dp_parquet('DPBOPKRP_RBP2.B033.DPTRBLGS.parquet')
  File "/pythonITD/cis_dev/jobs/cis_internal/CIS_PY_READER.py", line 318, in get_hive_dp_parquet
    raise FileNotFoundError(f"No 'Year =' folders found under {year_base_path}. Found: {os.listdir(year_base_path)}")
FileNotFoundError: No 'Year =' folders found under /host/dp/parquet. Found: ['test.txt', 'year=2025']
