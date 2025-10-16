1	import duckdb
2	from CIS_PY_READER import host_parquet_path,parquet_output_path,csv_output_path
3	import datetime
4	today = datetime.date.today()
5	batch_date = today - datetime.timedelta(days=1)
6	today_year, today_month, today_day = today.year, today.month, today.day
7	year, month, day = batch_date.year, batch_date.month, batch_date.day
8	con = duckdb.connect()
9	custout = con.execute(f"""
10	           '{batch_date.strftime("%Y%m%d")}' AS CUSTMNTDATE
11	    FROM '{host_parquet_path("ALLCUST_FB.parquet")}'
12	masscls = """
13	""".format(year=year,month=month,day=day)
14	masscls_bnk = """
15	""".format(year=year,month=month,day=day)
16	verify = """
17	""".format(year=year,month=month,day=day)
18	    "AMLHRC_EXTRACT_MASSCLS"            : masscls,
19	    "AMLHRC_EXTRACT_MASSCLS_BNKSTFF"    : masscls_bnk,
20	    "AMLHRC_EXTRACT_VERIFY"             : verify
21	queries = {
22	for name, query in queries.items():
23	    parquet_path = parquet_output_path(name)
24	    csv_path = csv_output_path(name)
25	    con.execute(f"""
26	    COPY ({query})
27	    TO '{parquet_path}'
