# ---------------------------------------------------------
# 3. Write TXT output (with header + | delimiter)
# ---------------------------------------------------------
txt_queries = {
    "UNLOAD_CIHRCRVT_EXCEL": out
}

header = [
    "DETAIL LISTING FOR CIHRCRVT",
    "MONTH|BRCH_CODE|ACCT_TYPE|ACCT_NO|CUSTNO|CUSTID|CUST_NAME|"
    "NATIONALITY|ACCT_OPENDATE|OVERRIDING_INDC|OVERRIDING_OFFCR|"
    "OVERRIDING_REASON|DOWJONES_INDC|FUZZY_INDC|FUZZY_SCORE|"
    "NOTED_BY|RETURNED_BY|ASSIGNED_TO|NOTED_DATE|RETURNED_DATE|"
    "ASSIGNED_DATE|COMMENT_BY|COMMENT_DATE|SAMPLING_INDC|RETURN_STATUS|"
    "RECORD_STATUS|FUZZY_SCREEN_DATE"
]

for txt_name, txt_query in txt_queries.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:

        # write header
        for h in header:
            f.write(h + "\n")

        # write rows
        for _, row in df_txt.iterrows():
            fields = [
                row["HRV_MONTH"],
                row["HRV_BRCH_CODE"],
                row["HRV_ACCT_TYPE"],
                row["HRV_ACCT_NO"],
                row["HRV_CUSTNO"],
                row["HRV_CUSTID"],
                row["HRV_CUST_NAME"],
                row["HRV_NATIONALITY"],
                row["HRV_ACCT_OPENDATE"],
                row["HRV_OVERRIDING_INDC"],
                row["HRV_OVERRIDING_OFFCR"],
                row["HRV_OVERRIDING_REASON"],
                row["HRV_DOWJONES_INDC"],
                row["HRV_FUZZY_INDC"],
                row["HRV_FUZZY_SCORE"],
                row["HRV_NOTED_BY"],
                row["HRV_RETURNED_BY"],
                row["HRV_ASSIGNED_TO"],
                row["HRV_NOTED_DATE"],
                row["HRV_RETURNED_DATE"],
                row["HRV_ASSIGNED_DATE"],
                row["HRV_COMMENT_BY"],
                row["HRV_COMMENT_DATE"],
                row["HRV_SAMPLING_INDC"],
                row["HRV_RETURN_STATUS"],
                row["HRV_RECORD_STATUS"],
                row["HRV_FUZZY_SCREEN_DATE"]
            ]

            # Write "|" delimited line (null → blank)
            line = "|".join("" if v is None else str(v) for v in fields)
            f.write(line + "\n")
