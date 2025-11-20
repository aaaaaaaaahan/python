# Single dictionary for all TXT queries
txt_queries = {
    "CIS_EMPLOYEE_RESIGN_NOTFOUND": out1,
    "CIS_EMPLOYEE_RESIGN": out2
}

# Loop through all TXT outputs
for txt_name, txt_query in txt_queries.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            if txt_name == "CIS_EMPLOYEE_RESIGN_NOTFOUND":
                # Field layout for notfound
                line = (
                    f"{str(row['REMARKS']).ljust(25)}"
                    f"{str(row['ORGID']).ljust(13)}"
                    f"{str(row['STAFFID']).ljust(9)}"
                    f"{str(row['ALIAS']).ljust(15)}"
                    f"{str(row['HRNAME']).zfill(40)}"
                    f"{str(row['CUSTNO']).zfill(11)}"
                )
            else:
                # Field layout for CIS_EMPLOYEE_RESIGN
                line = (
                    f"{str(row['STAFFID']).ljust(10)}"
                    f"{str(row['CUSTNO']).ljust(11)}"
                    f"{str(row['HRNAME']).ljust(40)}"
                    f"{str(row['CUSTNAME']).ljust(40)}"
                    f"{str(row['ALIASKEY']).zfill(3)}"
                    f"{str(row['ALIAS']).zfill(15)}"
                    f"{str(row['PRIMSEC']).zfill(1)}"
                    f"{str(row['ACCTCODE']).zfill(5)}"
                    f"{str(row['ALIAS']).zfill(20)}"
                )
            f.write(line + "\n")
