# ---------------------------------------------------------------------
# Output as TXT (loop for multiple queries)
# ---------------------------------------------------------------------
txt_queries = {
    "CIS_DJW_DPACCT_INDV": f"""
        SELECT *
        FROM MERGE
        WHERE INDORG = 'I'
        """,
    "CIS_DJW_DPACCT_ORG": f"""
        SELECT *
        FROM MERGE
        WHERE INDORG = 'O'
        """
}

for txt_name, txt_query in txt_queries.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")

    res = con.execute(txt_query)
    df_txt = res.fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{str(row['CUSTNO']).ljust(20)}"
                f"{str(row['ACCTCODE']).ljust(5)}"
                f"{str(row['ACCTNOX']).ljust(20)}"
                f"{str(row['OPENDX']).ljust(10)}"
            )
            f.write(line + "\n")
