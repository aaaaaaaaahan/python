txt_queries = {
    "WINDOW_SIGNATOR_CA0801_MERGED": merged
}

for txt_name, txt_query in txt_queries.items():

    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():

            line = (
                " " * 20 +                                                 # positions 1–20 blank
                str(row["ACCTNO"]).ljust(11) +                             # 21–31  ACCTNO  ($11.)
                " " * (54 - 32) +                                          # pad to position 54
                str(row["IC_NUMBER"]).ljust(20) +                          # 54–73 IC_NUMBER ($20.)
                " " * (76 - 74) +                                          # pad to position 76
                str(row["NAME"]).ljust(40) +                               # 76–115 NAME ($40.)
                "Y" +                                                      # 116 constant
                str(row["STATUS"])[:1] +                                   # 117 STATUS ($1.)
                str(row["BRHABV"]).ljust(3) +                              # 118–120 BRHABV ($3.)
                str(row["BRANCH"]).zfill(3)                                # 121–123 BRANCH (Z03.)
            )

            f.write(line + "\n")
