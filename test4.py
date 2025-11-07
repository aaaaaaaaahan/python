title = "DETAIL LISTING FOR CIDOWFZT"
delimiter = "|"
txt_path = csv_output_path(f"CIHRCFZP_EXCEL_{report_date}").replace(".csv", ".txt")

res = con.execute(query)
columns = [desc[0] for desc in res.description]
rows = res.fetchall()

# remove year/month/day columns from TXT
skip_cols = {"year", "month", "day"}
keep_idx = [i for i, c in enumerate(columns) if c not in skip_cols]
keep_cols = [columns[i] for i in keep_idx]

with open(txt_path, "w", encoding="utf-8") as f:
    f.write(f"{title}\n")
    f.write(delimiter.join(keep_cols) + "\n")
    for row in rows:
        f.write(delimiter.join(str(row[i]) if row[i] is not None else "" for i in keep_idx) + "\n")

print(f"✅ TXT output created: {txt_path}")
