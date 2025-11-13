# TOTLIST (overview)
for rec in totlist:
    code, desc, count = rec
    desc_with_code = f"{code} {desc}"   # <-- prepend error code here
    f.write(f"{desc_with_code:<47}{count:>12}\n")
