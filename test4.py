# ---------------------------------------------------------------------
# Output as TXT
# ---------------------------------------------------------------------
def safe_str(val):
    """Convert None to empty string, otherwise str(val)."""
    return "" if val is None else str(val)

txt_queries = {
    "CIS_LONGNAME_NONE": out
}

for txt_name, txt_query in txt_queries.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                f"{safe_str(row.get('HOLDCONO')).rjust(2)}"
                f"{safe_str(row.get('BANKNO')).rjust(2)}"
                f"{safe_str(row.get('CUSTNO')).ljust(20)}"
                f"{safe_str(row.get('RECTYPE')).rjust(2)}"
                f"{safe_str(row.get('RECSEQ')).rjust(2)}"
                f"{safe_str(row.get('EFFDATE')).rjust(5)}"
                f"{safe_str(row.get('PROCESSTIME')).ljust(8)}"
                f"{safe_str(row.get('ADRHOLDCONO')).rjust(2)}"
                f"{safe_str(row.get('ADRBANKNO')).rjust(2)}"
                f"{safe_str(row.get('ADRREFNO')).rjust(6)}"
                f"{safe_str(row.get('CUSTTYPE')).ljust(1)}"
                f"{safe_str(row.get('KEYFIELD1')).ljust(15)}"
                f"{safe_str(row.get('KEYFIELD2')).ljust(10)}"
                f"{safe_str(row.get('KEYFIELD3')).ljust(5)}"
                f"{safe_str(row.get('KEYFIELD4')).ljust(5)}"
                f"{safe_str(row.get('LINECODE')).ljust(1)}"
                f"{safe_str(row.get('NAMELINE')).ljust(40)}"
                f"{safe_str(row.get('LINECODE1')).ljust(1)}"
                f"{safe_str(row.get('NAMETITLE1')).ljust(40)}"
                f"{safe_str(row.get('LINECODE2')).ljust(1)}"
                f"{safe_str(row.get('NAMETITLE2')).ljust(40)}"
                f"{safe_str(row.get('SALUTATION')).ljust(40)}"
                f"{safe_str(row.get('TITLECODE')).rjust(4)}"
                f"{safe_str(row.get('FIRSTMID')).ljust(30)}"
                f"{safe_str(row.get('SURNAME')).ljust(20)}"
                f"{safe_str(row.get('SURNAMEKEY')).ljust(3)}"
                f"{safe_str(row.get('SUFFIXCODE')).ljust(2)}"
                f"{safe_str(row.get('APPENDCODE')).ljust(2)}"
                f"{safe_str(row.get('PRIMPHONE')).rjust(6)}"
                f"{safe_str(row.get('PPHONELTH')).rjust(2)}"
                f"{safe_str(row.get('SECPHONE')).rjust(6)}"
                f"{safe_str(row.get('SPHONELTH')).rjust(2)}"
                f"{safe_str(row.get('TELEXPHONE')).rjust(6)}"
                f"{safe_str(row.get('TPHONELTH')).rjust(2)}"
                f"{safe_str(row.get('FAXPHONE')).rjust(6)}"
                f"{safe_str(row.get('FPHONELTH')).rjust(2)}"
                f"{safe_str(row.get('LASTCHANGE')).ljust(10)}"
                f"{safe_str(row.get('PARSEIND')).ljust(1)}"
            )
            f.write(line + "\n")
