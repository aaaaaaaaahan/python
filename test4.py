# ---------------------------------------------------------------------
# Output as TXT with fixed-width fields (truncated if too long)
# ---------------------------------------------------------------------
txt_queries = {
    "CIS_LONGNAME_NONE": out
}

for txt_name, txt_query in txt_queries.items():
    txt_path = csv_output_path(f"{txt_name}_{report_date}").replace(".csv", ".txt")
    df_txt = con.execute(txt_query).fetchdf()

    with open(txt_path, "w", encoding="utf-8") as f:
        for _, row in df_txt.iterrows():
            line = (
                str(row.get('HOLDCONO',''))[-2:].rjust(2) +
                str(row.get('BANKNO',''))[-3:].rjust(3) +
                str(row.get('CUSTNO',''))[:20].ljust(20) +
                str(row.get('RECTYPE',''))[-2:].rjust(2) +
                str(row.get('RECSEQ',''))[-2:].rjust(2) +
                str(row.get('EFFDATE',''))[:5].rjust(5) +
                str(row.get('PROCESSTIME',''))[:8].ljust(8) +
                str(row.get('ADRHOLDCONO',''))[-2:].rjust(2) +
                str(row.get('ADRBANKNO',''))[-2:].rjust(2) +
                str(row.get('ADRREFNO',''))[:6].rjust(6) +
                str(row.get('CUSTTYPE',''))[:1].ljust(1) +
                str(row.get('KEYFIELD1',''))[:15].ljust(15) +
                str(row.get('KEYFIELD2',''))[:10].ljust(10) +
                str(row.get('KEYFIELD3',''))[:5].ljust(5) +
                str(row.get('KEYFIELD4',''))[:5].ljust(5) +
                str(row.get('LINECODE',''))[:1].ljust(1) +
                str(row.get('NAMELINE',''))[:40].ljust(40) +
                str(row.get('LINECODE1',''))[:1].ljust(1) +
                str(row.get('NAMETITLE1',''))[:40].ljust(40) +
                str(row.get('LINECODE2',''))[:1].ljust(1) +
                str(row.get('NAMETITLE2',''))[:40].ljust(40) +
                str(row.get('SALUTATION',''))[:40].ljust(40) +
                str(row.get('TITLECODE',''))[-4:].rjust(4) +
                str(row.get('FIRSTMID',''))[:30].ljust(30) +
                str(row.get('SURNAME',''))[:20].ljust(20) +
                str(row.get('SURNAMEKEY',''))[:3].ljust(3) +
                str(row.get('SUFFIXCODE',''))[:2].ljust(2) +
                str(row.get('APPENDCODE',''))[:2].ljust(2) +
                str(row.get('PRIMPHONE',''))[-6:].rjust(6) +
                str(row.get('PPHONELTH',''))[-2:].rjust(2) +
                str(row.get('SECPHONE',''))[-6:].rjust(6) +
                str(row.get('SPHONELTH',''))[-2:].rjust(2) +
                str(row.get('TELEXPHONE',''))[-6:].rjust(6) +
                str(row.get('TPHONELTH',''))[-2:].rjust(2) +
                str(row.get('FAXPHONE',''))[-6:].rjust(6) +
                str(row.get('FPHONELTH',''))[-2:].rjust(2) +
                str(row.get('LASTCHANGE',''))[:10].ljust(10) +
                str(row.get('PARSEIND',''))[:1].ljust(1)
            )
            f.write(line + "\n")
