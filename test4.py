report_lines.append(f"{brnbr:>7}  {brabbrv or '':<3}  {brname or '':<20}  {addr1 or '':<35}  {phone or '':<11}  {brstcode if brstcode is not None else '':>3}")
report_lines.append(f"{'':45}{addr2 or '':<35}")
report_lines.append(f"{'':45}{addr3 or '':<35}")
