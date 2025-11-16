# ============================
# STEP 2: PROCESS HRC FIELDS (cast to VARCHAR first)
# ============================
processed_hrc = ",\n".join([
    f"CASE WHEN LPAD(CAST({h} AS VARCHAR),3,'0')='002' THEN '   ' ELSE LPAD(CAST({h} AS VARCHAR),3,'0') END AS {h}C"
    for h in hrc_list
])

# ============================
# STEP 3: FILTER BANK EMPLOYEES (cast to VARCHAR first)
# ============================
filter_condition = " OR ".join([f"LPAD(CAST({h} AS VARCHAR),3,'0')='002'" for h in hrc_list])
