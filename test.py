report_base = pl.concat(
    [mergedp3, mergeln, mergesdb, mergeuni, mergecom],
    how="diagonal_relaxed"
)

report_df = (
    report_base
    .with_columns([
        # default NILs
        pl.col("DEMODESC").fill_null("").replace("", "NIL").alias("DEMODESC"),
        pl.col("MASCODESC").fill_null("").replace("", "NIL").alias("MASCODESC"),
        pl.col("SICCODE").fill_null("").replace("", "NIL").alias("SICCODE"),
        pl.col("MSICDESC").fill_null("").replace("", "NIL").alias("MSICDESC"),
        # TEMP_* per SAS logic
        pl.when(pl.col("CURBAL").is_not_null() & (pl.col("CURBAL") != 0))
          .then(pl.col("CURBAL")).otherwise(pl.lit(0.0)).alias("TEMP_CURBAL"),
        pl.when(pl.col("CURR_AMT_DR") > 0).then(pl.col("CURR_AMT_DR")).otherwise(0.0).alias("TEMP_CURR_AMT_DR"),
        pl.when(pl.col("CURR_AMT_CR") > 0).then(pl.col("CURR_AMT_CR")).otherwise(0.0).alias("TEMP_CURR_AMT_CR"),
        pl.when(pl.col("PREV_AMT_DR") > 0).then(pl.col("PREV_AMT_DR")).otherwise(0.0).alias("TEMP_PREV_AMT_DR"),
        pl.when(pl.col("PREV_AMT_CR") > 0).then(pl.col("PREV_AMT_CR")).otherwise(0.0).alias("TEMP_PREV_AMT_CR"),
        pl.when(pl.col("CURR_CYC_DR") > 0).then(pl.col("CURR_CYC_DR")).otherwise(0).alias("TEMP_CURR_CYC_DR"),
        pl.when(pl.col("CURR_CYC_CR") > 0).then(pl.col("CURR_CYC_CR")).otherwise(0).alias("TEMP_CURR_CYC_CR"),
        pl.when(pl.col("PREV_CYC_DR") > 0).then(pl.col("PREV_CYC_DR")).otherwise(0).alias("TEMP_PREV_CYC_DR"),
        pl.when(pl.col("PREV_CYC_CR") > 0).then(pl.col("PREV_CYC_CR")).otherwise(0).alias("TEMP_PREV_CYC_CR"),
        # holds
        pl.when(pl.col("AMT_1") > 0).then(pl.col("AMT_1")).otherwise(0.0).alias("TEMP_AMT_1"),
        pl.when(pl.col("AMT_2") > 0).then(pl.col("AMT_2")).otherwise(0.0).alias("TEMP_AMT_2"),
        pl.when(pl.col("AMT_3") > 0).then(pl.col("AMT_3")).otherwise(0.0).alias("TEMP_AMT_3"),
    ])
    # zero out TEMP_* if APPL_CODE not in ('FD','CA','SA')
    .with_columns([
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_CURBAL")).otherwise(0.0).alias("TEMP_CURBAL"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_CURR_AMT_DR")).otherwise(0.0).alias("TEMP_CURR_AMT_DR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_CURR_AMT_CR")).otherwise(0.0).alias("TEMP_CURR_AMT_CR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_PREV_AMT_DR")).otherwise(0.0).alias("TEMP_PREV_AMT_DR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_PREV_AMT_CR")).otherwise(0.0).alias("TEMP_PREV_AMT_CR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_CURR_CYC_DR")).otherwise(0).alias("TEMP_CURR_CYC_DR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_CURR_CYC_CR")).otherwise(0).alias("TEMP_CURR_CYC_CR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_PREV_CYC_DR")).otherwise(0).alias("TEMP_PREV_CYC_DR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_PREV_CYC_CR")).otherwise(0).alias("TEMP_PREV_CYC_CR"),
    ])
    .with_row_count("NO", offset=1)
    .select([
        pl.col("ROW_NO"),
        pl.col("ALIASKEY"), pl.col("ALIAS"), pl.col("CUSTNAME"),
        pl.col("CUSTNO"), pl.col("DEMODESC"), pl.col("MASCODESC"),
        pl.col("SICCODE"), pl.col("MSICDESC"), pl.col("ACCTNOC"),
        pl.col("BRANCH_ABBR"), pl.col("ACCTSTATUS"),
        pl.col("DATEOPEN"), pl.col("DATECLSE"),
        pl.col("SDBIND"), pl.col("SDBBRH"),
        pl.col("TEMP_CURBAL"), pl.col("TEMP_CURR_CYC_DR"), pl.col("TEMP_CURR_AMT_DR"),
        pl.col("TEMP_CURR_CYC_CR"), pl.col("TEMP_CURR_AMT_CR"),
        pl.col("TEMP_PREV_CYC_DR"), pl.col("TEMP_PREV_AMT_DR"),
        pl.col("TEMP_PREV_CYC_CR"), pl.col("TEMP_PREV_AMT_CR"),
        pl.col("ACCT_PST_IND"), pl.col("ACCT_PST_REASON"),
        pl.col("TOT_HOLD"), pl.col("SEQID_1"), pl.col("TEMP_AMT_1"),
        pl.col("DESC_1"), pl.col("SOURCE_1"),
        pl.col("SEQID_2"), pl.col("TEMP_AMT_2"),
        pl.col("DESC_2"), pl.col("SOURCE_2"),
        pl.col("SEQID_3"), pl.col("TEMP_AMT_3"),
        pl.col("DESC_3"), pl.col("SOURCE_3"),
    ])
)

# Write semicolon-delimited report file
report_header_lines = [
    "LIST OF CUSTOMERS INFORMATION",
    ";".join([
        "NO","ID TYPE","ID NUMBER","CUST NAME","CIS NUMBER","OCCUPATION",
        "MASCO","SIC CODE","MSIC BIS TYPE","ACCT NUMBER","ACCT BRANCH",
        "ACCT STATUS","DATE ACCT OPEN","DATE ACCT CLOSED","SDB(YES/NO)",
        "BR SDB MAINTAN","CURRENT BALANCE","CURR CYC DR","CURR AMT DR",
        "CURR CYC CR","CURR AMT CR","PREV CYC DR","PREV AMT DR",
        "PREV CYC CR","PREV AMT CR","POST INDICATOR","POST INDICATOR REASON",
        "TOTAL OF HOLD","SEQ OF HOLD(1)","AMT OF HOLD(1)","DESCRIP OF HOLD(1)","SOURCE(1)",
        "SEQ OF HOLD(2)","AMT OF HOLD(2)","DESCRIP OF HOLD(2)","SOURCE(2)",
        "SEQ OF HOLD(3)","AMT OF HOLD(3)","DESCRIP OF HOLD(3)","SOURCE(3)"
    ])
]

report_body = report_df.with_columns([pl.all().cast(pl.Utf8, strict=False).fill_null("")])

with open("CMDREPORT.txt", "w", encoding="utf-8") as f:
    for line in report_header_lines:
        f.write(line + "\n")
    for row in report_body.iter_rows():
        f.write(";".join("" if v is None else str(v) for v in row) + "\n")
