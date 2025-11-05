# ---------------------------------------------------------------------
# Output as Parquet and CSV (explicit fields, match SAS PUT layout)
# ---------------------------------------------------------------------

goodot_fields = """
    SELECT
        BANKNUM,
        CUSTBRCH,
        CUSTNO,
        CUSTNAME,
        RACE,
        CITIZENSHIP,
        INDORG,
        PRIMSEC,
        CUSTLASTDATECC,
        CUSTLASTDATEYY,
        CUSTLASTDATEMM,
        CUSTLASTDATEDD,
        ALIASKEY,
        ALIAS,
        HRCCODES,
        BRANCH,
        ACCTCODE,
        ACCTNO,
        OPDATE,
        ACCSTAT,
        COSTCTR,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM GOODOT
""".format(year=year, month=month, day=day)

badot_fields = """
    SELECT
        BANKNUM,
        CUSTBRCH,
        CUSTNO,
        CUSTNAME,
        RACE,
        CITIZENSHIP,
        INDORG,
        PRIMSEC,
        CUSTLASTDATECC,
        CUSTLASTDATEYY,
        CUSTLASTDATEMM,
        CUSTLASTDATEDD,
        ALIASKEY,
        ALIAS,
        HRCCODES,
        BRANCH,
        ACCTCODE,
        ACCTNO,
        OPDATE,
        ACCSTAT,
        COSTCTR,
        {year} AS year,
        {month} AS month,
        {day} AS day
    FROM BADOT
""".format(year=year, month=month, day=day)

conv_fields = """
    SELECT
        BANKNUM,
        CUSTBRCH,
        CUSTNO,
        CUSTNAME,
        RACE,
        CITIZENSHIP,
        INDORG,
