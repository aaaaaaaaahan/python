# Example: cleaning ALIAS file after reading
alias = con.execute("""
    SELECT
        regexp_replace(IC, ';', '', 'g') AS IC,
        regexp_replace(ALIAS, ';', '', 'g') AS ALIAS
    FROM read_csv_auto('ALIAS.csv', delim=';', header=True)
""").fetchdf()
