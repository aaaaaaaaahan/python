#---------------------------------------#
# Part 3 - COMBINE + DEDUP              #
#---------------------------------------#

# Combine LEFT + RIGHT and drop expired (EXPDATE < batchdate)
all_output = con.execute(f"""
    SELECT 
        l.CUSTNO1, l.INDORG1, l.CODE1, l.DESC1,
        l.CUSTNO2, r.INDORG2, r.CODE2, r.DESC2,
        l.EXPDATE,
        l.CUSTNAME1, l.ALIAS1, r.CUSTNAME2, r.ALIAS2,
        l.OLDIC1, l.BASICGRPCODE1, r.OLDIC2, r.BASICGRPCODE2,
        l.EFFDATE
    FROM LEFTOUT l
    LEFT JOIN RIGHTOUT r ON l.CUSTNO2 = r.CUSTNO2
    WHERE (l.EXPDATE IS NULL OR l.EXPDATE >= DATE '{batchdate.strftime("%Y-%m-%d")}')
""").arrow()

# Dedup
all_output_unique = con.execute("""
    SELECT DISTINCT CUSTNO1, CUSTNO2, CODE1, CODE2,
           INDORG1, DESC1, INDORG2, DESC2, EXPDATE,
           CUSTNAME1, ALIAS1, CUSTNAME2, ALIAS2,
           OLDIC1, BASICGRPCODE1, OLDIC2, BASICGRPCODE2,
           EFFDATE
    FROM all_output
    ORDER BY CUSTNO1
""").arrow()

duplicates = con.execute("""
    SELECT * FROM all_output
    EXCEPT SELECT * FROM all_output_unique
""").arrow()

