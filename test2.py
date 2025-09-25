-- Step 4 - Unique records (keep first only)
CREATE VIEW UNQREC AS
SELECT sub.*,
       {year} AS year,
       {month} AS month,
       {day} AS day
FROM (
    SELECT a.*,
           ROW_NUMBER() OVER (
               PARTITION BY 
                   a.CUSTNO1, a.INDORG1, a.CODE1, a.DESC1,
                   a.CUSTNO2, a.INDORG2, a.CODE2, a.DESC2,
                   a.EXPDATE,
                   a.CUSTNAME1, a.ALIAS1,
                   a.CUSTNAME2, a.ALIAS2,
                   a.OLDIC1, a.BASICGRPCODE1,
                   a.OLDIC2, a.BASICGRPCODE2,
                   a.EFFDATE
               ORDER BY a.CUSTNO1, a.CODE1
           ) AS rn
    FROM alloutput a
) sub
WHERE sub.rn = 1
ORDER BY CUSTNO1, CODE1;


-- Step 5 - Duplicate records (all rows except the first)
CREATE VIEW DUPREC AS
SELECT sub.*,
       {year} AS year,
       {month} AS month,
       {day} AS day
FROM (
    SELECT a.*,
           ROW_NUMBER() OVER (
               PARTITION BY 
                   a.CUSTNO1, a.INDORG1, a.CODE1, a.DESC1,
                   a.CUSTNO2, a.INDORG2, a.CODE2, a.DESC2,
                   a.EXPDATE,
                   a.CUSTNAME1, a.ALIAS1,
                   a.CUSTNAME2, a.ALIAS2,
                   a.OLDIC1, a.BASICGRPCODE1,
                   a.OLDIC2, a.BASICGRPCODE2,
                   a.EFFDATE
               ORDER BY a.CUSTNO1, a.CODE1
           ) AS rn
    FROM alloutput a
) sub
WHERE sub.rn > 1
ORDER BY CUSTNO1, CODE1;
