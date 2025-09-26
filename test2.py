-- Create twisted version of alloutput
CREATE VIEW alloutput_twist AS
SELECT 
    l.CUSTNO2   AS CUSTNO1,
    l.INDORG2   AS INDORG1,
    l.CODE2     AS CODE1,
    l.DESC2     AS DESC1,
    l.CUSTNO1   AS CUSTNO2,
    l.INDORG1   AS INDORG2,
    l.CODE1     AS CODE2,
    l.DESC1     AS DESC2,
    l.EXPDATE,
    l.CUSTNAME2 AS CUSTNAME1,
    l.ALIAS2    AS ALIAS1,
    l.CUSTNAME1 AS CUSTNAME2,
    l.ALIAS1    AS ALIAS2,
    l.OLDIC2    AS OLDIC1,
    l.BASICGRPCODE2 AS BASICGRPCODE1,
    l.OLDIC1    AS OLDIC2,
    l.BASICGRPCODE1 AS BASICGRPCODE2,
    l.EFFDATE
FROM alloutput l;

-- Combine original and twisted
CREATE VIEW alloutput_full AS
SELECT * FROM alloutput
UNION ALL
SELECT * FROM alloutput_twist;

##########################################
#Step 4#
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
    FROM alloutput_full a
) sub
WHERE sub.rn = 1
ORDER BY CUSTNO1, CODE1;

#############################################
#Step 5#
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
    FROM alloutput_full a
) sub
WHERE sub.rn > 1
ORDER BY CUSTNO1, CODE1;
