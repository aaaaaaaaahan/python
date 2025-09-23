CREATE VIEW out1_imis AS
SELECT
    '"' || CUSTNO1   || '"' AS CUSTNO1,
    '"' || CUSTTYPE1 || '"' AS CUSTTYPE1,
    '"' || RLENCODE1 || '"' AS RLENCODE1,
    '"' || DESC1     || '"' AS DESC1,
    '"' || CUSTNO    || '"' AS CUSTNO,
    '"' || CUSTTYPE  || '"' AS CUSTTYPE,
    '"' || RLENCODE  || '"' AS RLENCODE,
    '"' || "DESC"    || '"' AS "DESC",
    '"' || COALESCE(ACCTCODE, '') || '"' AS ACCTCODE,
    '"' || COALESCE(ACCTNO, '')   || '"' AS ACCTNO,
    '"' || CUSTNAME1 || '"' AS CUSTNAME1,
    '"' || ALIAS1    || '"' AS ALIAS1,
    '"' || CUSTNAME  || '"' AS CUSTNAME,
    '"' || ALIAS     || '"' AS ALIAS,
    '"' || {day}     || '"' AS day,
    '"' || {month}   || '"' AS month,
    '"' || {year}    || '"' AS year
FROM out1;
