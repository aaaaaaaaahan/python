CREATE TABLE mergefound_expanded AS
SELECT
    custno, rectype, branch, filecode, staffno, staffname,
    code
FROM mergefound,
UNNEST(ARRAY[C01,C02,C03,C04,C05,C06,C07,C08,C09,C10,
             C11,C12,C13,C14,C15,C16,C17,C18,C19,C20]) AS t(code)
WHERE code IS NOT NULL AND code != 0
