WHERE CUSTTYPE = 'I'
  AND trim(NAMELINE) != ''
  AND (KEYFIELD1 IS NULL OR trim(KEYFIELD1) = '')
  AND coalesce(trim(SECND_WORD), '') = ''
