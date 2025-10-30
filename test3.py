regexp_extract(trim(regexp_replace(NAMELINE, ' +', ' ')), '^[^ ]+ +([^ ]+)', 1) AS SECND_WORD
