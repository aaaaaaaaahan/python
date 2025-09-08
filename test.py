DATA OUT;
  SET ADDRAELE1;
  FILE OUTFILE;
  IF _N_ = 1 THEN DO;
     PUT @01   'CIS #'
         @12   '-'
         @13   'ADDR REF'
         @25   'ADDLINE1'
         @66   'ADDLINE2'
         @107  'ADDLINE3'
         @148  'ADDLINE4'
         @189  'ADDLINE5'
         @230  ' '
         @236  'ZIP'
         @241  'CITY'
         @267  'COUNTRY'
         @279  ' '
         @285  'ZIP'
         @291  'CITY'
         @317  'COUNTRY'     ;
  END;
    NEW_ZIP     = LEFT(NEW_ZIP)     ;
    NEW_CITY    = LEFT(NEW_CITY)    ;
    NEW_COUNTRY = LEFT(NEW_COUNTRY) ;
    IF NEW_ZIP = ' ' THEN DELETE;
     PUT @01   CUSTNO            $11.
         @12   '-'
         @13   ADDREF            Z11.
         @25   LINE1ADR          $40.
         @66   LINE2ADR          $40.
         @107  LINE3ADR          $40.
         @148  LINE4ADR          $40.
         @189  LINE5ADR          $40.
         @230  '*OLD*'
         @236  ZIP               $05.
         @241  CITY              $25.
         @267  COUNTRY           $10.
         @279  '*NEW*'
         @285  NEW_ZIP           $05.
         @291  NEW_CITY          $25.
         @317  STATEX            $3.
         @321  NEW_COUNTRY       $10. ;
  RUN;

DATA UPD;          /*    UPDATE FILE */
  SET ADDRAELE1;
  FILE UPDFILE;
    NEW_ZIP     = LEFT(NEW_ZIP)     ;
    NEW_CITY    = LEFT(NEW_CITY)    ;
    NEW_COUNTRY = LEFT(NEW_COUNTRY) ;
    IF NEW_ZIP = ' ' THEN DELETE;
    NEW_CITY=UPCASE(NEW_CITY);
     PUT @01   CUSTNO            $11.
         @12   ADDREF            Z11.
         @23   NEW_CITY          $25.
         @48   STATEX            $3.
         @51   NEW_ZIP           $05.
         @56   NEW_COUNTRY       $10. ;
  RETURN;
  RUN;
