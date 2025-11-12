Name file:
INPUT  @001 HOLDCONO        S370FPD2.
       @003 BANKNO          S370FPD2.
       @005 CUSTNO          $EBCDIC20.
       @025 RECTYPE         S370FPD2.
       @027 RECSEQ          S370FPD2.
       @029 EFFDATE         S370FPD5.
       @034 PROCESSTIME     $EBCDIC8.
       @042 ADRHOLDCONO     S370FPD2.
       @044 ADRBANKNO       S370FPD2.
       @046 ADDREF          S370FPD6.
       @052 INDORG          $EBCDIC1.
       @053 KEYFIELD1       $EBCDIC15.
       @068 KEYFIELD2       $EBCDIC10.
       @078 KEYFIELD3       $EBCDIC5.
       @083 KEYFIELD4       $EBCDIC5.
       @088 LINECODE        $EBCDIC1.
       @089 CUSTNAME        $EBCDIC40.
       @129 LINECODE1       $EBCDIC1.
       @130 NAMETITLE1      $EBCDIC40.
       @170 LINECODE2       $EBCDIC1.
       @171 NAMETITLE2      $EBCDIC40.
       @211 SALUTATION      $EBCDIC40.
       @251 TITLECODE       S370FPD2.
       @253 FIRSTMID        $EBCDIC30.
       @283 SURNAME         $EBCDIC20.
       @303 SURNAMEKEY      $EBCDIC3.
       @306 SUFFIXCODE      S370FPD2.
       @308 APPENDCODE      S370FPD2.
       @310 PRIPHONE        S370FPD6.
       @316 PPHONELTH       S370FPD2.
       @318 SECPHONE        S370FPD6.
       @324 SPHONELTH       S370FPD2.
       @326 MOBILEPH        S370FPD6.
       @332 TPHONELTH       S370FPD2.
       @334 FAX             S370FPD6.
       @340 FPHONELTH       S370FPD2.
       @343 LASTCHANGE      $EBCDIC10.
       @353 NAMEFMT         $EBCDIC1.
       ;

rmrk file:
INPUT  @001  BANKNO             $EBCDIC03.  
       @004  APPLCODE           $EBCDIC05.  
       @009  CUSTNO             $EBCDIC11.  
       @029  EFFDATE            $EBCDIC15.  
       @044  RMKKEYWORD         $EBCDIC08.  
       @052  LONGNAME           $EBCDIC150. 
       @352  RMKOPERATOR        $EBCDIC08.  
       @360  EXPIREDATE         $EBCDIC10.  
       @370  LASTMNTDATE        $EBCDIC10. 
       ;
