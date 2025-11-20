convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//FOR#UPDT EXEC SAS609
//CUSTCODE DD DISP=SHR,DSN=CUSTCODE
//RESIGNED DD DISP=SHR,DSN=CIS.EMPLOYEE.RESIGN
//UPDFILE  DD DSN=CIS.EMPLOYEE.RESIGN.RMV(+1),
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(50,50),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=250,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *

OPTIONS IMSDEBUG=N YEARCUTOFF=1950 SORTDEV=3390 ERRORS=0;
OPTIONS NODATE NONUMBER NOCENTER;
TITLE;
DATA CUSTCODE;
   INFILE CUSTCODE;
      INPUT @1   CUSTNO       $11.
            @29  CODE01       $3.
            @32  CODE02       $3.
            @35  CODE03       $3.
            @38  CODE04       $3.
            @41  CODE05       $3.
            @44  CODE06       $3.
            @47  CODE07       $3.
            @50  CODE08       $3.
            @53  CODE09       $3.
            @56  CODE10       $3.
            @59  CODE11       $3.
            @62  CODE12       $3.
            @65  CODE13       $3.
            @68  CODE14       $3.
            @71  CODE15       $3.
            @74  CODE16       $3.
            @77  CODE17       $3.
            @80  CODE18       $3.
            @83  CODE19       $3.
            @86  CODE20       $3.;
      IF CODE01 = '002' OR CODE02 = '002' OR CODE03 = '002'
      OR CODE04 = '002' OR CODE05 = '002' OR CODE06 = '002'
      OR CODE07 = '002' OR CODE08 = '002' OR CODE09 = '002'
      OR CODE10 = '002' OR CODE11 = '002' OR CODE12 = '002'
      OR CODE13 = '002' OR CODE15 = '002' OR CODE16 = '002'
      OR CODE17 = '002' OR CODE18 = '002' OR CODE19 = '002'
      OR CODE20 = '002' ;
RUN;
PROC SORT  DATA=CUSTCODE; BY CUSTNO;RUN;
PROC PRINT DATA=CUSTCODE(OBS=2);TITLE 'CUSTOMER CODE';RUN;

DATA RESIGNED;
   INFILE RESIGNED;
     INPUT @01   STAFFID           $10.
           @11   CUSTNO            $11.
           @22   HRNAME            $40.
           @62   CUSTNAME          $40.
           @102  ALIASKEY          $03.
           @105  ALIAS             $15.
           @120  PRIMSEC           $1.
           @121  ACCTCODE          $5.
           @126  ACCTNOC           $20.;
  RUN;
PROC SORT  DATA=RESIGNED NODUPKEY; BY CUSTNO;RUN;
PROC PRINT DATA=RESIGNED;TITLE 'RESIGNED STAFF';RUN;

DATA MERGE1;
   MERGE CUSTCODE(IN=D) RESIGNED(IN=E); BY CUSTNO;
   IF D AND E;
RUN;

DATA SHIFT1;
   SET MERGE1;
      IF CODE01 = '002' THEN DO;  /* BANK EMPLOYEE ON POSITION 01 */
         CODE01 = CODE02;
         CODE02 = CODE03;
         CODE03 = CODE04;
         CODE04 = CODE05;
         CODE05 = CODE06;
         CODE06 = CODE07;
         CODE07 = CODE08;
         CODE08 = CODE09;
         CODE09 = CODE10;
         CODE10 = CODE11;
         CODE11 = CODE12;
         CODE12 = CODE13;
         CODE13 = CODE14;
         CODE14 = CODE15;
         CODE15 = CODE16;
         CODE16 = CODE17;
         CODE17 = CODE18;
         CODE18 = CODE19;
         CODE19 = CODE20;
         CODE20 = '000';
      END;
      IF CODE02 = '002' THEN DO;  /* BANK EMPLOYEE ON POSITION 02 */
         CODE02 = CODE03;
         CODE03 = CODE04;
         CODE04 = CODE05;
         CODE05 = CODE06;
         CODE06 = CODE07;
         CODE07 = CODE08;
         CODE08 = CODE09;
         CODE09 = CODE10;
         CODE10 = CODE11;
         CODE11 = CODE12;
         CODE12 = CODE13;
         CODE13 = CODE14;
         CODE14 = CODE15;
         CODE15 = CODE16;
         CODE16 = CODE17;
         CODE17 = CODE18;
         CODE18 = CODE19;
         CODE19 = CODE20;
         CODE20 = '000';
      END;
      IF CODE03 = '002' THEN DO;  /* BANK EMPLOYEE ON POSITION 03 */
         CODE03 = CODE04;
         CODE04 = CODE05;
         CODE05 = CODE06;
         CODE06 = CODE07;
         CODE07 = CODE08;
         CODE08 = CODE09;
         CODE09 = CODE10;
         CODE10 = CODE11;
         CODE11 = CODE12;
         CODE12 = CODE13;
         CODE13 = CODE14;
         CODE14 = CODE15;
         CODE15 = CODE16;
         CODE16 = CODE17;
         CODE17 = CODE18;
         CODE18 = CODE19;
         CODE19 = CODE20;
         CODE20 = '000';
      END;
      IF CODE04 = '002' THEN DO;  /* BANK EMPLOYEE ON POSITION 04 */
         CODE04 = CODE05;
         CODE05 = CODE06;
         CODE06 = CODE07;
         CODE07 = CODE08;
         CODE08 = CODE09;
         CODE09 = CODE10;
         CODE10 = CODE11;
         CODE11 = CODE12;
         CODE12 = CODE13;
         CODE13 = CODE14;
         CODE14 = CODE15;
         CODE15 = CODE16;
         CODE16 = CODE17;
         CODE17 = CODE18;
         CODE18 = CODE19;
         CODE19 = CODE20;
         CODE20 = '000';
      END;
      IF CODE05 = '002' THEN DO;  /* BANK EMPLOYEE ON POSITION 05 */
         CODE05 = CODE06;
         CODE06 = CODE07;
         CODE07 = CODE08;
         CODE08 = CODE09;
         CODE09 = CODE10;
         CODE10 = CODE11;
         CODE11 = CODE12;
         CODE12 = CODE13;
         CODE13 = CODE14;
         CODE14 = CODE15;
         CODE15 = CODE16;
         CODE16 = CODE17;
         CODE17 = CODE18;
         CODE18 = CODE19;
         CODE19 = CODE20;
         CODE20 = '000';
      END;
      IF CODE06 = '002' THEN DO;  /* BANK EMPLOYEE ON POSITION 06 */
         CODE06 = CODE07;
         CODE07 = CODE08;
         CODE08 = CODE09;
         CODE09 = CODE10;
         CODE10 = CODE11;
         CODE11 = CODE12;
         CODE12 = CODE13;
         CODE13 = CODE14;
         CODE14 = CODE15;
         CODE15 = CODE16;
         CODE16 = CODE17;
         CODE17 = CODE18;
         CODE18 = CODE19;
         CODE19 = CODE20;
         CODE20 = '000';
      END;
      IF CODE07 = '002' THEN DO;  /* BANK EMPLOYEE ON POSITION 07 */
         CODE07 = CODE08;
         CODE08 = CODE09;
         CODE09 = CODE10;
         CODE10 = CODE11;
         CODE11 = CODE12;
         CODE12 = CODE13;
         CODE13 = CODE14;
         CODE14 = CODE15;
         CODE15 = CODE16;
         CODE16 = CODE17;
         CODE17 = CODE18;
         CODE18 = CODE19;
         CODE19 = CODE20;
         CODE20 = '000';
      END;
      IF CODE08 = '002' THEN DO;  /* BANK EMPLOYEE ON POSITION 08 */
         CODE08 = CODE09;
         CODE09 = CODE10;
         CODE10 = CODE11;
         CODE11 = CODE12;
         CODE12 = CODE13;
         CODE13 = CODE14;
         CODE14 = CODE15;
         CODE15 = CODE16;
         CODE16 = CODE17;
         CODE17 = CODE18;
         CODE18 = CODE19;
         CODE19 = CODE20;
         CODE20 = '000';
      END;
      IF CODE09 = '002' THEN DO;  /* BANK EMPLOYEE ON POSITION 09 */
         CODE09 = CODE10;
         CODE10 = CODE11;
         CODE11 = CODE12;
         CODE12 = CODE13;
         CODE13 = CODE14;
         CODE14 = CODE15;
         CODE15 = CODE16;
         CODE16 = CODE17;
         CODE17 = CODE18;
         CODE18 = CODE19;
         CODE19 = CODE20;
         CODE20 = '000';
      END;
      IF CODE10 = '002' THEN DO;  /* BANK EMPLOYEE ON POSITION 10 */
         CODE10 = CODE11;
         CODE11 = CODE12;
         CODE12 = CODE13;
         CODE13 = CODE14;
         CODE14 = CODE15;
         CODE15 = CODE16;
         CODE16 = CODE17;
         CODE17 = CODE18;
         CODE18 = CODE19;
         CODE19 = CODE20;
         CODE20 = '000';
      END;
RUN;
PROC SORT  DATA=SHIFT1; BY CUSTNO;RUN;
PROC PRINT DATA=SHIFT1;TITLE 'SHIFT CUSTOMER CODE';RUN;

DATA UPD1;
  SET SHIFT1;BY CUSTNO;
  FILE UPDFILE;
        PUT @1   CUSTNO       $11.
            @29  CODE01       $3.
            @32  CODE02       $3.
            @35  CODE03       $3.
            @38  CODE04       $3.
            @41  CODE05       $3.
            @44  CODE06       $3.
            @47  CODE07       $3.
            @50  CODE08       $3.
            @53  CODE09       $3.
            @56  CODE10       $3.
            @59  CODE11       $3.
            @62  CODE12       $3.
            @65  CODE13       $3.
            @68  CODE14       $3.
            @71  CODE15       $3.
            @74  CODE16       $3.
            @77  CODE17       $3.
            @80  CODE18       $3.
            @83  CODE19       $3.
            @86  CODE20       $3.
            @89  STAFFID      $10.
            @99  HRNAME       $40.;
  RETURN;
  RUN;
