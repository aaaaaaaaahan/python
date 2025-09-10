//CCRNMX3B JOB 224T,CLASS=A,MSGCLASS=X,REGION=8M,NOTIFY=&SYSUID         00010104
//STATS#01 EXEC SAS609
//NAMEFILE DD DISP=SHR,DSN=CCRIS.CISNAME.TEMP
//RMRKFILE DD DISP=SHR,DSN=CCRIS.CISRMRK.LONGNAME
//OUTFILE  DD DSN=CCRIS.CISNAME.GDG,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(300,300),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=350,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
DATA NAME;
  INFILE NAMEFILE;                          /* SOURCE CICRNMTL */
  INPUT  @001  CUSTNO             $11.
         @012  CUSTNAME           $40.
         @052  ADREFNO            $11.
         @063  PRIPHONE           $11.
         @074  SECPHONE           $11.
         @085  CUSTTYPE           $ 1.
         @086  CUSTNAME           $40.
         @126  MOBILEPHONE        $11. ;
RUN;
PROC SORT  DATA=NAME NODUPKEY; BY CUSTNO ;RUN;
PROC PRINT DATA=NAME(OBS=5);TITLE 'NAME';RUN;

DATA RMRK;
  INFILE RMRKFILE;                          /* SOURCE CCRMRK1B */
  INPUT  @001  BANKNO             $03.
         @004  APPLCODE           $05.
         @009  CUSTNO             $11.
         @029  EFFDATE            $15.
         @044  RMKKEYWORD         $08.
         @052  LONGNAME           $150.     /* RMKLINE1-3      */
         @352  RMKOPERATOR        $08.
         @360  EXPIREDATE         $10.
         @370  LASTMNTDATE        $10. ;

RUN;
PROC SORT  DATA=RMRK NODUPKEY; BY CUSTNO ;RUN;
PROC PRINT DATA=RMRK(OBS=5);TITLE 'REMARKS';RUN;

DATA MERGE;
     MERGE NAME (IN=A) RMRK (IN=B); BY CUSTNO;
     IF A;
RUN;
PROC SORT  DATA=MERGE; BY CUSTNO ;RUN;
PROC PRINT DATA=MERGE(OBS=5);TITLE 'MERGE';RUN;

  /*----------------------------------------------------------------*/
  /*   OUTPUT DETAIL REPORT                                         */
  /*----------------------------------------------------------------*/
 DATA OUT;
   SET MERGE;
   FILE OUTFILE;
   PUT    @001  CUSTNO             $11.
          @012  CUSTNAME           $40.
          @052  ADREFNO            $11.
          @063  PRIPHONE           $11.
          @074  SECPHONE           $11.
          @085  CUSTTYPE           $ 1.
          @086  CUSTNAME           $40.
          @126  MOBILEPHONE        $11.
          @137  LONGNAME           $150.  ;  /* ESMR 2016-2207*/
 RUN;

