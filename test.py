convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&csv
assumed all the input file ady convert to parquet can directly use it

//CIRMKEF1 JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=8M,NOTIFY=&SYSUID       JOB95844
//*---------------------------------------------------------------------
//INITDASD EXEC PGM=IEFBR14
//DEL1     DD DSN=CIRMKEFF.UPDATE,
//            DISP=(MOD,DELETE,DELETE),UNIT=SYSDA,SPACE=(TRK,(0))
//*---------------------------------------------------------------------
//STATS#01 EXEC SAS609
//RMKFILE  DD DISP=SHR,DSN=UNLOAD.CIRMRKS.FB
//CIRMKUPD DD DSN=CIRMKEFF.UPDATE,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(CYL,(200,100),RLSE),UNIT=SYSDA,
//            DCB=(LRECL=360,BLKSIZE=0,RECFM=FB)
//SASLIST  DD SYSOUT=X
//SYSIN    DD *
OPTIONS NOCENTER;

DATA OKAY DUPNI;
   INFILE RMKFILE;
   INPUT  @003   BANK_NO           PD2.
          @005   APPL_CODE         $ 5.
          @010   APPL_NO           $20.
          @031   EFF_DATE          PD8.
          @031   EFF_DATE2         PD6.
          @039   RMK_KEYWORD       $ 8.
          @055   RMK_LINE_1        $60.
          @115   RMK_LINE_2        $60.
          @175   RMK_LINE_3        $60.
          @235   RMK_LINE_4        $60.
          @295   RMK_LINE_5        $60. ;
          IF APPL_CODE   IN ('CUST ');
RUN;

PROC SORT DATA=OKAY NODUPKEY DUPOUT=DUPNI;
       BY APPL_NO EFF_DATE;
          RUN;

PROC PRINT DATA=OKAY(OBS=20);TITLE 'OKAY';RUN;
PROC PRINT DATA=DUPNI(OBS=20);TITLE 'DUP';RUN;

DATA LATEST;
SET DUPNI;
BY APPL_NO EFF_DATE;
IF FIRST.EFF_DATE THEN DO;
GROUP_ID+1;
EFF_DATE_ADD=1;
END;
ELSE EFF_DATE_ADD+1;
RUN;
PROC PRINT DATA=LATEST(OBS=30);TITLE 'EFF DATE TO ADD';RUN;

DATA OUT_UPDATE;
  SET LATEST;
  FILE CIRMKUPD;
     PUT  @001   BANK_NO           Z3.
          @004   APPL_CODE         $5.
          @009   APPL_NO           $20.
          @029   EFF_DATE          Z15.
          @044   RMK_KEYWORD       $8.
          @052   RMK_LINE_1        $60.
          @112   RMK_LINE_2        $60.
          @172   RMK_LINE_3        $60.
          @232   RMK_LINE_4        $60.
          @292   RMK_LINE_5        $60.
          @352   EFF_DATE_ADD      Z2.
          ;
  RUN;
