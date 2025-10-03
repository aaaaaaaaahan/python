convert program to python with duckdb and pyarrow
duckdb for process input file
pyarrow use for output
assumed all the input file ady convert to parquet can directly use it

//CCRRMK3B JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=8M,NOTIFY=&SYSUID       JOB56238
//*---------------------------------------------------------------------
//* COMPILE REMARK FILE WITH CUST DAILY TO GET CIS NO AND PRISEC IND
//*---------------------------------------------------------------------
//CIRMKCA  EXEC SAS609
//SORTWK01 DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//CISFILE  DD DISP=SHR,DSN=CIS.CUST.DAILY
//RMKCAFLE DD DISP=SHR,DSN=CCRIS.CISRMRK.ACC
//ENHFILE  DD DSN=CCRIS.CISRMRK.ACC.ENH,                      00170000
//            DISP=(NEW,CATLG,DELETE),UNIT=SYSDA,                       00170000
//            SPACE=(CYL,(2000,2000),RLSE),                             00170000
//            DCB=(LRECL=500,BLKSIZE=0,RECFM=FB)                        00170000
//SASLIST  DD SYSOUT=X
//SYSIN    DD *                                                         00170000

DATA CIS;
   SET CISFILE.CUSTDLY;
   KEEP CUSTNO ACCTNOC ACCTCODE PRISEC
   ;
   IF ACCTNO   > 1000000000;                  /* UAT A LOT JUNK */

   RUN ;
PROC SORT  DATA=CIS NODUPKEY; BY ACCTCODE ACCTNOC ;RUN;
PROC PRINT DATA=CIS(OBS=20);TITLE 'CIS';RUN;

DATA RMK_CA;
   INFILE RMKCAFLE;
   INPUT @001     BANKNO                      $03.
         @004     ACCTCODE                    $05.
         @009     ACCTNOC                     $20.
         @029     EFF_DATE                    $15.
         @044     RMK_KEYWORD                 $08.
         @052     RMK_LINE_1                  $60.
         @112     RMK_LINE_2                  $60.
         @172     RMK_LINE_3                  $60.
         @232     RMK_LINE_4                  $60.
         @292     RMK_LINE_5                  $60.
         @352     RMK_OPERATOR                $08.
         @360     EXPIRE_DATE                 $10.
         @370     LAST_MNT_DATE               $10. ;
   KEYWORD  = UPCASE(KEYWORD);
RUN;
PROC SORT  DATA=RMK_CA ; BY ACCTCODE ACCTNOC ;RUN;
PROC PRINT DATA=RMK_CA(OBS=20);TITLE 'REMARK CA';RUN;

DATA ENH_RMRK;
MERGE RMK_CA(IN=A) CIS(IN=B);
BY ACCTCODE ACCTNOC;
IF A;
RUN;

DATA OUT_ENH_RMRK;
SET ENH_RMRK;
FILE ENHFILE;
     PUT @001     BANKNO                      $03.
         @004     ACCTCODE                    $05.
         @009     ACCTNOC                     $20.
         @029     EFF_DATE                    $15.
         @044     RMK_KEYWORD                 $08.
         @052     RMK_LINE_1                  $60.
         @112     RMK_LINE_2                  $60.
         @172     RMK_LINE_3                  $60.
         @232     RMK_LINE_4                  $60.
         @292     RMK_LINE_5                  $60.
         @352     RMK_OPERATOR                $08.
         @360     EXPIRE_DATE                 $10.
         @370     LAST_MNT_DATE               $10.
         @380     CUSTNO                      $20.
         @400     PRISEC                      PD2.
         ;
RUN;
//**********************************************************************
//*  TO SEGREGATE A FILE FOR R&R ISLAMIC FINANCE
//*  KEYWORD - "RRICUST "
//**********************************************************************
//RRICUST  EXEC PGM=ICETOOL
//TOOLMSG  DD SYSOUT=*
//DFSMSG   DD SYSOUT=*
//INDD01   DD DISP=SHR,DSN=CCRIS.CISRMRK.ACC.ENH
//OUTDD01  DD DSN=CCRIS.CISRMRK.ACC.RRICUST,
//            DISP=(NEW,CATLG,DELETE),UNIT=SYSDA,                       00170000
//            DCB=(LRECL=500,BLKSIZE=0,RECFM=FB),                       00170000
//            SPACE=(CYL,(200,100),RLSE)                                00170000
//CPYACNTL DD *
 SORT FIELDS=(4,5,CH,A,9,20,CH,A,29,9,CH,D)
 INCLUDE COND=(44,8,CH,EQ,C'RRICUST ')
//TOOLIN   DD *
  SORT FROM(INDD01) TO(OUTDD01) USING(CPYA)
//**********************************************************************
//* GET DUPLICATE REMARKS
//**********************************************************************
//DUPRMKT  EXEC PGM=ICETOOL
//TOOLMSG  DD SYSOUT=*
//DFSMSG   DD SYSOUT=*
//INDD01   DD DISP=SHR,DSN=CCRIS.CISRMRK.ACC.ENH
//OUTDD01  DD DSN=CCRIS.CISRMRK.ACC.DUP,
//            DISP=(NEW,CATLG,DELETE),UNIT=SYSDA,
//            DCB=(LRECL=500,BLKSIZE=0,RECFM=FB),
//            SPACE=(CYL,(200,100),RLSE)
//TOOLIN   DD *
   SELECT FROM(INDD01) TO(OUTDD01) ON(1,25,CH) ON(44,308,CH) ALLDUPS
