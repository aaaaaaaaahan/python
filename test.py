convert program to python with duckdb and pyarrow
duckdb for process input file
pyarrow for output csv
assumed all the input file ady convert to parquet can directly use it

//CISDBFRP JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=8M,NOTIFY=&SYSUID       J0021327
//*********************************************************************
//INITDASD EXEC PGM=IEFBR14
//DEL1     DD DSN=CIS.SDB.MATCH.FRPT,
//            DISP=(MOD,DELETE,DELETE),UNIT=SYSDA,SPACE=(TRK,(0))
//*********************************************************************
//* FULL REPORT
//*********************************************************************
//COPYFILE EXEC PGM=ICEGENER
//SYSPRINT DD SYSOUT=X
//SYSUT1   DD DISP=SHR,DSN=CIS.SDB.MATCH.DWJ
//         DD DISP=SHR,DSN=CIS.SDB.MATCH.RHL
//SYSUT2   DD DSN=CIS.SDB.MATCH.FULL(+1),
//            DISP=(NEW,CATLG,DELETE),
//            UNIT=SYSDA,SPACE=(TRK,(10,10),RLSE),
//            DCB=(LRECL=200,BLKSIZE=0,RECFM=FB)
//SYSIN    DD DUMMY
//*********************************************************************
//* MATCH STAFF IC AND NAME AGAINST CIS RECORDS ICIRHHC1
//* (1) IC MATCH, (2) NAME AND IC MATCH, (3) NAME AND (4) NAME AND DOB
//*********************************************************************
//MATCHREC EXEC SAS609
//IEFR1ER   DD DUMMY
//SORTWK01  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK02  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK03  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK04  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//CTRLDATE  DD DISP=SHR,DSN=SRSCTRL1(0)
//DWJLST    DD DISP=SHR,DSN=CIS.SDB.MATCH.DWJ
//          DD DISP=SHR,DSN=CIS.SDB.MATCH.RHL
//RPTFILE   DD DSN=CIS.SDB.MATCH.FRPT,
//             DISP=(NEW,CATLG,DELETE),UNIT=SYSDA,
//             SPACE=(CYL,(50,50),RLSE),
//             DCB=(LRECL=134,BLKSIZE=0,RECFM=FBA)
//SASLIST   DD SYSOUT=X
//SYSIN     DD *
OPTIONS NOCENTER;
 /*----------------------------------------------------------------*/
 /*  IMIS SINGLE VIEW FILES                                        */
 /*----------------------------------------------------------------*/
 DATA DWJ;
      INFILE DWJLST;
      INPUT   @001  BOXNO        $06.
              @007  SDBNAME      $40.
              @050  IDNUMBER     $20.
              @070  BRANCH       $05. ;
   RUN;
 PROC SORT  DATA=DWJ NODUPKEY;BY BRANCH BOXNO SDBNAME IDNUMBER;RUN;
 PROC PRINT DATA=DWJ(OBS=5);TITLE 'ALL LIST ONLY'; RUN;
 /*----------------------------------------------------------------*/
 /*    SET DATES FOR REPORT                                        */
 /*----------------------------------------------------------------*/
DATA SRSDATE;
   INFILE CTRLDATE;
     INPUT @001  SRSYY    4.
           @005  SRSMM    2.
           @007  SRSDD    2.;
   REPTDATE=MDY(SRSMM,SRSDD,SRSYY);
   CALL SYMPUT('RDATE',PUT(REPTDATE,DDMMYY10.));
RUN;

DATA REPORT;
   IF TRN=0 THEN DO;
      FILE RPTFILE PRINT HEADER=NEWPAGE;
      PUT _PAGE_;
      PUT  /@15    '**********************************';
      PUT  /@15    '*                                *';
      PUT  /@15    '*       NO MATCHING RECORDS      *';
      PUT  /@15    '*                                *';
      PUT  /@15    '**********************************';
   END;

   RETAIN TRN;
   SET DWJ NOBS=TRN END=EOF;BY BRANCH BOXNO SDBNAME IDNUMBER;
   FILE RPTFILE NOTITLE PRINT HEADER=NEWPAGE;

   IF  LINECNT >= 52 OR FIRST.BRANCH    THEN DO;
      PUT _PAGE_;
   END;

      LINECNT + 6;
      BRCNT + 1;
      LINECNT + 1;
      PUT  @2    BOXNO             $06.
           @13   SDBNAME           $40.
           @54   IDNUMBER          $20.;
      LINECNT + 2;

   IF LINECNT > 55 THEN DO; LINK NEWPAGE; LINECNT=0; END;

   IF EOF THEN DO;
      PUT @055 '                      ';
      PUT @055 '****END OF REPORT ****';
      PUT @055 '                      ';
   END;
   RETURN;

   NEWPAGE:
      PAGECNT +1;
      LINECNT = 0;
   /* LINECNT +7; */
      PUT @1   'REPORT ID   : SDB/SCREEN/FULL'
          @55  'PUBLIC BANK BERHAD'
          @94  'PAGE        : ' PAGECNT   4.
         /@1   'PROGRAM ID  : CISDBFRP'
          @94  'REPORT DATE : '  "&RDATE"
         /@1   'BRANCH      : 0000001'
          @050 'SDB FULL DATABASE SCREENING'
         /@050 '===========================' ;
      PUT  @2    'BOX NO'
           @13   'NAME (HIRER S NAME)'
           @54   'CUSTOMER ID';
      PUT @001 '----------------------------------------'
          @041 '----------------------------------------'
          @081 '----------------------------------------'
          @121 '----------';
   RETURN;
RUN;
 /*----------------------------------------------------------------*/
 /* GENERATE REPORT                                                */
 /*----------------------------------------------------------------*/
DATA _NULL_;
  SET DWJ END=EOF; BY BRANCH BOXNO SDBNAME IDNUMBER;
  FILE RPTFILE PRINT HEADER=NEWPAGE NOTITLE;
  LINECNT = 0.;

  IF  LINECNT >= 52 OR FIRST.BRANCH    THEN DO;
     PUT _PAGE_;
  END;

    LINECNT + 6;
    BRCNT + 1;

  LINECNT + 1;
  PUT  @2    BOXNO             $06.
       @13   SDBNAME           $40.
       @54   IDNUMBER          $20.;
  BRCUST   + 1;
  GRCUST   + 1;

     IF EOF THEN DO;
        PUT /@3    'GRAND TOTAL OF ALL BRANCHES = '
             @35   GRCUST     9.;
     END;
      RETURN;

  NEWPAGE :
    PAGECNT+1;
    LINECNT = 0;

      PUT @1   'REPORT ID   : SDB/SCREEN/FULL'
          @55  'PUBLIC BANK BERHAD'
          @94  'PAGE        : ' PAGECNT   4.
         /@1   'PROGRAM ID  : CISDBFRP'
          @94  'REPORT DATE : '  "&RDATE"
         /@1   'BRANCH      : ' BRANCH $5.
          @050 'SDB FULL DATABASE SCREENING'
         /@050 '===========================' ;
      PUT  @2    'BOX NO'
           @13   'NAME (HIRER S NAME)'
           @54   'CUSTOMER ID';
      PUT @001 '----------------------------------------'
          @041 '----------------------------------------'
          @081 '----------------------------------------'
          @121 '----------';
    LINECNT = 9;
  RETURN;
RUN;
