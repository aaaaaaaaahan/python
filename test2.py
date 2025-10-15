convert program to python with duckdb and pyarrow
duckdb for process input file
output to txt file
assumed all the input file ady convert to parquet can directly use it
CIS.SDB.MATCH.FULL this is the same parquet but different date today and yesterday

//CISDBNRP JOB MSGCLASS=X,MSGLEVEL=(1,1),REGION=8M,NOTIFY=&SYSUID       J0108893
//*********************************************************************
//INITDASD EXEC PGM=IEFBR14
//DEL1     DD DSN=CIS.SDB.MATCH.NRPT,
//            DISP=(MOD,DELETE,DELETE),UNIT=SYSDA,SPACE=(TRK,(0))
//*********************************************************************
//* MATCH STAFF IC AND NAME AGAINST CIS RECORDS
//* (1) IC MATCH, (2) NAME AND IC MATCH, (3) NAME AND (4) NAME AND DOB
//*********************************************************************
//MATCHREC EXEC SAS609
//IEFR1ER   DD DUMMY
//SORTWK01  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK02  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK03  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//SORTWK04  DD UNIT=SYSDA,SPACE=(CYL,(1000,500))
//CTRLDATE  DD DISP=SHR,DSN=SRSCTRL1(0)
//OLDLST    DD DISP=SHR,DSN=CIS.SDB.MATCH.FULL(-1)
//NEWLST    DD DISP=SHR,DSN=CIS.SDB.MATCH.FULL(0)
//RPTFILE   DD DSN=CIS.SDB.MATCH.NRPT,
//             DISP=(NEW,CATLG,DELETE),UNIT=SYSDA,
//             SPACE=(CYL,(100,100),RLSE),
//             DCB=(LRECL=134,BLKSIZE=0,RECFM=FBA)
//SASLIST   DD SYSOUT=X
//SYSIN     DD *
OPTIONS NOCENTER;
 /*----------------------------------------------------------------*/
 /*  OLD LIST                                                      */
 /*----------------------------------------------------------------*/
 DATA OLD;
      INFILE OLDLST;
      INPUT   @001  BOXNO        $06.
              @007  SDBNAME      $40.
              @050  IDNUMBER     $20.
              @070  BRANCH       $05. ;
   RUN;
 PROC SORT  DATA=OLD NODUPKEY;BY BOXNO SDBNAME IDNUMBER;RUN;
 PROC PRINT DATA=OLD(OBS=5);TITLE 'OLD LIST ONLY'; RUN;
 /*----------------------------------------------------------------*/
 /*  NEW LIST                                                      */
 /*----------------------------------------------------------------*/
 DATA NEW;
      INFILE NEWLST;
      INPUT   @001  BOXNO        $06.
              @007  SDBNAME      $40.
              @050  IDNUMBER     $20.
              @070  BRANCH       $05. ;
   RUN;
 PROC SORT  DATA=NEW NODUPKEY;BY BOXNO SDBNAME IDNUMBER;RUN;
 PROC PRINT DATA=NEW(OBS=5);TITLE 'NEW LIST ONLY'; RUN;

 DATA COMP;
      MERGE OLD(IN=O) NEW(IN=N);BY BOXNO SDBNAME IDNUMBER;
      IF N AND (NOT O);
      RUN;
 PROC SORT  DATA=COMP NODUPKEY;BY BRANCH BOXNO SDBNAME IDNUMBER;RUN;
 PROC PRINT DATA=COMP(OBS=5);TITLE 'COM LIST ONLY'; RUN;

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

 /*----------------------------------------------------------------*/
 /* GENERATE REPORT                                                */
 /*----------------------------------------------------------------*/
DATA _NULL_;
   IF TRN=0 THEN DO;
      BRANCH = '00000' ;
      FILE RPTFILE PRINT HEADER=NEWPAGE;
      PUT _PAGE_;
      PUT  /@15    '**********************************';
      PUT  /@15    '*                                *';
      PUT  /@15    '*       NO MATCHING RECORDS      *';
      PUT  /@15    '*                                *';
      PUT  /@15    '**********************************';
   END;

   RETAIN TRN;
  SET COMP NOBS=TRN END=EOF; BY BRANCH BOXNO SDBNAME IDNUMBER;
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

      PUT @1   'REPORT ID   : SDB/SCREEN/NEW'
          @55  'PUBLIC BANK BERHAD'
          @94  'PAGE        : ' PAGECNT   4.
         /@1   'PROGRAM ID  : CISDBNRP'
          @94  'REPORT DATE : '  "&RDATE"
         /@1   'BRANCH      : ' BRANCH $5.
          @050 'SDB NEW RECORDS SCREENING'
         /@050 '==========================' ;
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
