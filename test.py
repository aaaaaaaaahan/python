convert program to python with duckdb and pyarrow
duckdb for process input file and output parquet&txt
assumed all the input file ady convert to parquet can directly use it

//CIBRRPTB JOB BR-FILE,'BRANCH-INFO-RPT',MSGCLASS=A,MSGLEVEL=(1,1),     JOB14350
//         USER=OPCC,
//         REGION=64M,NOTIFY=&SYSUID,CLASS=A
//*
/*JOBPARM S=S1M2
//*
//**********************************************************************
//*         E-BANKING - PRINT BRANCH GENERAL INFO REPORT (PBB)
//**********************************************************************
//*
//*********************************************************************
//* DELETE OUTPUT FILES
//*********************************************************************
//*DELETE   EXEC PGM=IEFBR14,COND=(0,NE)
//*DEL1     DD DSN=EBANK.BRANCH.OFFICER.COMBINE.RPT,
//*         DISP=(MOD,DELETE,DELETE),
//*         SPACE=(CYL,(100,200),RLSE)
//**********************************************************************
//SAS609   EXEC SAS609,REGION=0M,WORK='50000,50000'
//CONFIG    DD DISP=SHR,DSN=SYS3.SAS.V609.CNTL(BATCHXA)
//STEPLIB   DD DISP=(SHR,PASS),DSN=&LOAD
//          DD DISP=SHR,DSN=SYS3.SAS.V609.LIBRARY
//IEFRDER   DD DUMMY
//INSFILE   DD DSN=EBANK.BRANCH.OFFICER.COMBINE,
//          DISP=SHR
//*OUTRPT    DD DSN=EBANK.BRANCH.OFFICER.COMBINE.RPT,
//*          DISP=(NEW,CATLG,DELETE),
//*          UNIT=(SYSDA,5),
//*          SPACE=(CYL,(100,50),RLSE),
//*          DCB=(LRECL=133,BLKSIZE=0,RECFM=FB)
//DFSVSAMP  DD DSN=RBP2.IB330P.CONTROL(IBAMS#00),DISP=SHR
//SASLIST   DD SYSOUT=X
//SYSIN     DD *
OPTIONS IMSDEBUG=N YEARCUTOFF=1950 SORTDEV=3390 ERRORS=0;
OPTIONS NODATE NONUMBER;
TITLE;

 /*----------------------------------------------------------------*/
 /*          PRINT 'BRANCH GENERAL INFO REPORT' (PBB)              */
 /*----------------------------------------------------------------*/

DATA GETDATE;
     DT=TODAY();
     DD=PUT(DAY(DT),Z2.);
     MM=PUT(MONTH(DT),Z2.);
     CCYY=PUT(YEAR(DT),Z4.);
     YY = SUBSTR(PUT(CCYY, 4.),3,2);
     CALL SYMPUT('DAY', PUT(DD,2.));
     CALL SYMPUT('MONTH', PUT(MM,2.));
     CALL SYMPUT('YEAR', PUT(YY,2.));
RUN;

DATA BRFILE;
   INFILE INSFILE;
   INPUT @1    BANKNBR            $1.
         @2    BRNBR              7.
         @9    BRABBRV            $3.
         @12   BRNAME             $20.
         @32   BRADDRL1           $35.
         @67   BRADDRL2           $35.
         @102  BRADDRL3           $35.
         @137  BRPHONE            $11.
         @148  BRSTCODE           3.
         @150  BRRPS              $4;
RUN;

DATA _NULL_;
  SET BRFILE END=EOF;
  FILE PRINT HEADER=NEWPAGE NOTITLE;
  LINECNT = 0;
  FORMAT BANKNAME $45.;

  IF LINECNT >= 52 THEN
     PUT _PAGE_;

     PUT @1    BRNBR             7.
         @12   BRABBRV           $3.
         @20   BRNAME            $20.
         @45   BRADDRL1          $35.
         @88   BRPHONE           $11.
         @105  BRSTCODE          3.
        /@45   BRADDRL2          $35.
        /@45   BRADDRL3          $35.///;

    LINECNT + 6;
    BRCNT + 1;

  IF EOF THEN DO;
    PUT @1   'TOTAL NUMBER OF BRANCH = ' BRCNT    4.;
  END;
  RETURN;

  NEWPAGE :
    PAGECNT+1;
    LINECNT = 0;

    IF BANKNBR = 'B' THEN
       BANKNAME = 'PUBLIC BANK BERHAD';
    ELSE IF BANKNBR = 'F' THEN
            BANKNAME = 'PUBLIC FINANCE BERHAD';

    PUT @1   'REPORT ID   : BNKCTL/BR/FILE/RPTS'
        @55   BANKNAME
        @94  'PAGE        : ' PAGECNT   4.
       /@1   'PROGRAM ID  : CIBRRPTB'
        @94  'REPORT DATE : ' "&DAY" '/' "&MONTH" '/' "&YEAR"
       /@52  'BRANCH GENERAL INFO REPORT'
       /@52  '==========================';

    PUT ///@2   'BR NBR'
        @12  'ABBRV'
        @20  'NAME'
        @45  'ADDRESS'
        @88  'PHONE'
        @106 'STATE CODE';
    PUT @2   '------'
        @12  '-----'
        @20  '----'
        @45  '-------'
        @88  '-----'
        @106 '----------';

    LINECNT = 8;
  RETURN;
RUN;
